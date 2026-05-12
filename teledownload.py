import customtkinter as ctk
import tkinter as tk
from tkinter import filedialog, messagebox
import asyncio
import threading
import os
import time
import json
import string
import logging
from pathlib import Path
from collections import defaultdict

# ── Appearance ──────────────────────────────────────────────────────────────
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

# ── Constants ────────────────────────────────────────────────────────────────
MAX_CONCURRENT_DOWNLOADS = 5
BATCH_SIZE = 200
SAVE_STATE_EVERY = 25
CONNECTION_RETRIES = 5
FLOOD_SLEEP_THRESHOLD = 60
PREFETCH_BUFFER = 100
STATE_FILE = "download_state.json"



class TelegramDownloader:
    def __init__(self, api_id, api_hash, log_callback=None):
        self.api_id = int(api_id)
        self.api_hash = api_hash
        self.log = log_callback or print
        self.client = None
        self.semaphore = None
        self.download_lock = None
        self.supported_chars = string.ascii_letters + string.digits + "-_."
        self.max_retries = 3
        self.retry_delay = 5
        self.state_file = STATE_FILE
        self._cancel = False
        self.load_state()

    # ── async bootstrap ────────────────────────────────────────────────────
    def _init_async(self):
        from telethon import TelegramClient
        self.client = TelegramClient(
            "tg_downloader_session",
            self.api_id,
            self.api_hash,
            connection_retries=CONNECTION_RETRIES,
            flood_sleep_threshold=FLOOD_SLEEP_THRESHOLD,
            request_retries=5,
        )
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
        self.download_lock = asyncio.Lock()

    # ── state ──────────────────────────────────────────────────────────────
    def load_state(self):
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.all_state = data.get("channels", {})
                    for s in self.all_state.values():
                        pm = s.get("processed_messages", [])
                        s["processed_messages"] = set(pm) if isinstance(pm, list) else set()
                        ps = s.get("processed_stories", [])
                        s["processed_stories"] = set(ps) if isinstance(ps, list) else set()
            else:
                self.all_state = {}
        except Exception as e:
            self.log(f"[warn] Could not load state: {e}")
            self.all_state = {}

    def get_state(self, key):
        k = str(key)
        if k not in self.all_state:
            self.all_state[k] = {
                "last_message_id": 0,
                "downloaded_files": 0,
                "processed_messages": set(),
                "channel_name": f"target_{key}",
                "last_accessed": time.time(),
            }
        return self.all_state[k]

    def save_state(self):
        try:
            to_save = {
                cid: {
                    "last_message_id": s["last_message_id"],
                    "downloaded_files": s["downloaded_files"],
                    "processed_messages": list(s["processed_messages"]),
                    "processed_stories": list(s.get("processed_stories", set())),
                    "channel_name": s["channel_name"],
                    "last_accessed": s.get("last_accessed", time.time()),
                }
                for cid, s in self.all_state.items()
            }
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump({"channels": to_save, "timestamp": time.time(), "version": "2.1"}, f, indent=2, ensure_ascii=False)
        except Exception as e:
            self.log(f"[warn] Could not save state: {e}")

    def list_state(self):
        return self.all_state

    def reset_state(self, key):
        k = str(key)
        if k in self.all_state:
            del self.all_state[k]
            self.save_state()
            return True
        return False

    # ── entity resolution ──────────────────────────────────────────────────
    async def resolve_target(self, target):
        """
        Resolves channels, groups, AND user profiles.
        Returns (input_entity, full_entity, kind)
        kind: 'channel' | 'group' | 'user'
        """
        from telethon.tl.types import Channel, Chat, User, PeerChannel, PeerChat, PeerUser
        from telethon.tl.functions.channels import GetChannelsRequest
        from telethon.tl.functions.messages import GetChatsRequest

        s = str(target).strip()

        # Username / invite link / non-numeric
        if s.startswith("@") or "t.me/" in s or not s.lstrip("-").isdigit():
            full = await self.client.get_entity(s)
            inp = await self.client.get_input_entity(full)
            kind = "user" if isinstance(full, User) else ("channel" if isinstance(full, Channel) else "group")
            return inp, full, kind

        int_id = int(s)
        abs_id = abs(int_id)
        s_abs = str(abs_id)
        is_channel_fmt = s_abs.startswith("100") and len(s_abs) > 10
        bare_id = int(s_abs[3:]) if is_channel_fmt else abs_id

        # Try channel peer
        if is_channel_fmt:
            try:
                full = await self.client.get_entity(PeerChannel(bare_id))
                inp = await self.client.get_input_entity(full)
                return inp, full, "channel"
            except Exception:
                pass

        # Try group
        try:
            full = await self.client.get_entity(PeerChat(abs_id))
            inp = await self.client.get_input_entity(full)
            return inp, full, "group"
        except Exception:
            pass

        # Try user
        try:
            full = await self.client.get_entity(PeerUser(abs_id))
            inp = await self.client.get_input_entity(full)
            return inp, full, "user"
        except Exception:
            pass

        # Fallback
        full = await self.client.get_entity(int_id)
        inp = await self.client.get_input_entity(full)
        from telethon.tl.types import User, Channel
        kind = "user" if isinstance(full, User) else ("channel" if isinstance(full, Channel) else "group")
        return inp, full, kind

    # ── download orchestrator ───────────────────────────────────────────────
    async def download(self, target, folder, progress_cb=None, status_cb=None):
        """
        Main entry point.
        target: channel ID, @username, user ID, invite link …
        folder: destination directory
        """
        self._cancel = False
        target_key = str(target).strip()
        state = self.get_state(target_key)

        inp, full, kind = await self.resolve_target(target)

        # Nice display name
        display = getattr(full, "title", None) or \
                  (f"@{full.username}" if getattr(full, "username", None) else None) or \
                  (f"{getattr(full,'first_name','')} {getattr(full,'last_name','')}".strip()) or \
                  target_key
        state["channel_name"] = display

        os.makedirs(folder, exist_ok=True)
        self.log(f"[info] Target: {display} ({kind})")
        self.log(f"[info] Folder: {folder}")
        self.log(f"[info] Resuming from msg #{state['last_message_id']}")

        if status_cb:
            status_cb(f"Connecting to: {display}")

        pending = []
        scanned = 0
        save_counter = 0

        # For user profiles we iterate their sent messages via search
        # For channels/groups we use iter_messages on the entity directly
        iter_kwargs = dict(
            min_id=state["last_message_id"],
            reverse=True,
            wait_time=0,
            limit=None,
        )

        async for message in self.client.iter_messages(inp, **iter_kwargs):
            if self._cancel:
                self.log("[info] Download cancelled.")
                break

            if message.id in state["processed_messages"]:
                continue

            scanned += 1
            state["last_message_id"] = max(state["last_message_id"], message.id)

            if message.media:
                task = asyncio.create_task(
                    self._bounded_download(message, folder, state)
                )
                pending.append(task)

                if progress_cb:
                    progress_cb(state["downloaded_files"], scanned)

                if len(pending) >= PREFETCH_BUFFER:
                    done, pending_set = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                    pending = list(pending_set)
                    save_counter += len(done)

                    if save_counter >= SAVE_STATE_EVERY:
                        state["last_accessed"] = time.time()
                        self.save_state()
                        save_counter = 0

                        if status_cb:
                            status_cb(f"Scanned {scanned} msgs | {state['downloaded_files']} files downloaded")
            else:
                state["processed_messages"].add(message.id)

        if pending:
            self.log(f"[info] Waiting for {len(pending)} remaining downloads…")
            if status_cb:
                status_cb(f"Finishing {len(pending)} remaining downloads…")
            await asyncio.gather(*pending, return_exceptions=True)

        state["last_accessed"] = time.time()
        self.save_state()

        # ── Stories (separate Telegram API endpoint) ───────────────────────
        if not self._cancel:
            if status_cb:
                status_cb(f"Fetching stories for {display}…")
            stories_folder = os.path.join(folder, "stories")
            stories_count = await self._download_stories(inp, full, kind, stories_folder, state, progress_cb, status_cb)
            self.log(f"[info] Stories downloaded: {stories_count}")

        state["last_accessed"] = time.time()
        self.save_state()

        if status_cb:
            status_cb(f"Done — {state['downloaded_files']} files downloaded")
        self.log(f"[done] {display}: {state['downloaded_files']} files total")

    async def _bounded_download(self, message, folder, state):
        async with self.semaphore:
            success = await self._download_file(message, folder, state)
            async with self.download_lock:
                state["processed_messages"].add(message.id)
            return success

    async def _download_file(self, message, folder, state):
        msg_id = message.id
        for attempt in range(self.max_retries):
            try:
                orig = message.file.name if message.file else None
                size = (message.file.size or 0) if message.file else 0
                name = self._safe_name(orig, msg_id)
                path = os.path.join(folder, name)

                if os.path.exists(path):
                    if size == 0 or os.path.getsize(path) == size:
                        return True
                    os.remove(path)

                await self.client.download_media(message, file=path)

                if os.path.exists(path):
                    async with self.download_lock:
                        state["downloaded_files"] += 1
                    mb = size / 1024 / 1024
                    self.log(f"[{state['downloaded_files']}] {name} — {mb:.1f} MB")
                    return True
                continue

            except Exception as e:
                self.log(f"[warn] Attempt {attempt+1}/{self.max_retries} failed for #{msg_id}: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
        self.log(f"[error] All retries failed for message #{msg_id}")
        return False

    # ── Stories ────────────────────────────────────────────────────────────
    async def _download_stories(self, inp, full, kind, folder, state, progress_cb=None, status_cb=None):

        from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument

        if "processed_stories" not in state:
            state["processed_stories"] = set()
        elif isinstance(state["processed_stories"], list):
            state["processed_stories"] = set(state["processed_stories"])

        os.makedirs(folder, exist_ok=True)
        total_new = 0

        # ── helper: download one StoryItem ────────────────────────────────
        async def _save_story(story, subfolder, label):
            nonlocal total_new
            story_id = getattr(story, "id", None)
            if story_id is None:
                return
            story_key = f"story_{story_id}"
            if story_key in state["processed_stories"]:
                return

            media = getattr(story, "media", None)
            if not media:
                state["processed_stories"].add(story_key)
                return

            date = getattr(story, "date", None)
            if date is not None:
                ts = date.timestamp() if hasattr(date, "timestamp") else float(date)
                date_str = time.strftime("%Y%m%d_%H%M%S", time.gmtime(ts))
            else:
                date_str = str(int(time.time()))

            if isinstance(media, MessageMediaPhoto):
                ext = "jpg"
            elif isinstance(media, MessageMediaDocument):
                doc = getattr(media, "document", None)
                mime = getattr(doc, "mime_type", "video/mp4") if doc else "video/mp4"
                ext = mime.split("/")[-1].replace("quicktime", "mov")
            else:
                ext = "bin"

            os.makedirs(subfolder, exist_ok=True)
            file_name = f"story_{story_id}_{date_str}.{ext}"
            file_path = os.path.join(subfolder, file_name)

            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                state["processed_stories"].add(story_key)
                return

            for attempt in range(self.max_retries):
                try:
                    await self.client.download_media(media, file=file_path)
                    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                        total_new += 1
                        state["downloaded_files"] += 1
                        state["processed_stories"].add(story_key)
                        self.log(f"[{label}] {file_name}")
                        if progress_cb:
                            progress_cb(state["downloaded_files"], 0)
                        return
                    else:
                        self.log(f"[warn] Story #{story_id} empty file, retrying…")
                except Exception as e:
                    self.log(f"[warn] Story #{story_id} attempt {attempt+1}/{self.max_retries}: {e}")
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(self.retry_delay * (attempt + 1))
            self.log(f"[error] Gave up on story #{story_id}")

        # ── shared paginator for pinned + archive ─────────────────────────
        async def _fetch_paginated(request_cls, subfolder, label):
            offset_id = 0
            fetched = 0
            while not self._cancel:
                result = await self.client(request_cls(peer=inp, offset_id=offset_id, limit=100))
                # Both return stories.Stories which has .count and .stories[]
                page = getattr(result, "stories", [])
                total = getattr(result, "count", None)
                if not page:
                    break
                fetched += len(page)
                if status_cb and total:
                    status_cb(f"Fetching {label} stories… {fetched}/{total}")
                for story in page:
                    if self._cancel:
                        return fetched
                    await _save_story(story, subfolder, label)
                offset_id = page[-1].id
                if (total is not None and fetched >= total) or len(page) < 100:
                    break
                await asyncio.sleep(0.3)
            return fetched

        # ── 1. Active stories ─────────────────────────────────────────────
        try:
            from telethon.tl.functions.stories import GetPeerStoriesRequest
            if status_cb:
                status_cb("Fetching active stories…")
            result = await self.client(GetPeerStoriesRequest(peer=inp))
            # Returns stories.PeerStories; the inner .stories is a PeerStories TL object
            peer_stories_obj = getattr(result, "stories", None)
            active = getattr(peer_stories_obj, "stories", []) if peer_stories_obj else []
            self.log(f"[info] Active stories: {len(active)}")
            for story in active:
                if self._cancel:
                    break
                await _save_story(story, os.path.join(folder, "active"), "active")
        except Exception as e:
            self.log(f"[warn] Active stories unavailable: {e}")

        # ── 2. Pinned / highlighted stories ───────────────────────────────
        try:
            from telethon.tl.functions.stories import GetPinnedStoriesRequest
            if status_cb:
                status_cb("Fetching pinned stories…")
            n = await _fetch_paginated(GetPinnedStoriesRequest, os.path.join(folder, "pinned"), "pinned")
            self.log(f"[info] Pinned stories fetched: {n}")
        except Exception as e:
            self.log(f"[warn] Pinned stories unavailable: {e}")

        # ── 3. Archive — available for any peer (privacy permitting) ──────
        try:
            from telethon.tl.functions.stories import GetStoriesArchiveRequest
            if status_cb:
                status_cb("Fetching story archive…")
            n = await _fetch_paginated(GetStoriesArchiveRequest, os.path.join(folder, "archive"), "archive")
            self.log(f"[info] Archived stories fetched: {n}")
            if n == 0:
                self.log("[info] Archive empty or hidden by target's privacy settings.")
        except Exception as e:
            self.log(f"[warn] Story archive unavailable: {e}")

        self.save_state()
        return total_new

    def _safe_name(self, name, msg_id):
        if not name:
            return f"file_{msg_id}_{int(time.time())}"
        name = "".join(c for c in name if c in self.supported_chars)
        if "." not in name:
            name += f"_{msg_id}.unknown"
        return name[:200]

    # ── public run helpers ─────────────────────────────────────────────────
    def cancel(self):
        self._cancel = True

    async def run_download(self, target, folder, progress_cb=None, status_cb=None):
        self._init_async()
        await self.client.start()
        try:
            await self.download(target, folder, progress_cb, status_cb)
        finally:
            await self.client.disconnect()

    async def run_list(self):
        self._init_async()
        await self.client.start()
        try:
            return self.list_state()
        finally:
            await self.client.disconnect()


# ════════════════════════════════════════════════════════════════════════════
# GUI
# ════════════════════════════════════════════════════════════════════════════
ACCENT   = "#2B8DFF"
SURFACE  = "#1E1E2E"
CARD_BG  = "#252535"
FG       = "#E0E0F0"
FG_MUTED = "#888899"
SUCCESS  = "#3DDC84"
WARN     = "#FF9F43"
ERR      = "#FF6B6B"

CRED_FILE = "tg_credentials.json"


class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Telegram Media Downloader")
        self.geometry("820x680")
        self.minsize(700, 580)
        self.configure(fg_color=SURFACE)

        self.downloader: TelegramDownloader | None = None
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._running = False

        self._build_ui()
        self._load_credentials()

    # ── UI BUILD ────────────────────────────────────────────────────────────
    def _build_ui(self):
        # Header
        header = ctk.CTkFrame(self, fg_color=CARD_BG, corner_radius=0, height=64)
        header.pack(fill="x", padx=0, pady=0)
        ctk.CTkLabel(
            header, text="⇣  Telegram Media Downloader",
            font=ctk.CTkFont(family="SF Pro Display", size=20, weight="bold"),
            text_color=FG
        ).pack(side="left", padx=24, pady=16)
        ctk.CTkLabel(
            header, text="channels · groups · users",
            font=ctk.CTkFont(size=12), text_color=FG_MUTED
        ).pack(side="left", pady=16)

        # Tabs
        self.tabs = ctk.CTkTabview(self, fg_color=SURFACE, segmented_button_fg_color=CARD_BG,
                                   segmented_button_selected_color=ACCENT,
                                   segmented_button_unselected_color=CARD_BG,
                                   text_color=FG)
        self.tabs.pack(fill="both", expand=True, padx=16, pady=(12, 12))

        self.tabs.add("Download")
        self.tabs.add("Sessions")
        self.tabs.add("API Credentials")

        self._build_download_tab()
        self._build_sessions_tab()
        self._build_creds_tab()

    # ── DOWNLOAD TAB ────────────────────────────────────────────────────────
    def _build_download_tab(self):
        tab = self.tabs.tab("Download")

        # Target input
        ctk.CTkLabel(tab, text="Target  (channel ID, @username, group ID, t.me/ link)",
                     text_color=FG_MUTED, font=ctk.CTkFont(size=12)).pack(anchor="w", pady=(12, 2))
        self.target_var = ctk.StringVar()
        ctk.CTkEntry(tab, textvariable=self.target_var, placeholder_text="e.g. @username  or  -100123456789  or  t.me/channel",
                     height=40, font=ctk.CTkFont(size=14)).pack(fill="x")

        # Folder
        ctk.CTkLabel(tab, text="Download folder", text_color=FG_MUTED,
                     font=ctk.CTkFont(size=12)).pack(anchor="w", pady=(14, 2))
        folder_row = ctk.CTkFrame(tab, fg_color="transparent")
        folder_row.pack(fill="x")
        self.folder_var = ctk.StringVar(value=str(Path.home() / "Downloads" / "TelegramMedia"))
        ctk.CTkEntry(folder_row, textvariable=self.folder_var, height=40,
                     font=ctk.CTkFont(size=13)).pack(side="left", fill="x", expand=True)
        ctk.CTkButton(folder_row, text="Browse", width=90, height=40,
                      fg_color=CARD_BG, hover_color="#333348",
                      command=self._browse_folder).pack(side="left", padx=(8, 0))

        # Status bar
        self.status_label = ctk.CTkLabel(tab, text="Ready.", text_color=FG_MUTED,
                                          font=ctk.CTkFont(size=13))
        self.status_label.pack(anchor="w", pady=(14, 0))

        # Progress
        self.progress_bar = ctk.CTkProgressBar(tab, height=8, progress_color=ACCENT)
        self.progress_bar.pack(fill="x", pady=(6, 0))
        self.progress_bar.set(0)

        self.progress_label = ctk.CTkLabel(tab, text="", text_color=FG_MUTED,
                                            font=ctk.CTkFont(size=11))
        self.progress_label.pack(anchor="w")

        # Buttons
        btn_row = ctk.CTkFrame(tab, fg_color="transparent")
        btn_row.pack(fill="x", pady=(16, 0))
        self.start_btn = ctk.CTkButton(btn_row, text="Start Download", height=42,
                                        font=ctk.CTkFont(size=14, weight="bold"),
                                        fg_color=ACCENT, hover_color="#1a6fd4",
                                        command=self._start)
        self.start_btn.pack(side="left", fill="x", expand=True)
        self.cancel_btn = ctk.CTkButton(btn_row, text="Cancel", height=42, width=110,
                                         fg_color="#3a2020", hover_color="#5a2a2a",
                                         text_color=ERR,
                                         command=self._cancel, state="disabled")
        self.cancel_btn.pack(side="left", padx=(10, 0))

        # Log
        ctk.CTkLabel(tab, text="Log", text_color=FG_MUTED,
                     font=ctk.CTkFont(size=12)).pack(anchor="w", pady=(18, 2))
        self.log_box = ctk.CTkTextbox(tab, height=180, font=ctk.CTkFont(family="Menlo", size=12),
                                       fg_color=CARD_BG, text_color=FG, wrap="word")
        self.log_box.pack(fill="both", expand=True)
        self.log_box.configure(state="disabled")

    # ── SESSIONS TAB ────────────────────────────────────────────────────────
    def _build_sessions_tab(self):
        tab = self.tabs.tab("Sessions")
        ctk.CTkLabel(tab, text="Previously downloaded targets", text_color=FG_MUTED,
                     font=ctk.CTkFont(size=12)).pack(anchor="w", pady=(12, 8))

        btn_row = ctk.CTkFrame(tab, fg_color="transparent")
        btn_row.pack(fill="x", pady=(0, 10))
        ctk.CTkButton(btn_row, text="Refresh", width=100, height=34,
                      fg_color=CARD_BG, hover_color="#333348",
                      command=self._refresh_sessions).pack(side="left")
        self.reset_btn = ctk.CTkButton(btn_row, text="Reset selected", width=140, height=34,
                                        fg_color="#3a2020", hover_color="#5a2a2a",
                                        text_color=ERR,
                                        command=self._reset_selected)
        self.reset_btn.pack(side="left", padx=(8, 0))

        self.session_list = ctk.CTkScrollableFrame(tab, fg_color=CARD_BG, corner_radius=8)
        self.session_list.pack(fill="both", expand=True)
        self._session_vars: dict[str, tk.BooleanVar] = {}
        self._refresh_sessions()

    # ── CREDS TAB ───────────────────────────────────────────────────────────
    def _build_creds_tab(self):
        tab = self.tabs.tab("API Credentials")
        ctk.CTkLabel(tab, text="Get your credentials at  my.telegram.org  → API development tools",
                     text_color=FG_MUTED, font=ctk.CTkFont(size=12)).pack(anchor="w", pady=(16, 12))

        ctk.CTkLabel(tab, text="API ID", text_color=FG_MUTED, font=ctk.CTkFont(size=12)).pack(anchor="w")
        self.api_id_var = ctk.StringVar()
        ctk.CTkEntry(tab, textvariable=self.api_id_var, placeholder_text="12345678",
                     height=40).pack(fill="x")

        ctk.CTkLabel(tab, text="API Hash", text_color=FG_MUTED,
                     font=ctk.CTkFont(size=12)).pack(anchor="w", pady=(12, 0))
        self.api_hash_var = ctk.StringVar()
        ctk.CTkEntry(tab, textvariable=self.api_hash_var, placeholder_text="0a1b2c3d…",
                     height=40, show="•").pack(fill="x")

        ctk.CTkButton(tab, text="Save credentials", height=40, fg_color=ACCENT,
                      hover_color="#1a6fd4", command=self._save_credentials).pack(pady=(16, 0))
        self.cred_status = ctk.CTkLabel(tab, text="", font=ctk.CTkFont(size=12))
        self.cred_status.pack(anchor="w", pady=(6, 0))

    # ── ACTIONS ─────────────────────────────────────────────────────────────
    def _browse_folder(self):
        path = filedialog.askdirectory()
        if path:
            self.folder_var.set(path)

    def _load_credentials(self):
        if os.path.exists(CRED_FILE):
            try:
                with open(CRED_FILE) as f:
                    data = json.load(f)
                self.api_id_var.set(str(data.get("api_id", "")))
                self.api_hash_var.set(data.get("api_hash", ""))
            except Exception:
                pass

    def _save_credentials(self):
        api_id = self.api_id_var.get().strip()
        api_hash = self.api_hash_var.get().strip()
        if not api_id or not api_hash:
            self.cred_status.configure(text="Both fields required.", text_color=ERR)
            return
        try:
            with open(CRED_FILE, "w") as f:
                json.dump({"api_id": int(api_id), "api_hash": api_hash}, f)
            self.cred_status.configure(text="Saved!", text_color=SUCCESS)
        except Exception as e:
            self.cred_status.configure(text=f"Error: {e}", text_color=ERR)

    def _log(self, msg):
        def _do():
            self.log_box.configure(state="normal")
            self.log_box.insert("end", msg + "\n")
            self.log_box.see("end")
            self.log_box.configure(state="disabled")
        self.after(0, _do)

    def _set_status(self, msg):
        self.after(0, lambda: self.status_label.configure(text=msg))

    def _set_progress(self, files, scanned):
        def _do():
            self.progress_label.configure(text=f"{files} files  ·  {scanned} messages scanned")
            # Animate the bar indeterminately
            val = (files % 50) / 50
            self.progress_bar.set(val)
        self.after(0, _do)

    def _start(self):
        target = self.target_var.get().strip()
        folder = self.folder_var.get().strip()
        api_id = self.api_id_var.get().strip()
        api_hash = self.api_hash_var.get().strip()

        if not target:
            messagebox.showerror("Error", "Please enter a target (channel/username/ID).")
            return
        if not api_id or not api_hash:
            messagebox.showerror("Error", "Please enter API credentials in the 'API Credentials' tab.")
            self.tabs.set("API Credentials")
            return
        if not folder:
            messagebox.showerror("Error", "Please choose a download folder.")
            return

        self._running = True
        self.start_btn.configure(state="disabled")
        self.cancel_btn.configure(state="normal")
        self.progress_bar.set(0)
        self.progress_label.configure(text="")

        self.downloader = TelegramDownloader(api_id, api_hash, log_callback=self._log)

        def run():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            try:
                self._loop.run_until_complete(
                    self.downloader.run_download(
                        target, folder,
                        progress_cb=self._set_progress,
                        status_cb=self._set_status,
                    )
                )
            except Exception as e:
                self._log(f"[error] {e}")
                self.after(0, lambda: self.status_label.configure(text=f"Error: {e}", text_color=ERR))
            finally:
                self._loop.close()
                self._running = False
                self.after(0, self._on_done)

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()

    def _cancel(self):
        if self.downloader:
            self.downloader.cancel()
            self._log("[info] Cancelling after current downloads finish…")
            self._set_status("Cancelling…")

    def _on_done(self):
        self.start_btn.configure(state="normal")
        self.cancel_btn.configure(state="disabled")
        self.progress_bar.set(1)
        self._refresh_sessions()

    def _refresh_sessions(self):
        for widget in self.session_list.winfo_children():
            widget.destroy()
        self._session_vars.clear()

        if not os.path.exists(STATE_FILE):
            ctk.CTkLabel(self.session_list, text="No sessions yet.", text_color=FG_MUTED).pack()
            return

        try:
            with open(STATE_FILE) as f:
                data = json.load(f).get("channels", {})
        except Exception:
            return

        for cid, s in data.items():
            var = tk.BooleanVar()
            self._session_vars[cid] = var
            row = ctk.CTkFrame(self.session_list, fg_color="transparent")
            row.pack(fill="x", pady=3)
            ctk.CTkCheckBox(row, text="", variable=var, width=24).pack(side="left")
            ctk.CTkLabel(row, text=s.get("channel_name", cid),
                         font=ctk.CTkFont(size=13, weight="bold"), text_color=FG,
                         anchor="w").pack(side="left", padx=(4, 12))
            ctk.CTkLabel(row,
                         text=f"{s.get('downloaded_files', 0)} files  ·  ID: {cid}",
                         font=ctk.CTkFont(size=11), text_color=FG_MUTED,
                         anchor="w").pack(side="left")

    def _reset_selected(self):
        to_reset = [cid for cid, var in self._session_vars.items() if var.get()]
        if not to_reset:
            messagebox.showinfo("Reset", "Select at least one session to reset.")
            return
        if not messagebox.askyesno("Confirm", f"Reset {len(to_reset)} session(s)?"):
            return
        # Load from file, remove, save
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE) as f:
                data = json.load(f)
            for cid in to_reset:
                data["channels"].pop(cid, None)
            with open(STATE_FILE, "w") as f:
                json.dump(data, f, indent=2)
        self._refresh_sessions()


# ════════════════════════════════════════════════════════════════════════════
# ENTRY
# ════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    app = App()
    app.mainloop()
