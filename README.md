# 📥 Telegram Media Downloader

An async desktop application for bulk-downloading media from Telegram channels, groups, and user profiles. Handles rate limits, interrupted sessions, and concurrent downloads automatically — with a clean dark-mode GUI.

---

## ✨ Features

- **Supports channels, groups, and user profiles** — download from any target by username or user ID
- **Concurrent async downloads** — up to 5 simultaneous downloads via `asyncio` semaphore
- **Session persistence** — tracks what's already been downloaded per channel in a local JSON state file; resuming never re-downloads completed files
- **Automatic rate-limit handling** — detects Telegram flood-wait responses and sleeps accordingly
- **Auto-retry logic** — retries failed downloads up to 3 times with a delay before giving up
- **Cancel at any time** — gracefully stops after completing current in-progress downloads
- **Session manager tab** — view all past download sessions, see file counts per channel, and reset sessions individually
- **API credentials tab** — enter and save your Telegram API credentials locally (never transmitted anywhere)
- **Live log output** — see real-time download progress and any errors in the log panel
- **Dark mode UI** — built with CustomTkinter

---

## ⚙️ Configuration (key internals)

| Setting | Value |
|---------|-------|
| Max concurrent downloads | 5 |
| Message batch size | 200 |
| State saved every | 25 files |
| Connection retries | 5 |
| Flood sleep threshold | 60 seconds |
| Prefetch buffer | 100 messages |

---

## 🚀 Getting Started

### 1. Get your Telegram API credentials

You need a free API key from Telegram:

1. Go to [my.telegram.org](https://my.telegram.org)
2. Log in with your phone number
3. Click **"API development tools"**
4. Create a new app — you'll get an **`api_id`** and **`api_hash`**

### 2. Clone the repository

```bash
git clone https://github.com/yourusername/telegram-media-downloader.git
cd telegram-media-downloader
```

### 3. Install dependencies

```bash
pip install customtkinter telethon
```

### 4. Run

```bash
python tele_download.py
```

---

## 📋 Requirements

- Python 3.10+
- customtkinter
- telethon

---

## 🖥️ Usage

1. **API Credentials tab** — enter your `api_id` and `api_hash` and click Save
2. **Download tab:**
   - Enter a target (channel username, group username, or user ID)
   - Choose a folder to save media to
   - Click **Start**
3. The app will authenticate with Telegram on first run (a `.session` file is created locally)
4. Progress and logs appear in real time in the log panel
5. Click **Cancel** at any time to stop gracefully
6. **Sessions tab** — view completed sessions and reset them if you want to re-download

---

## 🔒 Security Notes

> ⚠️ **Never share or upload the following files — they contain your Telegram credentials:**

- `*.session` — your Telegram authentication session token
- `credentials.json` — your saved API ID and hash
- `download_state.json` — your download history

These files are created locally and should be added to `.gitignore` if you fork this project.

A ready-to-use `.gitignore`:
```
*.session
credentials.json
download_state.json
__pycache__/
*.pyc
```

---

## 📁 Output

Downloaded media is saved to the folder you specify in the app. Files are named and organised automatically based on the source channel and message data.

---

## 📄 License

MIT License — free to use, modify, and distribute.
