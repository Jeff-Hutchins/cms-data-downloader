# cms-data-downloader

A simple Python script that downloads the latest CMS Hospital datasets from data.cms.gov, processes them (snake_case column names), and saves them locally. Only downloads files modified since the last run.

## Features
- Daily incremental updates using `last_run.json`
- Parallel downloads with `ThreadPoolExecutor`
- Logging to console + `runs.log`
- Rotating log files

## Requirements
- Python 3.8+
- See `requirements.txt`

## Installation
Using Bash
- git clone https://github.com/Jeff-Hutchins/cms-data-downloader.git
- cd cms-data-downloader
- pip install -r requirements.txt

## Automation – Daily Scheduled Runs

To run the script automatically every day (example: at 3:00 AM), use one of these methods.

### Option 1: Windows Task Scheduler

1. Open Task Scheduler (Win + S → "Task Scheduler")
2. Create Task → General tab:
   - Name: "CMS Data Downloader"
   - Check "Run whether user is logged on or not"
   - Check "Run with highest privileges"
3. Triggers → New → Daily → 3:00 AM
4. Actions → New:
   - Action: Start a program
   - Program/script: `C:\Users\USERNAME\Anaconda3\python.exe` (or your Python path)
   - Arguments: `cms_csv_download.py`
   - Start in: path to your project folder
5. Save → enter password if prompted

### Option 2: WSL + cron

If you have Windows Subsystem for Linux (WSL) installed (e.g. Ubuntu):

1. Open your WSL terminal
2. Install cron if needed:
   ```bash
   sudo apt update && sudo apt install cron
