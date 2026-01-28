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
```bash
- git clone https://github.com/Jeff-Hutchins/cms-data-downloader.git
- cd cms-data-downloader
- pip install -r requirements.txt

