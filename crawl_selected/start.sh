#!/bin/sh
scrapyd-deploy
python -u /data/semantic_crawl/crawl_selected/app.py >> /data/semantic_crawl/logs/crawl_selected_stdout.log 2>&1 &