#!/bin/zsh
# 双击打开「评论抓抓抓 2.0」界面
cd "$(dirname "$0")"
(sleep 1.5 && open "http://127.0.0.1:5050") &
python3 app.py
