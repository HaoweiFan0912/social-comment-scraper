#!/usr/bin/env python3
"""评论抓抓抓 2.0 本地网页界面。运行: python3 app.py，然后打开 http://127.0.0.1:5050"""
from __future__ import annotations

import csv
import json
import os
import re
import threading
import time
from pathlib import Path

from flask import Flask, abort, jsonify, request, send_file, send_from_directory

import social_comments as sc

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "output"
app = Flask(__name__, static_folder=str(ROOT / "static"), static_url_path="/static")

_lock = threading.Lock()
JOB: dict = {"running": False, "logs": [], "result": None, "error": None, "params": None,
             "status": {}, "started": None, "finished": None}


def _on_log(line: str) -> None:
    with _lock:
        JOB["logs"].append(line)
        m = re.search(r"■\s+(\w+) 完成：(\d+)", line)
        if m:
            JOB["status"][m.group(1)] = {"state": "done", "count": int(m.group(2))}


sc.log_sink = _on_log


def _worker(params: dict) -> None:
    try:
        res = sc.run_job(**params)
        with _lock:
            JOB["result"] = res
    except Exception as e:  # noqa: BLE001
        with _lock:
            JOB["error"] = str(e)
            JOB["logs"].append(f"❌ {e}")
    finally:
        with _lock:
            JOB["running"] = False
            JOB["finished"] = time.time()


def _token_ok() -> bool:
    try:
        sc.get_token()
        return True
    except RuntimeError:
        return False


def _run_dir(name: str) -> Path:
    d = (OUT / name).resolve()
    if d.parent != OUT.resolve() or not d.is_dir():
        abort(404)
    return d


@app.get("/")
def index():
    return send_from_directory(ROOT / "static", "index.html")


@app.get("/api/status")
def status():
    return jsonify({"token": _token_ok(), "platforms": sc.PLATFORMS, "actors": sc.ACTORS})


@app.post("/api/token")
def save_token():
    tok = (request.json or {}).get("token", "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_\-]{20,}", tok):
        return jsonify({"ok": False, "error": "token 格式不对"}), 400
    env = ROOT / ".env"
    lines = env.read_text(encoding="utf-8").splitlines() if env.exists() else []
    lines = [l for l in lines if not l.startswith("APIFY_TOKEN=")] + [f"APIFY_TOKEN={tok}"]
    env.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.environ["APIFY_TOKEN"] = tok
    return jsonify({"ok": True})


@app.post("/api/run")
def start_run():
    b = request.json or {}
    topic = str(b.get("topic", "")).strip()
    platforms = [p for p in b.get("platforms", []) if p in sc.PLATFORMS]
    if not topic:
        return jsonify({"ok": False, "error": "请填写话题"}), 400
    if not platforms:
        return jsonify({"ok": False, "error": "至少选一个平台"}), 400
    if not _token_ok():
        return jsonify({"ok": False, "error": "还没有配置 Apify token"}), 400
    try:
        params = dict(topic=topic, platforms=platforms,
                      posts=max(1, min(100, int(b.get("posts", 5)))),
                      comments=max(1, min(2000, int(b.get("comments", 50)))),
                      budget=max(0.01, min(100.0, float(b.get("budget", 2)))),
                      max_usd_per_run=max(0.01, min(50.0, float(b.get("max_usd_per_run", 0.5)))))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "参数格式不对"}), 400
    with _lock:
        if JOB["running"]:
            return jsonify({"ok": False, "error": "已经有任务在跑了"}), 409
        JOB.update(running=True, logs=[], result=None, error=None, params=params,
                   status={p: {"state": "running", "count": 0} for p in platforms},
                   started=time.time(), finished=None)
    threading.Thread(target=_worker, args=(params,), daemon=True).start()
    return jsonify({"ok": True})


@app.get("/api/job")
def job():
    since = int(request.args.get("since", 0))
    with _lock:
        return jsonify({**{k: JOB[k] for k in ("running", "result", "error", "params", "status",
                                               "started", "finished")},
                        "logs": JOB["logs"][since:], "log_total": len(JOB["logs"])})


@app.get("/api/runs")
def runs():
    items = []
    if OUT.exists():
        for d in sorted(OUT.iterdir(), key=lambda p: p.name, reverse=True):
            csv_path = d / "comments_all.csv"
            if not csv_path.exists():
                continue
            meta = {}
            if (d / "job.json").exists():
                meta = json.loads((d / "job.json").read_text(encoding="utf-8"))
            per, total = {}, 0
            with csv_path.open(encoding="utf-8-sig") as f:
                for r in csv.DictReader(f):
                    per[r["platform"]] = per.get(r["platform"], 0) + 1
                    total += 1
            cost = None
            summ = d / "run_summary.csv"
            if summ.exists():
                for r in csv.reader(summ.open(encoding="utf-8-sig")):
                    if r and r[0] == "TOTAL":
                        cost = float(r[3])
            m = re.match(r"(.*)_(\d{8})_(\d{6})$", d.name)
            topic = meta.get("topic") or (m.group(1).replace("_", " ") if m else d.name)
            when = f"{m.group(2)[:4]}-{m.group(2)[4:6]}-{m.group(2)[6:]} {m.group(3)[:2]}:{m.group(3)[2:4]}" if m else ""
            items.append({"name": d.name, "topic": topic, "when": when, "per_platform": per,
                          "total": total, "cost": cost})
    return jsonify(items)


@app.get("/api/runs/<name>/comments")
def comments(name):
    d = _run_dir(name)
    with (d / "comments_all.csv").open(encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    return jsonify(rows)


@app.get("/api/runs/<name>/download")
def download(name):
    d = _run_dir(name)
    return send_file(d / "comments_all.csv", as_attachment=True, download_name=f"{name}.csv",
                     mimetype="text/csv")


if __name__ == "__main__":
    print("打开 http://127.0.0.1:5050")
    app.run(host="127.0.0.1", port=int(os.environ.get("PORT", 5050)), debug=False, threaded=True)
