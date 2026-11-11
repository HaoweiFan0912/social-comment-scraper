#!/usr/bin/env python3
"""
多平台话题评论抓取（Apify actors）→ 统一格式 → 合并导出 CSV

支持平台: x, facebook, youtube, reddit, linkedin, threads
用法示例:
  python3 social_comments.py --topic "electric vehicles" --test
  python3 social_comments.py --topic "AI agents" --platforms reddit,youtube --posts 5 --comments 50
  python3 social_comments.py --topic "AI agents" --platforms all --budget 3

需要环境变量 APIFY_TOKEN（或在同目录 .env 里写 APIFY_TOKEN=xxx）。
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests

API = "https://api.apify.com/v2"
ROOT = Path(__file__).resolve().parent

# 每个平台用的 actor（都选的是同类里最便宜的）。想换 actor 改这里即可。
ACTORS = {
    "x_search": "xquik/x-tweet-scraper",                       # $0.15 / 1k 推文
    "x_comments": "xquik/x-reply-scraper",                     # $0.15 / 1k 回复
    "facebook_search": "scrapeforge/facebook-search-posts",    # $2.59 / 1k 帖子（相关性比 topapi 好很多）
    "facebook_search_backup": "topapi/facebook-search-posts-scraper",  # 主 actor 返回 0 条时用，$2.4 / 1k
    "facebook_comments": "thedoor/facebook-comment-scraper",   # $0.25 / 1k 评论
    "youtube_search": "grow_media/youtube-search-api",         # $1 / 1k 视频
    "youtube_comments": "supreme_coder/youtube-comment-scraper",  # $0.25 / 1k 评论
    "reddit": "automation-lab/reddit-scraper",                 # 帖子 $1.15 / 1k，评论 $0.58 / 1k
    "reddit_backup": "clearpath/reddit-post-comments-bulk-scraper",  # 主 actor 被限流时用，帖子 $2.99 / 1k，评论 $0.99 / 1k
    "linkedin_search": "datadoping/linkedin-posts-search-scraper",  # $1.55 / 1k 帖子（每次至少 10 条）
    "linkedin_comments": "harvestapi/linkedin-post-comments",  # $2 / 1k 评论
    "threads": "lergassy/threads-scraper",                     # $2 / 1k 行（帖子和回复都算）
}

PLATFORMS = ["x", "facebook", "youtube", "reddit", "linkedin", "threads"]

COLUMNS = [
    "platform", "topic", "post_url", "post_text", "post_author", "post_comment_count",
    "comment_id", "comment_text", "comment_author", "comment_time", "comment_likes",
    "is_reply", "parent_id", "comment_url", "scraped_at",
]

_print_lock = threading.Lock()
log_sink = None  # UI 会把它设成一个函数，用来接收日志


def log(msg: str) -> None:
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    with _print_lock:
        print(line, flush=True)
        if log_sink:
            log_sink(line)


# ---------------------------------------------------------------- helpers
def g(d, *paths, default=""):
    """取第一个非空字段。路径支持点号，如 'author.name'。"""
    for path in paths:
        cur = d
        for key in path.split("."):
            if isinstance(cur, dict):
                cur = cur.get(key)
            else:
                cur = None
                break
        if cur not in (None, "", [], {}):
            return cur
    return default


def to_iso(v):
    """数字时间戳（秒/毫秒）或 X 的 'Fri Jan 09 20:07:28 +0000 2026' 转 ISO；其它原样返回。"""
    if isinstance(v, (int, float)) and v > 0:
        if v > 1e12:
            v = v / 1000
        return datetime.fromtimestamp(v, tz=timezone.utc).isoformat()
    if isinstance(v, str):
        try:
            return datetime.strptime(v, "%a %b %d %H:%M:%S %z %Y").isoformat()
        except ValueError:
            pass
    return v or ""


STOPWORDS = {"and", "or", "the", "a", "an", "of", "in", "on", "for", "to", "with", "vs", "&"}


def keywords(topic):
    return [w for w in re.findall(r"[\w一-鿿]+", topic.lower()) if w not in STOPWORDS]


def hits(text, topic):
    """帖子文本命中了几个话题关键词（整词匹配，忽略大小写和停用词）。"""
    text = (text or "").lower()
    return sum(bool(re.search(rf"(?<![\w]){re.escape(w)}(?![\w])", text)) for w in keywords(topic))


def filter_relevant(posts, topic, text_key):
    """优先留包含全部关键词的帖子；没有就留包含任一关键词的；还没有就原样返回。"""
    n = len(keywords(topic))
    scored = [(hits(g(p, *text_key), topic), p) for p in posts]
    full = [p for s, p in scored if s == n]
    if full:
        return full
    part = [p for s, p in sorted(scored, key=lambda x: -x[0]) if s > 0]
    if part:
        log(f"   ⚠️ 没有帖子包含全部关键词，改用部分匹配（{len(part)} 条）")
        return part
    if posts:
        log(f"   ⚠️ 搜索结果都不含话题关键词，保留原结果，相关性可能较差")
    return posts


def num(v):
    if isinstance(v, (int, float)):
        return v
    if isinstance(v, str):
        s = v.strip().replace(",", "").upper()
        m = re.match(r"^([\d.]+)\s*([KM]?)$", s)
        if m:
            n = float(m.group(1)) * {"": 1, "K": 1e3, "M": 1e6}[m.group(2)]
            return int(n)
    return ""


def short(text, n=500):
    text = (text or "").replace("\r", " ").strip()
    return text if len(text) <= n else text[:n] + "…"


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


# ---------------------------------------------------------------- Apify
class Budget:
    """总预算守卫：每次 run 先按上限预留，跑完换成实际花费。保证总花费不超过 budget。"""

    def __init__(self, total: float, per_run: float):
        self.total, self.per_run = total, per_run
        self.spent = 0.0
        self.reserved = 0.0
        self.cv = threading.Condition()

    def reserve(self) -> bool:
        with self.cv:
            while self.spent + self.reserved + self.per_run > self.total + 1e-9:
                if self.reserved == 0:      # 没有在跑的任务，预算真的不够了
                    return False
                self.cv.wait()
            self.reserved += self.per_run
            return True

    def settle(self, actual: float) -> None:
        with self.cv:
            self.reserved -= self.per_run
            self.spent += actual
            self.cv.notify_all()


class Apify:
    def __init__(self, token: str, budget: Budget, raw_dir: Path, timeout_s: int = 900):
        self.h = {"Authorization": f"Bearer {token}"}
        self.budget = budget
        self.raw_dir = raw_dir
        self.timeout_s = timeout_s
        self.records = []  # (label, actor, status, cost, items)
        self._lock = threading.Lock()

    @staticmethod
    def _cost(run: dict) -> float:
        counts = run.get("chargedEventCounts") or {}
        events = ((run.get("pricingInfo") or {}).get("pricingPerEvent") or {}).get("actorChargeEvents") or {}
        total = 0.0
        for name, n in counts.items():
            ev = events.get(name) or {}
            price = ev.get("eventPriceUsd")
            if price is None:  # 分级定价，取 FREE 档
                tiers = ev.get("eventTieredPricingUsd") or {}
                price = (tiers.get("FREE") or {}).get("tieredEventPriceUsd", 0)
            total += float(n) * float(price or 0)
        return round(total, 5)

    def run(self, actor: str, payload: dict, label: str) -> list[dict]:
        if not self.budget.reserve():
            log(f"⚠️  {label}: 预算不够，跳过")
            self._record(label, actor, "SKIPPED_BUDGET", 0.0, 0, "")
            return []
        cost, status, items, run_id = 0.0, "ERROR", [], ""
        try:
            url = f"{API}/acts/{actor.replace('/', '~')}/runs"
            params = {"maxTotalChargeUsd": self.budget.per_run, "waitForFinish": 60}
            for attempt in range(6):  # 免费账号内存额度满时等一会儿重试
                r = requests.post(url, headers=self.h, params=params, json=payload, timeout=90)
                if r.status_code == 402 and "memory" in r.text.lower():
                    log(f"   {label}: 账号内存额度已满，30 秒后重试…")
                    time.sleep(30)
                    continue
                break
            if r.status_code >= 400:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")
            run = r.json()["data"]
            run_id = run["id"]
            log(f"▶  {label}: {actor} 已启动 (run {run_id})")
            t0, aborted = time.time(), False
            while run["status"] in ("READY", "RUNNING", "TIMING-OUT", "ABORTING"):
                if not aborted and time.time() - t0 > self.timeout_s:
                    aborted = True
                    requests.post(f"{API}/actor-runs/{run['id']}/abort", headers=self.h, timeout=30)
                    log(f"⚠️  {label}: 超时，已中止")
                r = requests.get(f"{API}/actor-runs/{run['id']}", headers=self.h,
                                 params={"waitForFinish": 60}, timeout=90)
                r.raise_for_status()
                run = r.json()["data"]
            status = run["status"]
            cost = self._settled_cost(run)
            items = self._items(run["defaultDatasetId"])
            safe = re.sub(r"[^\w.-]", "_", label)
            (self.raw_dir / f"{safe}.json").write_text(
                json.dumps({"actor": actor, "input": payload, "status": status, "cost_usd": cost,
                            "chargedEventCounts": run.get("chargedEventCounts"), "items": items},
                           ensure_ascii=False, indent=1), encoding="utf-8")
            flag = "✅" if status == "SUCCEEDED" else "⚠️ "
            log(f"{flag} {label}: {status}，{len(items)} 条，${cost:.4f}")
        except Exception as e:  # noqa: BLE001
            log(f"❌ {label}: {e}")
        finally:
            self.budget.settle(cost)
            self._record(label, actor, status, cost, len(items), run_id)
        return items

    def reconcile(self) -> None:
        """全部跑完后再拉一次每个 run 的实际扣费（计费事件结算有延迟）。"""
        time.sleep(10)
        fixed = []
        for label, actor, status, cost, n, run_id in self.records:
            if run_id:
                r = requests.get(f"{API}/actor-runs/{run_id}", headers=self.h, timeout=60)
                if r.ok:
                    d = r.json()["data"]
                    cost = round(max(cost, float(d.get("usageTotalUsd") or 0), self._cost(d)), 5)
            fixed.append((label, actor, status, cost, n, run_id))
        self.records = fixed

    def _settled_cost(self, run: dict) -> float:
        """计费事件在 run 结束后要过一会儿才结算完。反复拉取，直到金额连续两次不变。"""
        def calc(r):  # 按次计费(PPE)的 actor，usageTotalUsd 就是实际扣费
            return round(max(float(r.get("usageTotalUsd") or 0), self._cost(r)), 5)

        prev, stable = calc(run), 0
        for _ in range(15):
            time.sleep(3)
            r = requests.get(f"{API}/actor-runs/{run['id']}", headers=self.h, timeout=60)
            if not r.ok:
                continue
            cur = calc(r.json()["data"])
            stable = stable + 1 if cur == prev else 0
            prev = cur
            if stable >= 2 and cur > 0:
                break
        return prev

    def _items(self, dataset_id: str) -> list[dict]:
        out, offset = [], 0
        while True:
            r = requests.get(f"{API}/datasets/{dataset_id}/items", headers=self.h,
                             params={"clean": "true", "format": "json", "offset": offset, "limit": 1000},
                             timeout=120)
            r.raise_for_status()
            batch = r.json()
            out.extend(batch)
            if len(batch) < 1000:
                return out
            offset += 1000

    def _record(self, *rec):
        with self._lock:
            self.records.append(rec)


# ---------------------------------------------------------------- 各平台
def mk(platform, topic, post, **c):
    row = {k: "" for k in COLUMNS}
    row.update(platform=platform, topic=topic, scraped_at=datetime.now(timezone.utc).isoformat())
    row.update({k: v for k, v in (post or {}).items() if k in COLUMNS})
    row.update(c)
    row["comment_text"] = short(row["comment_text"], 5000)
    return row


def pick_posts(posts, n, count_key):
    """优先选有评论的帖子，评论多的排前面。"""
    with_c = [p for p in posts if (num(p.get(count_key)) or 0) > 0]
    with_c.sort(key=lambda p: num(p.get(count_key)) or 0, reverse=True)
    rest = [p for p in posts if p not in with_c]
    return (with_c + rest)[:n]


def scrape_x(ap: Apify, topic, n_posts, n_comments):
    raw = ap.run(ACTORS["x_search"], {
        "searchTerms": [topic], "mode": "search", "queryType": "Top",
        "maxItems": n_posts, "maxItemsPerTarget": n_posts,
        "tweetTypes": {"excludeReplies": True, "excludeRetweets": True},
        "engagement": {"minReplies": 3},
    }, "x_search")
    tweets = [t for t in raw if t.get("id") and t.get("text")][:n_posts]
    posts = {str(t["id"]): dict(post_url=t.get("url", ""), post_text=short(t.get("text")),
                                post_author=g(t, "author.username", "author.name"),
                                post_comment_count=t.get("replyCount", "")) for t in tweets}
    if not posts:
        return []
    reps = ap.run(ACTORS["x_comments"], {
        "tweetIds": list(posts), "maxItemsPerTarget": n_comments, "maxItems": n_comments * len(posts),
        "scope": "direct", "includeHiddenReplies": False, "outputPreset": "flat",
    }, "x_comments")
    rows = []
    for c in reps:
        if not c.get("text") or c.get("isOriginalPost"):
            continue
        post = posts.get(str(g(c, "sourceTweetId", "rootTweetId", "conversationId")), {})
        rows.append(mk("x", topic, post, comment_id=c.get("id", ""), comment_text=c.get("text"),
                       comment_author=g(c, "authorUsername", "author.username"),
                       comment_time=to_iso(c.get("createdAt", "")), comment_likes=c.get("likeCount", ""),
                       is_reply=(c.get("depth") or 1) > 1, parent_id=c.get("inReplyToId", ""),
                       comment_url=c.get("url", "")))
    return rows


def scrape_facebook(ap: Apify, topic, n_posts, n_comments):
    query = " ".join(keywords(topic)) or topic
    raw = ap.run(ACTORS["facebook_search"], {
        "query": query, "search_type": "posts",
        "max_results": n_posts * 3,  # Apify 免费账号这个 actor 最多返回 5 条
    }, "facebook_search")
    if not [p for p in raw if p.get("url")]:
        log("   facebook: 主搜索 actor 没返回结果，换备用 actor")
        raw = ap.run(ACTORS["facebook_search_backup"], {
            "searchQueries": [query], "sortBy": "top",
            "maxPostsPerQuery": n_posts * 4, "maxItems": n_posts * 4, "compact": True,
        }, "facebook_search_backup")
    # Facebook 搜索是模糊匹配，会混进不相关帖子，这里按关键词过滤
    found = filter_relevant([p for p in raw if p.get("url")], topic, ("message", "text"))
    chosen = pick_posts(found, n_posts, "comments_count")
    posts = {p["url"]: dict(post_url=p["url"], post_text=short(g(p, "message", "text")),
                            post_author=g(p, "author.name", "author_name"),
                            post_comment_count=p.get("comments_count", "")) for p in chosen}
    if not posts:
        return []
    coms = ap.run(ACTORS["facebook_comments"], {
        "postUrls": list(posts), "targetComments": n_comments,
        "orderingMode": "MOST_RELEVANT", "includeReplies": False,
    }, "facebook_comments")
    rows = []
    for c in coms:
        if not c.get("text"):
            continue
        purl = c.get("post_url", "")
        post = posts.get(purl) or next((v for k, v in posts.items() if purl and (purl in k or k in purl)), {})
        rows.append(mk("facebook", topic, post or {"post_url": purl}, comment_id=c.get("id", ""),
                       comment_text=c.get("text"), comment_author=c.get("author_name", ""),
                       comment_time=to_iso(g(c, "comment_time", "timestamp")),
                       comment_likes=g(c, "reactions_count", "reactions.total", "likes_count", "likes"),
                       is_reply=bool(c.get("is_reply")), parent_id=c.get("replyto_comment_id", ""),
                       comment_url=c.get("comment_url", "")))
    return rows


def scrape_youtube(ap: Apify, topic, n_posts, n_comments):
    raw = ap.run(ACTORS["youtube_search"], {"q": topic, "maxResults": n_posts * 2, "order": "relevance"},
                 "youtube_search")
    vids = [v for v in raw if v.get("url") and "watch" in v.get("url", "") and not v.get("commentsTurnedOff")]
    chosen = pick_posts(vids, n_posts, "commentsCount")
    rows = []

    def one(i, v):
        # 这个 actor 每次至少要 10 条
        coms = ap.run(ACTORS["youtube_comments"], {"urls": [{"url": v["url"]}], "count": max(10, n_comments)},
                      f"youtube_comments_{i + 1}")
        post = dict(post_url=v["url"], post_text=short(v.get("title")), post_author=v.get("channelName", ""),
                    post_comment_count=v.get("commentsCount", ""))
        out = []
        for c in coms:
            if not c.get("commentText"):
                continue
            out.append(mk("youtube", topic, post, comment_id=c.get("commentId", ""),
                          comment_text=c.get("commentText"), comment_author=g(c, "author.displayName"),
                          comment_time=c.get("publishedTime", ""), comment_likes=num(c.get("likeCountNotliked")),
                          is_reply=(c.get("replyLevel") or 0) > 0,
                          comment_url=f"{v['url']}&lc={c.get('commentId', '')}"))
        return out

    with ThreadPoolExecutor(max_workers=3) as ex:
        for f in as_completed([ex.submit(one, i, v) for i, v in enumerate(chosen)]):
            rows.extend(f.result())
    return rows


def scrape_reddit(ap: Apify, topic, n_posts, n_comments):
    query = " ".join(keywords(topic)) or topic  # 去掉 and/the 之类，Reddit 会按全部关键词匹配
    payload = {
        "searchQuery": query, "sort": "relevance", "timeFilter": "year",
        "maxPostsPerSource": n_posts, "includeComments": True,
        "maxCommentsPerPost": n_comments, "commentDepth": 2,
    }
    raw = ap.run(ACTORS["reddit"], payload, "reddit")
    if not any(c.get("type") == "comment" for c in raw):
        # Reddit 会间歇性屏蔽某个 actor 的抓取，换另一个 actor（不同的抓取通道）
        log("   reddit: 主 actor 没拿到评论（Reddit 限流），换备用 actor")
        back = ap.run(ACTORS["reddit_backup"], {
            "queries": [query], "maxPostsPerQuery": n_posts, "searchSort": "relevance",
            "maxCommentsPerPost": n_comments, "sort": "top",
        }, "reddit_backup")
        raw = []
        for it in back:  # 转成主 actor 的字段格式，下面统一处理
            kind = str(it.get("_type") or ("comment" if it.get("body") else "post")).lower()
            raw.append({
                "type": "comment" if "comment" in kind else "post",
                "id": it.get("id", ""), "title": it.get("title", ""), "selfText": it.get("selftext", ""),
                "body": it.get("body", ""), "author": it.get("author", ""), "score": it.get("score", ""),
                "createdAt": it.get("createdAt") or to_iso(it.get("created_utc")),
                "numComments": it.get("num_comments", ""), "parentId": it.get("parentId") or "",
                "depth": it.get("depth") or 0, "postId": str(it.get("_post_id") or "").replace("t3_", ""),
                "postTitle": it.get("postTitle", ""), "url": it.get("url") or "",
                "permalink": it.get("permalink") or "",
            })
        for p in raw:  # 帖子行用自己的 id 当 postId
            if p["type"] == "post" and not p["postId"]:
                p["postId"] = p["id"]
    posts = {}
    for p in raw:
        if p.get("type") == "post":
            perm = p.get("permalink") or ""
            purl = perm if perm.startswith("http") else (f"https://www.reddit.com{perm}" if perm else p.get("url", ""))
            posts[str(p.get("id"))] = dict(
                post_url=purl, post_text=short(g(p, "title") + "\n" + g(p, "selfText")),
                post_author=p.get("author", ""), post_comment_count=p.get("numComments", ""))
    rows = []
    for c in raw:
        if c.get("type") != "comment" or not c.get("body"):
            continue
        pid = str(c.get("postId", "")).replace("t3_", "")
        post = posts.get(pid) or posts.get(f"t3_{pid}") or {"post_text": c.get("postTitle", "")}
        link = c.get("permalink", "")
        rows.append(mk("reddit", topic, post, comment_id=c.get("id", ""), comment_text=c.get("body"),
                       comment_author=c.get("author", ""), comment_time=c.get("createdAt", ""),
                       comment_likes=c.get("score", ""), is_reply=(c.get("depth") or 0) > 0,
                       parent_id=c.get("parentId", ""),
                       comment_url=link if link.startswith("http") else f"https://www.reddit.com{link}"))
    return rows


def scrape_linkedin(ap: Apify, topic, n_posts, n_comments):
    raw = ap.run(ACTORS["linkedin_search"], {
        "keywords": [topic], "max_posts": max(10, n_posts),  # 这个 actor 最少 10 条
        "sort_by": "relevance", "date_filter": "past-month",
    }, "linkedin_search")
    for p in raw:
        p["_c"] = g(p, "stats.comments", "comments", default=0)
    chosen = pick_posts([p for p in raw if p.get("post_url")], n_posts, "_c")
    posts = {p["post_url"]: dict(post_url=p["post_url"], post_text=short(g(p, "text", "content.text")),
                                 post_author=g(p, "author.name", "owner_name"), post_comment_count=p["_c"])
             for p in chosen}
    if not posts:
        return []
    coms = ap.run(ACTORS["linkedin_comments"], {
        "posts": list(posts), "maxItems": n_comments, "scrapeReplies": False, "profileScraperMode": "short",
    }, "linkedin_comments")
    rows = []
    for c in coms:
        if not c.get("commentary"):
            continue
        q = g(c, "query.post")
        post = posts.get(q) or {"post_url": q}
        rows.append(mk("linkedin", topic, post, comment_id=c.get("id", ""), comment_text=c.get("commentary"),
                       comment_author=g(c, "actor.name"), comment_time=g(c, "createdAt"),
                       comment_likes=g(c, "engagement.likes"), is_reply=False,
                       comment_url=c.get("linkedinUrl", "")))
    return rows


def scrape_threads(ap: Apify, topic, n_posts, n_comments):
    common = {"includeViews": False, "includeRunSummary": False}
    raw = ap.run(ACTORS["threads"], {
        **common, "mode": "search", "keywords": [topic], "maxPosts": n_posts, "maxItems": n_posts,
        "searchTypes": ["top"], "expandQueries": False, "minReplies": 1,
    }, "threads_search")
    found = [p for p in raw if p.get("url") and p.get("text") and p.get("type") not in ("notice", "error")]
    found = filter_relevant(found, topic, ("text",))
    chosen = pick_posts(found, n_posts, "replyCount")
    posts = {p["url"]: dict(post_url=p["url"], post_text=short(p.get("text")), post_author=p.get("username", ""),
                            post_comment_count=p.get("replyCount", "")) for p in chosen}
    if not posts:
        return []
    th = ap.run(ACTORS["threads"], {
        **common, "mode": "thread", "postUrls": list(posts), "maxRepliesPerPost": n_comments,
        "maxItems": (n_comments + 1) * len(posts),
    }, "threads_replies")
    root_codes = {u.rstrip("/").split("/")[-1]: u for u in posts}
    rows = []
    for c in th:
        if not c.get("text") or c.get("type") in ("notice", "error"):
            continue
        if c.get("url") in posts or c.get("code") in root_codes:
            continue  # 原帖本身
        parent = c.get("parentPostUrl") or root_codes.get(str(c.get("threadId", "")), "")
        post = posts.get(parent) or next((v for k, v in posts.items() if parent and parent.rstrip("/") in k), {})
        rows.append(mk("threads", topic, post or {"post_url": parent}, comment_id=c.get("id", ""),
                       comment_text=c.get("text"), comment_author=c.get("username", ""),
                       comment_time=g(c, "createdAt") or to_iso(c.get("timestamp")),
                       comment_likes=c.get("likeCount", ""), is_reply=True,
                       parent_id=c.get("parentPostId", ""), comment_url=c.get("url", "")))
    return rows


SCRAPERS = {
    "x": scrape_x, "facebook": scrape_facebook, "youtube": scrape_youtube,
    "reddit": scrape_reddit, "linkedin": scrape_linkedin, "threads": scrape_threads,
}


# ---------------------------------------------------------------- main
def parse_platforms(s: str) -> list[str]:
    alias = {"twitter": "x", "fb": "facebook", "yt": "youtube", "li": "linkedin", "thread": "threads"}
    items = [alias.get(p.strip().lower(), p.strip().lower()) for p in s.split(",") if p.strip()]
    if "all" in items:
        return PLATFORMS[:]
    bad = [p for p in items if p not in PLATFORMS]
    if bad:
        sys.exit(f"不认识的平台: {bad}，可选: {', '.join(PLATFORMS)} 或 all")
    return list(dict.fromkeys(items))


def get_token() -> str:
    load_dotenv(ROOT / ".env")
    token = os.environ.get("APIFY_TOKEN", "")
    if not token or token.startswith("PASTE_"):
        raise RuntimeError("缺少 APIFY_TOKEN：在 .env 里写 APIFY_TOKEN=你的token，或 export APIFY_TOKEN=...")
    return token


def run_job(topic: str, platforms: list[str], posts: int, comments: int, budget: float,
            max_usd_per_run: float, parallel: int = 3, out: str | None = None) -> dict:
    """跑一次完整抓取，返回结果摘要。命令行和 UI 共用。"""
    token = get_token()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    slug = re.sub(r"[^\w一-鿿-]+", "_", topic).strip("_")[:40] or "topic"
    out_dir = Path(out or ROOT / "output") / f"{slug}_{stamp}"
    raw_dir = out_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "job.json").write_text(json.dumps(
        {"topic": topic, "platforms": platforms, "posts": posts, "comments": comments, "budget": budget,
         "max_usd_per_run": max_usd_per_run, "started_at": datetime.now().isoformat()},
        ensure_ascii=False, indent=1), encoding="utf-8")

    log(f"话题: {topic} | 平台: {', '.join(platforms)} | 每平台 {posts} 帖 × {comments} 评论 | "
        f"预算 ${budget}（单次 run ≤ ${max_usd_per_run}）")
    client = Apify(token, Budget(budget, max_usd_per_run), raw_dir)

    all_rows, per_platform = [], {}
    with ThreadPoolExecutor(max_workers=max(1, parallel)) as ex:
        futs = {ex.submit(SCRAPERS[p], client, topic, posts, comments): p for p in platforms}
        for f in as_completed(futs):
            p = futs[f]
            try:
                rows = f.result()
            except Exception as e:  # noqa: BLE001
                log(f"❌ {p} 出错: {e}")
                rows = []
            per_platform[p] = len(rows)
            log(f"■  {p} 完成：{len(rows)} 条评论")
            all_rows.extend(rows)

    order = {p: i for i, p in enumerate(PLATFORMS)}
    all_rows.sort(key=lambda r: (order[r["platform"]], r["post_url"]))
    csv_path = out_dir / "comments_all.csv"
    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:  # utf-8-sig: Excel 打开中文不乱码
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(all_rows)

    log("核对实际扣费…")
    client.reconcile()
    total_cost = sum(r[3] for r in client.records)
    with (out_dir / "run_summary.csv").open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["step", "actor", "status", "cost_usd", "items", "run_id"])
        w.writerows(sorted(client.records))
        w.writerow(["TOTAL", "", "", round(total_cost, 4), "", ""])
    log(f"全部完成：{len(all_rows)} 条评论，花费 ${total_cost:.4f}")
    return {"out_dir": str(out_dir), "csv": str(csv_path), "per_platform": per_platform,
            "total": len(all_rows), "cost": round(total_cost, 4), "platforms": platforms}


def main():
    ap = argparse.ArgumentParser(description="多平台话题评论抓取 → 合并 CSV")
    ap.add_argument("--topic", required=True, help="话题关键词")
    ap.add_argument("--platforms", default="all",
                    help="逗号分隔，可选 x,facebook,youtube,reddit,linkedin,threads 或 all（默认 all）")
    ap.add_argument("--posts", type=int, default=5, help="每个平台取多少个帖子/视频（默认 5）")
    ap.add_argument("--comments", type=int, default=50, help="每个帖子最多抓多少条评论（默认 50）")
    ap.add_argument("--budget", type=float, default=2.0, help="本次总预算上限，美元（默认 2）")
    ap.add_argument("--max-usd-per-run", type=float, default=0.5, help="单个 actor run 的花费上限（默认 0.5）")
    ap.add_argument("--parallel", type=int, default=3, help="同时跑几个平台（默认 3，免费账号内存有限）")
    ap.add_argument("--out", default=str(ROOT / "output"), help="输出目录")
    ap.add_argument("--test", action="store_true",
                    help="轻量测试：每平台 2 帖 × 5 评论，总预算 $1，单次 run 上限 $0.05")
    a = ap.parse_args()

    if a.test:
        a.posts, a.comments, a.budget, a.max_usd_per_run = 2, 5, 1.0, 0.05

    try:
        res = run_job(a.topic, parse_platforms(a.platforms), a.posts, a.comments, a.budget,
                      a.max_usd_per_run, a.parallel, a.out)
    except RuntimeError as e:
        sys.exit(str(e))

    print("\n========== 结果 ==========")
    for p in res["platforms"]:
        print(f"  {p:<10} {res['per_platform'].get(p, 0):>5} 条评论")
    print(f"  {'合计':<9} {res['total']:>5} 条评论")
    print(f"  花费       ${res['cost']:.4f}（Apify 实际扣费，明细见 run_summary.csv）")
    print(f"  CSV: {res['csv']}")


if __name__ == "__main__":
    main()
