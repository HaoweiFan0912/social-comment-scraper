# 评论抓抓抓 2.0

按话题抓 X / Facebook / YouTube / Reddit / LinkedIn / Threads 的评论（通过 Apify actors），自动合并成一个 CSV。

## 准备

1. `.env` 里填 `APIFY_TOKEN=你的token`
2. Python 3.9+，只依赖 `requests`

## 网页界面

双击 `启动界面.command`（或运行 `python3 app.py`），浏览器打开 http://127.0.0.1:5050

- 填话题、勾平台（单个或多个）、设帖子数和评论数，下面会实时显示预计花费
- 「轻量测试模式」一键切到 2 帖 × 5 评论、预算 $1
- 运行时能看到每个平台的进度和实时日志
- 结果表可以按平台筛选、搜索，一键下载 CSV；「历史记录」里能打开以前每次的结果
- 右上角可以设置 / 更换 Apify token（只存在本机 .env）

界面只在本机运行（127.0.0.1），关掉终端窗口就停止。

## 命令行用法

```bash
# 轻量测试（每平台 2 帖 × 5 评论，总预算 $1，单次 run ≤ $0.05）
python3 social_comments.py --topic "AI and Insurance" --test

# 只跑部分平台
python3 social_comments.py --topic "AI and Insurance" --platforms reddit,youtube

# 正式跑：每平台 10 帖 × 100 评论，总预算 $5
python3 social_comments.py --topic "AI and Insurance" --posts 10 --comments 100 --budget 5
```

平台名：`x` `facebook` `youtube` `reddit` `linkedin` `threads`，或 `all`（默认）。别名 `twitter` `fb` `yt` `li` 也可以。

## 输出

`output/<话题>_<时间>/`
- `comments_all.csv`：合并后的评论（Excel 直接打开）。字段：platform, topic, post_url, post_text, post_author, post_comment_count, comment_id, comment_text, comment_author, comment_time, comment_likes, is_reply, parent_id, comment_url, scraped_at
- `run_summary.csv`：每一步用的 actor、状态、实际扣费
- `raw/`：每个 actor 的原始返回，方便排查

## 流程与 actor

| 平台 | 第 1 步：按话题找帖子 | 第 2 步：抓评论 |
|---|---|---|
| X | xquik/x-tweet-scraper | xquik/x-reply-scraper |
| Facebook | scrapeforge/facebook-search-posts（备用 topapi/facebook-search-posts-scraper） | thedoor/facebook-comment-scraper |
| YouTube | grow_media/youtube-search-api | supreme_coder/youtube-comment-scraper（每视频至少 10 条） |
| Reddit | automation-lab/reddit-scraper，一步完成（被限流时换 clearpath/reddit-post-comments-bulk-scraper） | 同左 |
| LinkedIn | datadoping/linkedin-posts-search-scraper（每次至少 10 帖） | harvestapi/linkedin-post-comments |
| Threads | lergassy/threads-scraper（search 模式） | 同一 actor 的 thread 模式（未登录最多约 25 条回复/帖） |

想换 actor 改 `social_comments.py` 顶部的 `ACTORS`。

## 已知限制

- Facebook、Threads 的搜索是模糊匹配，会混进不太相关的帖子；脚本会优先保留包含话题关键词的帖子。
- Reddit 会间歇性限流，脚本自动切换备用 actor。
- `scrapeforge` 的 Facebook 搜索在 Apify 免费账号下每次最多 5 条。
- 每个 run 都有花费上限（`--max-usd-per-run`），总预算（`--budget`）按实际扣费累计。
