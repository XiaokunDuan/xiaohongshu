#!/usr/bin/env python3
"""
小红书混合采集脚本（Playwright浏览器 + 网络请求拦截）

核心思路：
- 用Playwright访问博主主页，拦截浏览器发出的 /api/sns/web/v1/user_posted 请求
- 从拦截到的API响应中提取 note_id + xsec_token
- 再访问每篇笔记详情页，从 __INITIAL_STATE__ 提取正文
- 评论通过DOM提取

这样绕过了两个卡点：
1. 不依赖 __INITIAL_STATE__ 中的 noteId（已被SSR清空）
2. 不直接调用API（避免IP限流），而是让浏览器自然发请求

用法：
    python scraper/crawl_hybrid.py                # 从上次中断处继续
    python scraper/crawl_hybrid.py --test 3       # 测试前3个博主
    python scraper/crawl_hybrid.py --status       # 查看进度
    python scraper/crawl_hybrid.py --export       # 导出CSV
    python scraper/crawl_hybrid.py --reset-errors # 重置失败博主
"""

import argparse
import asyncio
import json
import os
import random
import re
import sqlite3
import sys
import time
from pathlib import Path

sys.stdout.reconfigure(line_buffering=True)

BASE_DIR = Path(__file__).parent.parent
PROGRESS_DB = BASE_DIR / "data" / "crawl_progress.db"
NOTES_DB = BASE_DIR / "data" / "notes.db"
CREATOR_IDS_FILE = BASE_DIR / "creator_ids.txt"
COOKIE_FILE = BASE_DIR / "data" / "xhs_cookie.txt"

PROXY = "http://127.0.0.1:7890"
COOKIE_STR = "a1=19b59fabb6bw93jydbj5sf9kdgrbbg27yx5f4hszh30000101682; web_session=040069b522744d19141ec9def63b4bdacdfdb9; webId=c4977872fab544a22e28fe3af3e3b242"

MIN_DELAY = 2.0
MAX_DELAY = 4.0


def parse_cookie_str(s):
    d = {}
    for item in s.split(";"):
        item = item.strip()
        if "=" in item:
            k, v = item.split("=", 1)
            d[k.strip()] = v.strip()
    return d


def load_cookie_str():
    """Load cookie from env or file so it can be refreshed without editing code."""
    env_cookie = os.environ.get("XHS_COOKIE", "").strip()
    if env_cookie:
        return env_cookie

    if COOKIE_FILE.exists():
        file_cookie = COOKIE_FILE.read_text(encoding="utf-8").strip()
        if file_cookie:
            return file_cookie

    return COOKIE_STR


def load_creator_ids():
    if not CREATOR_IDS_FILE.exists():
        return []
    with open(CREATOR_IDS_FILE, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def init_notes_db():
    conn = sqlite3.connect(NOTES_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS notes (
            note_id TEXT PRIMARY KEY, creator_id TEXT, creator_name TEXT,
            title TEXT, content TEXT, note_type TEXT,
            liked_count TEXT, collected_count TEXT, comment_count TEXT, share_count TEXT,
            tags TEXT, ip_location TEXT, time INTEGER, last_update_time INTEGER, crawled_at TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_creator ON notes(creator_id)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS comments (
            comment_id TEXT PRIMARY KEY, note_id TEXT, user_name TEXT, user_id TEXT,
            content TEXT, like_count TEXT, sub_comment_count TEXT,
            create_time TEXT, ip_location TEXT, crawled_at TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_comment_note ON comments(note_id)")
    conn.commit()
    return conn


def sync_creator_progress():
    """Ensure creator_progress contains all ids from creator_ids.txt."""
    creator_ids = load_creator_ids()
    if not creator_ids:
        return

    conn = sqlite3.connect(PROGRESS_DB)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(creator_progress)").fetchall()}
    if not cols:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS creator_progress (
                user_id TEXT PRIMARY KEY,
                name TEXT,
                expected_notes INTEGER,
                status TEXT DEFAULT 'pending',
                actual_notes INTEGER DEFAULT 0,
                started_at TEXT,
                finished_at TEXT,
                error TEXT
            )
        """)

    conn.executemany(
        """
        INSERT OR IGNORE INTO creator_progress
        (user_id, name, expected_notes, status, actual_notes)
        VALUES (?, '', 0, 'pending', 0)
        """,
        [(user_id,) for user_id in creator_ids],
    )
    conn.execute("UPDATE creator_progress SET status='pending' WHERE status='running'")
    conn.commit()
    conn.close()


def get_pending_creators(limit=None):
    conn = sqlite3.connect(PROGRESS_DB)
    query = "SELECT user_id, name, expected_notes FROM creator_progress WHERE status = 'pending' ORDER BY ROWID"
    if limit:
        query += f" LIMIT {limit}"
    rows = conn.execute(query).fetchall()
    conn.close()
    return rows


def update_progress(user_id, status, actual_notes=0, error=None):
    conn = sqlite3.connect(PROGRESS_DB)
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    if status == "running":
        conn.execute("UPDATE creator_progress SET status=?, started_at=? WHERE user_id=?", (status, now, user_id))
    elif status == "done":
        conn.execute("UPDATE creator_progress SET status=?, finished_at=?, actual_notes=? WHERE user_id=?",
                      (status, now, actual_notes, user_id))
    elif status == "error":
        conn.execute("UPDATE creator_progress SET status=?, finished_at=?, error=? WHERE user_id=?",
                      (status, now, error, user_id))
    conn.commit()
    conn.close()


def parse_state(html):
    """Extract __INITIAL_STATE__ JSON from HTML."""
    matches = re.findall(r'window\.__INITIAL_STATE__=({.*?})</script>', html)
    if not matches:
        return None
    raw = matches[0].replace('undefined', 'null')
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


async def intercept_note_list(page, user_id):
    """
    Visit user profile and intercept /api/sns/web/v1/user_posted responses
    to extract note_ids and xsec_tokens.

    Scrolls down to trigger pagination and collects all notes.
    """
    collected_notes = []
    api_responses = []

    async def on_response(response):
        url = response.url
        if "/api/sns/web/v1/user_posted" in url:
            try:
                data = await response.json()
                if data.get("success") and data.get("data"):
                    api_responses.append(data["data"])
            except Exception:
                pass

    page.on("response", on_response)

    profile_url = f"https://www.xiaohongshu.com/user/profile/{user_id}"
    try:
        resp = await page.goto(profile_url, wait_until="domcontentloaded", timeout=20000)
        if resp is None or resp.status != 200:
            if resp and resp.status == 461:
                print("  ⚠️ 461验证码，等30s...")
                await asyncio.sleep(30)
            page.remove_listener("response", on_response)
            return None
    except Exception as e:
        print(f"  ❌ 访问主页失败: {e}")
        page.remove_listener("response", on_response)
        return None

    html = await page.content()
    if any(token in html for token in ("登录", "login", "验证码", "访问频率", "IP_BLOCK")):
        print("  ❌ 当前 cookie 失效或触发风控，请更新 data/xhs_cookie.txt")
        page.remove_listener("response", on_response)
        return None

    # Wait for initial API response
    await page.wait_for_timeout(3000)

    # Parse initial API responses
    for resp_data in api_responses:
        notes = resp_data.get("notes", [])
        for n in notes:
            nid = n.get("note_id", "")
            xsec = n.get("xsec_token", "")
            if nid:
                collected_notes.append({"note_id": nid, "xsec_token": xsec})

    # Scroll to load more if has_more
    last_has_more = api_responses[-1].get("has_more", False) if api_responses else False
    scroll_count = 0
    max_scrolls = 20  # Safety limit

    while last_has_more and scroll_count < max_scrolls:
        scroll_count += 1
        prev_count = len(api_responses)

        # Scroll down to trigger next page load
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(2000)

        # Check if new response arrived
        if len(api_responses) > prev_count:
            new_data = api_responses[-1]
            notes = new_data.get("notes", [])
            for n in notes:
                nid = n.get("note_id", "")
                xsec = n.get("xsec_token", "")
                if nid:
                    collected_notes.append({"note_id": nid, "xsec_token": xsec})
            last_has_more = new_data.get("has_more", False)
        else:
            # No new response after scroll, try once more
            await page.wait_for_timeout(2000)
            if len(api_responses) == prev_count:
                break

    page.remove_listener("response", on_response)

    # Deduplicate
    seen = set()
    unique = []
    for n in collected_notes:
        if n["note_id"] not in seen:
            seen.add(n["note_id"])
            unique.append(n)

    return unique


async def get_note_detail_from_page(page, note_id):
    """Fetch note detail page and extract content from __INITIAL_STATE__."""
    url = f"https://www.xiaohongshu.com/explore/{note_id}"

    # Capture raw server response before JS modifies it
    raw_html = None
    async def capture_response(response):
        nonlocal raw_html
        if response.url.split("?")[0].rstrip("/") == url.rstrip("/") and "text/html" in response.headers.get("content-type", ""):
            try:
                raw_html = await response.text()
            except Exception:
                pass

    page.on("response", capture_response)

    try:
        resp = await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        page.remove_listener("response", capture_response)

        if resp is None:
            return None
        if resp.status == 461:
            print("    ⚠️ 461验证码")
            await asyncio.sleep(30)
            return None
        if resp.status != 200:
            return None

        html = raw_html or await page.content()

        if "IP_BLOCK" in html or "访问频率" in html:
            print("    ⚠️ IP限流，等60s...")
            await asyncio.sleep(60)
            return None

        state = parse_state(html)
        if not state:
            return None

        note_map = state.get("note", {}).get("noteDetailMap", {})
        for k, v in note_map.items():
            if k == "null":
                continue
            note = v.get("note", {})
            if not note:
                continue

            interact = note.get("interactInfo", {})
            tags = [t.get("name", "") for t in note.get("tagList", []) if isinstance(t, dict)]

            return {
                "note_id": note.get("noteId", note_id),
                "title": note.get("title", ""),
                "content": note.get("desc", ""),
                "note_type": note.get("type", ""),
                "liked_count": str(interact.get("likedCount", "")),
                "collected_count": str(interact.get("collectedCount", "")),
                "comment_count": str(interact.get("commentCount", "")),
                "share_count": str(interact.get("shareCount", "")),
                "tags": json.dumps(tags, ensure_ascii=False),
                "ip_location": note.get("ipLocation", ""),
                "time": note.get("time", 0),
                "last_update_time": note.get("lastUpdateTime", 0),
            }
    except Exception as e:
        page.remove_listener("response", capture_response)
        print(f"    ❌ 详情页失败: {e}")

    return None


async def get_comments_from_dom(page):
    """Extract comments from the currently loaded note detail page DOM."""
    comments = []
    try:
        # Wait for comments section to load
        await page.wait_for_timeout(1500)

        # Try to find comment elements
        comment_els = await page.query_selector_all('.comment-item, [class*="commentItem"], [class*="comment-inner"]')

        for el in comment_els:
            try:
                # Try multiple selectors for username
                user_el = await el.query_selector('.user-name, [class*="userName"], [class*="author"] .name, a[class*="name"]')
                user_name = await user_el.inner_text() if user_el else ""

                # Try multiple selectors for content
                content_el = await el.query_selector('.content, [class*="content"], [class*="commentContent"]')
                content = await content_el.inner_text() if content_el else ""

                if content:
                    comments.append({
                        "user_name": user_name.strip(),
                        "content": content.strip(),
                    })
            except Exception:
                continue
    except Exception:
        pass

    return comments


def save_note(notes_conn, note_data, creator_id, creator_name):
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    notes_conn.execute("""
        INSERT OR REPLACE INTO notes
        (note_id, creator_id, creator_name, title, content, note_type,
         liked_count, collected_count, comment_count, share_count,
         tags, ip_location, time, last_update_time, crawled_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        note_data["note_id"], creator_id, creator_name,
        note_data["title"], note_data["content"], note_data["note_type"],
        note_data["liked_count"], note_data["collected_count"],
        note_data["comment_count"], note_data["share_count"],
        note_data["tags"], note_data["ip_location"],
        note_data["time"], note_data["last_update_time"], now,
    ))
    notes_conn.commit()


def save_comments(notes_conn, note_id, comments):
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    saved = 0
    for i, c in enumerate(comments):
        # Generate a deterministic comment_id from note_id + index + content hash
        import hashlib
        content_hash = hashlib.md5(c["content"].encode()).hexdigest()[:8]
        comment_id = f"{note_id}_{i}_{content_hash}"

        notes_conn.execute("""
            INSERT OR IGNORE INTO comments
            (comment_id, note_id, user_name, user_id, content, like_count,
             sub_comment_count, create_time, ip_location, crawled_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            comment_id, note_id, c["user_name"], "", c["content"],
            "", "", "", "", now,
        ))
        saved += 1
    notes_conn.commit()
    return saved


async def crawl_creator(page, notes_conn, user_id, name, idx, total):
    """Crawl all notes for one creator using network interception."""
    print(f"\n[{idx}/{total}] {name or user_id}")
    update_progress(user_id, "running")

    # Step 1: Get note list via network interception
    notes_list = await intercept_note_list(page, user_id)

    if notes_list is None:
        print(f"  ❌ 无法获取主页")
        update_progress(user_id, "error", error="profile_fetch_failed")
        return 0

    if not notes_list:
        print(f"  ⚠️ 无笔记")
        update_progress(user_id, "done", actual_notes=0)
        return 0

    print(f"  找到 {len(notes_list)} 篇笔记（网络拦截），开始获取详情...")

    # Step 2: Fetch each note's detail
    success_count = 0
    consec_fail = 0

    for i, note_info in enumerate(notes_list):
        note_id = note_info["note_id"]

        # Skip if already in DB with content
        existing = notes_conn.execute("SELECT content FROM notes WHERE note_id=?", (note_id,)).fetchone()
        if existing and existing[0]:
            success_count += 1
            continue

        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        await asyncio.sleep(delay)

        detail = await get_note_detail_from_page(page, note_id)
        if detail:
            save_note(notes_conn, detail, user_id, name)
            success_count += 1
            consec_fail = 0

            # Also try to grab comments from the same page
            comments = await get_comments_from_dom(page)
            if comments:
                saved_c = save_comments(notes_conn, note_id, comments)
                if saved_c:
                    print(f"    +{saved_c}条评论")

            if (i + 1) % 10 == 0:
                print(f"  进度: {i+1}/{len(notes_list)}")
        else:
            consec_fail += 1
            if consec_fail >= 5:
                print(f"  ⚠️ 连续{consec_fail}次失败，跳过剩余")
                break

    print(f"  ✓ 完成: {success_count}/{len(notes_list)} 篇")
    update_progress(user_id, "done", actual_notes=success_count)
    return success_count


def show_status():
    conn = sqlite3.connect(PROGRESS_DB)
    stats = conn.execute("""
        SELECT status, COUNT(*), COALESCE(SUM(expected_notes), 0), COALESCE(SUM(actual_notes), 0)
        FROM creator_progress GROUP BY status
    """).fetchall()
    total = conn.execute("SELECT COUNT(*) FROM creator_progress").fetchone()[0]
    conn.close()

    notes_count = 0
    comments_count = 0
    if NOTES_DB.exists():
        nc = sqlite3.connect(NOTES_DB)
        notes_count = nc.execute("SELECT COUNT(*) FROM notes").fetchone()[0]
        try:
            comments_count = nc.execute("SELECT COUNT(*) FROM comments").fetchone()[0]
        except Exception:
            pass
        nc.close()

    print(f"\n{'='*50}")
    print(f"采集进度 (共 {total} 个博主)")
    print(f"notes.db: {notes_count} 篇笔记, {comments_count} 条评论")
    print(f"{'='*50}")
    for status, count, expected, actual in stats:
        pct = count / total * 100
        print(f"  {status:10s}: {count:5d} ({pct:5.1f}%) | 预期 {int(expected):>8,} | 实际 {int(actual):>6,}")
    print()


def export_csv():
    if not NOTES_DB.exists():
        print("notes.db 不存在")
        return
    import csv
    conn = sqlite3.connect(NOTES_DB)

    # Export notes
    cursor = conn.execute("SELECT * FROM notes")
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    out_path = BASE_DIR / "data" / "notes_export.csv"
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)
    print(f"已导出 {len(rows)} 条笔记到 {out_path}")

    # Export comments
    cursor = conn.execute("SELECT * FROM comments")
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    out_path = BASE_DIR / "data" / "comments_export.csv"
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)
    print(f"已导出 {len(rows)} 条评论到 {out_path}")

    conn.close()


async def main():
    parser = argparse.ArgumentParser(description="小红书混合采集 (Playwright + 网络拦截)")
    parser.add_argument("--test", type=int, default=0, help="测试前N个博主")
    parser.add_argument("--status", action="store_true", help="查看进度")
    parser.add_argument("--export", action="store_true", help="导出CSV")
    parser.add_argument("--reset-errors", action="store_true", help="重置失败博主")
    args = parser.parse_args()

    if args.status:
        show_status()
        return
    if args.export:
        export_csv()
        return
    if args.reset_errors:
        conn = sqlite3.connect(PROGRESS_DB)
        n = conn.execute("UPDATE creator_progress SET status='pending', error=NULL WHERE status='error'").rowcount
        conn.commit()
        conn.close()
        print(f"已重置 {n} 个")
        return

    from playwright.async_api import async_playwright

    sync_creator_progress()
    notes_conn = init_notes_db()
    limit = args.test if args.test > 0 else None
    pending = get_pending_creators(limit)

    if not pending:
        print("全部完成！")
        show_status()
        return

    total = len(pending)
    print(f"待采集: {total} 个博主")
    total_notes = 0

    cookie_str = load_cookie_str()
    cookie_dict = parse_cookie_str(cookie_str)
    required = {"a1", "web_session", "webId"}
    missing = sorted(k for k in required if not cookie_dict.get(k))
    if missing:
        print(f"cookie 缺少必要字段: {', '.join(missing)}")
        print(f"请更新 {COOKIE_FILE}，或设置环境变量 XHS_COOKIE")
        return

    async with async_playwright() as p:
        print("启动 Playwright (headless)...")
        browser = await p.chromium.launch(headless=True, proxy={"server": PROXY})
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
        )
        await context.add_cookies([
            {"name": k, "value": v, "domain": ".xiaohongshu.com", "path": "/"}
            for k, v in cookie_dict.items()
        ])
        page = await context.new_page()

        # Warm up: visit explore page first to establish session
        print("预热session...")
        await page.goto("https://www.xiaohongshu.com/explore", wait_until="domcontentloaded")
        await page.wait_for_timeout(3000)
        print("✓ 浏览器就绪")

        try:
            consec_zero = 0
            for idx, (user_id, name, expected) in enumerate(pending, 1):
                count = await crawl_creator(page, notes_conn, user_id, name, idx, total)
                total_notes += count

                if count == 0:
                    consec_zero += 1
                    if consec_zero >= 10:
                        print(f"\n⚠️ 连续{consec_zero}个博主0篇，可能session异常，停止")
                        break
                else:
                    consec_zero = 0

                delay = random.uniform(MIN_DELAY, MAX_DELAY)
                await asyncio.sleep(delay)

        except KeyboardInterrupt:
            conn = sqlite3.connect(PROGRESS_DB)
            conn.execute("UPDATE creator_progress SET status='pending' WHERE status='running'")
            conn.commit()
            conn.close()
            print(f"\n\n中断，已保存进度。共采集 {total_notes} 篇笔记。")

        await browser.close()

    show_status()


if __name__ == "__main__":
    asyncio.run(main())
