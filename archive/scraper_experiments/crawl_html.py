#!/usr/bin/env python3
"""
小红书 HTML 解析采集脚本（Playwright版）
用Playwright访问页面，从 __INITIAL_STATE__ 提取数据。
浏览器自动维护session，不会cookie过期。

用法：
    python scraper/crawl_html.py                # 从上次中断处继续
    python scraper/crawl_html.py --test 5       # 测试前5个博主
    python scraper/crawl_html.py --status       # 查看进度
    python scraper/crawl_html.py --export       # 导出 CSV
"""

import argparse
import asyncio
import json
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

PROXY = "http://127.0.0.1:7890"
COOKIE_STR = "a1=19b59fabb6bw93jydbj5sf9kdgrbbg27yx5f4hszh30000101682; web_session=040069b522744d19141ec9def63b4bdacdfdb9; webId=c4977872fab544a22e28fe3af3e3b242"

MIN_DELAY = 1.5
MAX_DELAY = 3.0


def parse_cookie_str(s):
    d = {}
    for item in s.split(";"):
        item = item.strip()
        if "=" in item:
            k, v = item.split("=", 1)
            d[k.strip()] = v.strip()
    return d


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
    conn.commit()
    return conn


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
    return json.loads(raw)


async def fetch_page(page, url, retries=2):
    """Fetch a page using Playwright. Intercept initial HTML before JS modifies it."""
    for attempt in range(retries + 1):
        try:
            # Capture the raw server response (before JS modifies __INITIAL_STATE__)
            raw_html = None
            async def capture_response(response):
                nonlocal raw_html
                if response.url.split("?")[0].rstrip("/") == url.rstrip("/") and "text/html" in (response.headers.get("content-type", "")):
                    try:
                        raw_html = await response.text()
                    except:
                        pass
            page.on("response", capture_response)
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=20000)
            page.remove_listener("response", capture_response)
            if resp is None:
                return None
            html = raw_html or await page.content()
            if "IP_BLOCK" in html or "访问频率" in html:
                print("  ⚠️ IP 被限流，等待60秒...")
                await asyncio.sleep(60)
                continue
            if resp.status == 461:
                print("  ⚠️ 461 验证码触发，等待30秒...")
                await asyncio.sleep(30)
                continue
            if resp.status != 200:
                print(f"  ⚠️ HTTP {resp.status}")
                return None
            return html
        except Exception as e:
            if attempt < retries:
                await asyncio.sleep(5)
            else:
                print(f"  ❌ 请求失败: {e}")
                return None
    return None


async def get_notes_from_profile(page, user_id):
    """Fetch user profile page and extract note list."""
    url = f"https://www.xiaohongshu.com/user/profile/{user_id}"
    html = await fetch_page(page, url)
    if not html:
        return None, None

    state = parse_state(html)
    if not state:
        return None, None

    user_data = state.get("user", {}).get("userPageData", {})
    basic = user_data.get("basicInfo", {})
    nickname = basic.get("nickname", "")

    notes_raw = state.get("user", {}).get("notes", [])
    if notes_raw is None:
        notes_raw = []

    all_notes = []
    for item in notes_raw:
        if isinstance(item, list):
            for sub in item:
                if isinstance(sub, dict) and "id" in sub:
                    all_notes.append(sub)
        elif isinstance(item, dict) and "id" in item:
            all_notes.append(item)

    return nickname, all_notes


async def get_note_detail(page, note_id):
    """Fetch note detail page and extract content."""
    url = f"https://www.xiaohongshu.com/explore/{note_id}"
    html = await fetch_page(page, url)
    if not html:
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

    return None


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


async def crawl_creator(page, notes_conn, user_id, name, idx, total):
    """Crawl all available notes for a single creator."""
    print(f"\n[{idx}/{total}] {name or user_id}")
    update_progress(user_id, "running")

    nickname, notes_list = await get_notes_from_profile(page, user_id)
    if notes_list is None:
        print(f"  ❌ 无法获取主页")
        update_progress(user_id, "error", error="profile_fetch_failed")
        return 0

    if not notes_list:
        print(f"  ⚠️ 无笔记")
        update_progress(user_id, "done", actual_notes=0)
        return 0

    creator_name = nickname or name
    print(f"  找到 {len(notes_list)} 篇笔记，开始获取详情...")

    success_count = 0
    consec_fail = 0
    for i, note_item in enumerate(notes_list):
        note_id = note_item.get("id", "")
        if not note_id:
            continue

        existing = notes_conn.execute("SELECT 1 FROM notes WHERE note_id=?", (note_id,)).fetchone()
        if existing:
            success_count += 1
            continue

        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        await asyncio.sleep(delay)

        detail = await get_note_detail(page, note_id)
        if detail:
            save_note(notes_conn, detail, user_id, creator_name)
            success_count += 1
            consec_fail = 0
            if (i + 1) % 10 == 0:
                print(f"  进度: {i+1}/{len(notes_list)}")
        else:
            consec_fail += 1
            if consec_fail >= 5:
                print(f"  ⚠️ 连续{consec_fail}次失败，跳过剩余笔记")
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
    if NOTES_DB.exists():
        nc = sqlite3.connect(NOTES_DB)
        notes_count = nc.execute("SELECT COUNT(*) FROM notes").fetchone()[0]
        nc.close()

    print(f"\n{'='*50}")
    print(f"采集进度 (共 {total} 个博主, notes.db: {notes_count} 篇)")
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
    cursor = conn.execute("SELECT * FROM notes")
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    conn.close()

    out_path = BASE_DIR / "data" / "notes_export.csv"
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)
    print(f"已导出 {len(rows)} 条到 {out_path}")


async def main():
    parser = argparse.ArgumentParser(description="小红书 HTML 采集 (Playwright)")
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

    cookie_dict = parse_cookie_str(COOKIE_STR)

    async with async_playwright() as p:
        print("启动 Playwright...")
        browser = await p.chromium.launch(headless=True, proxy={"server": PROXY})
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
        )
        await context.add_cookies([
            {"name": k, "value": v, "domain": ".xiaohongshu.com", "path": "/"}
            for k, v in cookie_dict.items()
        ])
        page = await context.new_page()
        print("✓ 浏览器就绪")

        try:
            consec_zero = 0
            for idx, (user_id, name, expected) in enumerate(pending, 1):
                count = await crawl_creator(page, notes_conn, user_id, name, idx, total)
                total_notes += count

                if count == 0:
                    consec_zero += 1
                    if consec_zero >= 10:
                        print(f"\n⚠️ 连续{consec_zero}个博主0篇，可能session异常，停止采集")
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
