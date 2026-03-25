#!/usr/bin/env python3
"""
小红书 HTML 解析采集脚本
从用户主页 HTML 提取笔记列表，再逐篇获取详情页。
每人首屏约30篇笔记。

用法：
    python scraper/crawl_html.py                # 从上次中断处继续
    python scraper/crawl_html.py --test 5       # 测试前5个博主
    python scraper/crawl_html.py --status       # 查看进度
    python scraper/crawl_html.py --export       # 导出 CSV
"""

import argparse
import json
import os
import random
import re
import sqlite3
import sys
import time
from pathlib import Path

import requests

# Ensure print output is flushed immediately (for nohup/redirect)
sys.stdout.reconfigure(line_buffering=True)

BASE_DIR = Path(__file__).parent.parent
PROGRESS_DB = BASE_DIR / "data" / "crawl_progress.db"
NOTES_DB = BASE_DIR / "data" / "notes.db"

PROXY = "http://127.0.0.1:7890"
PROXIES = {"http": PROXY, "https": PROXY}
COOKIE_STR = "a1=19b59fabb6bw93jydbj5sf9kdgrbbg27yx5f4hszh30000101682; web_session=040069b522744d19141ef23bf63b4bd5e8b770; webId=c4977872fab544a22e28fe3af3e3b242"
HEADERS = {
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "referer": "https://www.xiaohongshu.com/",
    "cookie": COOKIE_STR,
}

# Delay between requests (seconds)
MIN_DELAY = 1.5
MAX_DELAY = 3.0


def init_notes_db():
    conn = sqlite3.connect(NOTES_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS notes (
            note_id TEXT PRIMARY KEY,
            creator_id TEXT,
            creator_name TEXT,
            title TEXT,
            content TEXT,
            note_type TEXT,
            liked_count TEXT,
            collected_count TEXT,
            comment_count TEXT,
            share_count TEXT,
            tags TEXT,
            ip_location TEXT,
            time INTEGER,
            last_update_time INTEGER,
            crawled_at TEXT
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


def fetch_page(url, retries=2):
    """Fetch a page with retries."""
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=15)
            if resp.status_code == 200:
                if "IP_BLOCK" in resp.text or "访问频率" in resp.text:
                    print("  ⚠️ IP 被限流，等待60秒...")
                    time.sleep(60)
                    continue
                return resp.text
            elif resp.status_code == 461:
                print(f"  ⚠️ 461 验证码触发，等待30秒...")
                time.sleep(30)
                continue
            else:
                print(f"  ⚠️ HTTP {resp.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            if attempt < retries:
                time.sleep(5)
            else:
                print(f"  ❌ 请求失败: {e}")
                return None
    return None


def get_notes_from_profile(user_id):
    """Fetch user profile page and extract note list."""
    url = f"https://www.xiaohongshu.com/user/profile/{user_id}"
    html = fetch_page(url)
    if not html:
        return None, None

    state = parse_state(html)
    if not state:
        return None, None

    # Extract user info
    user_data = state.get("user", {}).get("userPageData", {})
    basic = user_data.get("basicInfo", {})
    nickname = basic.get("nickname", "")

    # Extract notes (nested list structure)
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


def get_note_detail(note_id):
    """Fetch note detail page and extract content."""
    url = f"https://www.xiaohongshu.com/explore/{note_id}"
    html = fetch_page(url)
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
    """Save a note to the database."""
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


def crawl_creator(notes_conn, user_id, name, idx, total):
    """Crawl all available notes for a single creator."""
    print(f"\n[{idx}/{total}] {name or user_id}")
    update_progress(user_id, "running")

    # Step 1: Get notes list from profile
    nickname, notes_list = get_notes_from_profile(user_id)
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

    # Step 2: Get detail for each note
    success_count = 0
    for i, note_item in enumerate(notes_list):
        note_id = note_item.get("id", "")
        if not note_id:
            continue

        # Check if already crawled
        existing = notes_conn.execute("SELECT 1 FROM notes WHERE note_id=?", (note_id,)).fetchone()
        if existing:
            success_count += 1
            continue

        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        time.sleep(delay)

        detail = get_note_detail(note_id)
        if detail:
            save_note(notes_conn, detail, user_id, creator_name)
            success_count += 1
            if (i + 1) % 10 == 0:
                print(f"  进度: {i+1}/{len(notes_list)}")
        else:
            print(f"  ⚠️ 笔记 {note_id} 详情获取失败")

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

    # Notes DB stats
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


def main():
    parser = argparse.ArgumentParser(description="小红书 HTML 采集")
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

    # Init
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

    try:
        for idx, (user_id, name, expected) in enumerate(pending, 1):
            count = crawl_creator(notes_conn, user_id, name, idx, total)
            total_notes += count

            # Profile page delay
            delay = random.uniform(MIN_DELAY, MAX_DELAY)
            time.sleep(delay)

    except KeyboardInterrupt:
        # Reset running to pending
        conn = sqlite3.connect(PROGRESS_DB)
        conn.execute("UPDATE creator_progress SET status='pending' WHERE status='running'")
        conn.commit()
        conn.close()
        print(f"\n\n中断，已保存进度。共采集 {total_notes} 篇笔记。")

    show_status()


if __name__ == "__main__":
    main()
