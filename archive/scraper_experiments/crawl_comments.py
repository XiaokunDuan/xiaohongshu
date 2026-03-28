#!/usr/bin/env python3
"""
小红书评论采集脚本（Playwright DOM提取）
从笔记详情页渲染后的DOM中提取评论，不走API，不需要签名。

用法：
    python scraper/crawl_comments.py --test 3       # 测试3条笔记
    python scraper/crawl_comments.py                 # 全量采集
    python scraper/crawl_comments.py --status        # 查看进度
"""

import argparse
import asyncio
import json
import sqlite3
import sys
import time
from pathlib import Path

sys.stdout.reconfigure(line_buffering=True)

BASE_DIR = Path(__file__).parent.parent
NOTES_DB = BASE_DIR / "data" / "notes.db"

PROXY = "http://127.0.0.1:7890"
COOKIE_STR = "a1=19b59fabb6bw93jydbj5sf9kdgrbbg27yx5f4hszh30000101682; web_session=040069b522744d19141ec9def63b4bdacdfdb9; webId=c4977872fab544a22e28fe3af3e3b242"


def parse_cookie_str(cookie_str):
    cookies = {}
    for item in cookie_str.split(";"):
        item = item.strip()
        if "=" in item:
            k, v = item.split("=", 1)
            cookies[k.strip()] = v.strip()
    return cookies


def init_comments_table():
    conn = sqlite3.connect(NOTES_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS comments (
            comment_id TEXT PRIMARY KEY,
            note_id TEXT,
            user_name TEXT,
            user_id TEXT,
            content TEXT,
            like_count TEXT,
            sub_comment_count TEXT,
            create_time TEXT,
            ip_location TEXT,
            crawled_at TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_comment_note ON comments(note_id)")
    conn.commit()
    return conn


def get_notes_to_crawl(limit=None):
    """Get notes that haven't had comments crawled yet."""
    conn = sqlite3.connect(NOTES_DB)
    # Prioritize normal (image-text) notes with content
    query = """
        SELECT n.note_id, n.title, n.creator_name
        FROM notes n
        LEFT JOIN (SELECT DISTINCT note_id FROM comments) c ON n.note_id = c.note_id
        WHERE c.note_id IS NULL
        ORDER BY n.note_type = 'normal' DESC, LENGTH(n.content) DESC
    """
    if limit:
        query += f" LIMIT {limit}"
    rows = conn.execute(query).fetchall()
    conn.close()
    return rows


async def extract_comments(page, note_id, max_scroll=3):
    """Navigate to note page and extract comments from DOM."""
    url = f"https://www.xiaohongshu.com/explore/{note_id}"
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        await page.wait_for_timeout(3000)

        # Scroll to load comments
        for _ in range(max_scroll):
            await page.evaluate("window.scrollBy(0, 500)")
            await page.wait_for_timeout(1000)

        # Extract comments from DOM
        comments = await page.evaluate("""
            () => {
                const results = [];
                // Try multiple selectors for comment containers
                const selectors = [
                    '.comment-item', '.note-comment', '.comment-inner',
                    '[class*="commentItem"]', '[class*="CommentItem"]',
                    '.parent-comment', '.list-container .comment'
                ];

                let commentEls = [];
                for (const sel of selectors) {
                    commentEls = document.querySelectorAll(sel);
                    if (commentEls.length > 0) break;
                }

                // If specific selectors don't work, try a more general approach
                if (commentEls.length === 0) {
                    // Look for comment section
                    const commentSection = document.querySelector(
                        '.comments-container, .comment-list, [class*="commentList"], [class*="CommentList"]'
                    );
                    if (commentSection) {
                        commentEls = commentSection.children;
                    }
                }

                for (const el of commentEls) {
                    const nameEl = el.querySelector(
                        '.name, .user-name, [class*="userName"], [class*="nickname"], .author-name'
                    );
                    const contentEl = el.querySelector(
                        '.content, .note-text, [class*="content"], [class*="commentContent"]'
                    );
                    if (contentEl && contentEl.textContent.trim()) {
                        results.push({
                            user_name: nameEl ? nameEl.textContent.trim() : '',
                            content: contentEl.textContent.trim(),
                        });
                    }
                }
                return results;
            }
        """)
        return comments
    except Exception as e:
        print(f"  Error extracting comments for {note_id}: {e}")
        return []


async def main():
    parser = argparse.ArgumentParser(description="小红书评论采集(Playwright)")
    parser.add_argument("--test", type=int, default=0, help="测试前N条笔记")
    parser.add_argument("--status", action="store_true", help="查看进度")
    args = parser.parse_args()

    if args.status:
        if not NOTES_DB.exists():
            print("notes.db 不存在")
            return
        conn = sqlite3.connect(NOTES_DB)
        try:
            total = conn.execute("SELECT COUNT(*) FROM comments").fetchone()[0]
            notes_with = conn.execute("SELECT COUNT(DISTINCT note_id) FROM comments").fetchone()[0]
            print(f"评论: {total} 条, 覆盖 {notes_with} 篇笔记")
        except sqlite3.OperationalError:
            print("comments 表不存在")
        conn.close()
        return

    from playwright.async_api import async_playwright

    comments_conn = init_comments_table()
    limit = args.test if args.test > 0 else None
    notes = get_notes_to_crawl(limit)

    if not notes:
        print("没有待采集评论的笔记")
        return

    print(f"待采集评论: {len(notes)} 篇笔记")
    cookie_dict = parse_cookie_str(COOKIE_STR)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            proxy={"server": PROXY},
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        )
        await context.add_cookies([
            {"name": k, "value": v, "domain": ".xiaohongshu.com", "path": "/"}
            for k, v in cookie_dict.items()
        ])

        page = await context.new_page()
        total_comments = 0

        for idx, (note_id, title, creator) in enumerate(notes, 1):
            print(f"[{idx}/{len(notes)}] {creator}: {title[:30]}")
            comments = await extract_comments(page, note_id)

            if comments:
                now = time.strftime("%Y-%m-%d %H:%M:%S")
                for i, c in enumerate(comments):
                    comment_id = f"{note_id}_{i}"
                    comments_conn.execute("""
                        INSERT OR IGNORE INTO comments
                        (comment_id, note_id, user_name, content, crawled_at)
                        VALUES (?, ?, ?, ?, ?)
                    """, (comment_id, note_id, c["user_name"], c["content"], now))
                comments_conn.commit()
                total_comments += len(comments)
                print(f"  {len(comments)} 条评论")
            else:
                print(f"  无评论或提取失败")

            await page.wait_for_timeout(2000)

        await browser.close()

    print(f"\n完成: 共采集 {total_comments} 条评论")
    comments_conn.close()


if __name__ == "__main__":
    asyncio.run(main())
