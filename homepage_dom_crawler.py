#!/usr/bin/env python3
"""
Minimal Xiaohongshu homepage crawler.

Pipeline:
1. Fetch homepage HTML and extract first-screen note IDs.
2. Open each note page with Playwright.
3. Extract note metadata from rendered DOM.
4. Extract first-screen comments from rendered DOM without scrolling.

This script intentionally does not use MediaCrawler and does not try to fetch
all notes or all comments.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import re
from pathlib import Path

import requests
from playwright.async_api import async_playwright


BASE_DIR = Path(__file__).parent
COOKIE_FILE = BASE_DIR / "data" / "xhs_cookie.txt"
OUTPUT_DIR = BASE_DIR / "data"
DEFAULT_POSTS_CSV = OUTPUT_DIR / "homepage_posts.csv"
DEFAULT_COMMENTS_CSV = OUTPUT_DIR / "homepage_comments.csv"
HOMEPAGE_URL = "https://www.xiaohongshu.com/explore"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Crawl first-screen Xiaohongshu homepage posts and first-screen comments"
    )
    parser.add_argument("--limit", type=int, default=10, help="Max number of homepage posts to process")
    parser.add_argument(
        "--posts-out",
        default=str(DEFAULT_POSTS_CSV),
        help="CSV output path for post metadata",
    )
    parser.add_argument(
        "--comments-out",
        default=str(DEFAULT_COMMENTS_CSV),
        help="CSV output path for first-screen comments",
    )
    return parser.parse_args()


def load_cookie() -> str:
    if not COOKIE_FILE.exists():
        raise FileNotFoundError(f"Cookie file not found: {COOKIE_FILE}")
    cookie = COOKIE_FILE.read_text(encoding="utf-8").strip()
    if not cookie:
        raise ValueError(f"Cookie file is empty: {COOKIE_FILE}")
    return cookie


def parse_cookie_for_browser(cookie_str: str) -> list[dict]:
    cookies = []
    for item in cookie_str.split(";"):
        item = item.strip()
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        cookies.append(
            {
                "name": key.strip(),
                "value": value.strip(),
                "domain": ".xiaohongshu.com",
                "path": "/",
            }
        )
    return cookies


def fetch_homepage_note_ids(cookie_str: str) -> list[str]:
    resp = requests.get(
        HOMEPAGE_URL,
        headers={"user-agent": USER_AGENT, "cookie": cookie_str},
        timeout=20,
    )
    resp.raise_for_status()

    note_ids: list[str] = []
    for note_id in re.findall(r"/explore/([A-Za-z0-9]+)", resp.text):
        if note_id not in note_ids:
            note_ids.append(note_id)
    return note_ids


async def text_or_empty(locator) -> str:
    try:
        text = (await locator.inner_text(timeout=1000)).strip()
    except Exception:
        return ""
    return re.sub(r"\s+", " ", text)


async def extract_post(page, note_id: str) -> dict:
    url = f"https://www.xiaohongshu.com/explore/{note_id}"
    response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_timeout(5000)

    title = await page.title()
    if "页面不见了" in title:
        return {
            "note_id": note_id,
            "url": url,
            "status": response.status if response else "",
            "page_title": title,
            "valid": "0",
            "author": "",
            "content": "",
            "comment_count_visible": "0",
        }

    author_selectors = [
        "[class*=author] [class*=name]",
        "[class*=user] [class*=name]",
        "[class*=username]",
        "a[href*='/user/profile/']",
    ]
    content_selectors = [
        "[class*=desc]",
        "[class*=content]",
        "[class*=note-content]",
        "[class*=detail-desc]",
    ]
    author = ""
    content = ""

    for selector in author_selectors:
        author = await text_or_empty(page.locator(selector).first)
        if author:
            break

    for selector in content_selectors:
        content = await text_or_empty(page.locator(selector).first)
        if content:
            break

    comments = await extract_first_screen_comments(page, note_id)
    return {
        "note_id": note_id,
        "url": url,
        "status": response.status if response else "",
        "page_title": title,
        "valid": "1",
        "author": author,
        "content": content,
        "comment_count_visible": str(len(comments)),
        "comments": comments,
    }


async def extract_first_screen_comments(page, note_id: str) -> list[dict]:
    selectors = [
        ".comment-item",
        ".note-comment",
        ".comment-inner",
        "[class*='commentItem']",
        "[class*='CommentItem']",
        ".parent-comment",
        ".list-container .comment",
        "[class*='comment-content']",
        "[class*='comment']",
    ]

    seen: set[str] = set()
    comments: list[dict] = []

    for selector in selectors:
        try:
            locator = page.locator(selector)
            count = await locator.count()
        except Exception:
            continue

        for index in range(count):
            try:
                text = (await locator.nth(index).inner_text(timeout=800)).strip()
            except Exception:
                continue

            normalized = re.sub(r"\s+", " | ", text)
            if len(normalized) < 4 or normalized in seen:
                continue

            seen.add(normalized)
            comments.append(
                {
                    "note_id": note_id,
                    "comment_index": str(len(comments) + 1),
                    "content": normalized,
                }
            )
    return comments


def write_posts_csv(path: Path, posts: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "note_id",
                "url",
                "status",
                "page_title",
                "valid",
                "author",
                "content",
                "comment_count_visible",
            ],
        )
        writer.writeheader()
        for post in posts:
            row = {key: value for key, value in post.items() if key != "comments"}
            writer.writerow(row)


def write_comments_csv(path: Path, posts: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["note_id", "comment_index", "content"],
        )
        writer.writeheader()
        for post in posts:
            for comment in post.get("comments", []):
                writer.writerow(comment)


async def run(limit: int, posts_out: Path, comments_out: Path) -> None:
    cookie = load_cookie()
    note_ids = fetch_homepage_note_ids(cookie)
    target_ids = note_ids[:limit]

    print(f"Homepage note ids found: {len(note_ids)}")
    print(f"Processing first {len(target_ids)} homepage posts")

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=USER_AGENT)
        await context.add_cookies(parse_cookie_for_browser(cookie))
        page = await context.new_page()

        posts: list[dict] = []
        for index, note_id in enumerate(target_ids, 1):
            print(f"[{index}/{len(target_ids)}] {note_id}")
            post = await extract_post(page, note_id)
            posts.append(post)
            print(
                f"  valid={post['valid']} visible_comments={post['comment_count_visible']} title={post['page_title'][:50]}"
            )

        await browser.close()

    write_posts_csv(posts_out, posts)
    write_comments_csv(comments_out, posts)

    valid_posts = sum(1 for post in posts if post["valid"] == "1")
    total_comments = sum(len(post.get("comments", [])) for post in posts)
    print(json.dumps(
        {
            "posts_total": len(posts),
            "posts_valid": valid_posts,
            "comments_total": total_comments,
            "posts_csv": str(posts_out),
            "comments_csv": str(comments_out),
        },
        ensure_ascii=False,
        indent=2,
    ))


def main() -> None:
    args = parse_args()
    asyncio.run(
        run(
            limit=args.limit,
            posts_out=Path(args.posts_out),
            comments_out=Path(args.comments_out),
        )
    )


if __name__ == "__main__":
    main()
