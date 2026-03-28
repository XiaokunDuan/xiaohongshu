#!/usr/bin/env python3
"""
Minimal Xiaohongshu crawler via Chrome DevTools Protocol.

Idea:
- connect to an already logged-in Chrome over CDP
- open a creator profile page
- listen to browser XHR/fetch responses
- capture user_posted responses for note lists
- open one captured note page
- capture comment/page responses for first-screen comments

This script does not compute signatures or call Xiaohongshu APIs directly.
The browser does that work; the script only listens to network responses.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import random
import re
import sqlite3
import time
from pathlib import Path
from typing import Any

from playwright.async_api import async_playwright


BASE_DIR = Path(__file__).parent
DAREN_CSV = BASE_DIR / "data" / "daren_clusters.csv"
PROGRESS_DB = BASE_DIR / "data" / "crawl_progress.db"
DEFAULT_OUTPUT = BASE_DIR / "data" / "cdp_intercept_sample.json"
DEFAULT_CDP = "http://127.0.0.1:9222"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Intercept Xiaohongshu note and comment responses over CDP")
    parser.add_argument("--user-id", help="Single creator user_id to test")
    parser.add_argument("--sample", type=int, default=1, help="Random sample size when --user-id is not set")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--cdp-url", default=DEFAULT_CDP, help="Chrome DevTools endpoint")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output JSON path")
    parser.add_argument("--update-progress", action="store_true", help="Write test results into crawl_progress.db")
    return parser.parse_args()


def load_creator_rows() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with DAREN_CSV.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            url = (row.get("达人官方地址") or "").strip()
            match = re.search(r"profile/([a-f0-9]{24})", url)
            if not match:
                continue
            rows.append(
                {
                    "user_id": match.group(1),
                    "name": (row.get("达人名称") or "").strip(),
                }
            )
    return rows


def choose_creators(user_id: str | None, sample: int, seed: int) -> list[dict[str, str]]:
    rows = load_creator_rows()
    if user_id:
        return [{"user_id": user_id, "name": ""}]
    rng = random.Random(seed)
    return rng.sample(rows, min(sample, len(rows)))


def ensure_progress_table() -> None:
    conn = sqlite3.connect(PROGRESS_DB)
    conn.execute(
        """
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
        """
    )
    conn.commit()
    conn.close()


def update_progress(user_id: str, status: str, error: str = "", actual_notes: int = 0) -> None:
    ensure_progress_table()
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(PROGRESS_DB)
    if status == "running":
        conn.execute(
            "UPDATE creator_progress SET status=?, started_at=? WHERE user_id=?",
            (status, now, user_id),
        )
    elif status == "done":
        conn.execute(
            "UPDATE creator_progress SET status=?, finished_at=?, actual_notes=? WHERE user_id=?",
            (status, now, actual_notes, user_id),
        )
    elif status == "error":
        conn.execute(
            "UPDATE creator_progress SET status=?, finished_at=?, error=? WHERE user_id=?",
            (status, now, error[:200], user_id),
        )
    conn.commit()
    conn.close()


class ResponseCollector:
    def __init__(self) -> None:
        self.user_posted_payloads: list[dict[str, Any]] = []
        self.comment_payloads: list[dict[str, Any]] = []

    async def handle(self, response) -> None:
        url = response.url
        if "user_posted" not in url and "comment/page" not in url:
            return
        try:
            payload = await response.json()
        except Exception:
            return

        if "user_posted" in url:
            self.user_posted_payloads.append({"url": url, "payload": payload})
        elif "comment/page" in url:
            self.comment_payloads.append({"url": url, "payload": payload})

    def extract_notes(self) -> list[dict[str, str]]:
        notes: list[dict[str, str]] = []
        seen: set[str] = set()
        for entry in self.user_posted_payloads:
            payload = entry.get("payload") or {}
            data = payload.get("data") or {}
            for note in data.get("notes", []) or []:
                note_id = str(note.get("note_id") or "").strip()
                xsec_token = str(note.get("xsec_token") or "").strip()
                if not note_id or note_id in seen:
                    continue
                seen.add(note_id)
                notes.append(
                    {
                        "note_id": note_id,
                        "xsec_token": xsec_token,
                        "title": str(note.get("display_title") or note.get("title") or ""),
                    }
                )
        return notes

    def extract_comments(self) -> list[dict[str, str]]:
        comments: list[dict[str, str]] = []
        seen: set[str] = set()
        for entry in self.comment_payloads:
            payload = entry.get("payload") or {}
            data = payload.get("data") or {}
            for comment in data.get("comments", []) or []:
                comment_id = str(comment.get("id") or comment.get("comment_id") or "").strip()
                content = str(comment.get("content") or "").strip()
                if not content:
                    continue
                key = comment_id or content
                if key in seen:
                    continue
                seen.add(key)
                user_info = comment.get("user_info") or {}
                comments.append(
                    {
                        "comment_id": comment_id,
                        "content": content,
                        "user_name": str(user_info.get("nickname") or ""),
                        "like_count": str(comment.get("like_count") or ""),
                    }
                )
        return comments


async def get_or_create_page(browser) -> Any:
    for context in browser.contexts:
        if context.pages:
            return context.pages[0]
    context = browser.contexts[0] if browser.contexts else await browser.new_context()
    return await context.new_page()


async def collect_creator_sample(page, creator: dict[str, str]) -> dict[str, Any]:
    collector = ResponseCollector()
    page.on("response", collector.handle)

    result: dict[str, Any] = {
        "user_id": creator["user_id"],
        "name": creator["name"],
        "profile_url": f"https://www.xiaohongshu.com/user/profile/{creator['user_id']}",
        "profile_title": "",
        "notes": [],
        "comments": [],
        "status": "error",
        "error": "",
    }

    try:
        await page.goto(result["profile_url"], wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(5000)
        result["profile_title"] = await page.title()

        notes = collector.extract_notes()
        result["notes"] = notes
        if not notes:
            result["error"] = "No user_posted notes captured from profile page"
            return result

        first_note = notes[0]
        note_url = f"https://www.xiaohongshu.com/explore/{first_note['note_id']}"
        if first_note["xsec_token"]:
            note_url += f"?xsec_token={first_note['xsec_token']}&xsec_source=pc_user"
        await page.goto(note_url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(5000)

        result["comments"] = collector.extract_comments()
        result["status"] = "done"
        return result
    except Exception as exc:
        result["error"] = str(exc)
        return result
    finally:
        try:
            page.remove_listener("response", collector.handle)
        except Exception:
            pass


async def run(args: argparse.Namespace) -> list[dict[str, Any]]:
    creators = choose_creators(args.user_id, args.sample, args.seed)
    async with async_playwright() as playwright:
        browser = await playwright.chromium.connect_over_cdp(args.cdp_url)
        page = await get_or_create_page(browser)
        results = []
        for index, creator in enumerate(creators, 1):
            print(f"[{index}/{len(creators)}] {creator['user_id']}")
            if args.update_progress:
                update_progress(creator["user_id"], "running")
            result = await collect_creator_sample(page, creator)
            print(
                f"  status={result['status']} notes={len(result['notes'])} comments={len(result['comments'])} title={result['profile_title'][:50]}"
            )
            if result["error"]:
                print(f"  error={result['error'][:200]}")
            if args.update_progress:
                if result["status"] == "done":
                    update_progress(creator["user_id"], "done", actual_notes=len(result["notes"]))
                else:
                    update_progress(creator["user_id"], "error", error=result["error"])
            results.append(result)
        await browser.close()
        return results


def main() -> None:
    args = parse_args()
    results = asyncio.run(run(args))
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Saved results to {output_path}")


if __name__ == "__main__":
    main()
