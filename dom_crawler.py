#!/usr/bin/env python3
"""
Lightweight Xiaohongshu crawler via Chrome CDP + DOM extraction.

Behavior:
- connect to an already logged-in Chrome on port 9222
- open creator profile pages from daren_clusters.csv
- extract first-screen visible note cards from DOM
- open each visible note using the real href from the DOM
- extract first-screen comments from the rendered DOM

This script intentionally does not use MediaCrawler and does not call signed
Xiaohongshu APIs directly.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import random
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).parent
DAREN_CSV = BASE_DIR / "data" / "daren_clusters.csv"
PROGRESS_DB = BASE_DIR / "data" / "crawl_progress.db"
OUTPUT_DIR = BASE_DIR / "data" / "dom_crawl"
POSTS_CSV = OUTPUT_DIR / "posts.csv"
COMMENTS_CSV = OUTPUT_DIR / "comments.csv"
CDP_URL = "http://127.0.0.1:9222"

# Faster randomized pacing.
PROFILE_LOAD_WAIT_SEC = 3.5
NOTE_LOAD_WAIT_SEC = 3.5
BETWEEN_NOTES_MIN_SEC = 2.0
BETWEEN_NOTES_MAX_SEC = 4.0
BETWEEN_CREATORS_MIN_SEC = 4.0
BETWEEN_CREATORS_MAX_SEC = 7.0


def progress_bar(current: int, total: int, width: int = 24) -> str:
    if total <= 0:
        return "[" + ("-" * width) + "]"
    filled = int(width * current / total)
    return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"


def connect_db() -> sqlite3.Connection:
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
    return conn


def sync_creators(conn: sqlite3.Connection) -> int:
    if not DAREN_CSV.exists():
        raise FileNotFoundError(f"CSV not found: {DAREN_CSV}")

    rows = []
    with DAREN_CSV.open("r", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            url = (row.get("达人官方地址") or "").strip()
            match = re.search(r"profile/([a-f0-9]{24})", url)
            if not match:
                continue
            uid = match.group(1)
            name = (row.get("达人名称") or "").strip()
            try:
                expected = int(float(row.get("笔记总数") or 0))
            except (TypeError, ValueError):
                expected = 0
            rows.append((uid, name, expected))

    conn.executemany(
        "INSERT OR IGNORE INTO creator_progress (user_id, name, expected_notes) VALUES (?, ?, ?)",
        rows,
    )
    conn.commit()
    return len(rows)


def get_pending(conn: sqlite3.Connection, limit: int = 0, seed: int = 42) -> list[tuple[str, str]]:
    rows = conn.execute(
        "SELECT user_id, name FROM creator_progress WHERE status='pending' ORDER BY ROWID"
    ).fetchall()
    if limit > 0:
        rng = random.Random(seed)
        rows = rng.sample(rows, min(limit, len(rows)))
    return rows


def mark_status(
    conn: sqlite3.Connection,
    uid: str,
    status: str,
    actual_notes: int = 0,
    error: str = "",
) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if status == "running":
        conn.execute(
            "UPDATE creator_progress SET status=?, started_at=?, error=NULL WHERE user_id=?",
            (status, now, uid),
        )
    else:
        conn.execute(
            """
            UPDATE creator_progress
            SET status=?, actual_notes=?, finished_at=?, error=?
            WHERE user_id=?
            """,
            (status, actual_notes, now, error[:200], uid),
        )
    conn.commit()


def print_status(conn: sqlite3.Connection) -> None:
    total = conn.execute("SELECT COUNT(*) FROM creator_progress").fetchone()[0]
    print(f"Total creators: {total}")
    for status, count in conn.execute(
        "SELECT status, COUNT(*) FROM creator_progress GROUP BY status ORDER BY status"
    ):
        pct = count / total * 100 if total else 0
        print(f"  {status:>8}: {count:5d} ({pct:.1f}%)")


def load_existing_creator_result(uid: str, name: str) -> dict[str, Any]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / f"{uid}.json"
    if not path.exists():
        return {
            "user_id": uid,
            "name": name,
            "profile_url": f"https://www.xiaohongshu.com/user/profile/{uid}?exSource=",
            "profile_title": "",
            "profile_metrics": {},
            "notes": [],
            "error": "",
        }
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {
            "user_id": uid,
            "name": name,
            "profile_url": f"https://www.xiaohongshu.com/user/profile/{uid}?exSource=",
            "profile_title": "",
            "profile_metrics": {},
            "notes": [],
            "error": "",
        }
    data["name"] = data.get("name") or name
    data.setdefault("notes", [])
    data.setdefault("profile_metrics", {})
    data.setdefault("profile_url", f"https://www.xiaohongshu.com/user/profile/{uid}?exSource=")
    data.setdefault("error", "")
    return data


def save_creator_result(result: dict[str, Any]) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / f"{result['user_id']}.json"
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def read_existing_keys(path: Path, key_fields: list[str]) -> set[tuple[str, ...]]:
    if not path.exists():
        return set()
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return {
            tuple((row.get(field) or "") for field in key_fields)
            for row in reader
        }


def ensure_posts_csv_schema() -> None:
    expected = [
        "creator_id",
        "creator_name",
        "note_id",
        "open_url",
        "title",
        "desc",
        "likes",
        "crawl_status",
        "comment_count_visible",
    ]
    if not POSTS_CSV.exists():
        return
    with POSTS_CSV.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames == expected:
            return
        rows = list(reader)
    with POSTS_CSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=expected)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "creator_id": row.get("creator_id", ""),
                    "creator_name": row.get("creator_name", ""),
                    "note_id": row.get("note_id", ""),
                    "open_url": row.get("open_url", ""),
                    "title": row.get("title", ""),
                    "desc": row.get("desc", ""),
                    "likes": row.get("likes", ""),
                    "crawl_status": row.get("crawl_status", ""),
                    "comment_count_visible": row.get("comment_count_visible", ""),
                }
            )


def ensure_comments_csv_schema() -> None:
    expected = [
        "creator_id",
        "note_id",
        "comment_index",
        "content",
        "likes",
    ]
    if not COMMENTS_CSV.exists():
        return
    with COMMENTS_CSV.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames == expected:
            return
        rows = list(reader)
    with COMMENTS_CSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=expected)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "creator_id": row.get("creator_id", ""),
                    "note_id": row.get("note_id", ""),
                    "comment_index": row.get("comment_index", ""),
                    "content": row.get("content", ""),
                    "likes": row.get("likes", ""),
                }
            )


async def sleep_range(min_seconds: float, max_seconds: float) -> None:
    duration = random.uniform(min_seconds, max_seconds)
    print(f"      sleep {duration:.1f}s", flush=True)
    await asyncio.sleep(duration)


async def extract_profile_metrics(page) -> dict[str, str]:
    return await page.evaluate(
        """
        () => {
          const text = (selector) => document.querySelector(selector)?.textContent?.trim() || '';
          return {
            fans: text('.user-info .data-info .data-item:nth-child(2) .count'),
            following: text('.user-info .data-info .data-item:nth-child(1) .count'),
            total_likes: text('.user-info .data-info .data-item:nth-child(3) .count'),
          };
        }
        """
    )


async def extract_notes_from_dom(page) -> list[dict[str, str]]:
    return await page.evaluate(
        """
        () => {
          const results = [];
          const seen = new Set();
          const items = document.querySelectorAll('section.note-item');
          for (const item of items) {
            const hiddenExplore = item.querySelector('a[href^="/explore/"]');
            const cover = item.querySelector('a.cover[href]');
            const href = (cover?.getAttribute('href') || '').trim();
            const fallbackHref = (hiddenExplore?.getAttribute('href') || '').trim();
            const title = item.querySelector('.footer .title span')?.textContent?.trim() || '';
            const likes = item.querySelector('.footer .like-wrapper .count')?.textContent?.trim() || '';

            let noteId = '';
            let openHref = '';

            const hiddenMatch = fallbackHref.match(/^\\/explore\\/([A-Za-z0-9]+)/);
            if (hiddenMatch) {
              noteId = hiddenMatch[1];
            }

            if (href.startsWith('/user/profile/')) {
              const coverMatch = href.match(/^\\/user\\/profile\\/[^/]+\\/([A-Za-z0-9]+)/);
              if (coverMatch) {
                noteId = noteId || coverMatch[1];
                openHref = href;
              }
            }

            if (!openHref && fallbackHref) {
              openHref = fallbackHref;
            }

            if (!noteId || !openHref) continue;
            if (seen.has(noteId)) continue;
            seen.add(noteId);

            results.push({
              note_id: noteId,
              href: openHref,
              title,
              likes
            });
          }
          return results;
        }
        """
    )


async def extract_note_detail(page) -> dict[str, Any]:
    return await page.evaluate(
        """
        () => {
          const text = (selector) => document.querySelector(selector)?.textContent?.trim() || '';
          const comments = [];
          const seen = new Set();

          for (const node of document.querySelectorAll('div.comment-item')) {
            const content =
              node.querySelector('.content .note-text span')?.textContent?.trim()
              || node.querySelector('.content .note-text')?.textContent?.trim()
              || '';
            const likes =
              node.querySelector('.interactions .count')?.textContent?.trim()
              || node.querySelector('.like .count')?.textContent?.trim()
              || '';
            if (!content || seen.has(content)) continue;
            seen.add(content);
            comments.push({
              content: content.substring(0, 1000),
              likes
            });
          }

          return {
            title: text('#detail-title') || text('.title'),
            desc:
              text('#detail-desc .note-text')
              || text('.desc .note-text')
              || text('.note-text'),
            comment_count_visible: comments.length,
            comments
          };
        }
        """
    )


def absolutize_href(href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return f"https://www.xiaohongshu.com{href}"


async def crawl_creator(page, uid: str, name: str, max_notes: int = 10) -> dict[str, Any]:
    result = load_existing_creator_result(uid, name)
    result["error"] = ""
    existing_note_ids = {note.get("note_id", "") for note in result["notes"]}

    try:
        await page.goto(result["profile_url"], wait_until="domcontentloaded", timeout=30000)
    except Exception as exc:
        result["error"] = f"profile_load_failed: {exc}"
        return result

    await page.wait_for_timeout(int(PROFILE_LOAD_WAIT_SEC * 1000))
    result["profile_title"] = await page.title()
    result["profile_metrics"] = await extract_profile_metrics(page)

    notes = await extract_notes_from_dom(page)
    if not notes:
        result["error"] = "no_visible_notes"
        return result

    notes = [note for note in notes if note["note_id"] not in existing_note_ids]
    print(f"    Found {len(notes)} new visible notes", flush=True)

    if not notes:
        return result

    if max_notes > 0:
        notes = notes[:max_notes]

    for index, note in enumerate(notes, 1):
        note_record = dict(note)
        note_record["open_url"] = absolutize_href(note_record["href"])
        try:
            await page.goto(note_record["open_url"], wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(int(NOTE_LOAD_WAIT_SEC * 1000))
            page_title = await page.title()
            note_record["page_title"] = page_title
            if "页面不见了" in page_title:
                note_record["crawl_status"] = "note_404"
                note_record["comments"] = []
            else:
                detail = await extract_note_detail(page)
                note_record.update(detail)
                note_record["crawl_status"] = (
                    "ok" if detail["comment_count_visible"] > 0 else "no_visible_comments"
                )
            result["notes"].append(note_record)
            save_creator_result(result)
            append_posts_csv(result["user_id"], result["name"], note_record)
            append_comments_csv(result["user_id"], note_record)
            print(
                f"    {progress_bar(index, len(notes))} note {index}/{len(notes)} "
                f"{note_record.get('note_id')} | {note_record['crawl_status']} | "
                f"comments={note_record.get('comment_count_visible', 0)}"
                ,
                flush=True,
            )
        except Exception as exc:
            note_record["crawl_status"] = "note_load_failed"
            note_record["error"] = str(exc)
            note_record["comments"] = []
            result["notes"].append(note_record)
            save_creator_result(result)
            append_posts_csv(result["user_id"], result["name"], note_record)
            print(
                f"    {progress_bar(index, len(notes))} note {index}/{len(notes)} "
                f"{note_record.get('note_id')} | ERROR {exc}",
                flush=True,
            )

        await sleep_range(BETWEEN_NOTES_MIN_SEC, BETWEEN_NOTES_MAX_SEC)

    return result


def append_posts_csv(creator_id: str, creator_name: str, note: dict[str, Any]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ensure_posts_csv_schema()
    exists = POSTS_CSV.exists()
    existing_keys = read_existing_keys(POSTS_CSV, ["creator_id", "note_id"])
    key = (creator_id, note.get("note_id", ""))
    if key in existing_keys:
        return
    with POSTS_CSV.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "creator_id",
                "creator_name",
                "note_id",
                "open_url",
                "title",
                "desc",
                "likes",
                "crawl_status",
                "comment_count_visible",
            ],
        )
        if not exists:
            writer.writeheader()
        writer.writerow(
            {
                "creator_id": creator_id,
                "creator_name": creator_name,
                "note_id": note.get("note_id", ""),
                "open_url": note.get("open_url", ""),
                "title": note.get("title", ""),
                "desc": note.get("desc", ""),
                "likes": note.get("likes", ""),
                "crawl_status": note.get("crawl_status", ""),
                "comment_count_visible": note.get("comment_count_visible", 0),
            }
        )


def append_comments_csv(creator_id: str, note: dict[str, Any]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ensure_comments_csv_schema()
    exists = COMMENTS_CSV.exists()
    existing_keys = read_existing_keys(COMMENTS_CSV, ["creator_id", "note_id", "comment_index"])
    with COMMENTS_CSV.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["creator_id", "note_id", "comment_index", "content", "likes"],
        )
        if not exists:
            writer.writeheader()
        for index, comment in enumerate(note.get("comments", []), 1):
            key = (creator_id, note.get("note_id", ""), str(index))
            if key in existing_keys:
                continue
            writer.writerow(
                {
                    "creator_id": creator_id,
                    "note_id": note.get("note_id", ""),
                    "comment_index": index,
                    "content": comment.get("content", ""),
                    "likes": comment.get("likes", ""),
                }
            )


async def run(limit: int = 0, seed: int = 42, max_notes: int = 10) -> None:
    conn = connect_db()
    synced = sync_creators(conn)
    print(f"Synced {synced} creators from CSV")

    pending = get_pending(conn, limit, seed=seed)
    if not pending:
        print("No pending creators.")
        print_status(conn)
        conn.close()
        return

    print(f"Will crawl {len(pending)} creators", flush=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    from playwright.async_api import async_playwright

    async with async_playwright() as playwright:
        try:
            browser = await playwright.chromium.connect_over_cdp(CDP_URL)
        except Exception as exc:
            print(f"ERROR: Cannot connect to Chrome CDP at {CDP_URL}: {exc}")
            conn.close()
            return

        ctx = browser.contexts[0] if browser.contexts else await browser.new_context()
        page = await ctx.new_page()

        for index, (uid, name) in enumerate(pending, 1):
            print(
                f"\n{progress_bar(index - 1, len(pending))} creator {index}/{len(pending)} "
                f"{name or uid}",
                flush=True,
            )
            mark_status(conn, uid, "running")
            try:
                result = await crawl_creator(page, uid, name, max_notes=max_notes)
                out_file = save_creator_result(result)

                if result["error"] and not result["notes"]:
                    mark_status(conn, uid, "error", error=result["error"])
                    print(f"    ERROR: {result['error']}", flush=True)
                else:
                    mark_status(conn, uid, "done", actual_notes=len(result["notes"]))
                    total_comments = sum(len(note.get("comments", [])) for note in result["notes"])
                    print(
                        f"    ✓ Saved {len(result['notes'])} notes / {total_comments} comments to {out_file.name}",
                        flush=True,
                    )
            except Exception as exc:
                mark_status(conn, uid, "error", error=str(exc))
                print(f"    FATAL: {exc}", flush=True)

            await sleep_range(BETWEEN_CREATORS_MIN_SEC, BETWEEN_CREATORS_MAX_SEC)

        await page.close()
        await browser.close()

    conn.close()
    print(f"\n{progress_bar(len(pending), len(pending))} Done.", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Lightweight Xiaohongshu DOM crawler via Chrome CDP")
    parser.add_argument("--status", action="store_true", help="Show progress and exit")
    parser.add_argument("--sample", type=int, default=0, help="Randomly test N pending creators")
    parser.add_argument("--resume", action="store_true", help="Crawl all pending creators")
    parser.add_argument("--reset-errors", action="store_true", help="Reset error rows to pending")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for --sample")
    parser.add_argument("--max-notes", type=int, default=10, help="Limit visible notes crawled per creator (default: 10)")
    args = parser.parse_args()

    conn = connect_db()
    sync_creators(conn)

    if args.status:
        print_status(conn)
        conn.close()
        return

    if args.reset_errors:
        count = conn.execute(
            "UPDATE creator_progress SET status='pending', error=NULL WHERE status='error'"
        ).rowcount
        conn.commit()
        conn.close()
        print(f"Reset {count} error rows to pending")
        return

    conn.close()

    if not args.resume and args.sample <= 0:
        print("Use --sample N to test, or --resume to crawl all pending")
        return

    limit = args.sample if args.sample > 0 else 0
    asyncio.run(run(limit=limit, seed=args.seed, max_notes=args.max_notes))


if __name__ == "__main__":
    main()
