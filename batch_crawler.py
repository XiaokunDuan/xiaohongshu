#!/usr/bin/env python3
"""
MediaCrawler-only runner for Xiaohongshu creator crawling.

This script intentionally stays thin:
- sync creator IDs from the master CSV into crawl_progress.db
- show crawl status
- reset failed or stale rows
- launch MediaCrawler once and let MediaCrawler manage per-creator progress

Usage:
    python batch_crawler.py
    python batch_crawler.py --status
    python batch_crawler.py --reset-errors
    python batch_crawler.py --rescue-running
"""

from __future__ import annotations

import argparse
import csv
import re
import sqlite3
import subprocess
import sys
from pathlib import Path


BASE_DIR = Path(__file__).parent
MEDIACRAWLER_DIR = BASE_DIR / "MediaCrawler"
DAREN_CSV = BASE_DIR / "data" / "daren_clusters.csv"
PROGRESS_DB = BASE_DIR / "data" / "crawl_progress.db"


def connect_progress_db() -> sqlite3.Connection:
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


def extract_creators_from_csv() -> list[tuple[str, str, int]]:
    if not DAREN_CSV.exists():
        raise FileNotFoundError(f"Master creator CSV not found: {DAREN_CSV}")

    rows: list[tuple[str, str, int]] = []
    with DAREN_CSV.open("r", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            url = (row.get("达人官方地址") or "").strip()
            match = re.search(r"profile/([a-f0-9]{24})", url)
            if not match:
                continue

            user_id = match.group(1)
            name = (row.get("达人名称") or "").strip()
            try:
                expected_notes = int(float(row.get("笔记总数") or 0))
            except (TypeError, ValueError):
                expected_notes = 0
            rows.append((user_id, name, expected_notes))
    return rows


def sync_creators(conn: sqlite3.Connection) -> int:
    creators = extract_creators_from_csv()
    conn.executemany(
        """
        INSERT OR IGNORE INTO creator_progress (user_id, name, expected_notes)
        VALUES (?, ?, ?)
        """,
        creators,
    )
    conn.commit()
    return len(creators)


def rescue_running(conn: sqlite3.Connection) -> int:
    count = conn.execute(
        "UPDATE creator_progress SET status='pending' WHERE status='running'"
    ).rowcount
    conn.commit()
    return count


def reset_errors(conn: sqlite3.Connection) -> int:
    count = conn.execute(
        "UPDATE creator_progress SET status='pending', error=NULL WHERE status='error'"
    ).rowcount
    conn.commit()
    return count


def print_status(conn: sqlite3.Connection) -> None:
    total = conn.execute("SELECT COUNT(*) FROM creator_progress").fetchone()[0]
    total_notes = conn.execute(
        "SELECT COALESCE(SUM(expected_notes), 0) FROM creator_progress"
    ).fetchone()[0]

    print(f"Creators: {total}")
    print(f"Expected notes: {total_notes:,}")

    for status, count, notes in conn.execute(
        """
        SELECT status, COUNT(*), COALESCE(SUM(expected_notes), 0)
        FROM creator_progress
        GROUP BY status
        ORDER BY status
        """
    ):
        pct = (count / total * 100) if total else 0
        print(f"{status:>8}: {count:5d} creators | {pct:5.1f}% | {int(notes):8,d} notes")

    recent_done = conn.execute(
        """
        SELECT name, actual_notes, finished_at
        FROM creator_progress
        WHERE status='done'
        ORDER BY finished_at DESC
        LIMIT 5
        """
    ).fetchall()
    if recent_done:
        print("\nRecent done:")
        for name, actual_notes, finished_at in recent_done:
            print(f"  {name or '<unknown>'} | {actual_notes} notes | {finished_at}")

    recent_error = conn.execute(
        """
        SELECT name, error, finished_at
        FROM creator_progress
        WHERE status='error'
        ORDER BY finished_at DESC
        LIMIT 5
        """
    ).fetchall()
    if recent_error:
        print("\nRecent errors:")
        for name, error, finished_at in recent_error:
            print(f"  {name or '<unknown>'} | {finished_at} | {error}")


def run_mediacrawler() -> int:
    if not MEDIACRAWLER_DIR.exists():
        raise FileNotFoundError(f"MediaCrawler directory not found: {MEDIACRAWLER_DIR}")

    result = subprocess.run(
        ["uv", "run", "python", "main.py"],
        cwd=MEDIACRAWLER_DIR,
        check=False,
    )
    return result.returncode


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Thin MediaCrawler runner for Xiaohongshu creator crawling"
    )
    parser.add_argument("--status", action="store_true", help="Show crawl status and exit")
    parser.add_argument("--reset-errors", action="store_true", help="Move error rows back to pending")
    parser.add_argument(
        "--rescue-running",
        action="store_true",
        help="Move stale running rows back to pending before exiting",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    conn = connect_progress_db()

    try:
        synced = sync_creators(conn)
        print(f"Synced creator rows from CSV: {synced}")

        if args.reset_errors:
            print(f"Reset error rows: {reset_errors(conn)}")
            return 0

        if args.rescue_running:
            print(f"Rescued running rows: {rescue_running(conn)}")
            return 0

        if args.status:
            print_status(conn)
            return 0

        rescued = rescue_running(conn)
        if rescued:
            print(f"Rescued stale running rows before launch: {rescued}")

        pending = conn.execute(
            "SELECT COUNT(*) FROM creator_progress WHERE status='pending'"
        ).fetchone()[0]
        if pending == 0:
            print("No pending creators.")
            print_status(conn)
            return 0

        print(f"Pending creators: {pending}")
        print("Launching MediaCrawler once. Per-creator progress will be updated inside MediaCrawler.")
    finally:
        conn.close()

    return_code = run_mediacrawler()
    print(f"MediaCrawler exit code: {return_code}")
    return return_code


if __name__ == "__main__":
    sys.exit(main())
