#!/usr/bin/env python3
"""
Backfill creator names from the master creator CSV into the progress DB and
existing dom_click_crawl JSON outputs.
"""

from __future__ import annotations

import csv
import json
import re
import sqlite3
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
MASTER_CSV = BASE_DIR / "data" / "daren_clusters.csv"
PROGRESS_DB = BASE_DIR / "data" / "crawl_progress.db"
DOM_CLICK_DIR = BASE_DIR / "data" / "dom_click_crawl"


def load_creator_map() -> dict[str, dict[str, str]]:
    mapping: dict[str, dict[str, str]] = {}
    with MASTER_CSV.open("r", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            url = (row.get("达人官方地址") or "").strip()
            match = re.search(r"profile/([a-f0-9]{24})", url)
            if not match:
                continue
            mapping[match.group(1)] = {
                "name": (row.get("达人名称") or "").strip(),
                "fans": (row.get("粉丝数") or "").strip(),
                "total_likes": (row.get("赞藏总数") or "").strip(),
            }
    return mapping


def backfill_progress_db(creator_map: dict[str, dict[str, str]]) -> int:
    conn = sqlite3.connect(PROGRESS_DB)
    updated = 0
    for user_id, payload in creator_map.items():
        name = payload.get("name", "")
        if not name:
            continue
        updated += conn.execute(
            "UPDATE creator_progress SET name=? WHERE user_id=? AND COALESCE(name, '')=''",
            (name, user_id),
        ).rowcount
    conn.commit()
    conn.close()
    return updated


def backfill_dom_click_json(creator_map: dict[str, dict[str, str]]) -> int:
    updated = 0
    for path in DOM_CLICK_DIR.glob("*.json"):
        user_id = path.stem
        payload = creator_map.get(user_id, {})
        name = payload.get("name", "")
        fans = payload.get("fans", "")
        total_likes = payload.get("total_likes", "")
        if not any([name, fans, total_likes]):
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        changed = False
        if name and not data.get("name"):
            data["name"] = name
            changed = True
        metrics = data.setdefault("profile_metrics", {})
        if fans and not metrics.get("fans"):
            metrics["fans"] = fans
            changed = True
        if total_likes and not metrics.get("total_likes"):
            metrics["total_likes"] = total_likes
            changed = True
        if not changed:
            continue
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        updated += 1
    return updated


def main() -> None:
    creator_map = load_creator_map()
    db_updates = backfill_progress_db(creator_map)
    json_updates = backfill_dom_click_json(creator_map)
    print(f"creator_map={len(creator_map)} progress_db_updated={db_updates} dom_click_json_updated={json_updates}")


if __name__ == "__main__":
    main()
