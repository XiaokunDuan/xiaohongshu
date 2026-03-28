#!/usr/bin/env python3
"""
Spider_XHS fallback adapter.

This adapter intentionally keeps the integration narrow:
- optionally runs an existing Spider_XHS checkout if available
- imports exported JSON note/comment data into the current sqlite schema
- updates note_progress so crawl_api.py can continue on the same pipeline

The external repository layout is not fully stable, so this module looks for
exported JSON files in a configurable output directory after execution.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass
class SpiderXHSImportResult:
    success: bool
    message: str
    imported_notes: int = 0
    imported_comments: int = 0
    output_dir: str = ""


def _candidate_output_dirs(spider_root: Path, creator_id: str):
    yield spider_root / "output" / creator_id
    yield spider_root / "data" / creator_id
    yield spider_root / "downloads" / creator_id
    yield spider_root / "output"
    yield spider_root / "data"
    yield spider_root / "downloads"


def _find_latest_output_dir(spider_root: Path, creator_id: str):
    candidates = []
    for directory in _candidate_output_dirs(spider_root, creator_id):
        if directory.exists() and directory.is_dir():
            candidates.append(directory)
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _find_json_files(output_dir: Path):
    return sorted(output_dir.rglob("*.json"), key=lambda p: p.stat().st_mtime)


def _extract_note_rows(payload, creator_id: str):
    rows = []
    if isinstance(payload, dict):
        note_candidates = []
        for key in ("notes", "items", "data", "note_list", "noteDetailMap"):
            value = payload.get(key)
            if isinstance(value, list):
                note_candidates.extend(value)
            elif isinstance(value, dict):
                note_candidates.extend(value.values())
        if not note_candidates and payload.get("note_id"):
            note_candidates = [payload]
    elif isinstance(payload, list):
        note_candidates = payload
    else:
        note_candidates = []

    for note in note_candidates:
        if not isinstance(note, dict):
            continue
        note_id = str(
            note.get("note_id")
            or note.get("id")
            or note.get("noteId")
            or note.get("source_note_id")
            or ""
        ).strip()
        if not note_id:
            continue

        interact = note.get("interact_info", {}) if isinstance(note.get("interact_info"), dict) else {}
        user_info = note.get("user", {}) if isinstance(note.get("user"), dict) else {}
        tag_list = note.get("tag_list", [])
        tags = []
        if isinstance(tag_list, list):
            for tag in tag_list:
                if isinstance(tag, dict) and tag.get("name"):
                    tags.append(tag["name"])
                elif isinstance(tag, str):
                    tags.append(tag)

        rows.append(
            (
                note_id,
                creator_id,
                str(user_info.get("nickname", note.get("creator_name", ""))),
                str(note.get("title", "")),
                str(note.get("desc", note.get("content", ""))),
                str(note.get("type", note.get("note_type", ""))),
                str(interact.get("liked_count", note.get("liked_count", ""))),
                str(interact.get("collected_count", note.get("collected_count", ""))),
                str(interact.get("comment_count", note.get("comment_count", ""))),
                str(interact.get("share_count", note.get("share_count", ""))),
                json.dumps(tags, ensure_ascii=False),
                str(note.get("ip_location", "")),
                int(note.get("time", 0) or 0),
                int(note.get("last_update_time", 0) or 0),
                time.strftime("%Y-%m-%d %H:%M:%S"),
                str(note.get("xsec_token", note.get("xsecToken", ""))),
            )
        )
    return rows


def _extract_comment_rows(payload):
    rows = []
    if isinstance(payload, dict):
        comment_candidates = []
        for key in ("comments", "data", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                comment_candidates.extend(value)
        if not comment_candidates and payload.get("content") and payload.get("note_id"):
            comment_candidates = [payload]
    elif isinstance(payload, list):
        comment_candidates = payload
    else:
        comment_candidates = []

    for comment in comment_candidates:
        if not isinstance(comment, dict):
            continue
        note_id = str(comment.get("note_id") or comment.get("noteId") or "").strip()
        comment_id = str(comment.get("id") or comment.get("comment_id") or "").strip()
        if not note_id or not comment_id:
            continue
        user_info = comment.get("user_info", {}) if isinstance(comment.get("user_info"), dict) else {}
        rows.append(
            (
                comment_id,
                note_id,
                str(user_info.get("nickname", comment.get("user_name", ""))),
                str(user_info.get("user_id", comment.get("user_id", ""))),
                str(comment.get("content", "")),
                str(comment.get("like_count", "")),
                str(comment.get("sub_comment_count", "")),
                str(comment.get("create_time", "")),
                str(comment.get("ip_location", "")),
                time.strftime("%Y-%m-%d %H:%M:%S"),
            )
        )
    return rows


def _import_json_exports(output_dir: Path, creator_id: str, notes_db_path: Path):
    json_files = _find_json_files(output_dir)
    if not json_files:
        return 0, 0

    imported_notes = 0
    imported_comments = 0
    conn = sqlite3.connect(notes_db_path)
    try:
        for file_path in json_files:
            try:
                payload = json.loads(file_path.read_text(encoding="utf-8"))
            except Exception:
                continue

            note_rows = _extract_note_rows(payload, creator_id)
            if note_rows:
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO notes
                    (note_id, creator_id, creator_name, title, content, note_type,
                     liked_count, collected_count, comment_count, share_count,
                     tags, ip_location, time, last_update_time, crawled_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    [row[:-1] for row in note_rows],
                )
                conn.executemany(
                    """
                    INSERT OR IGNORE INTO note_progress
                    (note_id, creator_id, xsec_token, detail_done, comments_done)
                    VALUES (?, ?, ?, 1, 0)
                    """,
                    [(row[0], creator_id, row[-1]) for row in note_rows],
                )
                imported_notes += len(note_rows)

            comment_rows = _extract_comment_rows(payload)
            if comment_rows:
                conn.executemany(
                    """
                    INSERT OR IGNORE INTO comments
                    (comment_id, note_id, user_name, user_id, content, like_count,
                     sub_comment_count, create_time, ip_location, crawled_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                    """,
                    comment_rows,
                )
                imported_comments += len(comment_rows)

        if imported_comments > 0:
            conn.execute(
                """
                UPDATE note_progress
                SET comments_done=1
                WHERE creator_id=?
                  AND note_id IN (SELECT DISTINCT note_id FROM comments)
                """,
                (creator_id,),
            )
        conn.commit()
    finally:
        conn.close()

    return imported_notes, imported_comments


def _run_spider_xhs(spider_root: Path, creator_id: str, cookie_str: str, timeout: int, proxy_url: str | None = None):
    env = os.environ.copy()
    output_dir = spider_root / "output" / f"{creator_id}_{int(time.time())}"
    output_dir.mkdir(parents=True, exist_ok=True)

    runner = r"""
import json
import sys
from pathlib import Path

sys.path.insert(0, '.')
from apis.xhs_pc_apis import XHS_Apis

creator_id = sys.argv[1]
cookie_str = sys.argv[2]
output_dir = Path(sys.argv[3])
proxy_url = sys.argv[4]
output_dir.mkdir(parents=True, exist_ok=True)

apis = XHS_Apis()
user_url = f'https://www.xiaohongshu.com/user/profile/{creator_id}?xsec_source=pc_search'
proxies = {'http': proxy_url, 'https': proxy_url} if proxy_url else None
success, msg, notes = apis.get_user_all_notes(user_url, cookie_str, proxies)
if not success:
    raise SystemExit(f'user_all_notes_failed:{msg}')

(output_dir / 'notes_index.json').write_text(json.dumps(notes, ensure_ascii=False), encoding='utf-8')

for note in notes:
    note_id = note.get('note_id', '')
    xsec_token = note.get('xsec_token', '')
    if not note_id:
        continue
    note_url = f'https://www.xiaohongshu.com/explore/{note_id}?xsec_token={xsec_token}'
    success, msg, note_info = apis.get_note_info(note_url, cookie_str, proxies)
    if success and note_info:
        (output_dir / f'note_{note_id}.json').write_text(json.dumps(note_info, ensure_ascii=False), encoding='utf-8')
    success, msg, comments = apis.get_note_all_comment(note_url, cookie_str, proxies)
    if success and comments is not None:
        for comment in comments:
            comment['note_id'] = note_id
        (output_dir / f'comments_{note_id}.json').write_text(json.dumps(comments, ensure_ascii=False), encoding='utf-8')
"""

    try:
        completed = subprocess.run(
            ["python", "-c", runner, creator_id, cookie_str, str(output_dir), proxy_url or ""],
            cwd=spider_root,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return False, f"Spider_XHS 执行超时({timeout}s)"
    except Exception as exc:
        return False, f"Spider_XHS 执行失败: {exc}"

    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        detail = stderr or stdout or f"exit {completed.returncode}"
        return False, f"Spider_XHS 退出异常: {detail[:200]}"
    return True, str(output_dir)


def import_creator_from_spider_xhs(creator_id, notes_db_path, progress_db_path, cookie_str="", spider_root=None, timeout=3600, proxy_url=None):
    notes_db_path = Path(notes_db_path)
    progress_db_path = Path(progress_db_path)
    spider_root = Path(spider_root) if spider_root else notes_db_path.parent.parent / "Spider_XHS"

    if not spider_root.exists():
        return SpiderXHSImportResult(False, f"Spider_XHS 路径不存在: {spider_root}")

    run_ok, run_message = _run_spider_xhs(spider_root, creator_id, cookie_str, timeout, proxy_url=proxy_url)
    if not run_ok:
        return SpiderXHSImportResult(False, run_message)

    output_dir = Path(run_message) if run_message else _find_latest_output_dir(spider_root, creator_id)
    if output_dir is None:
        return SpiderXHSImportResult(False, "Spider_XHS 执行后未找到输出目录")

    imported_notes, imported_comments = _import_json_exports(output_dir, creator_id, notes_db_path)
    if imported_notes == 0 and imported_comments == 0:
        return SpiderXHSImportResult(False, "Spider_XHS 输出中未识别到可导入的 JSON 数据", output_dir=str(output_dir))

    progress_conn = sqlite3.connect(progress_db_path)
    try:
        progress_conn.execute(
            """
            UPDATE creator_progress
            SET status=CASE WHEN ? > 0 THEN 'done' ELSE 'pending' END,
                error=NULL,
                actual_notes=COALESCE(actual_notes, 0) + ?,
                finished_at=CASE WHEN ? > 0 THEN ? ELSE finished_at END,
                api_phase=CASE WHEN ? > 0 THEN 'done' ELSE 'comments' END
            WHERE user_id=?
            """,
            (
                imported_comments,
                imported_notes,
                imported_comments,
                time.strftime("%Y-%m-%d %H:%M:%S"),
                imported_comments,
                creator_id,
            ),
        )
        progress_conn.commit()
    finally:
        progress_conn.close()

    return SpiderXHSImportResult(
        True,
        run_message,
        imported_notes=imported_notes,
        imported_comments=imported_comments,
        output_dir=str(output_dir),
    )
