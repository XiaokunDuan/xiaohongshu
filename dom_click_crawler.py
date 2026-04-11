#!/usr/bin/env python3
"""
Click-based Xiaohongshu crawler.

Uses the same progress DB as dom_crawler.py, but enters note pages by
clicking visible note cards on the creator profile page instead of calling
page.goto(note_url) for every note.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

from playwright.async_api import async_playwright

import dom_crawler as base


OUTPUT_DIR = Path(__file__).parent / "data" / "dom_click_crawl"
EXPERIMENT_DIR = Path(__file__).parent / "data" / "dom_click_experiments"


async def extract_visible_notes_with_index(page) -> list[dict[str, Any]]:
    return await page.evaluate(
        """
        () => {
          const results = [];
          const seen = new Set();
          const items = Array.from(document.querySelectorAll('section.note-item'));
          for (let i = 0; i < items.length; i++) {
            const item = items[i];
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

            if (!noteId || !openHref || seen.has(noteId)) continue;
            seen.add(noteId);

            results.push({
              dom_index: i,
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


async def click_note_by_index(page, dom_index: int) -> bool:
    locator = page.locator("section.note-item a.cover").nth(dom_index)
    if await locator.count() == 0:
        return False
    await locator.scroll_into_view_if_needed()
    async with page.expect_navigation(wait_until="domcontentloaded", timeout=30000):
        await locator.click()
    return True


async def inspect_note_page(page) -> dict[str, Any]:
    page_title = await page.title()
    status = "unknown"
    detail: dict[str, Any] = {"comments": [], "comment_count_visible": 0}
    if "安全限制" in page_title:
        status = "security_limited"
    elif "页面不见了" in page_title:
        status = "note_404"
    else:
        detail = await base.extract_note_detail(page)
        status = "ok" if detail["comment_count_visible"] > 0 else "no_visible_comments"
    return {
        "page_title": page_title,
        "crawl_status": status,
        **detail,
    }


async def inspect_note_with_optional_wait(
    page,
    *,
    base_wait_sec: float,
    security_grace_sec: float = 0.0,
    poll_ms: int = 500,
) -> dict[str, Any]:
    await page.wait_for_timeout(int(base_wait_sec * 1000))
    immediate = await inspect_note_page(page)
    result = {
        "immediate_status": immediate["crawl_status"],
        "immediate_title": immediate["page_title"],
        "final_status": immediate["crawl_status"],
        "final_title": immediate["page_title"],
        "grace_wait_sec": 0.0,
        "comments": immediate.get("comments", []),
        "comment_count_visible": immediate.get("comment_count_visible", 0),
        "title": immediate.get("title", ""),
        "desc": immediate.get("desc", ""),
    }
    if immediate["crawl_status"] != "security_limited" or security_grace_sec <= 0:
        return result

    waited_ms = 0
    max_ms = int(security_grace_sec * 1000)
    latest = immediate
    while waited_ms < max_ms:
        await page.wait_for_timeout(poll_ms)
        waited_ms += poll_ms
        latest = await inspect_note_page(page)
        if latest["crawl_status"] != "security_limited":
            break

    result.update(
        {
            "final_status": latest["crawl_status"],
            "final_title": latest["page_title"],
            "grace_wait_sec": waited_ms / 1000.0,
            "comments": latest.get("comments", []),
            "comment_count_visible": latest.get("comment_count_visible", 0),
            "title": latest.get("title", ""),
            "desc": latest.get("desc", ""),
        }
    )
    return result


def load_existing_result(uid: str, name: str) -> dict[str, Any]:
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


def save_result(result: dict[str, Any]) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / f"{result['user_id']}.json"
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


async def crawl_creator_click(
    page,
    uid: str,
    name: str,
    max_notes: int,
    security_grace_sec: float = 0.0,
) -> dict[str, Any]:
    result = load_existing_result(uid, name)
    result["error"] = ""
    existing_note_ids = {note.get("note_id", "") for note in result["notes"]}

    try:
        await page.goto(result["profile_url"], wait_until="domcontentloaded", timeout=30000)
    except Exception as exc:
        result["error"] = f"profile_load_failed: {exc}"
        return result

    await page.wait_for_timeout(int(base.PROFILE_LOAD_WAIT_SEC * 1000))
    result["profile_title"] = await page.title()
    result["profile_metrics"] = await base.extract_profile_metrics(page)

    notes = await extract_visible_notes_with_index(page)
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
        note_record["open_url"] = base.absolutize_href(note_record["href"])
        try:
            clicked = await click_note_by_index(page, note["dom_index"])
            if not clicked:
                note_record["crawl_status"] = "click_target_missing"
                note_record["comments"] = []
                note_record["comment_count_visible"] = 0
            else:
                inspected = await inspect_note_with_optional_wait(
                    page,
                    base_wait_sec=base.NOTE_LOAD_WAIT_SEC,
                    security_grace_sec=security_grace_sec,
                )
                note_record["page_title"] = inspected["final_title"]
                note_record["security_initial_status"] = inspected["immediate_status"]
                note_record["security_grace_wait_sec"] = inspected["grace_wait_sec"]
                note_record["crawl_status"] = inspected["final_status"]
                note_record["comments"] = inspected.get("comments", [])
                note_record["comment_count_visible"] = inspected.get("comment_count_visible", 0)
                note_record["title"] = inspected.get("title", "")
                note_record["desc"] = inspected.get("desc", "")
        except Exception as exc:
            note_record["crawl_status"] = "click_failed"
            note_record["error"] = str(exc)
            note_record["comments"] = []
            note_record["comment_count_visible"] = 0

        result["notes"].append(note_record)
        save_result(result)
        base.append_posts_csv(result["user_id"], result["name"], note_record)
        base.append_comments_csv(result["user_id"], note_record)
        print(
            f"    {base.progress_bar(index, len(notes))} note {index}/{len(notes)} "
            f"{note_record.get('note_id')} | {note_record['crawl_status']} | "
            f"comments={note_record.get('comment_count_visible', 0)}",
            flush=True,
        )

        try:
            await page.go_back(wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(int(base.PROFILE_LOAD_WAIT_SEC * 1000))
        except Exception:
            await page.goto(result["profile_url"], wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(int(base.PROFILE_LOAD_WAIT_SEC * 1000))

        await base.sleep_range(base.BETWEEN_NOTES_MIN_SEC, base.BETWEEN_NOTES_MAX_SEC)

    return result


async def compare_security_wait_for_creator(
    page,
    uid: str,
    name: str,
    max_notes: int,
    security_grace_sec: float,
) -> dict[str, Any]:
    profile_url = f"https://www.xiaohongshu.com/user/profile/{uid}?exSource="
    result = {
        "user_id": uid,
        "name": name,
        "profile_url": profile_url,
        "max_notes": max_notes,
        "security_grace_sec": security_grace_sec,
        "notes": [],
        "error": "",
    }
    try:
        await page.goto(profile_url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(int(base.PROFILE_LOAD_WAIT_SEC * 1000))
    except Exception as exc:
        result["error"] = f"profile_load_failed: {exc}"
        return result

    notes = await extract_visible_notes_with_index(page)
    if max_notes > 0:
        notes = notes[:max_notes]

    for index, note in enumerate(notes, 1):
        record = {
            "note_id": note["note_id"],
            "href": note["href"],
            "dom_index": note["dom_index"],
        }
        try:
            clicked = await click_note_by_index(page, note["dom_index"])
            if not clicked:
                record["direct_status"] = "click_target_missing"
                record["wait_status"] = "click_target_missing"
                record["wait_recovered"] = False
                record["direct_comments"] = 0
                record["wait_comments"] = 0
            else:
                inspected = await inspect_note_with_optional_wait(
                    page,
                    base_wait_sec=base.NOTE_LOAD_WAIT_SEC,
                    security_grace_sec=security_grace_sec,
                )
                record["direct_status"] = inspected["immediate_status"]
                record["wait_status"] = inspected["final_status"]
                record["wait_recovered"] = (
                    inspected["immediate_status"] == "security_limited"
                    and inspected["final_status"] != "security_limited"
                )
                record["direct_title"] = inspected["immediate_title"]
                record["wait_title"] = inspected["final_title"]
                record["grace_wait_sec"] = inspected["grace_wait_sec"]
                record["direct_comments"] = 0 if inspected["immediate_status"] != "ok" else inspected["comment_count_visible"]
                record["wait_comments"] = inspected["comment_count_visible"]
        except Exception as exc:
            record["direct_status"] = "click_failed"
            record["wait_status"] = "click_failed"
            record["wait_recovered"] = False
            record["error"] = str(exc)
            record["direct_comments"] = 0
            record["wait_comments"] = 0

        result["notes"].append(record)
        print(
            f"    {base.progress_bar(index, len(notes))} compare note {index}/{len(notes)} "
            f"{record['note_id']} | direct={record['direct_status']} | wait={record['wait_status']}",
            flush=True,
        )
        try:
            await page.go_back(wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(int(base.PROFILE_LOAD_WAIT_SEC * 1000))
        except Exception:
            await page.goto(profile_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(int(base.PROFILE_LOAD_WAIT_SEC * 1000))
        await base.sleep_range(base.BETWEEN_NOTES_MIN_SEC, base.BETWEEN_NOTES_MAX_SEC)

    return result


async def run(limit: int = 0, seed: int = 42, max_notes: int = 5, security_grace_sec: float = 0.0) -> None:
    conn = base.connect_db()
    synced = base.sync_creators(conn)
    print(f"Synced {synced} creators from CSV")

    pending = base.get_pending(conn, limit, seed=seed)
    if not pending:
        print("No pending creators.")
        base.print_status(conn)
        conn.close()
        return

    print(f"Will crawl {len(pending)} creators", flush=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as playwright:
        try:
            browser = await playwright.chromium.connect_over_cdp(base.CDP_URL)
        except Exception as exc:
            print(f"ERROR: Cannot connect to Chrome CDP at {base.CDP_URL}: {exc}")
            conn.close()
            return

        ctx = browser.contexts[0] if browser.contexts else await browser.new_context()
        page = await ctx.new_page()

        for index, (uid, name) in enumerate(pending, 1):
            print(
                f"\n{base.progress_bar(index - 1, len(pending))} creator {index}/{len(pending)} "
                f"{name or uid}",
                flush=True,
            )
            base.mark_status(conn, uid, "running")
            try:
                result = await crawl_creator_click(
                    page,
                    uid,
                    name,
                    max_notes=max_notes,
                    security_grace_sec=security_grace_sec,
                )
                out_file = save_result(result)
                if result["error"] and not result["notes"]:
                    base.mark_status(conn, uid, "error", error=result["error"])
                    print(f"    ERROR: {result['error']}", flush=True)
                else:
                    base.mark_status(conn, uid, "done", actual_notes=len(result["notes"]))
                    total_comments = sum(len(note.get("comments", [])) for note in result["notes"])
                    print(
                        f"    ✓ Saved {len(result['notes'])} notes / {total_comments} comments to {out_file.name}",
                        flush=True,
                    )
            except Exception as exc:
                base.mark_status(conn, uid, "error", error=str(exc))
                print(f"    FATAL: {exc}", flush=True)

            await base.sleep_range(base.BETWEEN_CREATORS_MIN_SEC, base.BETWEEN_CREATORS_MAX_SEC)

        await page.close()
        await browser.close()

    conn.close()
    print(f"\n{base.progress_bar(len(pending), len(pending))} Done.", flush=True)


async def run_compare_experiment(
    limit: int = 1,
    seed: int = 42,
    max_notes: int = 5,
    security_grace_sec: float = 4.0,
) -> Path:
    conn = base.connect_db()
    synced = base.sync_creators(conn)
    print(f"Synced {synced} creators from CSV")
    pending = base.get_pending(conn, limit, seed=seed)
    if not pending:
        raise RuntimeError("No pending creators available for experiment.")

    EXPERIMENT_DIR.mkdir(parents=True, exist_ok=True)
    report_rows: list[dict[str, Any]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.connect_over_cdp(base.CDP_URL)
        ctx = browser.contexts[0] if browser.contexts else await browser.new_context()
        page = await ctx.new_page()
        for index, (uid, name) in enumerate(pending, 1):
            print(
                f"\n{base.progress_bar(index - 1, len(pending))} experiment creator {index}/{len(pending)} {name or uid}",
                flush=True,
            )
            creator_result = await compare_security_wait_for_creator(
                page,
                uid,
                name,
                max_notes=max_notes,
                security_grace_sec=security_grace_sec,
            )
            for note in creator_result["notes"]:
                report_rows.append(
                    {
                        "creator_id": uid,
                        "creator_name": name,
                        **note,
                    }
                )
            await base.sleep_range(base.BETWEEN_CREATORS_MIN_SEC, base.BETWEEN_CREATORS_MAX_SEC)
        await page.close()
        await browser.close()

    conn.close()
    ts = asyncio.get_running_loop().time()
    out_path = EXPERIMENT_DIR / f"compare_wait_seed{seed}_n{limit}_notes{max_notes}_{int(ts)}.json"
    summary = {
        "limit": limit,
        "seed": seed,
        "max_notes": max_notes,
        "security_grace_sec": security_grace_sec,
        "rows": report_rows,
    }
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    direct_security = sum(1 for row in report_rows if row.get("direct_status") == "security_limited")
    wait_security = sum(1 for row in report_rows if row.get("wait_status") == "security_limited")
    recovered = sum(1 for row in report_rows if row.get("wait_recovered"))
    print(
        f"\nExperiment summary: notes={len(report_rows)} | direct_security={direct_security} | "
        f"wait_security={wait_security} | recovered={recovered}",
        flush=True,
    )
    print(f"Saved experiment to {out_path}", flush=True)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Click-based Xiaohongshu DOM crawler")
    parser.add_argument("--uid", help="Single creator user_id")
    parser.add_argument("--status", action="store_true", help="Show progress and exit")
    parser.add_argument("--sample", type=int, default=0, help="Randomly test N pending creators")
    parser.add_argument("--resume", action="store_true", help="Crawl all pending creators")
    parser.add_argument("--reset-errors", action="store_true", help="Reset error rows to pending")
    parser.add_argument("--recover-running", action="store_true", help="Reset stale running rows to pending")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for --sample")
    parser.add_argument("--max-notes", type=int, default=5, help="Limit visible notes crawled per creator (default: 5)")
    parser.add_argument("--security-grace-sec", type=float, default=0.0, help="After landing on a security page, wait this many extra seconds for auto-return before marking security_limited")
    parser.add_argument("--compare-wait", action="store_true", help="Run a non-persisting experiment comparing direct status vs post-click wait")
    args = parser.parse_args()

    conn = base.connect_db()
    base.sync_creators(conn)

    if args.status:
        base.print_status(conn)
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

    if args.recover_running:
        count = conn.execute(
            "UPDATE creator_progress SET status='pending', error=NULL WHERE status='running'"
        ).rowcount
        conn.commit()
        conn.close()
        print(f"Recovered {count} running rows to pending")
        return

    conn.close()

    if args.uid:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        async def _single():
            async with async_playwright() as playwright:
                browser = await playwright.chromium.connect_over_cdp(base.CDP_URL)
                ctx = browser.contexts[0] if browser.contexts else await browser.new_context()
                page = await ctx.new_page()
                try:
                    return await crawl_creator_click(
                        page,
                        args.uid,
                        args.uid,
                        args.max_notes,
                        security_grace_sec=args.security_grace_sec,
                    )
                finally:
                    await page.close()
                    await browser.close()
        result = asyncio.run(_single())
        out = save_result(result)
        print(f"Saved {out}", flush=True)
        return

    if not args.resume and args.sample <= 0:
        print("Use --uid USER_ID, --sample N, or --resume")
        return

    limit = args.sample if args.sample > 0 else 0
    if args.compare_wait:
        compare_limit = limit if limit > 0 else 1
        asyncio.run(
            run_compare_experiment(
                limit=compare_limit,
                seed=args.seed,
                max_notes=args.max_notes,
                security_grace_sec=max(args.security_grace_sec, 4.0),
            )
        )
    else:
        asyncio.run(
            run(
                limit=limit,
                seed=args.seed,
                max_notes=args.max_notes,
                security_grace_sec=args.security_grace_sec,
            )
        )


if __name__ == "__main__":
    main()
