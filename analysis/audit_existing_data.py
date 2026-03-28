#!/usr/bin/env python3

import csv
from collections import Counter, defaultdict
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
CREATOR_IDS_FILE = BASE_DIR / "creator_ids.txt"
NOTES_CSV = DATA_DIR / "notes_export.csv"
COMMENTS_CSV = DATA_DIR / "comments_export.csv"

OUTPUT_COVERED = DATA_DIR / "covered_creator_ids.txt"
OUTPUT_MISSING = DATA_DIR / "missing_creator_ids.txt"
OUTPUT_COVERAGE = DATA_DIR / "creator_coverage_summary.csv"
OUTPUT_NOTE_STATS = DATA_DIR / "creator_note_stats.csv"
OUTPUT_COMMENT_STATS = DATA_DIR / "creator_comment_stats.csv"
OUTPUT_SUMMARY = DATA_DIR / "data_audit_summary.md"


def load_creator_ids():
    with open(CREATOR_IDS_FILE, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def load_note_stats():
    note_count = Counter()
    non_empty_content_count = Counter()
    empty_content_count = Counter()
    creator_names = {}
    note_to_creator = {}

    with open(NOTES_CSV, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            creator_id = (row.get("creator_id") or "").strip()
            if not creator_id:
                continue

            creator_name = (row.get("creator_name") or "").strip()
            if creator_name and creator_id not in creator_names:
                creator_names[creator_id] = creator_name

            note_id = (row.get("note_id") or "").strip()
            if note_id:
                note_to_creator[note_id] = creator_id

            note_count[creator_id] += 1
            content = (row.get("content") or "").strip()
            if content:
                non_empty_content_count[creator_id] += 1
            else:
                empty_content_count[creator_id] += 1

    return {
        "note_count": note_count,
        "non_empty_content_count": non_empty_content_count,
        "empty_content_count": empty_content_count,
        "creator_names": creator_names,
        "note_to_creator": note_to_creator,
    }


def load_comment_stats(note_to_creator):
    comment_count = Counter()
    non_empty_comment_count = Counter()
    comment_note_sets = defaultdict(set)

    with open(COMMENTS_CSV, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            note_id = (row.get("note_id") or "").strip()
            creator_id = note_to_creator.get(note_id)
            if not creator_id:
                continue

            comment_count[creator_id] += 1
            if note_id:
                comment_note_sets[creator_id].add(note_id)

            content = (row.get("content") or "").strip()
            if content:
                non_empty_comment_count[creator_id] += 1

    comment_note_count = Counter(
        {creator_id: len(note_ids) for creator_id, note_ids in comment_note_sets.items()}
    )
    return {
        "comment_count": comment_count,
        "non_empty_comment_count": non_empty_comment_count,
        "comment_note_count": comment_note_count,
    }


def write_lines(path, values):
    with open(path, "w", encoding="utf-8") as f:
        for value in values:
            f.write(f"{value}\n")


def write_csv(path, fieldnames, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_summary(
    expected_creator_ids,
    covered_creator_ids,
    missing_creator_ids,
    note_stats,
    comment_stats,
):
    total_notes = sum(note_stats["note_count"].values())
    total_non_empty_notes = sum(note_stats["non_empty_content_count"].values())
    total_empty_notes = sum(note_stats["empty_content_count"].values())
    total_comments = sum(comment_stats["comment_count"].values())
    total_non_empty_comments = sum(comment_stats["non_empty_comment_count"].values())
    total_commented_notes = sum(comment_stats["comment_note_count"].values())
    creators_with_comments = sum(1 for v in comment_stats["comment_count"].values() if v > 0)

    with open(OUTPUT_SUMMARY, "w", encoding="utf-8") as f:
        f.write("# Data Audit Summary\n\n")
        f.write("## Coverage\n")
        f.write(f"- Theoretical creators: {len(expected_creator_ids)}\n")
        f.write(f"- Covered creators with notes: {len(covered_creator_ids)}\n")
        f.write(f"- Missing creators with no notes: {len(missing_creator_ids)}\n")
        f.write(f"- Creators with comments linked to notes: {creators_with_comments}\n\n")

        f.write("## Notes\n")
        f.write(f"- Total notes: {total_notes}\n")
        f.write(f"- Notes with non-empty content: {total_non_empty_notes}\n")
        f.write(f"- Notes with empty content: {total_empty_notes}\n\n")

        f.write("## Comments\n")
        f.write(f"- Total comments linked to known notes: {total_comments}\n")
        f.write(f"- Comments with non-empty content: {total_non_empty_comments}\n")
        f.write(f"- Notes with at least one linked comment: {total_commented_notes}\n\n")

        f.write("## Assessment\n")
        if total_non_empty_notes > 0:
            f.write("- Post text analysis is feasible on the current dataset.\n")
        else:
            f.write("- Post text analysis is not feasible because no non-empty note content is available.\n")

        if creators_with_comments >= 30:
            f.write("- Comment analysis has some creator spread, but still needs sampling review.\n")
        else:
            f.write("- Comment analysis is highly concentrated and should be treated as a narrow sub-sample.\n")


def main():
    expected_creator_ids = load_creator_ids()
    note_stats = load_note_stats()
    comment_stats = load_comment_stats(note_stats["note_to_creator"])

    covered_creator_ids = sorted(note_stats["note_count"].keys())
    missing_creator_ids = sorted(set(expected_creator_ids) - set(covered_creator_ids))

    write_lines(OUTPUT_COVERED, covered_creator_ids)
    write_lines(OUTPUT_MISSING, missing_creator_ids)

    coverage_rows = []
    note_rows = []
    comment_rows = []
    for creator_id in expected_creator_ids:
        creator_name = note_stats["creator_names"].get(creator_id, "")
        note_count = note_stats["note_count"].get(creator_id, 0)
        non_empty_note_count = note_stats["non_empty_content_count"].get(creator_id, 0)
        empty_note_count = note_stats["empty_content_count"].get(creator_id, 0)
        comment_note_count = comment_stats["comment_note_count"].get(creator_id, 0)
        comment_count = comment_stats["comment_count"].get(creator_id, 0)
        non_empty_comment_count = comment_stats["non_empty_comment_count"].get(creator_id, 0)

        coverage_rows.append(
            {
                "creator_id": creator_id,
                "creator_name": creator_name,
                "has_notes": int(note_count > 0),
                "note_count": note_count,
                "has_comments": int(comment_count > 0),
                "comment_note_count": comment_note_count,
                "comment_count": comment_count,
            }
        )
        note_rows.append(
            {
                "creator_id": creator_id,
                "creator_name": creator_name,
                "note_count": note_count,
                "non_empty_content_count": non_empty_note_count,
                "empty_content_count": empty_note_count,
            }
        )
        comment_rows.append(
            {
                "creator_id": creator_id,
                "creator_name": creator_name,
                "comment_note_count": comment_note_count,
                "comment_count": comment_count,
                "non_empty_comment_count": non_empty_comment_count,
            }
        )

    write_csv(
        OUTPUT_COVERAGE,
        [
            "creator_id",
            "creator_name",
            "has_notes",
            "note_count",
            "has_comments",
            "comment_note_count",
            "comment_count",
        ],
        coverage_rows,
    )
    write_csv(
        OUTPUT_NOTE_STATS,
        [
            "creator_id",
            "creator_name",
            "note_count",
            "non_empty_content_count",
            "empty_content_count",
        ],
        note_rows,
    )
    write_csv(
        OUTPUT_COMMENT_STATS,
        [
            "creator_id",
            "creator_name",
            "comment_note_count",
            "comment_count",
            "non_empty_comment_count",
        ],
        comment_rows,
    )
    write_summary(
        expected_creator_ids,
        covered_creator_ids,
        missing_creator_ids,
        note_stats,
        comment_stats,
    )

    print(f"covered_creator_ids={len(covered_creator_ids)}")
    print(f"missing_creator_ids={len(missing_creator_ids)}")
    print(f"total_notes={sum(note_stats['note_count'].values())}")
    print(f"total_comments={sum(comment_stats['comment_count'].values())}")


if __name__ == "__main__":
    main()
