#!/usr/bin/env python3
"""
Analyze semantic alignment across bio, post, and comment embeddings.

Outputs:
- data/embeddings_qwen3_8b/analysis/creator_similarity_summary.csv
- data/embeddings_qwen3_8b/analysis/post_similarity_summary.csv
- data/embeddings_qwen3_8b/analysis/similarity_overview.json
- reports/text_mining_full/embedding_similarity_report.md
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent
EMBED_DIR = BASE_DIR / "data" / "embeddings_qwen3_8b"
ANALYSIS_DIR = EMBED_DIR / "analysis"
REPORT_PATH = BASE_DIR / "reports" / "text_mining_full" / "embedding_similarity_report.md"
SNAPSHOT_DIR = BASE_DIR / "data" / "text_mining_full" / "snapshot"
TOPIC_DIR = BASE_DIR / "data" / "text_mining_full" / "topics"


def load_state(name: str) -> dict:
    return json.loads((EMBED_DIR / f"{name}_state.json").read_text())


def load_vectors(name: str) -> np.memmap:
    state = load_state(name)
    return np.memmap(
        EMBED_DIR / f"{name}_vectors.f16.bin",
        dtype=np.float16,
        mode="r",
        shape=(state["rows_total"], state["dim"]),
    )


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    a = a.astype(np.float32, copy=False)
    b = b.astype(np.float32, copy=False)
    denom = float(np.linalg.norm(a) * np.linalg.norm(b))
    if denom == 0:
        return 0.0
    return float(np.dot(a, b) / denom)


def series_stats(series: pd.Series) -> dict[str, float]:
    return {
        "count": int(series.count()),
        "mean": float(series.mean()),
        "std": float(series.std()),
        "min": float(series.min()),
        "p10": float(series.quantile(0.10)),
        "p25": float(series.quantile(0.25)),
        "p50": float(series.quantile(0.50)),
        "p75": float(series.quantile(0.75)),
        "p90": float(series.quantile(0.90)),
        "max": float(series.max()),
    }


def main() -> None:
    ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

    bio_meta = pd.read_csv(EMBED_DIR / "bio_meta.csv")
    posts_meta = pd.read_csv(EMBED_DIR / "posts_meta.csv")
    comments_meta = pd.read_csv(EMBED_DIR / "comments_meta.csv")

    creators = pd.read_csv(SNAPSHOT_DIR / "creator_snapshot.csv")[["creator_id", "creator_name", "bio", "cluster_k3"]]
    posts = pd.read_csv(TOPIC_DIR / "posts_with_topics.csv")
    topics = pd.read_csv(TOPIC_DIR / "post_topics.csv")[["topic_id", "topic_name"]]
    posts = posts.merge(topics, on="topic_id", how="left")
    comments = pd.read_csv(SNAPSHOT_DIR / "comments_snapshot.csv")

    bio_meta["creator_id"] = bio_meta["source_id"].astype(str)
    posts_meta["note_id"] = posts_meta["source_id"].astype(str)
    comments_meta["comment_id"] = comments_meta["source_id"].astype(str)

    posts["note_id"] = posts["note_id"].astype(str)
    posts["creator_id"] = posts["creator_id"].astype(str)
    comments = comments.rename(columns={comments.columns[0]: "comment_id"})
    comments["comment_id"] = comments["comment_id"].astype(str)
    comments["note_id"] = comments["note_id"].astype(str)
    comments["creator_id"] = comments["creator_id"].astype(str)

    posts_meta = posts_meta.merge(
        posts[["note_id", "creator_id", "creator_name", "title", "topic_name"]],
        on="note_id",
        how="left",
    )
    comments_meta = comments_meta.merge(
        comments[["comment_id", "note_id", "creator_id", "creator_name", "content"]],
        on="comment_id",
        how="left",
    )

    bio_vec = load_vectors("bio")
    posts_vec = load_vectors("posts")
    comments_vec = load_vectors("comments")

    bio_index_by_creator = dict(zip(bio_meta["creator_id"], bio_meta.index))
    post_index_by_note = dict(zip(posts_meta["note_id"], posts_meta.index))

    creator_rows = []
    for creator_id, group_idx in posts_meta.groupby("creator_id").indices.items():
        bio_idx = bio_index_by_creator.get(creator_id)
        if bio_idx is None:
            continue
        creator_posts = posts_vec[list(group_idx)].astype(np.float32)
        post_centroid = creator_posts.mean(axis=0)
        bio_post_sim = cosine_similarity(bio_vec[bio_idx], post_centroid)

        creator_comment_idxs = comments_meta.index[comments_meta["creator_id"] == creator_id].tolist()
        bio_comment_sim = np.nan
        post_comment_sim = np.nan
        if creator_comment_idxs:
            creator_comments = comments_vec[creator_comment_idxs].astype(np.float32)
            comment_centroid = creator_comments.mean(axis=0)
            bio_comment_sim = cosine_similarity(bio_vec[bio_idx], comment_centroid)
            post_comment_sim = cosine_similarity(post_centroid, comment_centroid)

        creator_rows.append(
            {
                "creator_id": creator_id,
                "bio_post_sim": bio_post_sim,
                "bio_comment_sim": bio_comment_sim,
                "post_comment_sim": post_comment_sim,
                "post_n": len(group_idx),
                "comment_n": len(creator_comment_idxs),
            }
        )

    creator_df = pd.DataFrame(creator_rows).merge(creators, on="creator_id", how="left")
    creator_df.to_csv(ANALYSIS_DIR / "creator_similarity_summary.csv", index=False)

    post_rows = []
    comment_groups = comments_meta.groupby("note_id").indices
    for note_id, post_idx in post_index_by_note.items():
        comment_idxs = comment_groups.get(note_id)
        if comment_idxs is None:
            continue
        creator_id = posts_meta.iloc[post_idx]["creator_id"]
        bio_idx = bio_index_by_creator.get(creator_id)
        post_vec_row = posts_vec[post_idx]
        comment_centroid = comments_vec[list(comment_idxs)].astype(np.float32).mean(axis=0)
        post_comment_sim = cosine_similarity(post_vec_row, comment_centroid)
        bio_post_sim = np.nan
        bio_comment_sim = np.nan
        if bio_idx is not None:
            bio_post_sim = cosine_similarity(bio_vec[bio_idx], post_vec_row)
            bio_comment_sim = cosine_similarity(bio_vec[bio_idx], comment_centroid)
        post_rows.append(
            {
                "note_id": note_id,
                "creator_id": creator_id,
                "creator_name": posts_meta.iloc[post_idx]["creator_name"],
                "topic_name": posts_meta.iloc[post_idx]["topic_name"],
                "title": posts_meta.iloc[post_idx]["title"],
                "comment_n": len(comment_idxs),
                "bio_post_sim": bio_post_sim,
                "post_comment_sim": post_comment_sim,
                "bio_comment_sim": bio_comment_sim,
            }
        )

    post_df = pd.DataFrame(post_rows)
    post_df.to_csv(ANALYSIS_DIR / "post_similarity_summary.csv", index=False)

    overview = {
        "creator_level": {
            "bio_post": series_stats(creator_df["bio_post_sim"].dropna()),
            "bio_comment": series_stats(creator_df["bio_comment_sim"].dropna()),
            "post_comment": series_stats(creator_df["post_comment_sim"].dropna()),
        },
        "post_level": {
            "bio_post": series_stats(post_df["bio_post_sim"].dropna()),
            "bio_comment": series_stats(post_df["bio_comment_sim"].dropna()),
            "post_comment": series_stats(post_df["post_comment_sim"].dropna()),
        },
    }
    (ANALYSIS_DIR / "similarity_overview.json").write_text(
        json.dumps(overview, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    top_creator_aligned = creator_df.sort_values("bio_post_sim", ascending=False).head(8)
    low_creator_aligned = creator_df.sort_values("bio_post_sim", ascending=True).head(8)
    top_post_closure = post_df.sort_values("post_comment_sim", ascending=False).head(10)

    lines = [
        "# Embedding Similarity Report",
        "",
        "## Creator-Level Summary",
        f"- Bio -> Posts mean similarity: {overview['creator_level']['bio_post']['mean']:.3f}",
        f"- Bio -> Comments mean similarity: {overview['creator_level']['bio_comment']['mean']:.3f}",
        f"- Posts -> Comments mean similarity: {overview['creator_level']['post_comment']['mean']:.3f}",
        "",
        "## Post-Level Summary",
        f"- Bio -> Single Post mean similarity: {overview['post_level']['bio_post']['mean']:.3f}",
        f"- Bio -> Comment Cluster mean similarity: {overview['post_level']['bio_comment']['mean']:.3f}",
        f"- Post -> Comment Cluster mean similarity: {overview['post_level']['post_comment']['mean']:.3f}",
        "",
        "## Top Creator Alignment",
    ]
    for _, row in top_creator_aligned.iterrows():
        lines.append(f"- {row['creator_name']} | sim={row['bio_post_sim']:.3f} | posts={int(row['post_n'])} | {str(row['bio'])[:80]}")
    lines.extend(["", "## Low Creator Alignment"])
    for _, row in low_creator_aligned.iterrows():
        lines.append(f"- {row['creator_name']} | sim={row['bio_post_sim']:.3f} | posts={int(row['post_n'])} | {str(row['bio'])[:80]}")
    lines.extend(["", "## Top Post-Comment Closure"])
    for _, row in top_post_closure.iterrows():
        lines.append(
            f"- {row['creator_name']} | {str(row['topic_name'])} | sim={row['post_comment_sim']:.3f} | comments={int(row['comment_n'])} | {str(row['title'])[:60]}"
        )
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(ANALYSIS_DIR)
    print(REPORT_PATH)


if __name__ == "__main__":
    main()
