#!/usr/bin/env python3
"""
Build resumable Qianfan embeddings for creator bios, posts, and comments.

Outputs are stored under data/embeddings_qwen3_8b/ as:
  - <corpus>_meta.csv
  - <corpus>_vectors.f16.bin  (float16 memmap)
  - <corpus>_state.json

Default config reads the local Qianfan settings from:
  /Users/dxk/code/AIEVI/journal-rag/.env.local
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import hashlib
import json
import math
import os
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
TEXT_DIR = DATA_DIR / "text_mining_full" / "snapshot"
OUT_DIR = DATA_DIR / "embeddings_qwen3_8b"
DEFAULT_ENV_PATH = Path("/Users/dxk/code/AIEVI/journal-rag/.env.local")

DEFAULT_BASE_URL = "https://qianfan.baidubce.com/v2"
DEFAULT_MODEL = "qwen3-embedding-8b"
DEFAULT_BATCH_SIZE = 16
DEFAULT_MIN_INTERVAL = 0.05
DEFAULT_MAX_RETRIES = 5
DEFAULT_RETRY_BASE = 10.0
MAX_CHARS = 8000


@dataclass
class EmbedConfig:
    api_key: str
    base_url: str
    model: str
    batch_size: int
    workers: int
    min_interval_secs: float
    max_retries: int
    retry_base_secs: float


class QianfanEmbedder:
    def __init__(self, config: EmbedConfig):
        self.config = config
        self.last_call = 0.0
        self._throttle_lock = threading.Lock()

    def _throttle(self) -> None:
        with self._throttle_lock:
            gap = time.time() - self.last_call
            if gap < self.config.min_interval_secs:
                time.sleep(self.config.min_interval_secs - gap)
            self.last_call = time.time()

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        payload = {
            "model": self.config.model,
            "input": [text[:MAX_CHARS] for text in texts],
        }
        url = f"{self.config.base_url.rstrip('/')}/embeddings"
        for attempt in range(1, self.config.max_retries + 1):
            self._throttle()
            req = urllib.request.Request(
                url,
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.config.api_key}",
                },
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=180) as resp:
                    data = json.loads(resp.read())
                return [item["embedding"] for item in data["data"]]
            except urllib.error.HTTPError as error:
                body = error.read().decode(errors="ignore")[:300]
                if error.code in (429, 500, 502, 503, 504):
                    time.sleep(self.config.retry_base_secs * attempt)
                    continue
                raise RuntimeError(f"HTTP {error.code}: {body}") from error
            except Exception as error:  # pragma: no cover - network variability
                time.sleep(self.config.retry_base_secs * attempt)
                if attempt == self.config.max_retries:
                    raise RuntimeError(f"Embedding failed: {error}") from error
        raise RuntimeError("Embedding failed after retries")


def parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def resolve_config(env_path: Path) -> EmbedConfig:
    local_env = parse_env_file(env_path)
    api_key = os.getenv("QIANFAN_API_KEY", "").strip() or local_env.get("QIANFAN_API_KEY", "")
    if not api_key:
        raise RuntimeError("No QIANFAN_API_KEY found in environment or local env file.")
    base_url = os.getenv("QIANFAN_BASE_URL", "").strip() or local_env.get("QIANFAN_BASE_URL", DEFAULT_BASE_URL)
    model = os.getenv("QIANFAN_EMBED_MODEL", "").strip() or local_env.get("QIANFAN_EMBED_MODEL", DEFAULT_MODEL)
    return EmbedConfig(
        api_key=api_key,
        base_url=base_url,
        model=model,
        batch_size=int(os.getenv("QIANFAN_EMBED_BATCH_SIZE", DEFAULT_BATCH_SIZE)),
        workers=int(os.getenv("QIANFAN_EMBED_WORKERS", "16")),
        min_interval_secs=float(os.getenv("QIANFAN_MIN_INTERVAL_SECS", DEFAULT_MIN_INTERVAL)),
        max_retries=int(os.getenv("QIANFAN_MAX_RETRIES", DEFAULT_MAX_RETRIES)),
        retry_base_secs=float(os.getenv("QIANFAN_RETRY_BASE_SECS", DEFAULT_RETRY_BASE)),
    )


def clean_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return " ".join(str(value).replace("\r", " ").replace("\n", " ").split()).strip()


def prepare_corpus(corpus: str) -> pd.DataFrame:
    if corpus == "bio":
        creators = pd.read_csv(TEXT_DIR / "creator_snapshot.csv")
        df = creators[["creator_id", "creator_name", "bio"]].copy()
        df["text"] = df["bio"].map(clean_text)
        df = df[df["text"].str.len() > 0].copy()
        df["row_id"] = df["creator_id"].map(lambda x: f"bio::{x}")
        df["source_id"] = df["creator_id"]
        df["source_name"] = df["creator_name"]
    elif corpus == "posts":
        posts = pd.read_csv(TEXT_DIR / "posts_snapshot.csv")
        df = posts[["note_id", "creator_id", "creator_name", "title", "desc"]].copy()
        df["text"] = (posts["title"].map(clean_text) + " " + posts["desc"].map(clean_text)).str.strip()
        df = df[df["text"].str.len() > 0].copy()
        df["row_id"] = df["note_id"].map(lambda x: f"post::{x}")
        df["source_id"] = df["note_id"]
        df["source_name"] = df["creator_name"]
    elif corpus == "comments":
        comments = pd.read_csv(TEXT_DIR / "comments_snapshot.csv")
        comment_id_col = "comment_id" if "comment_id" in comments.columns else comments.columns[0]
        df = comments[[comment_id_col, "note_id", "creator_id", "creator_name", "content"]].copy()
        df["text"] = df["content"].map(clean_text)
        df = df[df["text"].str.len() > 0].copy()
        df["row_id"] = df[comment_id_col].map(lambda x: f"comment::{x}")
        df["source_id"] = df[comment_id_col]
        df["source_name"] = df["creator_name"]
    else:  # pragma: no cover
        raise ValueError(f"Unsupported corpus: {corpus}")

    df["text_len"] = df["text"].str.len().astype(int)
    df["text_sha1"] = df["text"].map(lambda x: hashlib.sha1(x.encode("utf-8")).hexdigest())
    return df.reset_index(drop=True)


def corpus_paths(corpus: str) -> dict[str, Path]:
    return {
        "meta": OUT_DIR / f"{corpus}_meta.csv",
        "vectors": OUT_DIR / f"{corpus}_vectors.f16.bin",
        "state": OUT_DIR / f"{corpus}_state.json",
    }


def ensure_meta(corpus: str, df: pd.DataFrame) -> dict[str, Path]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    paths = corpus_paths(corpus)
    if not paths["meta"].exists():
        meta = df[["row_id", "source_id", "source_name", "text_len", "text_sha1"]].copy()
        meta.to_csv(paths["meta"], index=False, quoting=csv.QUOTE_MINIMAL)
    return paths


def infer_dim(embedder: QianfanEmbedder, sample_text: str) -> int:
    emb = embedder.embed_batch([sample_text])[0]
    return len(emb)


def load_or_init_state(paths: dict[str, Path], corpus: str, total_rows: int, dim: int, model: str) -> dict[str, object]:
    if paths["state"].exists():
        state = json.loads(paths["state"].read_text(encoding="utf-8"))
        if state.get("rows_total") != total_rows or state.get("dim") != dim:
            raise RuntimeError(f"Existing state for {corpus} does not match current corpus shape.")
        return state
    state = {
        "corpus": corpus,
        "model": model,
        "rows_total": total_rows,
        "dim": dim,
        "next_index": 0,
        "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    paths["state"].write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    return state


def open_memmap(path: Path, rows: int, dim: int) -> np.memmap:
    mode = "r+" if path.exists() else "w+"
    return np.memmap(path, dtype=np.float16, mode=mode, shape=(rows, dim))


def update_state(paths: dict[str, Path], state: dict[str, object], next_index: int) -> None:
    state["next_index"] = next_index
    state["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    paths["state"].write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def iter_batches(df: pd.DataFrame, start_index: int, batch_size: int) -> Iterable[tuple[int, pd.DataFrame]]:
    for start in range(start_index, len(df), batch_size):
        yield start, df.iloc[start : start + batch_size]


def embed_corpus(corpus: str, embedder: QianfanEmbedder) -> None:
    df = prepare_corpus(corpus)
    paths = ensure_meta(corpus, df)
    dim = infer_dim(embedder, df.iloc[0]["text"])
    state = load_or_init_state(paths, corpus, len(df), dim, embedder.config.model)
    mm = open_memmap(paths["vectors"], len(df), dim)

    start_index = int(state["next_index"])
    if start_index >= len(df):
        print(f"{corpus}: already complete ({len(df)} rows)")
        return

    t0 = time.time()
    completed_starts: set[int] = set()
    next_frontier = start_index

    def flush_frontier() -> int:
        nonlocal next_frontier
        while next_frontier in completed_starts:
            completed_starts.remove(next_frontier)
            span = min(embedder.config.batch_size, len(df) - next_frontier)
            next_frontier += span
        update_state(paths, state, next_frontier)
        return next_frontier

    def run_batch(batch_start: int, batch_df: pd.DataFrame) -> tuple[int, np.ndarray]:
        embeddings = embedder.embed_batch(batch_df["text"].tolist())
        return batch_start, np.asarray(embeddings, dtype=np.float16)

    batch_iter = iter_batches(df, start_index, embedder.config.batch_size)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(embedder.config.workers, 1)) as executor:
        inflight: dict[concurrent.futures.Future, tuple[int, int]] = {}

        def submit_more() -> None:
            while len(inflight) < max(embedder.config.workers, 1):
                try:
                    batch_start, batch_df = next(batch_iter)
                except StopIteration:
                    break
                future = executor.submit(run_batch, batch_start, batch_df.copy())
                inflight[future] = (batch_start, len(batch_df))

        submit_more()
        while inflight:
            done, _ = concurrent.futures.wait(
                tuple(inflight.keys()),
                return_when=concurrent.futures.FIRST_COMPLETED,
            )
            for future in done:
                batch_start, batch_size = inflight.pop(future)
                result_start, vectors = future.result()
                mm[result_start : result_start + batch_size] = vectors
                mm.flush()
                completed_starts.add(result_start)
                frontier = flush_frontier()
                elapsed = time.time() - t0
                speed = frontier / elapsed if elapsed > 0 else 0.0
                print(
                    f"{corpus}: {frontier}/{len(df)} "
                    f"({frontier / len(df):.1%}) "
                    f"inflight={len(inflight)} "
                    f"speed={speed:.1f} rows/s",
                    flush=True,
                )
            submit_more()


def benchmark(embedder: QianfanEmbedder, rows: int) -> None:
    posts = prepare_corpus("posts").head(rows)
    t0 = time.time()
    count = len(posts)
    batches = [(start, batch.copy()) for start, batch in iter_batches(posts, 0, embedder.config.batch_size)]

    def run(batch_df: pd.DataFrame) -> int:
        embedder.embed_batch(batch_df["text"].tolist())
        return len(batch_df)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(embedder.config.workers, 1)) as executor:
        futures = [executor.submit(run, batch) for _, batch in batches]
        for future in concurrent.futures.as_completed(futures):
            future.result()
    elapsed = time.time() - t0
    print(
        json.dumps(
            {
                "benchmark_rows": count,
                "elapsed_secs": round(elapsed, 3),
                "rows_per_sec": round(count / elapsed, 3) if elapsed else None,
                "batches": math.ceil(count / embedder.config.batch_size),
                "workers": embedder.config.workers,
                "model": embedder.config.model,
            },
            ensure_ascii=False,
        )
    )


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build full-text Qianfan embeddings")
    ap.add_argument(
        "--corpora",
        nargs="+",
        default=["bio", "posts", "comments"],
        choices=["bio", "posts", "comments"],
        help="Corpora to embed",
    )
    ap.add_argument("--env-path", default=str(DEFAULT_ENV_PATH))
    ap.add_argument("--benchmark-only", action="store_true")
    ap.add_argument("--benchmark-rows", type=int, default=160)
    ap.add_argument("--workers", type=int)
    ap.add_argument("--batch-size", type=int)
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    config = resolve_config(Path(args.env_path))
    if args.workers is not None:
        config.workers = args.workers
    if args.batch_size is not None:
        config.batch_size = args.batch_size
    print(
        json.dumps(
            {
                "base_url": config.base_url,
                "model": config.model,
                "batch_size": config.batch_size,
                "workers": config.workers,
                "min_interval_secs": config.min_interval_secs,
                "max_retries": config.max_retries,
                "retry_base_secs": config.retry_base_secs,
                "out_dir": str(OUT_DIR),
            },
            ensure_ascii=False,
        )
    )
    embedder = QianfanEmbedder(config)
    if args.benchmark_only:
        benchmark(embedder, rows=args.benchmark_rows)
        return
    for corpus in args.corpora:
        embed_corpus(corpus, embedder)


if __name__ == "__main__":
    main()
