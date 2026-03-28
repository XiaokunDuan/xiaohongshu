# xiaohongshu Workspace

This workspace currently mixes three different concerns:

- collected datasets and analysis outputs
- third-party crawler repositories
- local crawler experiments and orchestration scripts

The cleanup goal is to keep the data/results intact while isolating old crawler code before a rewrite.

## Current layout

- `data/`: collected sqlite/csv outputs and crawl state
- `analysis/`: analysis scripts and intermediate data
- `reports/`: exported charts and reports
- `reports/audit/`: audit summaries and derived coverage lists
- `MediaCrawler/`: third-party upstream crawler repository
- `Spider_XHS/`: third-party upstream crawler repository
- `batch_crawler.py`: simplified local entrypoint that syncs creator IDs and launches `MediaCrawler`
- `archive/scraper_experiments/`: archived local experiments that previously lived under `scraper/`
- `archive/batch_crawler_legacy.py`: previous batch wrapper kept only for reference

## Archived code

`archive/scraper_experiments/` contains overlapping local implementations:

- `crawl_api.py`: signed API crawler with `Spider_XHS` fallback
- `crawl_hybrid.py`: browser interception + HTML parsing crawler
- `crawl_html.py`: Playwright HTML parser
- `crawl_comments.py`: DOM-based comment crawler
- `crawl_adspower.py`, `test_*.py`: environment-specific or experimental utilities

These files are preserved for reference, but they are no longer treated as the main project entrypoints.

## Cleanup policy

- Keep `data/`, `analysis/`, and `reports/` unchanged
- Keep third-party repositories intact for comparison/reference
- Move duplicated local crawler experiments out of the main path
- Rewrite the next crawler implementation in a single dedicated location instead of adding another parallel script

## Directory intent

- `data/` should contain crawl inputs, raw exports, sqlite databases, and runtime state
- `reports/` should contain human-facing outputs, figures, and audit summaries
- `data/xhs_cookie.txt.example` is a safe template; the real `data/xhs_cookie.txt` stays local only

## Next step

## Current crawl path

The canonical crawl path is now:

1. `data/daren_clusters.csv` as the master creator list
2. `data/crawl_progress.db` as the only local progress state
3. `batch_crawler.py` as the thin launcher
4. `MediaCrawler/` as the only active crawler engine

The old local crawler experiments and the previous batch wrapper are archived only for reference.
