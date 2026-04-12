# Development Log

## Project Overview

This log tracks notable implementation work, known issues, and follow-up items for the Kedro-based property scraper project.

## 2026-03-22

### Completed

- Hardened Source A RES transaction loading so unreadable parquet files no longer silently force a huge historical rescrape.
- Added backup-based recovery helpers to restore the newest matching backup for Source A RES files.
- Replaced fragile `urllib` sitemap and health fetches with retry-aware HTTP utilities.
- Added live-state tracking support to `node_tracker` so nodes can store website-observed freshness signals alongside execution metadata.
- Switched Source A RES estate page-1 change detection from brittle HTTP-only probing to Playwright-based probes.
- Reduced Source A RES transaction browser overhead by reusing a shared Playwright context and parameterizing worker count.
- Split active Source A RES runtime paths into focused modules:
  - `src/property_scraper/pipelines/source_a_res/health.py`
  - `src/property_scraper/pipelines/source_a_res/estates.py`
- Updated README and tracking docs to reflect the current recovery and live-check model.

### Open TODOs

- Extract the Source A RES transaction scraper out of `src/property_scraper/pipelines/source_a_res/nodes.py` into its own focused module.
- Revisit transaction live probes if sitemap `lastmod` alone proves too coarse for skip decisions.
- Add targeted tests for backup restoration, node-tracker live-state comparisons, and estate page-1 probe behavior.

### Known Issues

- The Source A website can still return intermittent probe failures or slow pages, so some fallback behavior remains conservative.
- Existing runtime data files in `data/` may still need cleanup or manual verification if they were produced before the new corruption safeguards were added.
