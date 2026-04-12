# Configuration

This folder stores configuration files used by Kedro and related tooling.

- `conf/base/` — shared, non-sensitive project configuration (safe to commit)
- `conf/local/` — private overrides: real URLs, headers, cookies, tokens (never commit)

The runtime merges `base` then `local`, with `local` taking precedence.

---

## Pipeline Methodology

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║                       PROPERTY SCRAPER — DATA PIPELINE                         ║
╚══════════════════════════════════════════════════════════════════════════════════╝

 ┌─────────────────────── LAYER 0: SMART SKIP LOGIC ─────────────────────────────┐
 │                                                                                 │
 │  NodeExecutionTracker  (data/node_execution_tracker.json)                      │
 │                                                                                 │
 │  Three skip signals evaluated before any scrape node runs:                     │
 │  ① Live-state hash  — probe site (page count / sitemap); skip if unchanged    │
 │  ② Watermark date   — derive max(tx_date) from DB; skip pages older than that  │
 │  ③ ID-set cache     — load scraped IDs from parquet; skip already-seen rows    │
 │                                                                                 │
 │  If probe fails → fail-open (safe run), never silently skip                    │
 └─────────────────────────────────────────────────────────────────────────────────┘

                              │
                              ▼

╔═══════════════════════ LAYER 1: SCRAPE (01_raw/) ══════════════════════════════╗
║                                                                                 ║
║  source_a_res  ──────────────────────────────────────────────────────────────  ║
║  (Playwright / JS-heavy pages)                                                  ║
║  ① Estate listings     →  centaline_estate_lv_1.parquet                        ║
║  ② Estate details      →  centaline_estate_lv_2.parquet                        ║
║     skip if: live-state hash unchanged (estate_skip_days=28)                   ║
║  ③ Transactions        →  centaline_res_trans_lv_0.parquet                     ║
║     incremental: control_date = max(existing.date) + 1                         ║
║     early stop:  break pagination when tx_date < control_date                  ║
║                                                                                 ║
║  source_a_commercial  ───────────────────────────────────────────────────────  ║
║  (REST API, JSON)                                                               ║
║  ① Building listings   →  centanet_oir_buildings.parquet                       ║
║  ② Building details    →  centanet_oir_details.parquet  (02_intermediate)      ║
║     skip if: property_id already in existing parquet (ID-set cache)            ║
║  ③ Transactions        →  centaline_oir_trans_lv_0.parquet                     ║
║     per-district gap detection: if API_count − our_count > max(100, 10%)       ║
║     → force full re-fetch for that district only                               ║
║                                                                                 ║
║  source_b_res  ──────────────────────────────────────────────────────────────  ║
║  (REST API, JSON)                                                               ║
║  ① Estate listings     →  midland_res_estates.parquet                          ║
║  ② Transactions        →  midland_res_trans_lv_0.parquet                       ║
║     skip if: max(tx_date) >= today  (full skip, no HTTP calls)                 ║
║     else:    scrape from max(tx_date) + 1 day                                  ║
║     dedup:   (tx_date, estate_id, price, building_id, unit)                    ║
║                                                                                 ║
║  source_b_commercial  ───────────────────────────────────────────────────────  ║
║  (GraphQL / bilingual zh-hk + en fetch)                                        ║
║  ① Building listings   →  midland_ici_buildings.parquet                        ║
║  ② Building details    →  midland_ici_building_details.parquet (02_int.)       ║
║     skip if: building id already in existing parquet (ID-set cache)            ║
║     incremental save every 5 buildings to survive interruption                  ║
║  ③ Transactions        →  midland_ici_trans.parquet                            ║
║     fetch page 1 first → gap detection (API_total vs existing count)           ║
║     skip if: max_date >= today AND no gap detected                              ║
║     else:    reset start_date to 2000-01-01 when gap > max(100, 1%)            ║
║                                                                                 ║
╚═════════════════════════════════════════════════════════════════════════════════╝

                              │
                              ▼

╔══════════════════════ LAYER 2: JOIN (02_intermediate/) ════════════════════════╗
║                                                                                 ║
║  source_a_res                                                                   ║
║  transactions + estate details  →  source_a_res_trans_lv_1.parquet             ║
║  enrich: fuzzy-match estate name (threshold 85%) when code match fails         ║
║                                                                                 ║
║  source_a_commercial                                                            ║
║  transactions ──┬── exact ID match (propertyId)  ──────── ~60–80% matched     ║
║                 └── fuzzy name match (RapidFuzz, threshold 85%)  +5–15%        ║
║                     validation: name must be substring of building or vice versa║
║  → source_a_commercial_base.parquet                                             ║
║                                                                                 ║
║  source_b_res                                                                   ║
║  transactions + estate listings  →  source_b_res_base.parquet                  ║
║                                                                                 ║
║  source_b_commercial                                                            ║
║  transactions ── exact join (building_id = id)  →  source_b_commercial_base    ║
║                                                                                 ║
║  ┌─────────────────── BUILDING SUPPLEMENT LOOP ─────────────────────────────┐  ║
║  │                                                                           │  ║
║  │  Unmatched transactions → buildings.xlsx (02_intermediate)               │  ║
║  │  ├─ Source A_Res tab    (native rows + unmatched candidates, ranked)      │  ║
║  │  ├─ Source A_OIR tab                                                      │  ║
║  │  ├─ Source B_Res tab                                                      │  ║
║  │  └─ Source B_ICI tab                                                      │  ║
║  │                                                                           │  ║
║  │  User fills:  manual_canonical_name, manual_address,                      │  ║
║  │               manual_completion_year, manual_include (checkbox)           │  ║
║  │                                                                           │  ║
║  │  Re-run: scripts/build_manual_building_review_queue.py                    │  ║
║  │  → extracts approved rows (manual_include=True + name not empty)          │  ║
║  │  → writes back to source parquets with record_source=manual_workbook      │  ║
║  │  → refreshes consolidated_commercial_building_master.parquet              │  ║
║  │    (merges source_a_commercial + source_b_commercial approved buildings)  │  ║
║  └───────────────────────────────────────────────────────────────────────────┘  ║
║                                                                                 ║
╚═════════════════════════════════════════════════════════════════════════════════╝

                              │
                              ▼

╔══════════════════ LAYER 3: CLEANSE & TRANSFORM (data_process) ═════════════════╗
║                                                                                 ║
║  cleanse_source_a_res        — date formats, price/rental merge, age calc      ║
║  cleanse_source_a_commercial — transactionDate → yyyy-mm-dd, unit formatting   ║
║  cleanse_source_b_res        — column alignment, type casting, date parsing    ║
║  cleanse_source_b_commercial — bilingual field merge, floor/flat backfill      ║
║                                                                                 ║
║  select_source_*_columns     — enforce final output schema per source          ║
║     (tx_date / transactionDate renamed to 'date')                              ║
║                                                                                 ║
╚═════════════════════════════════════════════════════════════════════════════════╝

                              │
                              ▼

╔══════════════════════ LAYER 4: CROSS-CHECK & AUDIT ════════════════════════════╗
║                                                                                 ║
║  ① Cross-source overlap detection  (_find_cross_source_overlap)                ║
║     Match key: date (exact) + price (exact) + area (within 3%)                 ║
║     Residential:  source_a_res   ↔ source_b_res    (area vs area)              ║
║     Commercial:   source_a_oir   ↔ source_b_ici    (transactionArea vs area)   ║
║     Output: Overlap_Res / Overlap_Com sheets in Excel (omitted if no overlap)  ║
║                                                                                 ║
║  ② Run-to-run diff  (_generate_run_diff)                                        ║
║     Persists row counts → data/08_reporting/run_stats.json (30-run history)    ║
║     Logs delta table on every pipeline run:                                     ║
║       source_a_res          +145  (prev=48,203 → current=48,348)               ║
║       source_b_res           +89  (prev=35,124 → current=35,213)               ║
║       overlap_res_pairs        0  (no cross-source duplicates detected)         ║
║                                                                                 ║
╚═════════════════════════════════════════════════════════════════════════════════╝

                              │
                              ▼

╔══════════════════════ LAYER 5: EXPORT (03_primary/) ═══════════════════════════╗
║                                                                                 ║
║  merge_and_excel                                                                ║
║  ├─ RE_residential_2020-2023.xlsx                                               ║
║  │   ├─ Source A_Res   (centaline residential)                                 ║
║  │   ├─ Source B_Res   (midland residential)                                   ║
║  │   └─ Overlap_Res    (cross-source duplicates, if any)                       ║
║  ├─ RE_residential_2024-{year}.xlsx   (same sheet structure)                   ║
║  ├─ RE_commercial_2020-2023.xlsx                                                ║
║  │   ├─ Source A_OIR   (centaline office/industrial/retail)                    ║
║  │   ├─ Source B_ICI   (midland ICI)                                           ║
║  │   └─ Overlap_Com    (cross-source duplicates, if any)                       ║
║  └─ RE_commercial_2024-{year}.xlsx    (same sheet structure)                   ║
║                                                                                 ║
║  Parquet snapshots (for downstream analysis):                                   ║
║  centaline_res.parquet  │  centaline_oir.parquet                               ║
║  midland_res.parquet    │  midland_ici.parquet                                  ║
║  consolidated_commercial_building_master.parquet                               ║
║                                                                                 ║
╚═════════════════════════════════════════════════════════════════════════════════╝

                              │
                              ▼

╔══════════════════════ LAYER 6: REPORTING (08_reporting/) ══════════════════════╗
║                                                                                 ║
║  run_stats.json              — row counts + deltas per run (rolling 30 runs)   ║
║  building_match_audit_summary.csv  — fuzzy match quality per source            ║
║  fuzzy_match_stats.csv       — score distribution for fuzzy join               ║
║                                                                                 ║
╚═════════════════════════════════════════════════════════════════════════════════╝
```

---

## Key Parameters

| Parameter | Location | Purpose |
|-----------|----------|---------|
| `node_tracking.live_compare_enabled` | `webscraper.global` | Toggle live-state hash probing |
| `node_tracking.estate_skip_days` | `webscraper.global` | Days before re-scraping estate lists |
| `node_tracking.building_skip_days` | `webscraper.global` | Days before re-scraping building details |
| `source_a_res.transaction_full_rerun` | `webscraper.source_a_res` | Force full transaction re-scrape |
| `source_a_commercial.join.fuzzy_threshold` | `webscraper.source_a_commercial` | RapidFuzz score threshold (default 85) |
| `buildings.fuzzy_matching.residential_threshold` | `buildings` | Estate name match threshold (default 90) |
| `buildings.fuzzy_matching.commercial_threshold` | `buildings` | Building name match threshold (default 85) |
| `building_supplement.enabled` | `webscraper.building_supplement` | Toggle manual review workbook workflow |

---

## Local Configuration

Sensitive values live in `conf/local/` only. Create these files and populate from your credentials store:

**`conf/local/parameters.yml`** — real site URLs, request headers, cookies, API tokens

**`conf/local/catalog.yml`** — override dataset paths if your local filenames differ from `base`

Public aliases used by this repo (safe to reference in code and docs):

| Alias | Domain |
|-------|--------|
| `source_a_res` | Residential transactions — Source A |
| `source_a_commercial` | Office / Industrial / Retail — Source A |
| `source_b_res` | Residential transactions — Source B |
| `source_b_commercial` | Office / Industrial / Retail — Source B |
| `source_c` | Building reference data — Source C |

> See the [Kedro configuration docs](https://docs.kedro.org/en/stable/configuration/configuration_basics.html) for merging and environment override behaviour.
