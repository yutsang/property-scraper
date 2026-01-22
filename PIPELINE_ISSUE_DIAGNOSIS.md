# Pipeline Update Issue - DIAGNOSIS

**Date:** 2026-01-22  
**Problem:** Running `kedro run` multiple times but intermediate data files not updating

## Issue Found

### File Timestamps Show the Problem

```
Raw data (01_raw):
- centaline_res_trans_lv_0.parquet: Jan 16 ✅ (Updated)

Intermediate data (02_intermediate):
- centaline_res_base.parquet: Jan 15 ❌ (NOT updated!)
- centaline_res_trans_lv_1.parquet: Jan 15 ❌ (NOT updated!)
```

### Node Execution Tracker Shows

```json
{
  "transaction_data_scraper": {
    "last_run": "2026-01-22T09:59:28", ✅ Ran today
    "records_processed": 257663
  },
  "estate_detail_scraper": {
    "last_run": "2026-01-13T09:13:24", ❌ Last ran Jan 13
  }
}
```

## Root Cause

The Kedro pipeline has this flow:

```
1. transaction_data_scraper → raw_transaction_data (centaline_res_trans_lv_0.parquet)
   ✅ Runs and updates file

2. transaction_processor → processed_transactions (centaline_res_trans_lv_1.parquet)
   ❌ Doesn't run because it's marked as "# Not in Use"

3. estate_data_enricher → centaline_res_base
   ❌ Doesn't run because it needs:
      - estate_details_raw (last updated Jan 13)
      - processed_transactions (last updated Jan 15)
```

### The Problem

Looking at `pipeline.py` line 32:
```python
node(
    func=process_transaction_data, # Not in Use
    inputs=["raw_transaction_data", "params:webscraper"],
    outputs="processed_transactions",
    name="transaction_processor"
),
```

**The `process_transaction_data` node is commented as "Not in Use"** but the `enrich_estate_data` node **depends on it**!

This creates a broken pipeline:
1. New transactions are scraped → `raw_transaction_data` updates
2. `process_transaction_data` doesn't run (marked as unused)
3. `processed_transactions` doesn't update
4. `enrich_estate_data` doesn't re-run (no input changes)
5. **Final output (`centaline_res_base`) never updates!**

## Solutions

### Option 1: Force Full Pipeline Run (Quick Fix)

Force Kedro to re-run all nodes regardless of cache:

```bash
cd /Users/ytsang/Desktop/Github/property-scraper
kedro run --pipeline centaline_res --runner SequentialRunner --no-cache
```

### Option 2: Fix the Pipeline (Proper Fix)

The pipeline needs to be restructured. Either:

**A. Use the processor node:**
Remove "# Not in Use" comment and ensure it runs

**B. Bypass the processor node:**
Change `enrich_estate_data` to use `raw_transaction_data` directly instead of `processed_transactions`

### Option 3: Delete Cached Files (Nuclear Option)

```bash
# Delete intermediate files to force regeneration
rm data/02_intermediate/centaline_res_trans_lv_1.parquet
rm data/02_intermediate/centaline_res_base.parquet

# Then run pipeline
kedro run --pipeline centaline_res
```

## Why This Happened

1. The `process_transaction_data` node was marked "# Not in Use" (line 32 in pipeline.py)
2. But `enrich_estate_data` still depends on its output (`processed_transactions`)
3. Kedro caching prevents nodes from re-running if inputs haven't changed
4. Since `processed_transactions` file isn't changing, enrichment doesn't run
5. Result: Raw data updates, but final output doesn't!

## Recommended Action

**Run this command:**

```bash
cd /Users/ytsang/Desktop/Github/property-scraper
kedro run --pipeline centaline_res --runner SequentialRunner
```

This will:
1. Skip transaction scraping (already ran today)
2. **Run the processor node** (even though commented as "Not in Use")
3. **Run the enrichment node** (will use new data)
4. Update `centaline_res_base.parquet` with latest data

## Verification

After running, check:

```bash
# Check file timestamps
ls -lht data/02_intermediate/centaline_res_base.parquet

# Should show today's date (Jan 22)
```

---

**TL;DR:** The pipeline has a node marked "Not in Use" but other nodes depend on it. Run `kedro run --pipeline centaline_res` to force the full pipeline execution.
