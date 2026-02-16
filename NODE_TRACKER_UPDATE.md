# Node Tracker Update Summary

## Issue Resolved
The old node tracker was using a "days since last run" logic that would skip nodes based on arbitrary time periods (7 days for estates, 28 days for buildings, etc.). This has been **completely removed**.

## What Changed

### Before (Old Logic)
```python
# Old behavior - REMOVED
if days_since_last_run < building_skip:
    logger.info(f"Node 'scrape_midland_buildings' (building) last run 21 days ago (<28) - skipping")
    return False  # Skip the node!
```

This would show messages like:
```
Node 'scrape_midland_buildings' (building) last run 21 days ago (<28) - skipping
```

### After (New Logic - Data-Driven)

**For Transaction Nodes:**
```python
# Check actual data freshness
if max_date >= current_date:
    logger.info(f"✅ Node 'scrape_transactions': Data up-to-date (max: {max_date}) - skipping")
    return False  # Skip only if data is actually up-to-date
else:
    logger.info(f"📊 Node 'scrape_transactions': Data {days_behind} days behind - will scrape")
    return True  # Scrape because data is outdated
```

**For All Other Nodes (Estate, Building, Default):**
```python
# ALWAYS RUN - no more arbitrary skip periods
return True  # Always execute, no more "days since last run" checks
```

## Key Improvements

### 1. Data-Driven Decisions
- Transaction nodes now check **actual data** (max date in dataset vs current date)
- No more arbitrary "skip for X days" logic
- Scraping happens when data is actually outdated

### 2. Simplified Logic
- Estate and building nodes: **Always run** (no more skipping)
- Transaction nodes: **Only skip if data is current** (based on actual dates in data)

### 3. Better Logging
```
Old: Node 'scrape_midland_buildings' (building) last run 21 days ago (<28) - skipping
New: 📊 Node 'scrape_transactions': Data 3 days behind (max: 2026-01-31) - will scrape
```

## Configuration Changes

The following parameters in `parameters.yml` are now **unused** and can be removed:

```yaml
# DEPRECATED - No longer used
webscraper:
  tracking:
    estate_skip_days: 7        # ❌ No longer used
    building_skip_days: 28     # ❌ No longer used
    default_skip_days: 1       # ❌ No longer used
```

## Node Execution Behavior

### Transaction Nodes (4 nodes)
- `fetch_midland_transactions`
- `transaction_data_scraper` (Centaline)
- `midland_ici_transaction_scraper`
- `scrape_transaction` (Centaline OIR)

**Behavior:** Check max date in local data file:
- If `max_date >= today`: **SKIP** (data is current)
- If `max_date < today`: **RUN** (scrape missing days)
- If no file exists: **RUN** (initial scrape)

### Estate Nodes (4 nodes)
- `fetch_and_process_estate_data` (Midland Res)
- `estate_listing_scraper` (Centaline)
- `estate_detail_scraper` (Centaline)
- `estate_data_enricher` (Centaline)

**Behavior:** **ALWAYS RUN** (no skipping logic)

### Building Nodes (4 nodes)
- `scrape_midland_buildings`
- `scrape_midland_details`
- `scrape_building_listings` (Centaline OIR)
- `scrape_building_details` (Centaline OIR)

**Behavior:** **ALWAYS RUN** (no skipping logic)

### Processing Nodes (16 nodes)
All data cleaning, merging, and output nodes

**Behavior:** **ALWAYS RUN** (no skipping logic)

## Migration Notes

### What You Need to Do
**Nothing!** The changes are backward compatible.

### What Changed Automatically
1. ✅ Building and estate nodes will now run every time
2. ✅ Transaction nodes will intelligently check data freshness
3. ✅ No more "last run X days ago - skipping" messages
4. ✅ Better logging with emojis and clear status

### Optional Cleanup
You can remove these deprecated parameters from `conf/base/parameters.yml`:
- `estate_skip_days`
- `building_skip_days`
- `default_skip_days`

## Testing

To verify the changes work correctly:

```bash
# Run the full pipeline
kedro run

# You should see logs like:
# ✅ Node 'scrape_transactions': Data up-to-date (max: 2026-02-03) - skipping
# 📊 Node 'scrape_midland_buildings': Starting building scraper
```

No more "last run X days ago" messages!

## Benefits

1. **Smarter Execution**: Decisions based on actual data, not arbitrary timers
2. **Less Confusion**: No more mysterious "skipping" messages
3. **Better Control**: You can force re-runs by deleting data files
4. **Clearer Logs**: Emoji-based logging shows exactly what's happening

---
*Updated: February 3, 2026*
*Issue: "last run 21 days ago (<28) - skipping" - RESOLVED ✅*
