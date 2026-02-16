# Logging Guidelines for Property Scraper

## Overview
This document provides standardized logging guidelines for all 28 pipeline nodes to ensure consistent, readable, and informative output.

## Logging Levels

### INFO (Primary Level)
Use for:
- Node start/completion
- Major processing steps
- Progress indicators
- Data quality summaries
- Important decisions (skip/execute)

### WARNING
Use for:
- Recoverable errors
- Data quality issues
- Missing optional data
- Deprecated features

### ERROR
Use for:
- Critical failures
- Authentication errors
- Network failures
- Data corruption

### DEBUG
Use for:
- Detailed technical information
- Loop iterations (sample only)
- Individual record processing
- Development troubleshooting

**Note**: Avoid excessive DEBUG logs in production. Keep DEBUG for troubleshooting only.

## Recommended Logging Pattern for Each Node

### Node Start
```python
logger.info(f"{'='*60}")
logger.info(f"🚀 Starting {node_name}")
logger.info(f"{'='*60}")
```

### Progress Indicators
```python
# For scraping/fetching
logger.info(f"📊 Processing {total} items...")
logger.info(f"   ✓ Completed {i+1}/{total} ({(i+1)/total*100:.1f}%)")

# For data transformations
logger.info(f"🔄 Cleaning {column_name} column...")
logger.info(f"   ✅ Processed {count:,} values")

# For data validation
logger.info(f"✓ Data quality: {parse_rate:.1f}% valid records")
```

### Node Completion
```python
logger.info(f"✅ Completed {node_name}")
logger.info(f"   📊 Output: {len(df):,} records, {len(df.columns)} columns")
logger.info(f"{'='*60}\n")
```

## Node-Specific Patterns

### 1. Transaction Scraping Nodes (4 nodes)
**Key Information to Log:**
- Date range being scraped
- Number of records fetched
- Incremental vs full scrape mode
- API rate limiting status

**Example:**
```python
logger.info(f"🚀 Starting transaction scraping")
logger.info(f"   📅 Date range: {start_date} to {end_date}")
logger.info(f"   📊 Mode: {'Incremental' if incremental else 'Full historical'}")
# ... scraping ...
logger.info(f"✅ Scraped {len(transactions):,} transactions")
```

### 2. Building/Estate Scraping Nodes (4 nodes)
**Key Information to Log:**
- Number of areas/districts to process
- Buildings per area (summary)
- Total buildings scraped

**Example:**
```python
logger.info(f"🏢 Scraping buildings from {len(areas)} areas")
# Progress every 10 areas
if (i + 1) % 10 == 0:
    logger.info(f"   ✓ Progress: {i+1}/{len(areas)} areas ({(i+1)/len(areas)*100:.1f}%)")
logger.info(f"✅ Total buildings scraped: {total_buildings:,}")
```

### 3. Data Cleaning/Processing Nodes (9 nodes)
**Key Information to Log:**
- Input data shape
- Transformation steps (1 log per major step)
- Output data shape

**Example:**
```python
logger.info(f"🧹 Starting data cleaning")
logger.info(f"   📊 Input: {len(df):,} records, {len(df.columns)} columns")
logger.info(f"💰 Cleaning price column...")
logger.info(f"   ✅ Converted {valid_count:,} prices")
logger.info(f"✅ Cleaning complete")
logger.info(f"   📊 Output: {len(df):,} records, {len(df.columns)} columns")
```

### 4. Merge/Join Nodes (4 nodes)
**Key Information to Log:**
- Input data sizes
- Join strategy
- Match rate
- Output size

**Example:**
```python
logger.info(f"🔗 Merging datasets")
logger.info(f"   📊 Left: {len(df1):,} records")
logger.info(f"   📊 Right: {len(df2):,} records")
logger.info(f"   ✓ Match rate: {match_rate:.1f}%")
logger.info(f"✅ Merge complete: {len(merged):,} records")
```

## Emoji Usage Guide

Use emojis consistently for better readability:

- 🚀 Starting/Launching
- ✅ Completed/Success
- 📊 Data/Statistics
- 📅 Dates
- 🔄 Processing/Transforming
- 🧹 Cleaning
- 💰 Price-related
- 📏 Area/Size-related
- 🏢 Buildings
- 🔗 Merging/Joining
- ⚠️  Warnings
- ❌ Errors
- ✓ Progress checkpoints

## What to AVOID

### ❌ Too Much Logging
```python
# BAD - Too verbose
for i, row in df.iterrows():
    logger.debug(f"Processing row {i}: {row['name']}")  # 100,000+ logs!
```

### ❌ Too Little Logging
```python
# BAD - No feedback
def process_data(df):
    # ... 1000 lines of processing ...
    return df  # User has no idea what's happening
```

### ❌ Inconsistent Format
```python
# BAD - Inconsistent
logger.info("starting node")  # No structure
logger.info("Processing...")
logger.info("DONE!!!!")  # Inconsistent style
```

## Recommended Logging for All 28 Nodes

### Scraping Nodes (13 nodes)
- Start: Node name, target (area/district)
- Progress: Every 10% or significant milestone
- End: Total records scraped, duration

### Processing Nodes (9 nodes)
- Start: Node name, input shape
- Steps: One log per major transformation
- End: Output shape, records processed

### Join/Merge Nodes (4 nodes)
- Start: Input datasets and sizes
- Match info: Match rate, join strategy
- End: Output size

### Final Output Nodes (2 nodes)
- Start: File names being created
- Summary: Records per file, date ranges
- End: File paths and sizes

## Example: Complete Node Logging

```python
def scrape_transactions(area_codes: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    logger.info(f"{'='*60}")
    logger.info(f"🚀 Starting transaction scraper")
    logger.info(f"{'='*60}")
    logger.info(f"   📊 Areas to process: {len(area_codes)}")
    
    transactions = []
    total_areas = len(area_codes)
    
    for i, area in enumerate(area_codes['area_code']):
        # Scrape logic here...
        
        # Log progress every 10 areas
        if (i + 1) % 10 == 0:
            logger.info(f"   ✓ Progress: {i+1}/{total_areas} areas ({(i+1)/total_areas*100:.1f}%)")
    
    df = pd.DataFrame(transactions)
    
    logger.info(f"✅ Transaction scraping complete")
    logger.info(f"   📊 Total records: {len(df):,}")
    logger.info(f"   📅 Date range: {df['date'].min()} to {df['date'].max()}")
    logger.info(f"{'='*60}\n")
    
    return df
```

## Summary

**Good logging should:**
1. Be informative but concise
2. Show clear progress
3. Use consistent formatting
4. Include data quality metrics
5. Help users understand what's happening

**Avoid:**
1. Excessive DEBUG logs in loops
2. Unclear or cryptic messages
3. Missing progress indicators for long operations
4. Inconsistent formatting

---
*Last updated: February 3, 2026*
