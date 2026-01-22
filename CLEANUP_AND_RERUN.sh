#!/bin/bash

echo "================================================================================"
echo "CLEANUP AND RERUN SCRIPT - FULL FIX"
echo "================================================================================"
echo ""
echo "This script will:"
echo "  1. Delete corrupted enriched data (2.3M rows with duplicates)"
echo "  2. Delete raw transaction data (to re-scrape with fixed name extraction)"
echo "  3. Re-run the Kedro pipeline with all fixes applied"
echo ""
echo "Fixes applied:"
echo "  ✅ Property names now include main estate name"
echo "  ✅ Proper deduplication (no more 10x duplicates)"
echo "  ✅ Multi-threading enabled (5 parallel threads)"
echo ""
echo "================================================================================"
echo ""

# Change to project directory
cd /Users/ytsang/Desktop/Github/property-scraper

# Step 1: Backup files (just in case)
echo "Step 1: Backing up existing files..."
if [ -f "data/02_intermediate/centaline_res_base.parquet" ]; then
    cp data/02_intermediate/centaline_res_base.parquet data/02_intermediate/centaline_res_base.parquet.old_backup
    echo "✅ Backed up enriched data: centaline_res_base.parquet.old_backup"
fi

if [ -f "data/01_raw/centaline_res_trans_lv_0.parquet" ]; then
    cp data/01_raw/centaline_res_trans_lv_0.parquet data/01_raw/centaline_res_trans_lv_0.parquet.old_backup
    echo "✅ Backed up raw data: centaline_res_trans_lv_0.parquet.old_backup"
fi

echo ""

# Step 2: Delete files to force fresh scraping
echo "Step 2: Deleting old files to trigger fresh scraping..."
rm -f data/02_intermediate/centaline_res_base.parquet
rm -f data/01_raw/centaline_res_trans_lv_0.parquet
echo "✅ Deleted old data files"

echo ""

# Step 3: Run pipeline
echo "Step 3: Running Kedro pipeline with all fixes..."
echo "This will:"
echo "  - Scrape transactions with CORRECT property names"
echo "  - Process data with PROPER deduplication"
echo "  - Use 20 parallel threads for speed (4x faster!)"
echo ""
echo "This may take 15-20 minutes for full scraping..."
echo "Or 2-5 minutes for incremental updates"
echo ""

kedro run --pipeline centaline_res

echo ""
echo "================================================================================"
echo "VERIFICATION"
echo "================================================================================"
echo ""

# Step 4: Verify the output
echo "Checking data quality..."
python << 'EOF'
import pandas as pd

try:
    df = pd.read_parquet('data/02_intermediate/centaline_res_base.parquet')
    
    print(f"✅ Total rows: {len(df):,}")
    print(f"✅ Unique transaction_ids: {df['transaction_id'].nunique():,}")
    
    if len(df) == df['transaction_id'].nunique():
        print("✅ NO DUPLICATES - Data is clean!")
    else:
        dupes = len(df) - df['transaction_id'].nunique()
        print(f"⚠️  WARNING: {dupes:,} duplicate rows detected!")
    
    print(f"\n✅ Records with Name: {df['Name'].notna().sum():,} ({df['Name'].notna().sum()/len(df)*100:.1f}%)")
    print(f"✅ Records with area: {df['area'].notna().sum():,} ({df['area'].notna().sum()/len(df)*100:.1f}%)")
    print(f"✅ Latest transaction date: {df['date'].max()}")
    
except Exception as e:
    print(f"❌ ERROR: {e}")

EOF

echo ""
echo "================================================================================"
echo "COMPLETE!"
echo "================================================================================"
