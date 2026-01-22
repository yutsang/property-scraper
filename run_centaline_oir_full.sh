#!/bin/bash

echo "================================================================================"
echo "CENTALINE OIR FULL PIPELINE SCRAPING"
echo "================================================================================"
echo ""
echo "This script will run a complete scraping of Centaline OIR (Office/Industrial/Retail) data:"
echo "  1. Scrape building listings (53 areas)"
echo "  2. Scrape building details"
echo "  3. Scrape transaction data"
echo "  4. Join and process all data"
echo ""
echo "================================================================================"
echo ""

cd /Users/ytsang/Desktop/Github/property-scraper

# Check if we should force re-scraping
echo "Checking current data status..."
echo ""

if [ -f "data/node_execution_tracker.json" ]; then
    echo "Current execution tracker found:"
    python << 'EOF'
import json
with open('data/node_execution_tracker.json', 'r') as f:
    tracker = json.load(f)
    
if 'scrape_building_listings' in tracker:
    print(f"  - Building listings last run: {tracker['scrape_building_listings']['last_run']}")
if 'scrape_building_details' in tracker:
    print(f"  - Building details last run: {tracker['scrape_building_details']['last_run']}")
if 'scrape_transaction' in tracker:
    print(f"  - Transactions last run: {tracker['scrape_transaction']['last_run']}")
    print(f"    Records: {tracker['scrape_transaction']['metadata']['records_processed']:,}")
EOF
    echo ""
fi

echo "Options:"
echo "  1. Force full re-scraping (delete node tracker + data files)"
echo "  2. Run pipeline (will skip nodes if recently run)"
echo ""
read -p "Choose option (1 or 2, press Enter for option 2): " choice

if [ "$choice" = "1" ]; then
    echo ""
    echo "Option 1: FORCE FULL RE-SCRAPING"
    echo "Deleting old data files..."
    
    # Delete node tracker for OIR nodes
    if [ -f "data/node_execution_tracker.json" ]; then
        echo "  Backing up node tracker..."
        cp data/node_execution_tracker.json data/node_execution_tracker.json.backup
        
        # Remove OIR-related entries
        python << 'EOF'
import json
with open('data/node_execution_tracker.json', 'r') as f:
    tracker = json.load(f)

# Remove OIR-related nodes
for key in list(tracker.keys()):
    if 'building' in key.lower() or 'oir' in key.lower() or 'transaction' in key.lower():
        if key in ['scrape_building_listings', 'scrape_building_details', 'scrape_transaction', 'join_centaline_oir_data']:
            print(f"  Removing: {key}")
            del tracker[key]

with open('data/node_execution_tracker.json', 'w') as f:
    json.dump(tracker, f, indent=2)

print("  ✅ Updated node tracker")
EOF
    fi
    
    echo "  Deleting OIR data files..."
    rm -f data/01_raw/centanet_oir_buildings.parquet
    rm -f data/01_raw/centaline_oir_trans_lv_0.parquet
    rm -f data/02_intermediate/centanet_oir_details.parquet
    rm -f data/02_intermediate/centaline_oir_base.parquet
    echo "  ✅ Deleted old OIR data files"
    echo ""
fi

echo "================================================================================"
echo "Running Centaline OIR Pipeline"
echo "================================================================================"
echo ""

kedro run --pipeline centaline_oir

echo ""
echo "================================================================================"
echo "VERIFICATION"
echo "================================================================================"
echo ""

python << 'EOF'
import pandas as pd
import os

files = {
    'Building listings': 'data/01_raw/centanet_oir_buildings.parquet',
    'Building details': 'data/02_intermediate/centanet_oir_details.parquet',
    'Transactions': 'data/01_raw/centaline_oir_trans_lv_0.parquet',
    'Final base': 'data/02_intermediate/centaline_oir_base.parquet'
}

for name, filepath in files.items():
    if os.path.exists(filepath):
        df = pd.read_parquet(filepath)
        print(f"✅ {name:20s}: {len(df):7,} records")
    else:
        print(f"❌ {name:20s}: File not found")

EOF

echo ""
echo "================================================================================"
echo "COMPLETE!"
echo "================================================================================"
