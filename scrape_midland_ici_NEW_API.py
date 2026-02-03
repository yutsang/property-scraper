"""
Midland ICI - NEW API Scraper
Endpoint: https://data.midlandici.com.hk/search/v1/transaction
This API has current data (2026-01-30)!
"""

import requests
import pandas as pd
from datetime import datetime
import time
import json

def scrape_midland_ici_new_api(max_pages=None):
    """
    Scrape from the NEW Midland ICI API
    
    Key differences from old API:
    - Endpoint: data.midlandici.com.hk/search/v1/transaction (not /ics/property/transaction/json)
    - Requires session cookies (get from main page)
    - Returns list format with cursorHead/Tail pagination
    - Has CURRENT data (2026-01-30)
    """
    
    print("="*80)
    print("MIDLAND ICI - NEW API SCRAPER")
    print("="*80)
    
    # Step 1: Create session and get auth cookies
    print("\n1️⃣ Getting session cookies...")
    session = requests.Session()
    
    main_url = 'https://www.midlandici.com.hk/zh-hk/listing/transaction/ics'
    headers_main = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    }
    
    response = session.get(main_url, headers=headers_main, timeout=15)
    print(f"   Main page status: {response.status_code}")
    print(f"   Cookies: {len(session.cookies)}")
    
    time.sleep(1)
    
    # Step 2: Call the new API
    print("\n2️⃣ Fetching transactions from NEW API...")
    
    api_url = 'https://data.midlandici.com.hk/search/v1/transaction'
    headers_api = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'zh-HK,zh;q=0.9,en;q=0.8',
        'Referer': main_url,
    }
    
    all_transactions = []
    page = 1
    max_pages = max_pages or 999  # Default to unlimited
    
    while page <= max_pages:
        params = {
            'currency': 'HKD',
            'lang': 'zh-hk',
            'limit': 100,  # Max per page
            'page': page,
            'sort': 'txDate-desc',  # Newest first
            'unit': 'feet'
        }
        
        try:
            response_api = session.get(api_url, headers=headers_api, params=params, timeout=15)
            
            if response_api.status_code == 200:
                data = response_api.json()
                
                if isinstance(data, list) and len(data) > 0:
                    item = data[0]
                    total_count = item.get('count', 0)
                    results = item.get('results', [])
                    
                    if not results:
                        print(f"   Page {page}: No more data")
                        break
                    
                    all_transactions.extend(results)
                    print(f"   Page {page}: {len(results)} transactions (total: {len(all_transactions):,}/{total_count:,})")
                    
                    # Check if we got all data
                    if len(all_transactions) >= total_count:
                        print(f"   ✓ Fetched all {total_count:,} transactions!")
                        break
                    
                    page += 1
                    time.sleep(0.5)  # Be polite
                else:
                    print(f"   Page {page}: Unexpected format")
                    break
            else:
                print(f"   Page {page}: Status {response_api.status_code}")
                break
                
        except Exception as e:
            print(f"   Page {page}: Error - {e}")
            break
    
    # Step 3: Convert to DataFrame and analyze
    print(f"\n3️⃣ Processing {len(all_transactions):,} transactions...")
    
    if all_transactions:
        df = pd.DataFrame(all_transactions)
        
        print(f"\n📊 DATA SUMMARY:")
        print(f"   Total transactions: {len(df):,}")
        print(f"   Columns: {list(df.columns)}")
        
        if 'txDate' in df.columns:
            df['tx_date_parsed'] = pd.to_datetime(df['txDate'], errors='coerce')
            
            print(f"\n📅 DATE ANALYSIS:")
            print(f"   Oldest: {df['tx_date_parsed'].min().date()}")
            print(f"   Newest: {df['tx_date_parsed'].max().date()}")
            
            # Year-month distribution
            df['year_month'] = df['tx_date_parsed'].dt.to_period('M')
            monthly = df['year_month'].value_counts().sort_index()
            
            print(f"\n📊 MONTH-BY-MONTH (Last 12 months):")
            for period, count in monthly.tail(12).items():
                print(f"   {period}: {count:,}")
            
            # Check 2026
            df_2026 = df[df['tx_date_parsed'] >= '2026-01-01']
            print(f"\n✅ 2026 TRANSACTIONS: {len(df_2026):,}")
            
            if len(df_2026) > 0:
                print(f"\n🎉🎉🎉 SUCCESS! Found 2026 data!")
                print(f"\n2026 by month:")
                monthly_2026 = df_2026['year_month'].value_counts().sort_index()
                for period, count in monthly_2026.items():
                    print(f"   {period}: {count:,}")
        
        # Save to file
        output_file = 'midland_ici_NEW_API_data.parquet'
        df.to_parquet(output_file, index=False)
        print(f"\n✅ Saved to: {output_file}")
        
        return df
    
    return None


# ==================== MAIN ====================

if __name__ == "__main__":
    print("\n🚀 SCRAPING FROM NEW MIDLAND ICI API\n")
    
    # Test with first 10 pages
    df = scrape_midland_ici_new_api(max_pages=50)
    
    if df is not None and len(df) > 0:
        print("\n" + "="*80)
        print("✅ SUCCESS!")
        print("="*80)
        print(f"\nWe now have access to current Midland ICI data!")
        print(f"Total transactions: {len(df):,}")
        print(f"Newest: {df['tx_date_parsed'].max().date()}")
        print(f"\nNext step: Update the pipeline to use this new API endpoint")
    else:
        print("\n" + "="*80)
        print("⚠️ Need more investigation")
        print("="*80)
