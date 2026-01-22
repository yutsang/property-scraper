#!/usr/bin/env python3
"""
Identify which areas/districts have the highest percentage of null area records.
This helps target re-scraping efforts.
"""

import pandas as pd
import numpy as np

def analyze_null_areas():
    """Analyze null area distribution by district and building code"""
    
    print("="*80)
    print("NULL AREA ANALYSIS - Centaline Residential Data")
    print("="*80)
    print()
    
    # Load data
    df = pd.read_parquet('data/03_primary/centaline_res.parquet')
    
    # Filter non-carpark properties
    non_carpark = df[~df['Name'].str.contains('carpark', case=False, na=False)].copy()
    
    print(f"📊 Overall Statistics:")
    print(f"  Total records: {len(df):,}")
    print(f"  Non-carpark records: {len(non_carpark):,}")
    print(f"  Records with null area: {non_carpark['area'].isna().sum():,} ({non_carpark['area'].isna().sum()/len(non_carpark)*100:.1f}%)")
    print()
    
    # Analyze by district
    print("="*80)
    print("NULL AREA BY DISTRICT")
    print("="*80)
    print()
    
    district_stats = non_carpark.groupby('district').agg({
        'area': ['count', lambda x: x.isna().sum(), lambda x: x.notna().sum()]
    }).reset_index()
    district_stats.columns = ['district', 'total', 'null_area', 'with_area']
    district_stats['null_pct'] = (district_stats['null_area'] / district_stats['total'] * 100).round(1)
    district_stats = district_stats.sort_values('null_area', ascending=False)
    
    print("Top 20 districts by null area count:")
    print(district_stats.head(20).to_string(index=False))
    print()
    
    # Analyze by subdistrict
    print("="*80)
    print("NULL AREA BY SUBDISTRICT")
    print("="*80)
    print()
    
    subdistrict_stats = non_carpark.groupby('subdistrict').agg({
        'area': ['count', lambda x: x.isna().sum(), lambda x: x.notna().sum()]
    }).reset_index()
    subdistrict_stats.columns = ['subdistrict', 'total', 'null_area', 'with_area']
    subdistrict_stats['null_pct'] = (subdistrict_stats['null_area'] / subdistrict_stats['total'] * 100).round(1)
    subdistrict_stats = subdistrict_stats.sort_values('null_area', ascending=False)
    
    print("Top 30 subdistricts by null area count:")
    print(subdistrict_stats.head(30).to_string(index=False))
    print()
    
    # Analyze recent null areas (last 60 days)
    print("="*80)
    print("RECENT NULL AREAS (Last 60 days)")
    print("="*80)
    print()
    
    non_carpark['date'] = pd.to_datetime(non_carpark['date'])
    recent = non_carpark[non_carpark['date'] >= pd.Timestamp.now() - pd.Timedelta(days=60)]
    recent_null = recent[recent['area'].isna()]
    
    print(f"Recent records (last 60 days): {len(recent):,}")
    print(f"Recent records with null area: {len(recent_null):,} ({len(recent_null)/len(recent)*100:.1f}%)")
    print()
    
    if len(recent_null) > 0:
        print("Sample recent null area records:")
        sample = recent_null.nlargest(20, 'date')[['date', 'Name', 'Tower', 'district', 'subdistrict', 'price', 'building_code']]
        print(sample.to_string(index=False))
        print()
        
        # Get unique building codes for re-scraping
        unique_codes = recent_null['building_code'].dropna().unique()
        print(f"\n📋 {len(unique_codes)} unique building codes with recent null areas")
        print()
        
        # Generate re-scrape script
        print("="*80)
        print("RECOMMENDED RE-SCRAPE TARGETS")
        print("="*80)
        print()
        
        # Group by subdistrict to create re-scrape list
        rescrape_targets = recent_null.groupby('subdistrict').agg({
            'area': 'count',
            'building_code': 'first'
        }).reset_index()
        rescrape_targets.columns = ['subdistrict', 'null_count', 'sample_code']
        rescrape_targets = rescrape_targets.sort_values('null_count', ascending=False)
        
        print("Top subdistricts to re-scrape:")
        print(rescrape_targets.head(20).to_string(index=False))
        print()
        
        # Save to CSV for easy reference
        rescrape_targets.to_csv('null_area_rescrape_targets.csv', index=False)
        print("✓ Saved rescrape targets to: null_area_rescrape_targets.csv")

if __name__ == "__main__":
    analyze_null_areas()
