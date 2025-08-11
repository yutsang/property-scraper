#!/usr/bin/env python3
"""
Comprehensive analysis of buildings pipeline results.
Shows OSM geocoding success/failure rates and identifies buildings needing manual handling.
"""

import pandas as pd
import numpy as np
from pathlib import Path

def analyze_buildings_results():
    """Analyze the results of the buildings pipeline."""
    
    print("🏗️ BUILDINGS PIPELINE RESULTS ANALYSIS")
    print("=" * 60)
    
    # Load all datasets
    try:
        commercial_consolidated = pd.read_parquet('data/03_primary/consolidated_commercial_db.parquet')
        residential_consolidated = pd.read_parquet('data/03_primary/consolidated_residential_db.parquet')
        unmatched_commercial = pd.read_parquet('data/03_primary/unmatched_commercial_buildings.parquet')
        unmatched_residential = pd.read_parquet('data/03_primary/unmatched_residential_buildings.parquet')
        manual_review = pd.read_parquet('data/03_primary/manual_review_list.parquet')
        
        print("✅ All datasets loaded successfully")
    except Exception as e:
        print(f"❌ Error loading datasets: {e}")
        return
    
    print("\n📊 OVERALL STATISTICS")
    print("-" * 40)
    print(f"Total Commercial Buildings: {len(commercial_consolidated)}")
    print(f"Total Residential Buildings: {len(residential_consolidated)}")
    print(f"Unmatched Commercial: {len(unmatched_commercial)}")
    print(f"Unmatched Residential: {len(unmatched_residential)}")
    print(f"Buildings Needing Manual Review: {len(manual_review)}")
    
    # Analyze OSM geocoding results
    print("\n🗺️ OSM GEOCODING ANALYSIS")
    print("-" * 40)
    
    if 'osm_lat' in commercial_consolidated.columns:
        commercial_with_osm = len(commercial_consolidated[commercial_consolidated['osm_lat'].notna()])
        commercial_without_osm = len(commercial_consolidated[commercial_consolidated['osm_lat'].isna()])
        print(f"Commercial Buildings with OSM coordinates: {commercial_with_osm}")
        print(f"Commercial Buildings without OSM coordinates: {commercial_without_osm}")
        print(f"OSM Success Rate (Commercial): {commercial_with_osm/(commercial_with_osm+commercial_without_osm)*100:.1f}%")
    
    if 'osm_lat' in unmatched_commercial.columns:
        unmatched_with_osm = len(unmatched_commercial[unmatched_commercial['osm_lat'].notna()])
        unmatched_without_osm = len(unmatched_commercial[unmatched_commercial['osm_lat'].isna()])
        print(f"Unmatched Commercial with OSM coordinates: {unmatched_with_osm}")
        print(f"Unmatched Commercial without OSM coordinates: {unmatched_without_osm}")
        print(f"OSM Success Rate (Unmatched): {unmatched_with_osm/(unmatched_with_osm+unmatched_without_osm)*100:.1f}%")
    
    # Analyze Hong Kong territory results
    print("\n🇭🇰 HONG KONG TERRITORY ANALYSIS")
    print("-" * 40)
    
    if 'in_hk_territory' in commercial_consolidated.columns:
        hk_territory = len(commercial_consolidated[commercial_consolidated['in_hk_territory'] == True])
        outside_hk = len(commercial_consolidated[commercial_consolidated['in_hk_territory'] == False])
        print(f"Buildings in HK territory: {hk_territory}")
        print(f"Buildings outside HK territory: {outside_hk}")
    
    if 'in_hk_territory' in unmatched_commercial.columns:
        unmatched_hk = len(unmatched_commercial[unmatched_commercial['in_hk_territory'] == True])
        unmatched_outside = len(unmatched_commercial[unmatched_commercial['in_hk_territory'] == False])
        print(f"Unmatched buildings in HK territory: {unmatched_hk}")
        print(f"Unmatched buildings outside HK territory: {unmatched_outside}")
    
    # Show sample of successful OSM geocoding
    print("\n✅ SUCCESSFUL OSM GEOCODING EXAMPLES")
    print("-" * 40)
    
    if 'osm_lat' in commercial_consolidated.columns:
        successful_osm = commercial_consolidated[commercial_consolidated['osm_lat'].notna()].head(10)
        if not successful_osm.empty:
            print("Sample of buildings successfully geocoded by OSM:")
            for _, row in successful_osm.iterrows():
                print(f"  • {row['canonical_name']} ({row['canonical_district']})")
                print(f"    Coordinates: {row['osm_lat']:.6f}, {row['osm_lon']:.6f}")
                print(f"    In HK: {row.get('in_hk_territory', 'N/A')}")
                print()
    
    # Show sample of failed OSM geocoding
    print("\n❌ FAILED OSM GEOCODING EXAMPLES")
    print("-" * 40)
    
    if 'osm_lat' in unmatched_commercial.columns:
        failed_osm = unmatched_commercial[unmatched_commercial['osm_lat'].isna()].head(10)
        if not failed_osm.empty:
            print("Sample of buildings that OSM could not find:")
            for _, row in failed_osm.iterrows():
                print(f"  • {row['building_name']}")
                print(f"    District: {row.get('district', 'None')}")
                print(f"    Address: {row.get('geocoding_address', 'None')}")
                print()
    
    # Analyze manual review priorities
    print("\n🔍 MANUAL REVIEW ANALYSIS")
    print("-" * 40)
    
    if 'review_priority' in manual_review.columns:
        priority_counts = manual_review['review_priority'].value_counts()
        print("Priority breakdown:")
        for priority, count in priority_counts.items():
            print(f"  {priority.capitalize()}: {count}")
        
        # Show sample of buildings needing manual review
        print("\nSample buildings needing manual review:")
        sample_review = manual_review.head(10)
        for _, row in sample_review.iterrows():
            print(f"  • {row['building_name']}")
            print(f"    Source: {row['source']}")
            print(f"    District: {row.get('district', 'None')}")
            print(f"    Confidence: {row.get('confidence_score', 'N/A')}")
            print()
    
    # Show district analysis
    print("\n🏘️ DISTRICT ANALYSIS")
    print("-" * 40)
    
    if 'canonical_district' in commercial_consolidated.columns:
        district_counts = commercial_consolidated['canonical_district'].value_counts().head(10)
        print("Top 10 districts in consolidated commercial buildings:")
        for district, count in district_counts.items():
            print(f"  {district}: {count}")
    
    # Show source analysis
    print("\n📈 SOURCE ANALYSIS")
    print("-" * 40)
    
    if 'sources' in commercial_consolidated.columns:
        # Count sources (sources is a list, so we need to flatten)
        all_sources = []
        for sources in commercial_consolidated['sources']:
            if isinstance(sources, list):
                all_sources.extend(sources)
            else:
                all_sources.append(sources)
        
        source_counts = pd.Series(all_sources).value_counts()
        print("Data sources in consolidated commercial buildings:")
        for source, count in source_counts.items():
            print(f"  {source}: {count}")
    
    # Recommendations
    print("\n💡 RECOMMENDATIONS")
    print("-" * 40)
    
    total_unmatched = len(unmatched_commercial) + len(unmatched_residential)
    total_buildings = len(commercial_consolidated) + len(residential_consolidated)
    
    print(f"1. Manual Review Required: {total_unmatched} buildings ({total_unmatched/total_buildings*100:.1f}%)")
    print("2. Consider using Google Maps API for buildings without OSM coordinates")
    print("3. Review district assignments for buildings with 'None' district")
    print("4. Verify building names and addresses for failed geocoding")
    print("5. Consider fuzzy matching for similar building names")
    
    # Save detailed analysis to file
    print("\n💾 SAVING DETAILED ANALYSIS")
    print("-" * 40)
    
    # Create detailed analysis DataFrame
    analysis_data = []
    
    # Add consolidated buildings
    for _, row in commercial_consolidated.iterrows():
        analysis_data.append({
            'building_name': row['canonical_name'],
            'district': row['canonical_district'],
            'category': 'commercial',
            'status': 'consolidated',
            'osm_lat': row.get('osm_lat'),
            'osm_lon': row.get('osm_lon'),
            'in_hk_territory': row.get('in_hk_territory'),
            'sources': str(row['sources']),
            'confidence_score': row['confidence_score']
        })
    
    for _, row in residential_consolidated.iterrows():
        analysis_data.append({
            'building_name': row['canonical_name'],
            'district': row['canonical_district'],
            'category': 'residential',
            'status': 'consolidated',
            'osm_lat': None,
            'osm_lon': None,
            'in_hk_territory': None,
            'sources': str(row['sources']),
            'confidence_score': row['confidence_score']
        })
    
    # Add unmatched buildings
    for _, row in unmatched_commercial.iterrows():
        analysis_data.append({
            'building_name': row['building_name'],
            'district': row.get('district'),
            'category': 'commercial',
            'status': 'unmatched',
            'osm_lat': row.get('osm_lat'),
            'osm_lon': row.get('osm_lon'),
            'in_hk_territory': row.get('in_hk_territory'),
            'sources': row['source'],
            'confidence_score': row.get('confidence_score', 0.0)
        })
    
    analysis_df = pd.DataFrame(analysis_data)
    analysis_df.to_csv('buildings_analysis_results.csv', index=False)
    print("✅ Detailed analysis saved to 'buildings_analysis_results.csv'")
    
    # Save OSM failure analysis
    if 'osm_lat' in unmatched_commercial.columns:
        osm_failures = unmatched_commercial[unmatched_commercial['osm_lat'].isna()].copy()
        osm_failures.to_csv('osm_geocoding_failures.csv', index=False)
        print("✅ OSM geocoding failures saved to 'osm_geocoding_failures.csv'")
    
    print("\n🎉 Analysis complete!")

if __name__ == "__main__":
    analyze_buildings_results() 