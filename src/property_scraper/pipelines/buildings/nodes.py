"""
Centralized Buildings Pipeline Nodes
Handles building information consolidation for both commercial and residential properties.
Implements multi-stage matching: exact matching → OSM geocoding → Google Maps API (initialization only)
"""

import pandas as pd
import numpy as np
import time
import random
import os
import json
import logging
from typing import Dict, Any, List, Tuple, Optional, Union
from datetime import datetime, timedelta
import re
from tqdm import tqdm

# Geocoding libraries
try:
    import geopy
    from geopy.geocoders import Nominatim, GoogleV3
    from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
    GEOPY_AVAILABLE = True
except ImportError:
    GEOPY_AVAILABLE = False

# Fuzzy matching
try:
    from rapidfuzz import fuzz, process
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False

from ...utils.node_tracker import should_run_node, record_node_execution

    logger = logging.getLogger(__name__)
    
# ============ DATA EXTRACTION NODES ============

def extract_building_data_from_sources(
    centaline_res_base: pd.DataFrame,
    centaline_oir_base: pd.DataFrame,
    midland_res_base: pd.DataFrame,
    midland_ici_base: pd.DataFrame,
    leasinghub_building_listings: pd.DataFrame,
    params: Dict[str, Any]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extract building information from all sources and separate into commercial and residential.
        
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: (commercial_buildings, residential_buildings)
    """
    logger.info("Extracting building data from all sources")
    
    commercial_buildings = []
    residential_buildings = []
    
    # Extract from Centaline Residential
    if not centaline_res_base.empty:
        res_buildings = extract_centaline_residential_buildings(centaline_res_base)
        residential_buildings.extend(res_buildings)
    
    # Extract from Centaline OIR (Commercial)
    if not centaline_oir_base.empty:
        oir_buildings = extract_centaline_oir_buildings(centaline_oir_base)
        commercial_buildings.extend(oir_buildings)
    
    # Extract from Midland Residential
    if not midland_res_base.empty:
        midland_res_buildings = extract_midland_residential_buildings(midland_res_base)
        residential_buildings.extend(midland_res_buildings)
    
    # Extract from Midland ICI (Commercial)
    if not midland_ici_base.empty:
        midland_ici_buildings = extract_midland_ici_buildings(midland_ici_base)
        commercial_buildings.extend(midland_ici_buildings)
    
    # Extract from LeasingHub (Commercial)
    if not leasinghub_building_listings.empty:
        leasinghub_buildings = extract_leasinghub_buildings(leasinghub_building_listings)
        commercial_buildings.extend(leasinghub_buildings)
    
    # Convert to DataFrames
    commercial_df = pd.DataFrame(commercial_buildings) if commercial_buildings else pd.DataFrame()
    residential_df = pd.DataFrame(residential_buildings) if residential_buildings else pd.DataFrame()
    
    logger.info(f"Extracted {len(commercial_df)} commercial buildings and {len(residential_df)} residential buildings")
    
    return commercial_df, residential_df

def extract_centaline_residential_buildings(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Extract building information from Centaline residential data."""
    buildings = []
    
    # Use the correct column names from the actual dataset
    if 'Name' in df.columns and 'District' in df.columns:
        # Group by estate name and district
        grouped = df.groupby(['Name', 'District']).agg({
            'Address': 'first',
            'Region': 'first',
            'Subdistrict': 'first',
            'Code': 'first'
    }).reset_index()
    
    for _, row in grouped.iterrows():
            buildings.append({
                'building_name': clean_building_name(row['Name']),
                'original_name': row['Name'],
                'address': row['Address'] if pd.notna(row['Address']) else '',
                'district': row['District'] if pd.notna(row['District']) else '',
                'region': row['Region'] if pd.notna(row['Region']) else '',
                'subdistrict': row['Subdistrict'] if pd.notna(row['Subdistrict']) else '',
                'code': row['Code'] if pd.notna(row['Code']) else '',
                'source': 'centaline_res',
                'property_type': 'residential',
                'confidence_score': 1.0,
                'match_type': 'exact'
            })
    else:
        # Fallback: use extracted_estate_name if available
        if 'extracted_estate_name' in df.columns:
            grouped = df.groupby(['extracted_estate_name', 'district']).agg({
                'address': 'first',
                'region': 'first',
                'subdistrict': 'first',
                'code': 'first'
            }).reset_index()
            
            for _, row in grouped.iterrows():
                if pd.notna(row['extracted_estate_name']) and row['extracted_estate_name'].strip():
                    buildings.append({
                        'building_name': clean_building_name(row['extracted_estate_name']),
                        'original_name': row['extracted_estate_name'],
                        'address': row['address'] if pd.notna(row['address']) else '',
                        'district': row['district'] if pd.notna(row['district']) else '',
                        'region': row['region'] if pd.notna(row['region']) else '',
                        'subdistrict': row['subdistrict'] if pd.notna(row['subdistrict']) else '',
                        'code': str(row['code']) if pd.notna(row['code']) else '',
                        'source': 'centaline_res',
                        'property_type': 'residential',
                        'confidence_score': 1.0,
                        'match_type': 'exact'
                    })
    
    return buildings

def extract_centaline_oir_buildings(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Extract building information from Centaline OIR data."""
    buildings = []
    
    # Use the correct column names from the actual dataset
    if 'propertyNameEn' in df.columns and 'districtNameEn' in df.columns:
        # Group by building name and district
        grouped = df.groupby(['propertyNameEn', 'districtNameEn']).agg({
            'addressDisplayName': 'first',
            'zoneEn': 'first',
            'ibsBuildingID': 'first'
        }).reset_index()
        
        for _, row in grouped.iterrows():
            buildings.append({
                'building_name': clean_building_name(row['propertyNameEn']),
                'original_name': row['propertyNameEn'],
                'address': row['addressDisplayName'] if pd.notna(row['addressDisplayName']) else '',
                'district': row['districtNameEn'] if pd.notna(row['districtNameEn']) else '',
                'zone': row['zoneEn'] if pd.notna(row['zoneEn']) else '',
                'property_id': str(row['ibsBuildingID']) if pd.notna(row['ibsBuildingID']) else '',
                'source': 'centaline_oir',
                'property_type': 'commercial',
                'confidence_score': 1.0,
                'match_type': 'exact'
            })
    else:
        # Fallback: use building_name column if it exists
        if 'building_name' in df.columns:
            grouped = df.groupby(['building_name', 'district']).agg({
                'full_address': 'first',
                'zone': 'first',
                'property_id': 'first'
            }).reset_index()
            
            for _, row in grouped.iterrows():
                buildings.append({
                    'building_name': clean_building_name(row['building_name']),
                    'original_name': row['building_name'],
                    'address': row['full_address'] if pd.notna(row['full_address']) else '',
                    'district': row['district'] if pd.notna(row['district']) else '',
                    'zone': row['zone'] if pd.notna(row['zone']) else '',
                    'property_id': str(row['property_id']) if pd.notna(row['property_id']) else '',
                    'source': 'centaline_oir',
                    'property_type': 'commercial',
                    'confidence_score': 1.0,
                    'match_type': 'exact'
                })
    
    return buildings

def extract_midland_residential_buildings(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Extract building information from Midland residential data."""
    buildings = []
    
    # Use the correct column names from the actual dataset
    if 'estate' in df.columns and 'district' in df.columns:
        # Group by estate name and district
        grouped = df.groupby(['estate', 'district']).agg({
            'building': 'first',
            'region_name_trans': 'first',
            'subregion': 'first',
            'estate_id': 'first'
        }).reset_index()
        
        for _, row in grouped.iterrows():
            if pd.notna(row['estate']) and row['estate'].strip():
                buildings.append({
                    'building_name': clean_building_name(row['estate']),
                    'original_name': row['estate'],
                    'address': row['building'] if pd.notna(row['building']) else '',
                    'district': row['district'] if pd.notna(row['district']) else '',
                    'region': row['region_name_trans'] if pd.notna(row['region_name_trans']) else '',
                    'subregion': row['subregion'] if pd.notna(row['subregion']) else '',
                    'estate_id': str(row['estate_id']) if pd.notna(row['estate_id']) else '',
                    'source': 'midland_res',
                    'property_type': 'residential',
                    'confidence_score': 1.0,
                    'match_type': 'exact'
                })
    
    return buildings

def extract_midland_ici_buildings(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Extract building information from Midland ICI data."""
    buildings = []
    
    # Use the correct column names from the actual dataset
    if 'eng_name' in df.columns and 'dist_name_en' in df.columns:
        # Group by building name and district
        grouped = df.groupby(['eng_name', 'dist_name_en']).agg({
            'streetno': 'first',
            'chi_name': 'first',
            'ics_type': 'first',
            'building_id': 'first'
        }).reset_index()
        
        for _, row in grouped.iterrows():
            if pd.notna(row['eng_name']) and row['eng_name'].strip():
                buildings.append({
                    'building_name': clean_building_name(row['eng_name']),
                    'original_name': row['eng_name'],
                    'address_en': row['streetno'] if pd.notna(row['streetno']) else '',
                    'address_zh': row['chi_name'] if pd.notna(row['chi_name']) else '',
                    'district': row['dist_name_en'] if pd.notna(row['dist_name_en']) else '',
                    'property_type': row['ics_type'] if pd.notna(row['ics_type']) else '',
                    'building_id': str(row['building_id']) if pd.notna(row['building_id']) else '',
                    'source': 'midland_ici',
                    'property_type_category': 'commercial',
                    'confidence_score': 1.0,
                    'match_type': 'exact'
                })
    
    return buildings

def extract_leasinghub_buildings(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Extract building information from LeasingHub data."""
    buildings = []
    
    for _, row in df.iterrows():
        buildings.append({
            'building_name': clean_building_name(row['name']),
            'original_name': row['name'],
            'url': row['url'] if pd.notna(row['url']) else '',
            'property_type': row['property_type'] if pd.notna(row['property_type']) else '',
            'source': 'leasinghub',
            'property_type_category': 'commercial',
            'confidence_score': 1.0,
            'match_type': 'exact'
        })
    
    return buildings

# ============ BUILDING MATCHING NODES ============

def create_consolidated_building_databases(
    commercial_buildings: pd.DataFrame,
    residential_buildings: pd.DataFrame,
    params: Dict[str, Any]
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Create consolidated building databases with multi-stage matching.
    
    Returns:
        Tuple containing:
        - consolidated_commercial_db: Matched commercial buildings
        - consolidated_residential_db: Matched residential buildings  
        - unmatched_commercial: Unmatched commercial buildings
        - unmatched_residential: Unmatched residential buildings
    """
    logger.info("Creating consolidated building databases")
    
    # Stage 1: Exact matching within each category
    consolidated_commercial, unmatched_commercial = perform_exact_matching(
        commercial_buildings, 'commercial', params
    )
    
    consolidated_residential, unmatched_residential = perform_exact_matching(
        residential_buildings, 'residential', params
    )
    
    # Stage 2: OSM geocoding for unmatched buildings
    if not unmatched_commercial.empty:
        unmatched_commercial = geocode_with_osm(unmatched_commercial, params)
    
    if not unmatched_residential.empty:
        unmatched_residential = geocode_with_osm(unmatched_residential, params)
    
    # Stage 3: Location-based matching for geocoded buildings
    if not unmatched_commercial.empty:
        consolidated_commercial, unmatched_commercial = perform_location_based_matching(
            consolidated_commercial, unmatched_commercial, 'commercial', params
        )
    
    if not unmatched_residential.empty:
        consolidated_residential, unmatched_residential = perform_location_based_matching(
            consolidated_residential, unmatched_residential, 'residential', params
        )
    
    logger.info(f"Consolidated {len(consolidated_commercial)} commercial buildings, {len(unmatched_commercial)} unmatched")
    logger.info(f"Consolidated {len(consolidated_residential)} residential buildings, {len(unmatched_residential)} unmatched")
    
    return consolidated_commercial, consolidated_residential, unmatched_commercial, unmatched_residential

def perform_exact_matching(
    buildings_df: pd.DataFrame, 
    category: str, 
    params: Dict[str, Any]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Perform exact matching based on building name and district.
    
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: (matched_buildings, unmatched_buildings)
    """
    logger.info(f"Performing exact matching for {category} buildings")
    
    if buildings_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    
    # Create a composite key for matching
    buildings_df['match_key'] = buildings_df['building_name'].str.lower() + '|' + buildings_df['district'].str.lower()
    
    # Group by match key
    grouped = buildings_df.groupby('match_key').agg({
        'building_name': 'first',
        'original_name': lambda x: list(x),
        'address': lambda x: list(x),
        'district': 'first',
        'source': lambda x: list(x),
        'property_type': 'first',
        'confidence_score': 'mean'
    }).reset_index()
    
    # Create consolidated records
    consolidated = []
    for _, row in grouped.iterrows():
        # Determine canonical name (prefer Centaline for residential, LeasingHub for commercial)
        canonical_name = determine_canonical_name(row['original_name'], row['source'], category)
        
        # Determine canonical district
        canonical_district = determine_canonical_district(row['district'], row['source'], category)
        
        consolidated.append({
            'building_id': f"{category}_{len(consolidated):06d}",
            'canonical_name': canonical_name,
            'original_names': row['original_name'],
            'addresses': row['address'],
            'canonical_district': canonical_district,
            'sources': row['source'],
            'property_type': row['property_type'],
            'category': category,
            'confidence_score': row['confidence_score'],
            'match_type': 'exact',
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        })
    
    consolidated_df = pd.DataFrame(consolidated)
    
    # Buildings that couldn't be matched exactly
    unmatched_df = buildings_df[~buildings_df['match_key'].isin(grouped['match_key'])]
    
    return consolidated_df, unmatched_df

def determine_canonical_name(names: List[str], sources: List[str], category: str) -> str:
    """Determine the canonical name for a building based on source priority."""
    if len(names) == 1:
        return names[0]
    
    # Priority order for naming
    if category == 'residential':
        # Prefer Centaline over Midland for residential
        priority_order = ['centaline_res', 'midland_res']
    else:
        # Prefer LeasingHub over others for commercial
        priority_order = ['leasinghub', 'centaline_oir', 'midland_ici']
    
    # Find the first name from the highest priority source
    for source in priority_order:
        for i, src in enumerate(sources):
            if src == source:
                return names[i]
    
    # If no priority source found, return the first name
    return names[0]

def determine_canonical_district(district: str, sources: List[str], category: str) -> str:
    """Determine the canonical district based on source priority."""
    if category == 'residential':
        # Prefer Centaline for residential districts
        priority_order = ['centaline_res', 'midland_res']
    else:
        # Prefer LeasingHub for commercial districts
        priority_order = ['leasinghub', 'centaline_oir', 'midland_ici']
    
    # For now, return the district as is (can be enhanced later)
    return district

# ============ GEOCODING NODES ============

def geocode_with_osm(
    buildings_df: pd.DataFrame, 
    params: Dict[str, Any]
) -> pd.DataFrame:
    """
    Geocode buildings using OpenStreetMap Nominatim.
    """
    if not GEOPY_AVAILABLE:
        logger.warning("Geopy not available, skipping OSM geocoding")
        return buildings_df
    
    logger.info("Geocoding buildings with OSM")
    
    # Initialize geocoder
    geolocator = Nominatim(user_agent="property_scraper_buildings")
    
    # Load existing cache
    cache_file = params.get('geocoding_cache_file', 'data/03_primary/osm_geocoding_cache.json')
    cache = load_geocoding_cache(cache_file)
    
    results = []
    
    for idx, row in tqdm(buildings_df.iterrows(), total=len(buildings_df), desc="Geocoding with OSM"):
        # Build address for geocoding
        address = build_geocoding_address(row)
        
        # Check cache first
        if address in cache:
            lat, lon, location_type = cache[address]
        else:
            lat, lon, location_type = geocode_single_address_osm(address, geolocator)
            cache[address] = (lat, lon, location_type)
            
            # Save cache periodically
            if idx % 50 == 0:
                save_geocoding_cache(cache, cache_file)
        
        # Create result row
        result_row = row.copy()
        result_row['osm_lat'] = lat
        result_row['osm_lon'] = lon
        result_row['osm_location_type'] = location_type
        result_row['geocoding_address'] = address
        
        # Check if in Hong Kong
        result_row['in_hk_territory'] = is_in_hong_kong(lat, lon)
        
        # Calculate distance to district center if we have coordinates
        if lat and lon:
            district_distance = calculate_district_distance(lat, lon, row['district'])
            result_row['district_distance_km'] = district_distance
        else:
            result_row['district_distance_km'] = None
        
        results.append(result_row)
    
    # Save final cache
    save_geocoding_cache(cache, cache_file)
    
    return pd.DataFrame(results)

def geocode_single_address_osm(
    address: str, 
    geolocator, 
    max_retries: int = 3
) -> Tuple[Optional[float], Optional[float], str]:
    """Geocode a single address using OSM."""
    for attempt in range(max_retries):
        try:
            location = geolocator.geocode(f"{address}, Hong Kong", timeout=10)
            if location:
                return location.latitude, location.longitude, "osm"
            else:
                return None, None, "not_found"
        except (GeocoderTimedOut, GeocoderUnavailable) as e:
            if attempt == max_retries - 1:
                logger.warning(f"Failed to geocode {address}: {e}")
                return None, None, "error"
            time.sleep(random.uniform(1, 3))
    
    return None, None, "error"

def build_geocoding_address(row: pd.Series) -> str:
    """Build a geocoding address from building information."""
    parts = []
    
    # Add building name
    if pd.notna(row.get('building_name')) and row['building_name']:
        parts.append(str(row['building_name']))
    
    # Add address if available
    if pd.notna(row.get('address')) and row['address']:
        parts.append(str(row['address']))
    
    # Add district
    if pd.notna(row.get('district')) and row['district']:
        parts.append(str(row['district']))
    
    return ', '.join(parts)

def is_in_hong_kong(lat: float, lon: float) -> bool:
    """Check if coordinates are within Hong Kong territory."""
    if not lat or not lon:
        return False
    
    # Hong Kong bounding box (approximate)
    hk_bounds = {
        'min_lat': 22.1,
        'max_lat': 22.6,
        'min_lon': 113.8,
        'max_lon': 114.5
    }
    
    return (hk_bounds['min_lat'] <= lat <= hk_bounds['max_lat'] and 
            hk_bounds['min_lon'] <= lon <= hk_bounds['max_lon'])

def calculate_district_distance(lat: float, lon: float, district: str) -> Optional[float]:
    """Calculate distance from building to district center."""
    # District centers (approximate coordinates)
    district_centers = {
        'Central and Western': (22.2783, 114.1747),
        'Eastern': (22.2783, 114.2247),
        'Southern': (22.2483, 114.1547),
        'Wan Chai': (22.2783, 114.1747),
        'Sham Shui Po': (22.3283, 114.1547),
        'Kowloon City': (22.3283, 114.1947),
        'Kwun Tong': (22.3283, 114.2347),
        'Wong Tai Sin': (22.3483, 114.1947),
        'Yau Tsim Mong': (22.3083, 114.1747),
        'Islands': (22.2783, 113.9447),
        'Kwai Tsing': (22.3583, 114.1347),
        'North': (22.4983, 114.1347),
        'Sai Kung': (22.3783, 114.2747),
        'Sha Tin': (22.3983, 114.1947),
        'Tai Po': (22.4483, 114.1747),
        'Tsuen Wan': (22.3783, 114.1147),
        'Tuen Mun': (22.3983, 114.0147),
        'Yuen Long': (22.4483, 114.0347)
    }
    
    # Handle None or empty district
    if not district or pd.isna(district):
        return None
    
    # Find closest district center
    min_distance = float('inf')
    district_lower = str(district).lower()
    
    for district_name, (center_lat, center_lon) in district_centers.items():
        if district_name.lower() in district_lower:
            distance = haversine_distance(lat, lon, center_lat, center_lon)
            min_distance = min(min_distance, distance)
    
    return min_distance if min_distance != float('inf') else None

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate the great circle distance between two points on Earth."""
    from math import radians, cos, sin, asin, sqrt
    
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    
    # Radius of earth in kilometers
    r = 6371
    
    return c * r

# ============ LOCATION-BASED MATCHING ============

def perform_location_based_matching(
    consolidated_df: pd.DataFrame,
    unmatched_df: pd.DataFrame,
    category: str,
    params: Dict[str, Any]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Perform location-based matching for buildings with coordinates.
    """
    logger.info(f"Performing location-based matching for {category} buildings")
    
    if unmatched_df.empty:
        return consolidated_df, unmatched_df
    
    # Filter buildings with valid coordinates
    geocoded_df = unmatched_df[
        unmatched_df['osm_lat'].notna() & 
        unmatched_df['osm_lon'].notna() &
        unmatched_df['in_hk_territory'] == True
    ].copy()
    
    if geocoded_df.empty:
        return consolidated_df, unmatched_df
    
    # Distance threshold for matching (in km)
    distance_threshold = params.get('location_matching_distance_km', 0.5)
    
    matched_buildings = []
    unmatched_buildings = []
    
    for _, building in geocoded_df.iterrows():
            # Find closest building in consolidated database
            closest_match = find_closest_building(
            building['osm_lat'], 
            building['osm_lon'], 
            consolidated_df, 
            distance_threshold
            )
            
            if closest_match is not None:
            # Update consolidated building with new information
            updated_building = update_consolidated_building(closest_match, building)
            matched_buildings.append(updated_building)
        else:
            # Create new consolidated building
            new_consolidated = create_new_consolidated_building(building, category)
            matched_buildings.append(new_consolidated)
    
    # Update consolidated database
    if matched_buildings:
        new_consolidated_df = pd.DataFrame(matched_buildings)
        consolidated_df = pd.concat([consolidated_df, new_consolidated_df], ignore_index=True)
    
    # Remaining unmatched buildings
    remaining_unmatched = unmatched_df[
        ~unmatched_df.index.isin(geocoded_df.index)
    ]
    
    return consolidated_df, remaining_unmatched

def find_closest_building(
    target_lat: float, 
    target_lon: float, 
    candidates_df: pd.DataFrame, 
    max_distance: float
) -> Optional[pd.Series]:
    """Find the closest building within the specified distance."""
    min_distance = float('inf')
    closest_building = None
    
    for _, building in candidates_df.iterrows():
        if pd.notna(building.get('osm_lat')) and pd.notna(building.get('osm_lon')):
            distance = haversine_distance(
                target_lat, target_lon, 
                building['osm_lat'], building['osm_lon']
            )
            
            if distance <= max_distance and distance < min_distance:
            min_distance = distance
                closest_building = building
    
    return closest_building

def update_consolidated_building(existing: pd.Series, new_building: pd.Series) -> Dict[str, Any]:
    """Update existing consolidated building with new information."""
    updated = existing.to_dict()
    
    # Add new original name if not already present
    if new_building['original_name'] not in updated['original_names']:
        updated['original_names'].append(new_building['original_name'])
    
    # Add new source if not already present
    if new_building['source'] not in updated['sources']:
        updated['sources'].append(new_building['source'])
    
    # Update confidence score
    updated['confidence_score'] = (updated['confidence_score'] + new_building['confidence_score']) / 2
    
    # Update match type
    updated['match_type'] = 'location_based'
    updated['updated_at'] = datetime.now()
    
    return updated

def create_new_consolidated_building(building: pd.Series, category: str) -> Dict[str, Any]:
    """Create a new consolidated building record."""
    return {
        'building_id': f"{category}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{building.name}",
        'canonical_name': building['building_name'],
        'original_names': [building['original_name']],
        'addresses': [building.get('address', '')],
        'canonical_district': building['district'],
        'sources': [building['source']],
        'property_type': building.get('property_type', ''),
        'category': category,
        'confidence_score': building['confidence_score'],
        'match_type': 'new',
        'osm_lat': building.get('osm_lat'),
        'osm_lon': building.get('osm_lon'),
        'osm_location_type': building.get('osm_location_type'),
        'in_hk_territory': building.get('in_hk_territory'),
        'district_distance_km': building.get('district_distance_km'),
        'created_at': datetime.now(),
        'updated_at': datetime.now()
    }

# ============ UTILITY FUNCTIONS ============

def clean_building_name(name: str) -> str:
    """Clean and standardize building name."""
    if pd.isna(name) or not name:
        return ""
    
    # Convert to string and strip whitespace
    name = str(name).strip()
    
    # Handle Chinese/English mixed addresses
    # Extract English part before Chinese characters
    english_pattern = r'^([A-Za-z0-9\s\-\.]+?)(?=[\u4e00-\u9fff]|$)'
    match = re.match(english_pattern, name)
    
    if match:
        english_part = match.group(1).strip()
        
        # Check if this is a street address (starts with number)
        if english_part and english_part[0].isdigit():
            # This is likely a street address, try to extract building name
            parts = english_part.split()
            
            # Remove street indicators
            street_indicators = ['Street', 'Road', 'Avenue', 'Lane', 'Path', 'Terrace', 'Drive', 'Way']
            for indicator in street_indicators:
                if english_part.endswith(indicator):
                    # If it's just a street address, return empty (will be filtered out)
                    if len(parts) <= 2:
                        return ""
                    # Try to extract building name after street
                    return ' '.join(parts[2:])
            
            # If no street indicator found, return the full English part
            return english_part
        
        return english_part
    
    # Remove common suffixes/prefixes for non-street addresses
    suffixes_to_remove = [
        ' tower', ' plaza', ' centre', ' center', ' building', ' complex',
        ' estate', ' garden', ' court', ' mansion', ' house', ' residence'
    ]
    
    for suffix in suffixes_to_remove:
        if name.lower().endswith(suffix.lower()):
            name = name[:-len(suffix)]
    
    # Remove extra whitespace
    name = ' '.join(name.split())
    
    return name

def load_geocoding_cache(cache_file: str) -> Dict[str, Tuple[float, float, str]]:
    """Load geocoding cache from file."""
    try:
        with open(cache_file, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_geocoding_cache(cache: Dict[str, Tuple[float, float, str]], cache_file: str):
    """Save geocoding cache to file."""
    os.makedirs(os.path.dirname(cache_file), exist_ok=True)
    with open(cache_file, 'w') as f:
        json.dump(cache, f)

# ============ GOOGLE MAPS API INTEGRATION (FOR INITIALIZATION) ============

def initialize_with_google_maps(
    unmatched_buildings: pd.DataFrame,
    params: Dict[str, Any]
) -> pd.DataFrame:
    """
    Initialize unmatched buildings using Google Maps API (expensive, use sparingly).
    This should only be used during initial setup.
    """
    logger.info("Initializing unmatched buildings with Google Maps API")
    
    if not GEOPY_AVAILABLE:
        logger.warning("Geopy not available, skipping Google Maps geocoding")
        return unmatched_buildings
    
    # Check if Google API key is available
    google_api_key = params.get('google_maps_api_key')
    if not google_api_key:
        logger.warning("Google Maps API key not provided, skipping Google geocoding")
        return unmatched_buildings
    
    # Initialize Google geocoder
    geolocator = GoogleV3(api_key=google_api_key)
    
    # Load Google geocoding cache
    cache_file = params.get('google_geocoding_cache_file', 'data/03_primary/google_geocoding_cache.json')
    cache = load_geocoding_cache(cache_file)
    
    results = []
    
    for idx, row in tqdm(unmatched_buildings.iterrows(), total=len(unmatched_buildings), desc="Google Maps geocoding"):
        address = build_geocoding_address(row)
        
        # Check cache first
        if address in cache:
            lat, lon, location_type = cache[address]
        else:
            lat, lon, location_type = geocode_single_address_google(address, geolocator)
            cache[address] = (lat, lon, location_type)
            
            # Save cache periodically
            if idx % 10 == 0:  # Save more frequently for Google API
                save_geocoding_cache(cache, cache_file)
        
        # Create result row
        result_row = row.copy()
        result_row['google_lat'] = lat
        result_row['google_lon'] = lon
        result_row['google_location_type'] = location_type
        
        results.append(result_row)
    
    # Save final cache
    save_geocoding_cache(cache, cache_file)
    
    return pd.DataFrame(results)

def geocode_single_address_google(
    address: str, 
    geolocator, 
    max_retries: int = 3
) -> Tuple[Optional[float], Optional[float], str]:
    """Geocode a single address using Google Maps API."""
    for attempt in range(max_retries):
        try:
            location = geolocator.geocode(f"{address}, Hong Kong", timeout=10)
            if location:
                return location.latitude, location.longitude, "google"
            else:
                return None, None, "not_found"
            except Exception as e:
            if attempt == max_retries - 1:
                logger.warning(f"Failed to geocode {address} with Google: {e}")
                return None, None, "error"
            time.sleep(random.uniform(1, 3))
    
    return None, None, "error"

# ============ MANUAL INTERVENTION NODES ============

def create_manual_review_list(
    unmatched_commercial: pd.DataFrame,
    unmatched_residential: pd.DataFrame,
    params: Dict[str, Any]
) -> pd.DataFrame:
    """
    Create a list of buildings that need manual review.
    """
    logger.info("Creating manual review list")
    
    # Combine unmatched buildings
    all_unmatched = pd.concat([
        unmatched_commercial.assign(category='commercial'),
        unmatched_residential.assign(category='residential')
    ], ignore_index=True)
    
    # Add review flags
    all_unmatched['needs_manual_review'] = True
    all_unmatched['review_priority'] = all_unmatched['confidence_score'].apply(
        lambda x: 'high' if x < 0.5 else 'medium' if x < 0.8 else 'low'
    )
    
    # Sort by priority and confidence score
    all_unmatched = all_unmatched.sort_values(
        ['review_priority', 'confidence_score'], 
        ascending=[True, False]
    )
    
    return all_unmatched

def apply_manual_corrections(
    consolidated_commercial: pd.DataFrame,
    consolidated_residential: pd.DataFrame,
    manual_corrections: pd.DataFrame,
    params: Dict[str, Any]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Apply manual corrections to the consolidated databases.
    """
    logger.info("Applying manual corrections")
    
    # This function would apply corrections from a manually reviewed dataset
    # For now, return the original dataframes
    # Implementation would depend on the format of manual corrections
    
    return consolidated_commercial, consolidated_residential 