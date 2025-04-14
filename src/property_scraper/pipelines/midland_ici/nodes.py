###########################################

import pandas as pd
import requests
from tqdm import tqdm
import time
import logging
import os
from typing import Dict, List, Optional, Union, Any

def scrape_midland_buildings(
    area_codes: pd.DataFrame,
    params: Dict[str, Any],
    # csv_path: str, = inpuit
    # output_path: str = "midland_buildings.csv", = output
    log_level: int = logging.INFO,
) -> pd.DataFrame:
    """
    Scrape building information from Midland ICI GraphQL API for all districts
    and property types (Industrial, Office, Shop).
    
    Args:
        csv_path (str): Path to the CSV file containing district IDs
        output_path (str): Path where the output CSV will be saved
        request_delay (float): Delay between requests in seconds to avoid rate limiting
        max_retries (int): Maximum number of retries for failed requests
        log_level (int): Logging level (e.g., logging.INFO, logging.DEBUG)
        save_incremental (bool): Whether to save incremental results
        incremental_save_frequency (int): How often to save incremental results
        resume_from_existing (bool): Whether to resume from existing output file
        
    Returns:
        pd.DataFrame: DataFrame containing all scraped building information
    """
    # Configure logging
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger("midland_scraper")
    
    # Define property types
    property_types = {
        "mr_ind": "Industrial",
        "mr_comm": "Office",
        "mr_shop": "Shop"
    }
    
    # Define the GraphQL endpoint
    url = params['buildings_url']
    
    # Define the headers
    headers = params['headers']
    
    # Read the CSV file with district IDs
    # Read district information
    listing_file = params.get('midland_ici_building_listings', 'data/02_intermediate/midland_ici_buildings.parquet')
    
    try:
        logger.info(f"Successfully loaded {len(area_codes)} districts from area code file.")
    except Exception as e:
        logger.error(f"Failed to load area code CSV file: {str(e)}")
        return pd.DataFrame()
    
    # Filter out the "All Districts" row (ID=0)
    area_codes = area_codes[area_codes['ID'] != 0]
    logger.info(f"Filtered to {len(area_codes)} districts (excluding 'All Districts')")
    
    # Check if we should resume from existing file
    already_scraped = set()
    existing_buildings_df = pd.DataFrame()
    
    if params['resume_from_existing'] and os.path.exists(listing_file):
        try:
            existing_buildings_df = pd.read_csv(listing_file)
            logger.info(f"Found existing output file with {len(existing_buildings_df)} buildings")
            
            # Create a set of already scraped district-property type combinations
            if 'district_id' in existing_buildings_df.columns and 'property_type_code' in existing_buildings_df.columns:
                already_scraped = set(
                    existing_buildings_df[['district_id', 'property_type_code']].drop_duplicates().apply(
                        lambda x: f"{x['district_id']}_{x['property_type_code']}", axis=1
                    )
                )
                logger.info(f"Found {len(already_scraped)} already scraped district-property type combinations")
        except Exception as e:
            logger.warning(f"Could not read existing output file: {str(e)}. Starting from scratch.")
    
    # Initialize an empty list to store all building data
    all_buildings = []
    
    # Keep track of how many combinations we've processed for incremental saving
    processed_count = 0
    
    # Calculate total iterations for tqdm
    total_iterations = len(area_codes) * len(property_types)
    
    # Create a progress bar
    with tqdm(
        total=total_iterations,
        desc="Scraping progress",
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}"
    ) as pbar:
        for _, district in area_codes.iterrows():
            district_id = district['ID']
            district_name_en = district['Name_EN']
            district_name_cn = district['Name_CN']
            
            for sbu, property_type in property_types.items():
                # Update progress bar suffix
                pbar.set_postfix({
                    'District': district_name_en,
                    'Type': property_type
                })
                pbar.refresh()  # Force immediate update

                # Check if this combination has already been scraped
                combo_key = f"{district_id}_{sbu}"
                if combo_key in already_scraped:
                    #logger.info(f"Skipping already scraped {property_type} in {district_name_en}")
                    pbar.update(1)
                    continue
                
                # Update progress bar description
                #pbar.set_description(f"Scraping {property_type} in {district_name_en}")
                
                # Define the GraphQL query and variables
                payload = {
                    "query": """
                        query ($districtId: ID, $query: String, $sbu: String) {
                          buildings(districtId: $districtId, nameSearch: $query, sbu: $sbu) {
                            sbu
                            id
                            nameEn
                            nameZh
                            addressEn
                            addressZh
                            __typename
                          }
                        }
                    """,
                    "variables": {
                        "sbu": sbu,
                        "districtId": district_id,
                        "query": ""
                    }
                }
                
                # Implement retry mechanism
                retries = 0
                success = False
                
                while retries < params['max_retries'] and not success:
                    try:
                        # Make the POST request
                        response = requests.post(url, json=payload, headers=headers)
                        
                        # Check if the request was successful
                        if response.status_code == 200:
                            data = response.json()
                            
                            # Check if there are buildings in the response
                            if 'data' in data and 'buildings' in data['data']:
                                buildings = data['data']['buildings']
                                
                                if buildings:
                                    #logger.info(f"Found {len(buildings)} {property_type} buildings in {district_name_en}")
                                    
                                    # Add district and property type information to each building
                                    for building in buildings:
                                        building['district_id'] = district_id
                                        building['district_name_en'] = district_name_en
                                        building['district_name_cn'] = district_name_cn
                                        building['property_type'] = property_type
                                        building['property_type_code'] = sbu
                                        
                                        # Add to the list of all buildings
                                        all_buildings.append(building)
                                #else:
                                    #logger.info(f"No {property_type} buildings found in {district_name_en}")
                                
                                success = True
                            else:
                                logger.warning(f"Unexpected response structure for {district_name_en}, {property_type}")
                                retries += 1
                        else:
                            logger.warning(f"Request failed for {district_name_en}, {property_type} with status code {response.status_code}")
                            retries += 1
                    
                    except Exception as e:
                        logger.error(f"Error occurred for {district_name_en}, {property_type}: {str(e)}")
                        retries += 1
                    
                    # If this isn't the last retry and we haven't succeeded, wait before retrying
                    if retries < params['max_retries'] and not success:
                        time.sleep(params['request_delay'] * 2)  # Longer delay for retries
                
                # Update processed count
                processed_count += 1
                
                # Save incremental results if needed
                if params['save_incremental'] and processed_count % params['incremental_save_frequency'] == 0:
                    _save_incremental_results(
                        all_buildings, existing_buildings_df, listing_file, logger
                    )
                
                # Update progress bar
                pbar.update(1)
                
                # Add a small delay to avoid rate limiting
                time.sleep(params['request_delay'])
    
    # Convert the list of buildings to a DataFrame
    buildings_df = _process_and_save_results(
        all_buildings, existing_buildings_df, listing_file, logger
    )
    
    return buildings_df

def _process_and_save_results(
    new_buildings: List[Dict[str, Any]], 
    existing_buildings_df: pd.DataFrame,
    output_path: str,
    logger: logging.Logger
) -> pd.DataFrame:
    """
    Process new buildings data, combine with existing data if any, and save to CSV.
    
    Args:
        new_buildings: List of new building dictionaries
        existing_buildings_df: DataFrame of existing buildings (can be empty)
        output_path: Path to save the final CSV
        logger: Logger instance
        
    Returns:
        Combined DataFrame of all buildings
    """
    if not new_buildings and existing_buildings_df.empty:
        logger.warning("No buildings found in any district for any property type")
        return pd.DataFrame()
    
    # Convert new buildings to DataFrame
    if new_buildings:
        new_buildings_df = pd.DataFrame(new_buildings)
        
        # Add suffix information for clarity
        new_buildings_df['district_property_type'] = new_buildings_df.apply(
            lambda row: f"{row['district_name_en']}_{row['property_type']}", axis=1
        )
    else:
        new_buildings_df = pd.DataFrame()
    
    # Combine with existing data if there is any
    if not existing_buildings_df.empty and not new_buildings_df.empty:
        # Ensure 'district_property_type' exists in existing data
        if 'district_property_type' not in existing_buildings_df.columns:
            existing_buildings_df['district_property_type'] = existing_buildings_df.apply(
                lambda row: f"{row['district_name_en']}_{row['property_type']}" 
                if 'district_name_en' in existing_buildings_df.columns and 'property_type' in existing_buildings_df.columns 
                else "", axis=1
            )
        
        # Combine DataFrames
        combined_df = pd.concat([existing_buildings_df, new_buildings_df], ignore_index=True)
        
        # Remove duplicates based on building ID and property type
        if 'id' in combined_df.columns and 'property_type_code' in combined_df.columns:
            combined_df = combined_df.drop_duplicates(subset=['id', 'property_type_code'])
    elif not new_buildings_df.empty:
        combined_df = new_buildings_df
    else:
        combined_df = existing_buildings_df
    
    return combined_df

def _save_incremental_results(
    new_buildings: List[Dict[str, Any]], 
    existing_buildings_df: pd.DataFrame,
    output_path: str,
    logger: logging.Logger
) -> None:
    """
    Save incremental results to avoid data loss if the process is interrupted.
    
    Args:
        new_buildings: List of new building dictionaries
        existing_buildings_df: DataFrame of existing buildings (can be empty)
        output_path: Path to save the incremental CSV
        logger: Logger instance
    """
    if not new_buildings:
        logger.debug("No new buildings to save incrementally")
        return
    
    try:
        # Process and save results
        _process_and_save_results(
            new_buildings, existing_buildings_df, output_path, logger
        )
        #logger.info(f"Saved incremental results with {len(new_buildings)} new buildings")
    except Exception as e:
        logger.error(f"Failed to save incremental results: {str(e)}")

###########################################################################

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import os
import time

def clean_for_url(text):
    """Clean text for URL formatting by removing apostrophes and replacing spaces with hyphens."""
    text = text.replace("'", "")  # Remove apostrophes
    return '-'.join(text.split()).strip()

def construct_url(row):
    """Construct the URL for detailed scraping based on building information."""
    return f"https://www.midlandici.com.hk/ics/property/{row['__typename'].lower()}/details/{row['id']}/{clean_for_url(row['nameEn'])}?lang=english"

def scrape_with_requests(row):
    """Scrape building details using requests library."""
    try:
        url = construct_url(row)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        building_info = {
            'id': row['id'],
            'Building Name': row['nameEn'],
            'URL': url
        }

        # Extract meta information
        for block in soup.find_all('div', class_='meta-info-container'):
            title = block.find('div', class_='title')
            content = block.find('div', class_='content')
            
            if title and content:
                key = title.text.strip()
                value = content.text.strip()
                building_info[key] = value
                
        return building_info
    except Exception as e:
        print(f"Request failed for {row['nameEn']}: {str(e)}")
        return None

def process_buildings(
    building_listings: pd.DataFrame,#input_csv
    params:Dict[str, Any], 
    #, output_file)
    ) -> pd.DataFrame:
    """
    Process buildings with GET requests and save results to output file.
    
    Args:
        input_csv (str): Path to the input CSV file.
        output_file (str): Path to the output CSV file.
    
    Returns:
        pd.DataFrame: DataFrame containing the scraped information.
    """
    
    details_file = params.get('midland_ici_building_details', 'data/02_intermediate/midland_ici_building_details.parquet')

    
    # Check existing data
    existing_ids = set()
    if os.path.exists(details_file):
        existing_df = pd.read_csv(details_file)
        existing_ids = set(existing_df['id']) if 'id' in existing_df.columns else set()
    
    # Filter out buildings already in the output file
    df_to_scrape = building_listings[~building_listings['id'].isin(existing_ids)]
    
    if len(df_to_scrape) == 0:
        print("All buildings are already scraped. Nothing to do.")
        if os.path.exists(details_file):
            return pd.read_csv(details_file)
        else:
            return pd.DataFrame()
    
    results = []
    
    # Process buildings with tqdm progress bar and dynamic suffix
    with tqdm(total=len(df_to_scrape), desc="Processing buildings", unit="building") as pbar:
        for _, row in df_to_scrape[:5].iterrows():
            # Update tqdm suffix with building name only
            pbar.set_postfix_str(f"Building: {row['nameEn']}")
            
            result = scrape_with_requests(row)
            if result:
                results.append(result)
            
            # Add a small delay to avoid overwhelming the server
            time.sleep(1)
            
            pbar.update(1)

    # Save results to output file
    if results:
        df_results = pd.DataFrame(results)
        
        # Append new data to existing file or create a new one
        if os.path.exists(details_file):
            df_existing = pd.read_csv(details_file)
            df_combined = pd.concat([df_existing, df_results], ignore_index=True)
            #df_combined.to_csv(details_file, index=False)
            print(f"Added {len(df_results)} new records to {details_file}")
            return df_combined
        else:
            #df_results.to_csv(details_file, index=False)
            print(f"Saved {len(df_results)} buildings to {details_file}")
            return df_results

