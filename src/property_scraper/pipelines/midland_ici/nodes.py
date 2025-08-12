###########################################

import pandas as pd
import requests
from tqdm import tqdm
import time
import logging
import os
from typing import Dict, List, Optional, Union, Any
from datetime import datetime

# Import node tracking utilities
from ...utils.node_tracker import should_run_node, record_node_execution

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
    Includes node execution tracking to avoid re-running within configurable days.
    
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
    
    # Check if node should be run based on last execution date
    node_name = "scrape_midland_buildings"
    tracking_params = params.get('node_tracking', {})
    if not should_run_node(node_name, "building", tracking_params):
        logger.info(f"Node '{node_name}' last run within configured days - returning existing data")
        # Return existing data if available
        listing_file = params['midland_ici']['midland_ici_building_listings']
        if os.path.exists(listing_file):
            try:
                return pd.read_parquet(listing_file, engine='pyarrow')
            except Exception as e:
                logger.warning(f"Failed to load existing building listings: {e}")
                return pd.DataFrame()
        return pd.DataFrame()
    
    # Define property types
    property_types = {
        "mr_ind": "Industrial",
        "mr_comm": "Office",
        "mr_shop": "Shop"
    }
    
    # Define the GraphQL endpoint
    url = params['midland_ici']['buildings_url']
    
    # Define the headers
    headers = params['midland_ici']['headers']
    
    # Read the CSV file with district IDs
    # Read district information
    listing_file = params['midland_ici']['midland_ici_building_listings']
    
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
    
    if params['midland_ici']['resume_from_existing'] and os.path.exists(listing_file):
        try:
            existing_buildings_df = pd.read_parquet(listing_file, engine='pyarrow')
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
                max_retries = params['midland_ici']['max_retries']
                request_delay = params['midland_ici']['request_delay']
                while retries < max_retries and not success:
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
                    if retries < max_retries and not success:
                        time.sleep(request_delay * 2)  # Longer delay for retries
                
                # Update processed count
                processed_count += 1
                
                # Save incremental results if needed
                if params['midland_ici']['save_incremental'] and processed_count % params['midland_ici']['incremental_save_frequency'] == 0:
                    _save_incremental_results(
                        all_buildings, existing_buildings_df, listing_file, logger
                    )
                
                # Update progress bar
                pbar.update(1)
                
                # Add a small delay to avoid rate limiting
                time.sleep(request_delay)
    
    # Convert the list of buildings to a DataFrame
    buildings_df = _process_and_save_results(
        all_buildings, existing_buildings_df, listing_file, logger
    )
    
    # Record node execution
    record_node_execution(
        node_name="scrape_midland_buildings",
        node_type="building",
        metadata={
            "buildings_processed": len(buildings_df),
            "districts_processed": len(area_codes),
            "property_types_processed": len(property_types),
            "execution_time": datetime.now().isoformat()
        }
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
    
    # Save the combined DataFrame to the output file
    if not combined_df.empty:
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            combined_df.to_parquet(output_path, engine='pyarrow', index=False)
            logger.info(f"Saved {len(combined_df)} buildings to {output_path}")
        except Exception as e:
            logger.error(f"Failed to save buildings to {output_path}: {str(e)}")
    
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
    text = text.replace("+", "-")
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
            print("Checking Title:", title)
            print("Checking Title:", content)
            
            if title and content:
                key = title.text.strip()
                value = content.text.strip()
                building_info[key] = value
                
        return building_info
    except Exception as e:
        print(f"Request failed for {row['nameEn']}: {str(e)}")
        return None

def process_buildings(
    building_listings: pd.DataFrame,
    params: Dict[str, Any]
) -> pd.DataFrame:
    """
    Revised implementation with JSON data parsing for completion date
    """
    import time
    import pandas as pd
    import chromedriver_autoinstaller
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from bs4 import BeautifulSoup
    from tqdm import tqdm
    import os
    import logging
    import json
    import re
    from dateutil import parser
    
    logger = logging.getLogger(__name__)
    
    # Initialize driver once per execution
    def initialize_driver():
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options as ChromeOptions
        try:
            # Switchable by params:webscraper.global.use_edge
            use_edge = params.get('global', {}).get('use_edge', False)
        except Exception:
            use_edge = False

        if use_edge:
            import edgedriver_autoinstaller
            edgedriver_autoinstaller.install()
            opts = webdriver.EdgeOptions()
            opts.use_chromium = True
            opts.add_argument("--headless=new")
            opts.add_argument("--no-sandbox")
            opts.add_argument("--disable-dev-shm-usage")
            return webdriver.Edge(options=opts)
        else:
            import chromedriver_autoinstaller
            chromedriver_autoinstaller.install()
            opts = ChromeOptions()
            opts.add_argument("--headless=new")
            opts.add_argument("--no-sandbox")
            opts.add_argument("--disable-dev-shm-usage")
            return webdriver.Chrome(options=opts)
    
    driver = initialize_driver()

    # URL construction matching notebook
    def construct_url(row):
        base_url = "https://www.midlandici.com.hk/ics/property/"
        return f"{base_url}{row['__typename'].lower()}/details/{row['id']}/{row['nameEn'].replace(' ', '-')}?lang=english"

    # Core scraping function with JSON extraction
    def scrape_building_info(row):
        try:
            url = construct_url(row)
            driver.get(url)
            time.sleep(1.5)  # Allow JavaScript execution
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            info = {
                'id': row['id'],
                'Building Name': row['nameEn'],
                'URL': url
            }
            
            # Extract JSON data from script tag
            script_tag = soup.find('script', {'type': 'application/json'})
            if script_tag:
                try:
                    json_data = json.loads(script_tag.string)
                    building_data = json_data['props']['pageProps']['building']
                    
                    # Extract completion dates
                    info['Completion Date Raw'] = building_data.get('inTakeDate')
                    info['Completion Date Format'] = building_data.get('inTakeDateFormat')
                    
                    # Parse datetime with timezone handling
                    if info['Completion Date Raw']:
                        dt_str = info['Completion Date Raw'].split(' (')[0]  # Remove timezone name
                        info['Completion Date'] = parser.parse(dt_str).isoformat()
                except Exception as e:
                    logger.error(f"JSON parsing error: {str(e)[:100]}")

            # Existing meta-info parsing (keep for other fields)
            for block in soup.find_all('div', class_='meta-info-container'):
                title = block.find('div', class_='title')
                content = block.find('div', 'content')
                icon = block.find('div', 'icon')
                
                if title:
                    key = title.text.strip()
                    value = content.text.strip() if content else "N/A"
                    info[key] = value
                    
            return info
        except Exception as e:
            logger.error(f"Error scraping {row['nameEn']}: {str(e)[:100]}...")
            return None

    # Rest of the original implementation remains unchanged
    details_file = params.get('midland_ici_building_details', 'data/02_intermediate/midland_ici_building_details.parquet')
    existing_ids = set()
    existing_df = pd.DataFrame()
    
    if os.path.exists(details_file) and params.get('resume_from_existing', True):
        try:
            existing_df = pd.read_parquet(details_file, engine='pyarrow')
            existing_ids = set(existing_df['id']) if 'id' in existing_df.columns else set()
            logger.info(f"Found existing output file with {len(existing_df)} buildings")
        except Exception as e:
            logger.warning(f"Could not read existing output file: {str(e)}. Starting from scratch.")
    
    new_listings = building_listings[~building_listings['id'].isin(existing_ids)]
    if new_listings.empty:
        logger.info("All buildings already processed")
        return existing_df
    
    results = []
    try:
        with tqdm(total=len(new_listings), desc="Processing buildings") as pbar:
            for _, row in new_listings.iterrows():
                result = scrape_building_info(row)
                if result:
                    results.append(result)
                    if len(results) % 5 == 0:
                        pd.DataFrame(results).to_parquet(details_file, engine='pyarrow')
                pbar.update(1)
                
    except KeyboardInterrupt:
        logger.warning("User interrupt detected! Saving partial results...")
        pd.DataFrame(results).to_parquet(details_file.replace('.parquet', '_PARTIAL.parquet'), engine='pyarrow')
        
    finally:
        driver.quit()
    
    if results:
        final_df = pd.concat([existing_df, pd.DataFrame(results)], ignore_index=True)
        final_df.to_parquet(details_file, engine='pyarrow')
        return final_df
    
    return existing_df



####################################################################################
# Transaction

import requests
import time
import random
from dateutil.relativedelta import relativedelta
from datetime import datetime
from tqdm import tqdm
import json
import pandas as pd

def sanitize_parquet_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and normalize DataFrame columns for Parquet compatibility"""
    # Clean column names
    df.columns = df.columns.str.replace(r'[^a-zA-Z0-9_]', '_', regex=True)
    
    # Handle special columns
    if 'floor' in df.columns:
        df['floor'] = (
            df['floor']
            .astype(str)
            .str.replace(r'[\x00-\x1F\x7F-\x9F/]', '', regex=True)
            .str.strip()
        )
    
    # Convert object columns to string
    obj_cols = df.select_dtypes('object').columns
    df[obj_cols] = df[obj_cols].astype('string')
    
    return df

def ml_ici_scrape_trans(
    params: Dict[str, Any]) -> pd.DataFrame:
    """
    Scrape all transaction data from Midland ICI website and return as pandas DataFrame.
    Includes node execution tracking to avoid re-running on the same day.
    """
    
    # Initialize logging first
    logger = logging.getLogger(__name__)
    
    # Check if node should be run based on max date in dataset
    node_name = "ml_ici_scrape_trans"
    output_file = "data/01_raw/midland_ici_trans.parquet"
    tracking_params = params.get('node_tracking', {})
    
    if not should_run_node(node_name, "transaction", tracking_params, output_file):
        logger.info(f"Node '{node_name}' - dataset is up to date - returning existing data")
        if os.path.exists(output_file):
            try:
                return pd.read_parquet(output_file)
            except Exception as e:
                logger.warning(f"Failed to load existing transaction data: {e}")
                return pd.DataFrame()
        return pd.DataFrame()
    all_transactions = []
    
    # Generate month ranges
    month_ranges = []
    start_date = params['global']['start_date']
    current = datetime.now()
    
    # Convert string dates to datetime objects
    if isinstance(start_date, str):
        start_date = datetime.fromisoformat(start_date)
        
    while current >= start_date:
        month_end = current
        month_start = current.replace(day=1)
        month_ranges.append((month_start.strftime("%Y-%m-%d"), month_end.strftime("%Y-%m-%d")))
        current = month_start - relativedelta(days=1)
    
    # Scrape data for each month range
    with tqdm(month_ranges, desc="Processing Trans", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]") as month_pbar:
        for date_min, date_max in month_pbar:
            month_data = []
            cursor = 1
            
            while True:
                try:
                    response = requests.get(
                        params['midland_ici']['transaction_url'],
                        headers=params['midland_ici']['headers'],
                        params={
                            "ics_type": "",
                            "date_min": date_min,
                            "date_max": date_max,
                            "lang": "english",
                            "page_size": params['midland_ici']['max_page_size'],
                            "cursor": cursor,
                            "order": "tx_date-desc"
                        }
                    )
                    response.raise_for_status()
                    data = response.json()

                    if not data.get('transactions'):
                        break

                    # Update transaction tracking
                    month_data.extend(data['transactions'])
                    current_total = len(all_transactions) + len(month_data)
                    month_pbar.set_postfix_str(f"Trans: {current_total}")

                    # Pagination control
                    if (len(month_data) >= data.get('count', 0) or 
                        len(data['transactions']) < params['midland_ici']['max_page_size']):
                        break

                    cursor += 1
                    time.sleep(random.uniform(0.5, 1.5))

                except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                    print(f"Error: {e}. Retrying...")
                    time.sleep(5)
                    continue

            all_transactions.extend(month_data)
            
    trans_df = pd.DataFrame(all_transactions)
        
    # Add sanitization step
    trans_df = sanitize_parquet_columns(trans_df)
    
    # Ensure UTF-8 encoding
    trans_df = trans_df.map(
        lambda x: x.encode('utf-8', 'ignore').decode('utf-8') 
        if isinstance(x, str) else x
    )
    
    # Save the transaction data to file
    if not trans_df.empty:
        try:
            # Get the output file path from catalog
            output_file = "data/01_raw/midland_ici_trans.parquet"
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            trans_df.to_parquet(output_file, engine='pyarrow', index=False)
            logger.info(f"Saved {len(trans_df)} transactions to {output_file}")
        except Exception as e:
            logger.error(f"Failed to save transactions to {output_file}: {str(e)}")
    
    # Record node execution
    record_node_execution(
        node_name="ml_ici_scrape_trans",
        node_type="transaction",
        metadata={
            "records_processed": len(trans_df),
            "month_ranges_processed": len(month_ranges),
            "execution_time": datetime.now().isoformat()
        }
    )
    
    return trans_df


def midland_ici_join(
    transactions: pd.DataFrame,
    building_details: pd.DataFrame,
    params: Dict[str, Any],
) -> pd.DataFrame:
    """
    Join transaction data with building details.
    
    This function takes transaction data and building details and performs a join
    operation to create a unified dataset. The join operation is configurable
    through parameters, allowing for different join columns and join types.
    
    Args:
        transactions (pd.DataFrame): DataFrame containing transaction data (midland_ici_trans)
        building_details (pd.DataFrame): DataFrame containing building details (midland_ici_building_details)
        params (Dict[str, Any]): Parameters containing join configuration:
            - join_left_on: Column name in transactions DataFrame for join (default: 'building_id')
            - join_right_on: Column name in building_details DataFrame for join (default: 'id')
            - join_type: Type of join to perform (default: 'left')
            
    Returns:
        pd.DataFrame: A joined DataFrame containing transaction data enriched with building details
    """
    import logging
    logger = logging.getLogger(__name__)
    
    # Get join configuration from params or use defaults
    mici_params = params.get("midland_ici", {})
    join_params = mici_params.get("join", {})
    
    left_on = join_params.get('join_left_on', 'building_id')
    right_on = join_params.get('join_right_on', 'id')
    join_type = join_params.get('join_type', 'left')
    
    logger.info(f"Joining {len(transactions)} transactions with {len(building_details)} buildings")
    logger.info(f"Join configuration: {join_type} join with transactions.{left_on} and buildings.{right_on}")
    
    # Check if join keys exist in respective DataFrames
    if left_on not in transactions.columns:
        error_msg = f"Join key '{left_on}' missing from transactions DataFrame. Available columns: {list(transactions.columns)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    if right_on not in building_details.columns:
        error_msg = f"Join key '{right_on}' missing from building details DataFrame. Available columns: {list(building_details.columns)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Convert columns to common type
    if join_params.get('cast_join_keys', True):
        # Convert transaction ID to string
        transactions[left_on] = transactions[left_on].astype(str)
        # Convert building ID to string (if not already)
        building_details[right_on] = building_details[right_on].astype(str)
    
    # Verify type consistency
    if transactions[left_on].dtype != building_details[right_on].dtype:
        logger.error(f"Type mismatch after conversion: {transactions[left_on].dtype} vs {building_details[right_on].dtype}")
        raise TypeError("Join columns remain incompatible after type casting")
    
    
    # Perform merge
    joined_df = transactions.merge(
        building_details,
        left_on=left_on,
        right_on=right_on,
        how=join_type,
        suffixes=('', '_building')
    )
    
    logger.info(f"Join completed. Result has {len(joined_df)} rows")
    
    # Add a flag to indicate if a transaction has matched building details
    joined_df['has_building_match'] = joined_df[right_on].notnull()
    
    logger.info(f"Transactions with matching building details: {joined_df['has_building_match'].sum()}")
    
    return joined_df




