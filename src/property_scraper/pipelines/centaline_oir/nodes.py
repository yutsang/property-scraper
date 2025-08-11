import requests
import pandas as pd
from tqdm import tqdm
import time
import random
import os
from typing import Dict, Any, Tuple
from bs4 import BeautifulSoup
import re
import datetime
import logging
import urllib.parse
try:
    from rapidfuzz import fuzz, process
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False
    fuzz = None
    process = None

# Import node tracking utilities
from ...utils.node_tracker import should_run_node, record_node_execution

def scrape_building_listings(
    cl_oir_area_code: pd.DataFrame, 
    params: Dict[str, Any])  -> pd.DataFrame:
    """
    Scrape building listings from Centaline OIR.
    Includes node execution tracking to avoid re-running within 7 days.
    """
    
    # Initialize logging first
    logger = logging.getLogger(__name__)
    
    # Check if node should be run based on last execution date
    node_name = "scrape_building_listings"
    tracking_params = params.get('node_tracking', {})
    if not should_run_node(node_name, "estate", tracking_params):
        logger.info(f"Node '{node_name}' last run within configured days - returning existing data")
        # Return existing data if available
        listings_file = params['centaline_oir']['cl_oir_building_listings_file']
        if os.path.exists(listings_file):
            try:
                return pd.read_parquet(listings_file)
            except Exception as e:
                logger.warning(f"Failed to load existing building listings: {e}")
                return pd.DataFrame()
        return pd.DataFrame()

    # Read district information
    listings_file = params['centaline_oir']['cl_oir_building_listings_file']

    # Load existing data with backward compatibility
    existing_counts = {}
    if os.path.exists(listings_file):
        existing_df = pd.read_parquet(listings_file)
        existing_counts = existing_df['queriedDistrict'].value_counts().to_dict()

    # Initialize data storage
    data = []
    fields = [
        "queriedDistrict", "queriedCode", "propertyID", "buildingNameEn", 
        "address", "developers", "opDateDisplayName", "floorDisplayName", 
        "districtNameEn", "zoneEn", "sellCount", "rentCount", "transCount"
    ]

    # Configure progress bar with clear hierarchy
    with tqdm(total=len(cl_oir_area_code), desc="🏙️ Districts", position=0, mininterval=0.5, dynamic_ncols=True) as main_progress:
        for _, district_row in cl_oir_area_code.iterrows():
            district = district_row["District"]
            code = district_row["Code"]
            
            try:
                # Initial API check with random delay
                time.sleep(random.uniform(0.3, 1.2))
                initial_response = requests.get(
                    params['centaline_oir']['property_api_url'],
                    headers=params['centaline_oir']['headers'],
                    cookies=params['centaline_oir']['cookies'],
                    params={"PageSize": 1, "districtids": code, "lang": "EN"},
                    timeout=10
                )
                result_json = initial_response.json()
                total_count = result_json.get("data", {}).get("totalCount", 0)

                # Skip handling with immediate feedback
                if existing_counts.get(district, 0) == total_count:
                    main_progress.set_postfix_str(f"{district} ✓", refresh=True)
                    main_progress.update(1)
                    main_progress.refresh()
                    continue

                # Nested progress for pages
                with tqdm(total=total_count, desc=f"📄 {district}", position=1, leave=False, mininterval=0.3) as item_progress:
                    page_index = 1
                    collected = 0
                    
                    while True:
                        # Throttle requests
                        time.sleep(random.uniform(0.2, 0.8))
                        
                        # Fetch page
                        response = requests.get(
                            params['centaline_oir']['property_api_url'],
                            headers=params['centaline_oir']['headers'],
                            cookies=params['centaline_oir']['cookies'],
                            params={
                                "PageSize": 24,
                                "pageindex": page_index,
                                "districtids": code,
                                "lang": "EN"
                            }
                        )
                        result = response.json()
                        items = result.get("data", {}).get("items", [])

                        if not items:
                            break

                        # Process items
                        for item in items:
                            data.append({
                                "queriedDistrict": district,
                                "queriedCode": code,
                                "propertyID": item.get("propertyID"),
                                "buildingNameEn": item.get("buildingNameEn"),
                                "address": item.get("address"),
                                "developers": ", ".join(item.get("developers", [])),
                                "opDateDisplayName": item.get("opDateDisplayName"),
                                "floorDisplayName": item.get("floorDisplayName"),
                                "districtNameEn": item.get("areaInfo", {}).get("districtNameEn"),
                                "zoneEn": item.get("areaInfo", {}).get("zoneEn"),
                                "sellCount": item.get("sellCount"),
                                "rentCount": item.get("rentCount"),
                                "transCount": item.get("transCount")
                            })
                            collected += 1
                            
                            # Update item progress every 5 items
                            if collected % 5 == 0:
                                item_progress.update(5)
                                item_progress.set_postfix_str(f"Page {page_index}", refresh=True)
                        
                        # Final page update
                        item_progress.update(len(items) % 5)
                        item_progress.set_postfix_str(f"Page {page_index} ✔️", refresh=True)
                        page_index += 1

                    # Finalize district processing
                    item_progress.close()
                    main_progress.set_postfix_str(f"{district} {collected} items", refresh=True)
                
                main_progress.update(1)
                main_progress.refresh()
                
            except Exception as e:
                main_progress.set_postfix_str(f"🚨 {district}", refresh=True)
                tqdm.write(f"\nError processing {district}: {str(e)}")
                main_progress.update(1)
                continue

    # Save results
    new_df = pd.DataFrame(data)
    if os.path.exists(listings_file):
        combined_df = pd.concat([existing_df, new_df]).drop_duplicates(subset=["propertyID"])
        #combined_df.to_csv(existing_df, index=False)
        
        # Record node execution
        record_node_execution(
            node_name="scrape_building_listings",
            node_type="estate",
            metadata={
                "buildings_processed": len(combined_df),
                "districts_processed": len(cl_oir_area_code),
                "execution_time": datetime.datetime.now().isoformat()
            }
        )
        
        return combined_df

    else:
        #new_df.to_csv(existing_df, index=False)
        
        # Record node execution
        record_node_execution(
            node_name="scrape_building_listings",
            node_type="estate",
            metadata={
                "buildings_processed": len(new_df),
                "districts_processed": len(cl_oir_area_code),
                "execution_time": datetime.datetime.now().isoformat()
            }
        )
        
        return new_df
    
def scrape_building_details(
    building_listing_df: pd.DataFrame,
    params: Dict[str, Any]) -> pd.DataFrame:
    """
    Scrape building details from Centaline OIR with sophisticated anti-detection measures.
    Includes node execution tracking to avoid re-running within 7 days.
    """
    
    # Initialize logging first
    logger = logging.getLogger(__name__)
    
    # Check if node should be run based on last execution date
    node_name = "scrape_building_details"
    tracking_params = params.get('node_tracking', {})
    if not should_run_node(node_name, "estate", tracking_params):
        logger.info(f"Node '{node_name}' last run within configured days - returning existing data")
        # Return existing data if available
        details_file = params.get('centanet_oir_buildings_details', 'data/02_intermediate/centanet_oir_details.parquet')
        if os.path.exists(details_file):
            try:
                return pd.read_parquet(details_file, engine='pyarrow')
            except Exception as e:
                logger.warning(f"Failed to load existing building details: {e}")
                return pd.DataFrame()
        return pd.DataFrame()
    
    # Initialize persistent session for better anti-detection
    session = create_advanced_session()
    base_headers = create_realistic_headers()
    
    # Track session health
    session_requests = 0
    session_start_time = time.time()
    max_session_requests = 50  # Rotate session after 50 requests
    
    # Field mapping configuration
    detail_fields = {
        'District': 'district',
        'Address': 'full_address',
#        'Building_Name_CN': 'buildingNameCn',
        'Grade': 'grade',
        'Usage': 'usage',
        'Property Type': 'property_type',
        'Year of Completion': 'completion_year',
        'Title': 'title_status',
        'Management Company': 'management_company',
        'Developers': 'developers',
        'Transportation': 'transportation',
        'Floor': 'total_floors',
        'Floor Area': 'floor_area',
        'Height': 'ceiling_height',
        'A/C System': 'ac_system',
        'No. of Lift': 'lifts',
        'Carpark': 'carpark'
    }     
    
    # Read district information
    details_file = params.get('centanet_oir_buildings_details', 'data/02_intermediate/centanet_oir_details.parquet')

    # Load existing data if available
    existing_data = pd.DataFrame()
    scraped_ids = set()
    if os.path.exists(details_file):
        existing_data = pd.read_parquet(details_file, engine='pyarrow')
        scraped_ids = set(existing_data['property_id'].astype(str))

    # Prepare filtered input dataframe
    building_listing_df = building_listing_df.copy()
    building_listing_df['propertyID'] = building_listing_df['propertyID'].astype(str)
    todo_df = building_listing_df[~building_listing_df['propertyID'].isin(scraped_ids)]
    
    results = []
    timeout_count = 0
    max_timeouts = 5  # Stop after 5 consecutive timeouts
    success_count = 0
    error_count = 0
    total_processed = 0
    
    progress_bar = tqdm(todo_df.iterrows(), total=len(todo_df), desc="Scraping Details")
    for _, row in progress_bar:
        total_processed += 1
        try:
            # Session rotation logic
            session_requests += 1
            if session_requests >= max_session_requests or (time.time() - session_start_time) > 300:  # 5 minutes
                session = create_advanced_session()
                session_requests = 0
                session_start_time = time.time()
                time.sleep(random.uniform(2, 4))  # Brief pause during session rotation
            
            # URL sanitization
            district_slug = re.sub(r'[^a-z0-9]+', '-', row['districtNameEn'].lower()).strip('-')
            building_slug = re.sub(r'[^a-z0-9]+', '-', row['buildingNameEn'].lower()).strip('-')
            
            if row['zoneEn'] is None:
                url = (
                f"https://oir.centanet.com/en/property/office/"
                f"hk-{district_slug}-{building_slug}/detail/{row['propertyID']}/"
            )
            else: 
                zone_slug = re.sub(r'[^a-z0-9]+', '-', row['zoneEn'].lower()).strip('-')
                url = (
                    f"https://oir.centanet.com/en/property/office/"
                    f"hk-{zone_slug}-{district_slug}-{building_slug}/detail/{row['propertyID']}/"
                )

            # Advanced request with session persistence and realistic behavior
            headers = create_realistic_headers()
            cookies = get_advanced_cookies()
            
            # Add referer to make requests look more natural
            if session_requests > 1:
                headers['Referer'] = 'https://oir.centanet.com/en/property/office/'
            
            try:
                # Human-like browsing pattern
                if session_requests % 15 == 0:
                    # Occasionally visit the main page to look more natural
                    session.get('https://oir.centanet.com/en/property/office/', 
                              headers=headers, cookies=cookies, timeout=15)
                    time.sleep(random.uniform(2, 4))
                elif session_requests % 7 == 0:
                    # Sometimes visit a search page
                    search_url = 'https://oir.centanet.com/en/property/office/search'
                    session.get(search_url, headers=headers, cookies=cookies, timeout=15)
                    time.sleep(random.uniform(1, 3))
                
                # Add random "thinking time" before main request
                if random.random() < 0.3:  # 30% chance
                    time.sleep(random.uniform(0.5, 2))
                
                # Main request with advanced anti-detection
                response = session.get(
                    url, 
                    headers=headers, 
                    cookies=cookies, 
                    timeout=30,
                    allow_redirects=True
                )
                
                if response.status_code == 429:  # Rate limited
                    logger.warning(f"Rate limited (429) for {row['propertyID']} - backing off")
                    time.sleep(random.uniform(30, 60))  # Longer backoff for rate limiting
                    continue
                elif response.status_code != 200:
                    time.sleep(random.uniform(1, 3))
                    continue
                    
            except requests.exceptions.Timeout:
                timeout_count += 1
                
                if timeout_count >= max_timeouts:
                    logger.error(f"Too many consecutive timeouts ({timeout_count}). Stopping scraping to avoid being blocked.")
                    break
                
                # Exponential backoff for timeouts
                backoff_time = min(60, 2 ** timeout_count)  # Cap at 60 seconds
                time.sleep(backoff_time)
                continue
            except requests.exceptions.RequestException as e:
                time.sleep(random.uniform(2, 5))
                continue

            # Parse response
            soup = BeautifulSoup(response.content, 'html.parser')
            container = soup.find('section', class_='property-info')
            #print('check', url)
            if not container:
                continue

            # Extract details
            property_details = {'source_url': url}
            for col in container.find_all('div', class_='col'):
                title = col.find('p', class_='col-title')
                text = col.find('p', class_='col-text') or col.find('div', class_='col-text')
                
                if title and text:
                    key = detail_fields.get(title.text.strip())
                    if key:
                        property_details[key] = ' '.join(text.stripped_strings)

            # Add metadata
            #print('check', property_details)
            property_details.update({
                'property_id': row['propertyID'],
                'building_name': row['buildingNameEn'],
                'zone': row['zoneEn'],
                'district': row['districtNameEn']
            })
            
            results.append(property_details)
            success_count += 1
            timeout_count = 0  # Reset timeout counter on success
            progress_bar.set_postfix_str(f"✅ {success_count} | ❌ {error_count} | ⏱️ {timeout_count}")
            
            # Sophisticated adaptive delay strategy
            if len(results) % 20 == 0:
                # Extended break every 20 successful requests (simulate human behavior)
                time.sleep(random.uniform(10, 20))
            elif len(results) % 10 == 0:
                # Medium delay every 10 successful requests
                time.sleep(random.uniform(5, 8))
            elif len(results) % 5 == 0:
                # Short delay every 5 successful requests
                time.sleep(random.uniform(2, 4))
            else:
                # Normal delay with some randomness
                time.sleep(random.uniform(1.5, 3.5))

        except Exception as e:
            error_count += 1
            continue

    # Incremental saving with proper data validation
    if results:
        new_data = pd.DataFrame(results)
        
        # Validate new data before saving
        if new_data.empty:
            logger.warning("No valid data to save")
            return existing_data
        
        # Check for required columns
        required_cols = ['property_id', 'source_url']
        missing_cols = [col for col in required_cols if col not in new_data.columns]
        if missing_cols:
            logger.error(f"Missing required columns in new data: {missing_cols}")
            return existing_data
        
        # Remove any rows with empty property_id
        new_data = new_data.dropna(subset=['property_id'])
        if new_data.empty:
            logger.warning("No valid data after removing empty property_ids")
            return existing_data
        
        # Remove duplicates within new data
        new_data = new_data.drop_duplicates(subset=['property_id'], keep='last')
        
        # Combine with existing data
        if not existing_data.empty:
            # Remove any existing entries that are in new data
            existing_data = existing_data[~existing_data['property_id'].isin(new_data['property_id'])]
            combined_data = pd.concat([existing_data, new_data], ignore_index=True)
        else:
            combined_data = new_data
        
        # Final duplicate removal
        combined_data = combined_data.drop_duplicates(subset=['property_id'], keep='last')
        
        # Save only if we have valid data
        if not combined_data.empty:
            try:
                # Save to parquet with proper error handling
                combined_data.to_parquet(details_file, engine='pyarrow', index=False)
                logger.info(f"Successfully saved {len(combined_data)} records ({len(new_data)} new)")
            except Exception as e:
                logger.error(f"Failed to save data: {e}")
                # Return existing data if save fails
                return existing_data
        
        # Log summary statistics
        logger.info(f"Scraping Summary: {success_count} successful, {error_count} errors, {timeout_count} timeouts out of {total_processed} total requests")
        
        # Record node execution
        record_node_execution(
            node_name="scrape_building_details",
            node_type="estate",
            metadata={
                "buildings_processed": len(combined_data),
                "new_details_scraped": len(new_data),
                "total_requests": session_requests,
                "successful_requests": success_count,
                "error_requests": error_count,
                "timeouts_encountered": timeout_count,
                "execution_time": datetime.datetime.now().isoformat()
            }
        )
        
        return combined_data
    else:
        # Record node execution (no new data)
        record_node_execution(
            node_name="scrape_building_details",
            node_type="estate",
            metadata={
                "buildings_processed": len(existing_data),
                "new_details_scraped": 0,
                "total_requests": session_requests,
                "timeouts_encountered": timeout_count,
                "execution_time": datetime.datetime.now().isoformat()
            }
        )
        
        return existing_data



###########################################################################

# nodes.py
import time
import random
import requests
import pandas as pd
import datetime
from tqdm import tqdm
import urllib.parse
import os
import ast
from fake_useragent import UserAgent
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from typing import Tuple, Dict, Any

def get_random_user_agent():
    ua = UserAgent()
    return ua.random

def get_cookies():
    return {
        "cookie1": f"value1_{random.randint(1000, 9999)}",
        "cookie2": f"value2_{random.randint(1000, 9999)}"
    }

def create_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def create_advanced_session():
    """Create a session with advanced anti-detection measures."""
    session = requests.Session()
    
    # More sophisticated retry strategy
    retry = Retry(
        total=5,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504, 520, 521, 522, 523, 524],
        allowed_methods=["GET", "POST", "HEAD"]
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=20)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    # Set realistic connection settings
    session.headers.update({
        'Connection': 'keep-alive',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"macOS"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1'
    })
    
    return session

def create_realistic_headers():
    """Create realistic browser headers that rotate."""
    user_agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15"
    ]
    
    return {
        "User-Agent": random.choice(user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0"
    }

def get_advanced_cookies():
    """Generate more realistic cookies."""
    return {
        "gr_user_id": f"{random.randint(10000000, 99999999)}-{random.randint(10000000, 99999999)}",
        "session_id": f"session_{random.randint(100000, 999999)}",
        "user_preference": "en_US",
        "timezone": "Asia/Hong_Kong",
        "last_visit": str(int(time.time())),
        "visit_count": str(random.randint(1, 50))
    }

def safe_get_value(x, key, default=None):
    if isinstance(x, str):
        try:
            x = ast.literal_eval(x)
        except (SyntaxError, ValueError):
            return default
    if isinstance(x, dict):
        return x.get(key, default)
    return default

def fuzzy_match_properties(
    transaction_data: pd.DataFrame,
    building_details: pd.DataFrame,
    transaction_name_col: str = "propertyNameEn",
    building_name_col: str = "building_name",
    threshold: float = 80.0
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Perform fuzzy matching between transaction data and building details based on property names.
    
    Args:
        transaction_data: DataFrame with transaction records
        building_details: DataFrame with building detail records
        transaction_name_col: Column name for property names in transaction data
        building_name_col: Column name for building names in building details
        threshold: Minimum similarity score for a match (0-100)
    
    Returns:
        Tuple of (matched_df, match_stats_df)
    """
    logger = logging.getLogger(__name__)
    
    # Check if rapidfuzz is available
    if not RAPIDFUZZ_AVAILABLE:
        raise ImportError("rapidfuzz library is required for fuzzy matching. Install with: pip install rapidfuzz")
    
    # Prepare data for matching
    trans_names = transaction_data[transaction_name_col].dropna().unique()
    building_names = building_details[building_name_col].dropna().unique()
    
    logger.info(f"Fuzzy matching {len(trans_names)} unique transaction properties with {len(building_names)} building details")
    
    # Create mapping dictionary
    name_mapping = {}
    match_scores = {}
    
    # Progress bar for fuzzy matching
    with tqdm(trans_names, desc="Fuzzy matching properties") as pbar:
        for trans_name in pbar:
            if pd.isna(trans_name) or trans_name.strip() == "":
                continue
                
            # Find best match using fuzzy string matching
            match_result = process.extractOne(
                trans_name,
                building_names,
                scorer=fuzz.token_set_ratio
            )
            
            if match_result and match_result[1] >= threshold:
                building_name, score, _ = match_result
                name_mapping[trans_name] = building_name
                match_scores[trans_name] = score
                pbar.set_postfix_str(f"Found: {score:.1f}% match")
            else:
                pbar.set_postfix_str(f"No match (best: {match_result[1]:.1f}%)" if match_result else "No match")
    
    # Apply mapping to transaction data
    transaction_matched = transaction_data.copy()
    transaction_matched['matched_building_name'] = transaction_matched[transaction_name_col].map(name_mapping)
    transaction_matched['match_score'] = transaction_matched[transaction_name_col].map(match_scores)
    
    # Join with building details using matched names
    merged_df = transaction_matched.merge(
        building_details,
        left_on='matched_building_name',
        right_on=building_name_col,
        how='left',
        indicator=True
    )
    
    # Create match statistics
    match_stats = []
    for trans_name, building_name in name_mapping.items():
        match_stats.append({
            'transaction_name': trans_name,
            'matched_building_name': building_name,
            'match_score': match_scores[trans_name]
        })
    
    match_stats_df = pd.DataFrame(match_stats)
    
    # Add merge status
    merged_df["_merge_status"] = merged_df["_merge"].map({
        "left_only": "no_building_match",
        "right_only": "no_transaction_match", 
        "both": "matched"
    })
    merged_df = merged_df.drop(columns=["_merge"])
    
    # Log statistics
    merge_stats = merged_df["_merge_status"].value_counts()
    logger.info(f"Fuzzy matching completed:")
    for status, count in merge_stats.items():
        logger.info(f"  {status}: {count} rows")
    
    if not match_stats_df.empty:
        avg_score = match_stats_df['match_score'].mean()
        min_score = match_stats_df['match_score'].min()
        max_score = match_stats_df['match_score'].max()
        logger.info(f"Match quality: avg={avg_score:.1f}%, min={min_score:.1f}%, max={max_score:.1f}%")
    
    return merged_df, match_stats_df

def test_fuzzy_matching_accuracy(
    transaction_data: pd.DataFrame,
    building_details: pd.DataFrame,
    sample_size: int = 50,
    transaction_name_col: str = "propertyNameEn",
    building_name_col: str = "building_name"
) -> pd.DataFrame:
    """
    Test fuzzy matching accuracy with different thresholds and provide detailed analysis.
    
    Args:
        transaction_data: DataFrame with transaction records
        building_details: DataFrame with building detail records
        sample_size: Number of random samples to test manually
        transaction_name_col: Column name for property names in transaction data
        building_name_col: Column name for building names in building details
    
    Returns:
        DataFrame with test results and statistics
    """
    logger = logging.getLogger(__name__)
    
    if not RAPIDFUZZ_AVAILABLE:
        logger.error("rapidfuzz library is required for testing. Install with: pip install rapidfuzz")
        return pd.DataFrame()
    
    logger.info("Testing fuzzy matching accuracy with different thresholds...")
    
    # Test different thresholds
    thresholds = [70, 75, 80, 85, 90, 95]
    results = []
    
    trans_names = transaction_data[transaction_name_col].dropna().unique()
    building_names = building_details[building_name_col].dropna().unique()
    
    for threshold in thresholds:
        logger.info(f"Testing threshold: {threshold}%")
        
        matched_count = 0
        total_potential_matches = len(trans_names)
        
        for trans_name in trans_names:
            if pd.isna(trans_name) or trans_name.strip() == "":
                continue
                
            match_result = process.extractOne(
                trans_name,
                building_names,
                scorer=fuzz.token_set_ratio
            )
            
            if match_result and match_result[1] >= threshold:
                matched_count += 1
        
        match_rate = (matched_count / total_potential_matches) * 100
        
        results.append({
            'threshold': threshold,
            'matches_found': matched_count,
            'total_properties': total_potential_matches,
            'match_rate_percent': match_rate
        })
        
        logger.info(f"  Threshold {threshold}%: {matched_count}/{total_potential_matches} matched ({match_rate:.1f}%)")
    
    results_df = pd.DataFrame(results)
    
    # Sample detailed analysis
    logger.info(f"\nDetailed analysis of {min(sample_size, len(trans_names))} random samples:")
    
    sample_names = pd.Series(trans_names).sample(min(sample_size, len(trans_names)), random_state=42)
    detailed_results = []
    
    for trans_name in sample_names:
        if pd.isna(trans_name) or trans_name.strip() == "":
            continue
            
        # Get top 3 matches
        top_matches = process.extract(
            trans_name,
            building_names,
            scorer=fuzz.token_set_ratio,
            limit=3
        )
        
        logger.info(f"\nTransaction: '{trans_name}'")
        for i, (match_name, score, _) in enumerate(top_matches, 1):
            status = "✓ MATCH" if score >= 80 else "✗ No match"
            logger.info(f"  {i}. '{match_name}' ({score:.1f}%) {status}")
            
            detailed_results.append({
                'transaction_name': trans_name,
                'rank': i,
                'matched_name': match_name,
                'score': score,
                'is_match_80': score >= 80
            })
    
    detailed_df = pd.DataFrame(detailed_results)
    
    # Summary statistics
    if not detailed_df.empty:
        best_matches = detailed_df[detailed_df['rank'] == 1]
        high_quality_matches = (best_matches['score'] >= 90).sum()
        good_matches = (best_matches['score'] >= 80).sum()
        total_tested = len(best_matches)
        
        logger.info(f"\nSummary for {total_tested} test samples:")
        logger.info(f"  High quality matches (≥90%): {high_quality_matches} ({(high_quality_matches/total_tested)*100:.1f}%)")
        logger.info(f"  Good matches (≥80%): {good_matches} ({(good_matches/total_tested)*100:.1f}%)")
        logger.info(f"  Average best match score: {best_matches['score'].mean():.1f}%")
    
    return results_df

def scrape_transaction( 
    area_codes: pd.DataFrame,
    params: Dict[str, Any]
) -> pd.DataFrame:    

    """Kedro node for scraping and cleaning Centanet transaction data.
    Includes node execution tracking to avoid re-running on the same day.
    
    Args:
        parameters: Dictionary containing:
            - start_date: Start date in YYYY-MM-DD format
            - end_date: End date in YYYY-MM-DD format
            - pagesize: Number of records per page (default: 10000)
        area_codes: DataFrame containing area code mapping
    
    Returns:
        Tuple containing:
        - pd.DataFrame: Cleaned transaction data
        - Dict: Execution metadata including scraping statistics
    """
    
    # Initialize logging first
    logger = logging.getLogger(__name__)
    
    # Check if node should be run based on max date in dataset
    node_name = "scrape_transaction"
    input_path = params['centaline_oir']['cl_oir_transaction_file']
    tracking_params = params.get('node_tracking', {})
    
    if not should_run_node(node_name, "transaction", tracking_params, input_path):
        logger.info(f"Node '{node_name}' - dataset is up to date - returning existing data")
        # Return existing data if available
        if os.path.exists(input_path):
            try:
                return pd.read_parquet(input_path)
            except Exception as e:
                logger.warning(f"Failed to load existing transaction data: {e}")
                return pd.DataFrame()
        return pd.DataFrame()
    
    input_path = params['centaline_oir']['cl_oir_transaction_file']  # Get path from parameters
    
    # Set default dates
    #end_date = params.get("end_date", datetime.date.today().strftime("%Y-%m-%d"))
    
    # Set dynamic end date to current date
    if params.get("use_dynamic_end_date", True) or not params.get("end_date"):
        end_date = datetime.date.today().strftime("%Y-%m-%d")
        logger.info(f"Using dynamic end_date: {end_date}")
    else:
        end_date = params.get("end_date", datetime.date.today().strftime("%Y-%m-%d"))
    
    existing_df = pd.DataFrame()  # Initialize empty DataFrame
    if os.path.exists(input_path):
        try:
            existing_df = pd.read_parquet(input_path)
            logger.info(f"Loaded {len(existing_df)} existing transaction records from storage")
        except Exception as e:
            logger.warning(f"Failed to load existing data: {str(e)}")
            
        if not existing_df.empty:
            #latest_date = pd.to_datetime(existing_df["transactionDate"]).max()
            date_in_df = pd.to_datetime(existing_df["transactionDate"], errors='coerce')
            latest_date = date_in_df.max()#.strftime("%Y-%m-%d")
            
            start_date = (latest_date + pd.DateOffset(days=1)).strftime("%Y-%m-%d")
            end_date = datetime.date.today().strftime("%Y-%m-%d")
        else:
            start_date = params.get('centaline_oir', {}).get('start_date', params['global']['start_date'])
            end_date = params.get("end_date", datetime.date.today().strftime("%Y-%m-%d"))
    else:
        start_date = params.get('centaline_oir', {}).get('start_date', params['global']['start_date'])
        end_date = params.get("end_date", datetime.date.today().strftime("%Y-%m-%d"))

    try:
        start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        
        # Validate date range
        if start_dt >= end_dt:
            logger.warning(f"Start date {start_date} is not before end date {end_date}")
            return existing_df if not existing_df.empty else pd.DataFrame()
        
    except Exception as e:
        raise ValueError(f"Invalid date format: {e}") from e

    start_api = start_dt.strftime("%d/%m/%Y")
    end_api = end_dt.strftime("%d/%m/%Y")
    date_range = f"{start_api}-{end_api}"
    date_range_encoded = urllib.parse.quote(date_range)

    session = create_session()
    cookies = get_cookies()
    base_url = params['centaline_oir']['transaction_api_url']
    pagesize = params['centaline_oir']['page_size']

    all_results = []
    
    with tqdm(area_codes.iterrows(), total=area_codes.shape[0], desc="Processing Areas") as pbar:
        for idx, row in pbar:
            district_meta = {
                "district": row["District"],
                "start": datetime.datetime.now().isoformat(),
                "requests": 0,
                "pages": 0,
                "records": 0
            }

            region = row["Region"]
            district = row["District"]
            code = row["Code"]
            
            # Update progress bar with current district
            pbar.set_postfix_str(f"Processing {district}")

            # Rotate session every 10 areas
            if idx % 10 == 0:
                cookies = get_cookies()
                session = create_session()

            # First, get total count to determine appropriate pagesize
            check_url = (f"{base_url}?pageindex=1&pagesize=1"
                         f"&daterang={date_range_encoded}&sellpricetype=TOTAL&rentpricetype=TOTAL&districtids={code}&lang=EN")
            
            headers = {
                "User-Agent": get_random_user_agent(),
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://oir.centanet.com/",
                "Origin": "https://oir.centanet.com",
                "Connection": "keep-alive"
            }

            try:
                check_response = session.get(check_url, headers=headers, cookies=cookies, timeout=20)
                check_response.raise_for_status()
                check_json_data = check_response.json()
                total_count = check_json_data.get("data", {}).get("recordList", {}).get("totalCount", 0)
                district_meta["requests"] += 1
                
                if total_count == 0:
                    pbar.set_postfix_str(f"{district}: 0 records")
                    continue
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to get total count for {district}: {e}")
                pbar.set_postfix_str(f"{district}: ERROR")
                continue

            # Determine appropriate pagesize - cannot exceed total_count
            effective_pagesize = min(pagesize, total_count)
            page_index = 1
            area_results = []

            while True:
                url = (f"{base_url}?pageindex={page_index}&pagesize={effective_pagesize}"
                       f"&daterang={date_range_encoded}&sellpricetype=TOTAL&rentpricetype=TOTAL&districtids={code}&lang=EN")

                try:
                    response = session.get(url, headers=headers, cookies=cookies, timeout=20)
                    response.raise_for_status()
                    json_data = response.json()
                    logger.debug(f"Request URL: {url}")
                    district_meta["requests"] += 1
                except requests.exceptions.RequestException as e:
                    logger.error(f"Request failed for {district}: {e}")
                    break

                if json_data.get("responseCode") != 1:
                    logger.warning(f"API returned error for {district}: {json_data}")
                    break

                items = json_data.get("data", {}).get("recordList", {}).get("items", [])
                
                if not items:
                    break

                # Extract all the data fields the user wants
                for item in items:
                    # Add region/district metadata
                    item["Region"] = region
                    item["District"] = district
                    item["AreaCode"] = code
                    
                    # Flatten nested structures for easier processing
                    price_info = item.get("priceInfo") or {}
                    area_info = item.get("areaInfo") or {}
                    post_url_info = item.get("postUrlInfo") or {}
                    property_url_info = item.get("propertyUrlInfo") or {}
                    
                    # Create flattened record with all required fields
                    flattened_item = {
                        # Core transaction info
                        "id": item.get("id"),
                        "ibsTransactionID": item.get("ibsTransactionID"),
                        "ibsBuildingID": item.get("ibsBuildingID"),
                        "ibsPropertyNo": item.get("ibsPropertyNo"),
                        "ibsDept": item.get("ibsDept"),
                        "deptDisplayName": item.get("deptDisplayName"),
                        "centabldg": item.get("centabldg"),
                        "transactionDate": item.get("transactionDate"),
                        "transactionType": item.get("transactionType"),
                        
                        # Property names
                        "propertyNameCn": item.get("propertyNameCn"),
                        "propertyNameSc": item.get("propertyNameSc"),
                        "propertyNameEn": item.get("propertyNameEn"),
                        "propertyUsageEn": item.get("propertyUsageEn"),
                        "propertyUsageDisplayName": item.get("propertyUsageDisplayName"),
                        
                        # Unit details
                        "floor": item.get("floor"),
                        "unit": item.get("unit"),
                        "isPriceEstimated": item.get("isPriceEstimated"),
                        "transactionArea": item.get("transactionArea"),
                        "transactionAreaDisplayName": item.get("transactionAreaDisplayName"),
                        
                        # Transaction metadata
                        "agency": item.get("agency"),
                        "remarks": item.get("remarks"),
                        "source": item.get("source"),
                        "sourceDisplayName": item.get("sourceDisplayName"),
                        
                        # Price information
                        "price": price_info.get("price"),
                        "priceDisplayName": price_info.get("priceDisplayName"),
                        "pricePostTypeDisplayName": price_info.get("pricePostTypeDisplayName"),
                        "avgPrice": price_info.get("avgPrice"),
                        "avgPriceDisplayName": price_info.get("avgPriceDisplayName"),
                        "rental": price_info.get("rental"),
                        "rentalDisplayName": price_info.get("rentalDisplayName"),
                        "rentPostTypeDisplayName": price_info.get("rentPostTypeDisplayName"),
                        "avgRental": price_info.get("avgRental"),
                        "avgRentalDisplayName": price_info.get("avgRentalDisplayName"),
                        "gains_Price": price_info.get("gains_Price"),
                        "gains_Rental": price_info.get("gains_Rental"),
                        "priceTo": price_info.get("priceTo"),
                        "rentalTo": price_info.get("rentalTo"),
                        "unitPriceTo": price_info.get("unitPriceTo"),
                        "unitRentalTo": price_info.get("unitRentalTo"),
                        "priceDesc": price_info.get("priceDesc"),
                        
                        # Area information
                        "districtID": area_info.get("districtID"),
                        "districtNameCn": area_info.get("districtNameCn"),
                        "districtNameEn": area_info.get("districtNameEn"),
                        "districtNameSc": area_info.get("districtNameSc"),
                        "zoneEn": area_info.get("zoneEn"),
                        
                        # Additional fields
                        "ibsContractID": item.get("ibsContractID"),
                        "isVideo": item.get("isVideo"),
                        "addressDisplayName": item.get("addressDisplayName"),
                        "isPost": item.get("isPost"),
                        
                                            # URL info
                    "postUrlInfo_centabldg": post_url_info.get("centabldg"),
                    "postUrlInfo_propertyNameEn": post_url_info.get("propertyNameEn"),
                    "postUrlInfo_propertyUsageEn": post_url_info.get("propertyUsageEn"),
                    "postUrlInfo_districtID": post_url_info.get("districtID"),
                    "postUrlInfo_districtNameEn": post_url_info.get("districtNameEn"),
                    "postUrlInfo_zoneEn": post_url_info.get("zoneEn"),
                    
                    # Property ID for joining (from propertyUrlInfo)
                    "propertyId": property_url_info.get("propertyId"),
                    
                    # Metadata
                    "Region": region,
                    "District": district,
                    "AreaCode": code
                    }
                    
                    area_results.append(flattened_item)
                
                district_meta["pages"] += 1
                district_meta["records"] += len(items)
                
                # Check if we got all records (no need to paginate further)
                if len(items) < effective_pagesize or district_meta["records"] >= total_count:
                    break
                    
                page_index += 1
                time.sleep(random.uniform(1, 3))

            # Outside the while loop, but inside the for loop
            if area_results:
                all_results.extend(area_results)
                district_meta["end"] = datetime.datetime.now().isoformat()
                pbar.set_postfix_str(f"{district}: {len(area_results)} records")
            else:
                pbar.set_postfix_str(f"{district}: 0 records")
            
            time.sleep(random.uniform(3, 5))

    # Clean and transform data
    if not all_results:
        logger.warning("No new transaction records collected in this scrape")
        if not existing_df.empty:
            logger.info(f"Returning {len(existing_df)} existing records")
            return existing_df
        raise ValueError("No transaction data available - both existing and new collections are empty")
    
    # Data is already flattened, just create DataFrame
    df_clean = pd.DataFrame(all_results)
    logger.info(f"Collected {len(df_clean)} new transaction records")
    
    # Combine with existing data
    final_df = pd.concat([existing_df, df_clean], ignore_index=True)
    
    # Deduplicate combined dataset
    final_df = final_df.drop_duplicates(
        subset=["id", "propertyNameEn", "transactionDate", "unit"], 
        keep="last"
    )
    logger.info(f"Final dataset after deduplication: {len(final_df)} records")
    
    # Record node execution
    record_node_execution(
        node_name="scrape_transaction",
        node_type="transaction",
        metadata={
            "records_processed": len(final_df),
            "areas_processed": len(area_codes),
            "execution_time": datetime.datetime.now().isoformat()
        }
    )

    return final_df
    

def join_transaction_with_building_details( # <- not yet add incremental 
    transaction_data: pd.DataFrame,
    building_details: pd.DataFrame,
    params: Dict[str, Any]
) -> pd.DataFrame:
    """Enhanced join function with fuzzy matching fallback"""
    
    # Initialize logging
    logger = logging.getLogger(__name__)
    
    join_params = params.get("join", {})
    
    # Get join configuration with defaults
    use_fuzzy_matching = join_params.get("use_fuzzy_matching", True)
    fuzzy_threshold = join_params.get("fuzzy_threshold", 80.0)
    transaction_name_col = join_params.get("transaction_name_col", "propertyNameEn")
    building_name_col = join_params.get("building_name_col", "building_name")
    
    # Debug logging
    logger.info(f"Joining transaction data ({len(transaction_data)} rows) with building details ({len(building_details)} rows)")
    
    # Check if name columns exist for fuzzy matching
    if transaction_name_col not in transaction_data.columns:
        logger.error(f"Transaction name column '{transaction_name_col}' not found. Available columns: {list(transaction_data.columns)}")
        raise KeyError(f"Column '{transaction_name_col}' not found in transaction data")
    
    if building_name_col not in building_details.columns:
        logger.error(f"Building name column '{building_name_col}' not found. Available columns: {list(building_details.columns)}")
        raise KeyError(f"Column '{building_name_col}' not found in building details")

    # Use fuzzy matching approach
    if use_fuzzy_matching:
        logger.info(f"Using fuzzy matching: transaction[{transaction_name_col}] -> building[{building_name_col}]")
        logger.info(f"Fuzzy matching threshold: {fuzzy_threshold}%")
        
        try:
            merged_df, match_stats_df = fuzzy_match_properties(
                transaction_data,
                building_details,
                transaction_name_col,
                building_name_col,
                fuzzy_threshold
            )
            
            # Save match statistics for analysis
            if not match_stats_df.empty:
                match_stats_file = params.get('match_stats_file', 'data/08_reporting/fuzzy_match_stats.csv')
                os.makedirs(os.path.dirname(match_stats_file), exist_ok=True)
                match_stats_df.to_csv(match_stats_file, index=False)
                logger.info(f"Match statistics saved to: {match_stats_file}")
                
                # Show sample matches for verification
                logger.info("Sample fuzzy matches:")
                for _, row in match_stats_df.head(10).iterrows():
                    logger.info(f"  '{row['transaction_name']}' -> '{row['matched_building_name']}' ({row['match_score']:.1f}%)")
            
            return merged_df
            
        except ImportError:
            logger.error("rapidfuzz library not found. Please install: pip install rapidfuzz")
            raise ImportError("rapidfuzz library is required for fuzzy matching. Install with: pip install rapidfuzz")
        except Exception as e:
            logger.error(f"Fuzzy matching failed: {e}")
            raise
    
    else:
        # Fallback to exact matching (original logic)
        left_key = join_params.get("left_on", "propertyId")
        right_key = join_params.get("right_on", "property_id")
        join_type = join_params.get("type", "left")
        cast_keys = join_params.get("cast_join_keys", True)

        logger.info(f"Using exact matching: transaction[{left_key}] -> building[{right_key}]")
        
        # Check if join keys exist
        if left_key not in transaction_data.columns:
            logger.error(f"Join key '{left_key}' not found in transaction data. Available columns: {list(transaction_data.columns)}")
            raise KeyError(f"Column '{left_key}' not found in transaction data")
        
        if right_key not in building_details.columns:
            logger.error(f"Join key '{right_key}' not found in building details. Available columns: {list(building_details.columns)}")
            raise KeyError(f"Column '{right_key}' not found in building details")

        # Type conversion if enabled
        if cast_keys:
            transaction_data = transaction_data.copy()
            building_details = building_details.copy()
            transaction_data[left_key] = transaction_data[left_key].astype(str)
            building_details[right_key] = building_details[right_key].astype(str)
        
        # Check for null values in join keys
        left_nulls = transaction_data[left_key].isnull().sum()
        right_nulls = building_details[right_key].isnull().sum()
        
        if left_nulls > 0:
            logger.warning(f"Found {left_nulls} null values in transaction join key '{left_key}'")
        if right_nulls > 0:
            logger.warning(f"Found {right_nulls} null values in building details join key '{right_key}'")
        
        # Perform the join
        merged_df = transaction_data.merge(
            building_details,
            left_on=left_key,
            right_on=right_key,
            how=join_type,
            indicator=True
        )
        
        # Add merge status column
        merged_df["_merge_status"] = merged_df["_merge"].map({
            "left_only": "transaction_only",
            "right_only": "detail_only", 
            "both": "matched"
        })
        merged_df = merged_df.drop(columns=["_merge"])
        
        # Log join statistics
        merge_stats = merged_df["_merge_status"].value_counts()
        logger.info(f"Join completed. Result: {len(merged_df)} rows")
        for status, count in merge_stats.items():
            logger.info(f"  {status}: {count} rows")
        
        return merged_df

############################### Final Database Cleansing ###############################


