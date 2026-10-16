import requests
import pandas as pd
import numpy as np
import ast
from typing import List, Dict, Any
import time
import random
from requests.exceptions import RequestException
from pandas import json_normalize
import json
from tqdm import tqdm
from typing import List, Dict, Any
from tenacity import retry, wait_exponential, stop_after_attempt
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import os
from bs4 import BeautifulSoup
import glob 
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging

# Import node tracking utilities
from ...utils.node_tracker import should_run_node, record_node_execution, get_node_status
from ...utils.incremental import merge_snapshot
from ...utils.source_runtime import get_optional_setting, get_required_setting

# Configure logging
logger = logging.getLogger(__name__)

############# Area Code Start #############

def get_district_data(url, headers):
    """
    Fetch district data from Source B API
    
    Args:
        url: API endpoint URL
        headers: HTTP headers including authorization
        
    Returns:
        DataFrame: Processed district data
    """
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Validate successful response
    return response.json()

def process_district_data(api_response):
    """
    Transform nested JSON structure into flat table format
    
    Args:
        api_response: JSON response from API
        
    Returns:
        DataFrame: Processed district data in tabular format
    """
    processed_records = []
    for region in api_response:
        for district in region.get('int_district', []):
            for subdistrict in district.get('int_sm_district', []):
                processed_records.append({
                    'subdistrict_code': subdistrict['id'],
                    'parent_district_id': district['id'],
                    'parent_district_name': district['name'],
                    'subdistrict_name': subdistrict['name'],
                    'sort_order': subdistrict['seq']
                })
    return pd.DataFrame(processed_records)

def fetch_district_codes(params: Dict[str, Any]) -> pd.DataFrame:
    """
    Main function to fetch and process district codes
    
    Returns:
        DataFrame: Processed district data
    """
    url = get_required_setting(params, "source_b_res", "site", "districts_url")
    headers = get_required_setting(params, "source_b_res", "site", "district_headers")
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    def flatten_district_data(api_response):
        records = []
        for region in api_response:
            for district in region.get('int_district', []):
                for subdistrict in district.get('int_sm_district', []):
                    records.append({
                        'subdistrict_code': subdistrict['id'],
                        'parent_district_id': district['id'],
                        'parent_district_name': district['name'],
                        'subdistrict_name': subdistrict['name'],
                        'sort_order': subdistrict['seq']
                    })
        return records
    
    district_data = flatten_district_data(response.json())
    df = pd.DataFrame(district_data)
    return df.astype('string')

############# Area Code End #############

############# Estate Start #############

def load_district_data(district_codes_df: pd.DataFrame, params: Dict[str, Any] = {}) -> pd.DataFrame:
    """
    Load and clean district data.
    Includes node execution tracking to avoid re-running within 7 days.
    """
    # Initialize logging
    logger = logging.getLogger(__name__)
    
    # Deduplicate district codes — always instant, no scraping involved
    df = district_codes_df.copy()
    df.drop_duplicates(subset=['subdistrict_code', 'subdistrict_name'], inplace=True)
    logger.info(f"📍 Loaded {len(df)} unique districts")
    return df

@retry(wait=wait_exponential(multiplier=1, min=4, max=10),
      stop=stop_after_attempt(5))
def fetch_estates_data(
    district_code: str, params: Dict[str, Any], limit: int = 500
) -> List[Dict]:
    base_url = get_required_setting(params, "source_b_res", "site", "estate_search_url")
    results = []
    page = 1
    headers = get_required_setting(params, "source_b_res", "site", "estate_headers")
    
    #pbar = tqdm(desc=f"Fetching pages for {district_code}", initial=0)
    #try:
    while True:
        params = {
            "ad": "true",
            "lang": "en",
            "currency": "HKD",
            "unit": "feet",
            "search_behavior": "normal",
            "intsmdist_ids": district_code,
            "page": page,
            "limit": limit
        }
        try:
            response = requests.get(base_url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()
                
            if 'result' in data and data['result']:
                processed_data = process_nested_data(data['result'])
                results.extend(processed_data)
                #pbar.total = data.get('total_pages', 1)
                #pbar.update(1)
                page += 1
            else:
                break
                    
        except RequestException as e:
            logger.error(f"Stopped fetching data at page {page} for district code {district_code} due to error: {e}")
            break
    #finally:
    #     pbar.close()
    return results

def process_nested_data(data: List[Dict]) -> List[Dict]:
    processed_records = []
    for item in data:
        item = process_amenities(item)
        processed_records.extend(process_market_stats(item))
        item.pop('market_stat_monthly', None)
    return processed_records

def process_amenities(record: Dict) -> Dict:
    if 'amenities' in record and isinstance(record['amenities'], str):
        try:
            amenities = json.loads(record['amenities'].replace("'", '"'))
            for idx, amenity in enumerate(amenities, 1):
                record[f'amenity_{idx}_type'] = amenity.get('type')
                record[f'amenity_{idx}_name'] = amenity.get('name')
                record[f'amenity_{idx}_walking'] = amenity.get('walking_minute')
        except (ValueError, SyntaxError):
            pass
        record.pop('amenities', None)
    return record

def process_market_stats(record: Dict) -> List[Dict]:
    expanded = []
    monthly_data = record.get('market_stat_monthly')
    
    if monthly_data and isinstance(monthly_data, str):
        try:
            cleaned = monthly_data.replace("'", '"').replace('None', 'null')
            monthly_stats = json.loads(cleaned)
            for stat in monthly_stats:
                new_record = record.copy()
                new_record.update({
                    'monthly_date': stat.get('date'),
                    'monthly_avg_net_ft_price': stat.get('avg_net_ft_price')
                })
                expanded.append(new_record)
        except (ValueError, SyntaxError, json.JSONDecodeError) as e:
            logger.warning(f"Error parsing market stats: {e}")
            expanded.append(record)
    else:
        expanded.append(record)
    return expanded


def _probe_district_estate_count(district_code: str, base_url: str, headers: dict) -> int:
    """Quick API probe: return the total estate count for a district code, or -1 on error."""
    try:
        resp = requests.get(
            base_url,
            params={
                "ad": "true", "lang": "en", "currency": "HKD", "unit": "feet",
                "search_behavior": "normal", "intsmdist_ids": district_code,
                "page": 1, "limit": 1
            },
            headers=headers,
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()
        # The API echoes total_count or we infer from empty result
        if "total_count" in data:
            return int(data["total_count"])
        if "total" in data:
            return int(data["total"])
        # Fallback: if result page is non-empty the district has data (>0)
        if data.get("result"):
            return -1   # has data but count unknown — always refresh
        return 0        # empty district
    except Exception:
        return -1       # on error always refresh


def load_existing_estates(params: Dict[str, Any]) -> pd.DataFrame:
    """Load the current estate snapshot as an explicit Kedro node."""
    output_file = params.get("source_b_res", {}).get(
        "estate_output_path",
        "data/01_raw/midland_res_estates.parquet",
    )
    if not os.path.exists(output_file):
        return pd.DataFrame()
    return pd.read_parquet(output_file)


def process_estate_data(
    district_df: pd.DataFrame,
    existing_df: pd.DataFrame,
    params: Dict[str, Any] = {},
) -> pd.DataFrame:
    """Fetch Source B Residential estate data per district.

    For each district:
    - Probe the API for its current total estate count.
    - Compare with DB count; skip the district if they match.
    - Fetch all pages only for districts that changed.
    """
    logger = logging.getLogger(__name__)
    # Build the auth headers once (same as fetch_estates_data)
    estate_base_url = get_required_setting(params, "source_b_res", "site", "estate_search_url")
    headers = get_required_setting(params, "source_b_res", "site", "estate_headers")

    # Load existing DB counts per district code
    db_counts: dict[str, int] = {}
    if "intsmdist_ids" in existing_df.columns:
        db_counts = existing_df.groupby("intsmdist_ids").size().to_dict()
    elif "district_code" in existing_df.columns:
        db_counts = existing_df.groupby("district_code").size().to_dict()

    logger.info(f"🏘️  Checking {len(district_df)} districts (DB has {len(existing_df):,} existing records)")

    all_estate_data: list[dict] = []
    skipped = changed = 0

    with tqdm(total=len(district_df), desc="Estate districts") as pbar:
        for _, row in district_df.iterrows():
            code = row["subdistrict_code"]
            name = row["subdistrict_name"]

            # Probe API for current count
            api_count = _probe_district_estate_count(code, estate_base_url, headers)
            db_count = db_counts.get(str(code), 0)

            if api_count >= 0 and api_count == db_count:
                # District unchanged — keep existing rows for this district
                if not existing_df.empty:
                    col = "intsmdist_ids" if "intsmdist_ids" in existing_df.columns else "district_code"
                    if col in existing_df.columns:
                        all_estate_data.extend(
                            existing_df[existing_df[col].astype(str) == str(code)].to_dict("records")
                        )
                skipped += 1
                pbar.update(1)
                pbar.set_postfix({"skipped": skipped, "changed": changed, "dist": name[:15]})
                continue

            # District changed or unknown — fetch all pages
            reason = f"api={api_count} db={db_count}" if api_count >= 0 else "count unknown"
            district_data = fetch_estates_data(code, params=params, limit=5000)
            all_estate_data.extend(district_data)
            changed += 1
            pbar.update(1)
            pbar.set_postfix({"skipped": skipped, "changed": changed, "dist": name[:15]})

    logger.info(f"✅ {skipped} districts skipped (unchanged), {changed} re-fetched")

    if not all_estate_data:
        logger.warning("No estate data — returning existing data unchanged")
        return existing_df

    df = _deduplicate_estates_by_id(pd.DataFrame(all_estate_data))
    logger.info(f"Prepared {len(df):,} estate records for catalog persistence")
    return df

def deep_flatten_json(df: pd.DataFrame, column: str) -> pd.DataFrame:
    def flatten_record(record, parent_key='', sep='_'):
        items = []
        if isinstance(record, dict):
            for k, v in record.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, (dict, list)):
                    items.extend(flatten_record(v, new_key, sep=sep).items())
                else:
                    items.append((new_key, v))
        elif isinstance(record, list):
            for i, item in enumerate(record):
                items.extend(flatten_record(item, f"{parent_key}{sep}{i}", sep=sep).items())
        else:
            items.append((parent_key, record))
        return dict(items)
    
    flattened = df[column].apply(lambda x: flatten_record(x) if pd.notnull(x) else {})
    return pd.DataFrame(flattened.tolist()).add_prefix(f'{column}_')


def _deduplicate_estates_by_id(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse identical estate payloads and reject conflicting stable IDs."""
    if df.empty or "id" not in df.columns:
        return df

    duplicate_ids = df.loc[df["id"].duplicated(keep=False), "id"].dropna().unique()
    for estate_id in duplicate_ids:
        records = df.loc[df["id"] == estate_id].to_dict("records")
        canonical_payloads = {
            json.dumps(record, sort_keys=True, default=str, ensure_ascii=False)
            for record in records
        }
        if len(canonical_payloads) > 1:
            raise ValueError(f"Conflicting estate records for id={estate_id}")

    return df.drop_duplicates(subset=["id"], keep="first").reset_index(drop=True)


def normalize_all_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = _deduplicate_estates_by_id(df.copy())
    json_columns = [
        'sm_district', 'region', 'subregion', 'district',
        'combined_district', 'int_district', 'int_sm_district',
        'location', 'developer', 'property_stat', 'market_stat',
        'index_component_estate', 'parent_estate'
    ]
    
    for col in json_columns:
        if col in df.columns:
            def _safe_parse_json(x):
                if isinstance(x, dict):
                    return x
                if isinstance(x, str) and x.strip():
                    try:
                        return json.loads(x.replace("'", '"'))
                    except (json.JSONDecodeError, ValueError):
                        return {}
                return {}
            df[col] = df[col].apply(_safe_parse_json)
            flattened = deep_flatten_json(df, col)
            df = pd.concat([df.drop(col, axis=1), flattened], axis=1)

    numeric_cols = [
        'total_unit_count', 'total_block_count', 'primary_school_net',
        'property_stat_sell_count', 'property_stat_rent_count',
        'market_stat_ft_price', 'market_stat_net_ft_price',
        'market_stat_tx_count', 'market_stat_net_ft_price_chg',
        'market_stat_pre_net_ft_price', 'market_stat_ft_price_chg',
        'market_stat_pre_ft_price', 'market_stat_total_tx_amount',
        'monthly_avg_net_ft_price', 'index_component_estate_net_ft_price',
        'index_component_estate_net_ft_price_chg'
    ]
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    bool_cols = ['hos', 'show']
    for col in bool_cols:
        if col in df.columns:
            # Proper bool parsing: handle strings "True"/"False"/0/1 correctly
            df[col] = df[col].map(
                lambda x: bool(x) if isinstance(x, (int, float, bool, np.bool_))
                else str(x).strip().lower() in ('true', '1', 'yes')
                if pd.notna(x) else False
            )

    date_cols = ['first_op_date', 'monthly_date']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    url_cols = ['url_desc', 'buy_listing_url_desc', 'rent_listing_url_desc', 'photo']
    for col in url_cols:
        if col in df.columns:
            df[col] = df[col].str.replace(r'\\"', '', regex=False)

    if 'parent_estate_id' in df.columns:
        df['parent_estate_id'] = df['parent_estate_id'].fillna('')
    if 'parent_estate_name' in df.columns:
        df['parent_estate_name'] = df['parent_estate_name'].fillna('')

    # Remove duplicate columns first
    df = df.loc[:,~df.columns.duplicated()]
    
    # Define columns to drop (only if they exist)
    columns_to_drop = [
        'buy_listing_url_desc', 'rent_listing_url_desc', 'photo', 'hos', 'icon', 'show', 
        'region_id', 'subregion_id', 'district_id', 'sm_district_id', 'combined_district_id', 
        'combined_district_name', 'market_stat_ft_price', 'market_stat_ft_price_chg', 
        'market_stat_pre_ft_price', 'index_component_estate_net_ft_price', 
        'index_component_estate_net_ft_price_chg', 'market_stat_monthly_1_date', 
        'market_stat_monthly_2_date', 'market_stat_monthly_3_date', 'market_stat_monthly_4_date', 
        'market_stat_monthly_5_date', 'market_stat_monthly_6_date', 'market_stat_monthly_7_date', 
        'market_stat_monthly_8_date', 'market_stat_monthly_9_date', 'market_stat_monthly_10_date'
    ]
    
    # Only drop columns that actually exist in the DataFrame
    existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
    if existing_columns_to_drop:
        df = df.drop(columns=existing_columns_to_drop)
    return df.dropna(axis=1, how='all')



############# Estate End #############

############# Transaction Start #############

def get_soup(
    url: str,
    params: Dict = {},
    headers: Dict = {},
    delay_min: float = 1.0,
    delay_max: float = 3.0,
) -> Dict:
    """
    Make an API request and return the JSON response
    
    Args:
        url: API endpoint URL
        params: Query parameters
        headers: HTTP headers
        delay_min: Minimum delay in seconds
        delay_max: Maximum delay in seconds
        
    Returns:
        Dict: JSON response
    """
    if not headers:
        raise ValueError(
            "Missing Source B request headers. Define source_b_res.site.transaction_headers "
            "in conf/local/parameters.yml."
        )

    try:
        # Add a configurable random delay to avoid rate limiting
        time.sleep(random.uniform(delay_min, delay_max))
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        # Handle specific HTTP error codes
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 401:
            logger.error(f"Authentication failed (401) for URL: {url}")
            logger.error("The Bearer token may be expired or invalid. Please check the authentication token.")
            raise requests.exceptions.HTTPError(f"401 Client Error: Unauthorized for url: {url}")
        elif response.status_code == 403:
            logger.error(f"Access forbidden (403) for URL: {url}")
            logger.error("The request may be blocked by anti-bot measures or IP restrictions.")
            raise requests.exceptions.HTTPError(f"403 Client Error: Forbidden for url: {url}")
        elif response.status_code == 429:
            logger.error(f"Rate limit exceeded (429) for URL: {url}")
            logger.error("Too many requests. Consider implementing longer delays between requests.")
            # Wait longer and retry once
            time.sleep(10)
            response = requests.get(url, headers=headers, params=params, timeout=30)
            if response.status_code == 200:
                return response.json()
            else:
                raise requests.exceptions.HTTPError(f"429 Client Error: Too Many Requests for url: {url}")
        elif response.status_code >= 400:
            logger.error(f"HTTP {response.status_code} error for URL: {url}")
            raise requests.exceptions.HTTPError(f"{response.status_code} Client Error for url: {url}")
        
        response.raise_for_status()  # Will raise HTTPError for other bad requests
        return response.json()
        
    except requests.exceptions.Timeout:
        logger.error(f"Request timeout for URL: {url}")
        raise
    except requests.exceptions.ConnectionError:
        logger.error(f"Connection error for URL: {url}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for URL: {url}: {e}")
        raise

# Function to extract fields based on a mapping
def extract_fields(result: Dict, field_map: Dict) -> Dict:
    """
    Extract fields from a nested dictionary using a field map
    
    Args:
        result: A nested dictionary
        field_map: A mapping of output fields to paths in the nested dictionary
        
    Returns:
        Dict: Extracted fields
    """
    row = {}
    for key, path in field_map.items():
        temp = result
        for p in path:
            temp = temp.get(p, None)
            if temp is None:  # Stop if any part of the path is missing
                break
        row[key] = temp
    return row

# Define a field map for nested fields
FIELD_MAP = {
    'id': ['id'],
    'region_id': ['region', 'id'],
    'region_name': ['region', 'name'],
    'subregion_id': ['subregion', 'id'],
    'subregion': ['subregion', 'name'],
    'district_id': ['district', 'id'],
    'district': ['district', 'name'],
    'sm_district_id': ['sm_district', 'id'],
    'sm_district': ['sm_district', 'name'],
    'combined_district_id': ['combined_district', 'id'],
    'combined_district': ['combined_district', 'name'],
    'int_district_id': ['int_district', 'id'],
    'int_district': ['int_district', 'name'],
    'int_sm_district_id': ['int_sm_district', 'id'],
    'int_sm_district': ['int_sm_district', 'name'],
    'estate_id': ['estate', 'id'],
    'estate': ['estate', 'name'],
    'building_id': ['building', 'id'],
    'building': ['building', 'name'],
    'building_first_op_date': ['building', 'first_op_date'],
    'unit': ['unit', 'id'],
    'floor': ['floor'],
    'floor_level': ['floor_level', 'name'],
    'floor_level_id': ['floor_level', 'id'],
    'flat': ['flat'],
    'area': ['area'],
    'net_area': ['net_area'],
    'price': ['price'],
    'tags': ['tags'],
    'tx_date': ['tx_date'],
    'tx_type': ['tx_type'],
    'last_tx_date': ['last_tx_date'],
    'holding_period': ['holding_period'],
    'last_price': ['last_price'],
    'mkt_type': ['mkt_type'],
    'source': ['source'],
    'original_source': ['original_source'],
    'update_date': ['update_date'],
    'gain': ['gain'],
    'transaction_type': ['transaction_type'],
    'url_desc': ['url_desc'],
    'location': ['location'],
    'unit_price_net': ['unit_price_net'],
}

def scrape_data(
    district_df: pd.DataFrame,
    base_url: str,
    params: Dict,
    headers: Dict[str, str],
    delay_min: float = 1.0,
    delay_max: float = 3.0,
) -> List[Dict]:
    """
    Scrape transaction data for a list of district IDs
    
    Args:
        district_df: DataFrame containing district information
        base_url: API endpoint URL
        params: Query parameters
        delay_min: Minimum delay between requests
        delay_max: Maximum delay between requests
        
    Returns:
        List[Dict]: List of transaction data
    """
    all_data = []
    authentication_failed = False
    
    district_ids = district_df['subdistrict_code'].dropna().unique().tolist()
    
    with tqdm(total=len(district_ids), desc='Scraping district IDs') as pbar:
        for district_id in district_ids:
            params['intsmdist_ids'] = district_id
            page = 1
            
            while True:
                params['page'] = page
                try:
                    data = get_soup(
                        base_url,
                        params=params,
                        headers=headers,
                        delay_min=delay_min,
                        delay_max=delay_max,
                    )
                    
                    results = data.get('result', [])
                    if not results:
                        break
                    
                    for result in results:
                        row = extract_fields(result, FIELD_MAP)
                        if any(row.values()):  # Only append rows with valid data
                            all_data.append(row)
                    
                    page += 1
                
                except requests.exceptions.HTTPError as e:
                    if "401" in str(e):
                        if not authentication_failed:
                            logger.error("Authentication failed for all requests.")
                            logger.error(
                                "Check source_b_res.site.transaction_headers in conf/local/parameters.yml."
                            )
                            authentication_failed = True
                        break  # Stop trying this district if authentication failed
                    else:
                        logger.warning(f"HTTP error for district ID {district_id}, page {page}: {e}")
                        break
                except Exception as e:
                    logger.warning(f"Error occurred while scraping district ID {district_id}, page {page}: {e}")
                    break
            
            # Update progress bar after processing each district ID
            pbar.update(1)
            district_name = district_df[district_df['subdistrict_code'] == district_id]['subdistrict_name'].values[0] if len(district_df[district_df['subdistrict_code'] == district_id]) > 0 else district_id
            pbar.set_postfix({
                'current': district_name,
                'transactions': len(all_data)
            })
            
            # If authentication failed for the first district, no point continuing
            if authentication_failed and len(all_data) == 0:
                logger.error("Stopping scraping due to authentication failure.")
                break

    if authentication_failed and len(all_data) == 0:
        logger.error("No data was scraped due to authentication issues.")
        logger.error("Check source_b_res.site.transaction_headers in conf/local/parameters.yml.")
    
    return all_data


def load_existing_transactions(params: Dict[str, Any]) -> pd.DataFrame:
    """Load the current transaction snapshot as an explicit Kedro node."""
    path = params["source_b_res"]["res_trans_path"]
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_parquet(path)


def fetch_transactions(
    district_codes_df: pd.DataFrame,
    existing_df: pd.DataFrame,
    params: Dict[str, Any],
) -> pd.DataFrame:
    """Fetch Source B Residential transaction data with date-based incremental updates.

    Decision logic (no time-based skip):
    - If DB exists and max_date >= today → already up-to-date, return existing.
    - Otherwise → scrape from max_date+1 (or full history if no DB).
    """
    base_url = get_required_setting(params, "source_b_res", "site", "transaction_search_url")
    transaction_headers = get_required_setting(params, "source_b_res", "site", "transaction_headers")
    parameters = {
        "ad": "true",
        "chart": "true",
        "lang": "en",
        "currency": "HKD",
        "unit": "feet",
        "search_behavior": "normal",
        "limit": 1000
    }

    # Determine scraping mode from actual data, not a timer
    if not existing_df.empty:
        date_cols = ["tx_date", "date", "transaction_date"]
        max_date = None
        for col in date_cols:
            if col in existing_df.columns:
                parsed = pd.to_datetime(existing_df[col], errors="coerce")
                if parsed.notna().any():
                    max_date = parsed.max().date()
                    break
        if max_date and max_date >= datetime.now().date():
            logger.info(f"✅ Source B Res transactions up-to-date (max: {max_date}) — skipping")
            return existing_df
        if max_date:
            logger.info(f"📊 Incremental mode: DB max date {max_date} → scraping from {max_date + timedelta(days=1)}")
        else:
            logger.info("📊 No valid dates found in DB — full historical scrape")
        parameters["tx_date"] = "1year"
    else:
        logger.info("🚀 Full historical mode — no existing data")

    # Scrape new transactions
    # Get rate limiting parameters from config
    delay_min = params.get('source_b_res', {}).get('request_delay_min', 1.0)
    delay_max = params.get('source_b_res', {}).get('request_delay_max', 3.0)
    
    transactions_data = scrape_data(
        district_codes_df,
        base_url,
        parameters,
        transaction_headers,
        delay_min,
        delay_max,
    )
    
    # Handle case where no data was scraped (e.g., due to authentication issues)
    if not transactions_data:
        logger.warning("⚠️ No transaction data was scraped. This may be due to authentication issues or API changes.")
        if not existing_df.empty:
            logger.info("Returning existing data since no new data was available.")
            return existing_df
        else:
            logger.warning("No existing data available and no new data scraped. Returning empty DataFrame.")
            return pd.DataFrame()
    
    # Create DataFrame from scraped data
    new_df = pd.DataFrame(transactions_data)
    
    # Only try to drop columns if they exist in the DataFrame.
    # 'id' is kept (was dropped before): it's globally unique and is what the
    # LANDREG deep-pull supplement (fetch_landreg_deep_supplement) matches
    # district-search rows against alongside (tx_date, building_id, floor,
    # flat, price).
    columns_to_drop = ['region_id', 'subregion_id', 'district_id',
                       'sm_district_id', 'combined_district_id', 'combined_district', 'original_source']
    existing_columns_to_drop = [col for col in columns_to_drop if col in new_df.columns]
    
    if existing_columns_to_drop:
        new_df = new_df.drop(columns=existing_columns_to_drop, axis=1)

    # Merge and deduplicate. LANDREG / LANDREG_DEEP_PULL rows are excluded from
    # this merge and passed through untouched — fetch_landreg_deep_supplement
    # owns their identity (content-keyed on building_id/tx_date/floor/flat/price)
    # and runs right after this node. Folding them into this composite key
    # (tx_date, estate_id, price, building_id, unit) is unsafe: 'unit' is null
    # for every source here, so the key is effectively 4 columns, and the 1.5M+
    # LANDREG_DEEP_PULL rows added by the 2026-09 one-time merge make same-day/
    # same-building/same-price collisions across genuinely different units
    # common enough to silently discard real transactions (observed: >100k rows
    # lost in one incremental run before this fix).
    if not existing_df.empty:
        is_landreg = existing_df.get("source", pd.Series(dtype=object)).isin(["LANDREG", "LANDREG_DEEP_PULL"])
        existing_landreg = existing_df[is_landreg]
        existing_other = existing_df[~is_landreg]
        # The district-search API returns some LANDREG-tagged rows directly
        # (not only via the deep-pull supplement), and this scrape's lookback
        # re-fetches the same window every run. Dropping them here (not just
        # excluding *existing* LANDREG rows above) is what actually prevents
        # duplication: without this, a LANDREG transaction re-scraped on run N
        # has nothing to dedupe against (existing_other excludes LANDREG) and
        # piles up next to the untouched existing_landreg copy from run N-1 —
        # observed live as the same id/tx_date/price appearing 2-3x after a
        # few incremental runs, once per run that re-saw it.
        new_is_landreg = new_df.get("source", pd.Series(dtype=object)).isin(["LANDREG", "LANDREG_DEEP_PULL"])
        new_landreg_count = int(new_is_landreg.sum())
        new_other = new_df[~new_is_landreg]
        merged_other = merge_snapshot(
            existing_other,
            new_other,
            ['tx_date', 'estate_id', 'price', 'building_id', 'unit'],
        )
        combined_df = pd.concat([existing_landreg, merged_other], ignore_index=True)
        logger.info(
            f"➡️ Merged {len(new_other)} new non-LANDREG transactions with existing {len(existing_other)} "
            f"non-LANDREG records ({len(existing_landreg)} LANDREG rows passed through untouched, "
            f"{new_landreg_count} freshly-scraped LANDREG rows discarded as redundant with the deep supplement)"
        )
    else:
        combined_df = new_df

    logger.info(f"💾 Prepared {len(combined_df)} transactions for saving")
    
    # Record node execution
    record_node_execution(
        node_name="fetch_source_b_transactions",
        node_type="transaction",
        metadata={
            "records_processed": len(combined_df),
            "districts_processed": len(district_codes_df),
            "execution_time": datetime.now().isoformat()
        }
    )
    
    return combined_df


_LANDREG_DEEP_PULL_BASE = "https://data.midland.com.hk/info/v1/transactions/buildings/"


def _safe_float(v) -> Optional[float]:
    if v in (None, ""):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _landreg_match_key(building_id, tx_date, floor, flat, price):
    """Same content-based key used to validate the one-time deep-pull merge
    (~95% overlap with existing LANDREG rows) — building_id/tx_date/floor/flat
    plus rounded price, since the deep-pull endpoint carries no id comparable
    to district-search rows scraped before 'id' was retained."""
    try:
        p = round(float(price)) if price is not None and price == price and price != "" else None
    except (TypeError, ValueError):
        p = None
    return (building_id, tx_date, str(floor), str(flat), p)


def _fetch_building_deep_pull(session: requests.Session, headers: dict, building_id: str, delay: float) -> list:
    """One building's full LANDREG history via the per-building endpoint
    (see scripts/midland_res_buildings_pull.py, which validated this endpoint
    against the full one-time pull)."""
    backoff = 20.0
    for _ in range(5):
        try:
            r = session.get(
                _LANDREG_DEEP_PULL_BASE + building_id,
                headers=headers,
                params={"lang": "en", "unit": "feet", "currency": "HKD"},
                timeout=40,
            )
        except RequestException as exc:
            logger.warning("LANDREG deep-pull network error for %s (%s); backing off %.0fs", building_id, exc, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 300)
            continue
        if r.status_code == 200:
            time.sleep(delay)
            try:
                payload = r.json()
            except ValueError:
                return []
            rows = []
            for unit in payload.get("data", []):
                unit_fields = {k: unit.get(k) for k in
                               ("unit_id", "unit_type", "floor", "floor_seq", "flat", "flat_name", "area", "net_area")}
                for tx in unit.get("transactions", []):
                    rows.append({"building_id": building_id, **unit_fields, **tx})
            return rows
        if r.status_code == 404:
            time.sleep(delay)
            return []
        if r.status_code == 429 or r.status_code >= 500:
            logger.warning("LANDREG deep-pull HTTP %d for %s — backing off %.0fs", r.status_code, building_id, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 300)
            continue
        logger.warning("LANDREG deep-pull HTTP %d for %s — giving up", r.status_code, building_id)
        return []
    logger.warning("LANDREG deep-pull: too many failures for %s — skipping", building_id)
    return []


def fetch_landreg_deep_supplement(
    transactions_df: pd.DataFrame,
    params: Dict[str, Any],
) -> pd.DataFrame:
    """Monthly sweep: deep-pull LANDREG history for buildings with recent
    activity, to catch registrations that land later than the district-search
    scrape (fetch_transactions) sees them.

    The per-building endpoint reaches back to ~1996 but ONLY carries LANDREG
    sale records — zero rentals, zero self-reported (MIDLAND/HKP/SRPE) sales
    (verified against the full one-time pull). It is therefore merged against
    the existing LANDREG rows, never used to replace them: rows the sweep
    re-confirms are superseded by the fresher deep-pull version (via a content
    key, since the endpoint predates the retained 'id' field for older scraped
    rows); rows it doesn't cover are left untouched, and every non-LANDREG row
    (MIDLAND/SRPE/HKP/rentals) is passed through unchanged.
    """
    sb_params = params.get("source_b_res", {})
    if not sb_params.get("enable_landreg_deep_supplement", True):
        return transactions_df
    if transactions_df.empty or "building_id" not in transactions_df.columns:
        return transactions_df

    interval_days = int(sb_params.get("deep_supplement_interval_days", 30))
    status = get_node_status("fetch_landreg_deep_supplement")
    days_since = status.get("days_since_last_run")
    if days_since is not None and days_since < interval_days:
        logger.info(
            "Skipping LANDREG deep supplement (ran %d days ago, interval=%d)",
            days_since, interval_days,
        )
        return transactions_df

    active_months = int(sb_params.get("deep_supplement_active_months", 9))
    cutoff = datetime.now().date() - timedelta(days=active_months * 30)
    parsed_dates = pd.to_datetime(transactions_df["tx_date"], errors="coerce")
    active_mask = parsed_dates.dt.date >= cutoff
    building_ids = sorted(
        transactions_df.loc[active_mask, "building_id"].dropna().astype(str).unique().tolist()
    )
    max_buildings = sb_params.get("deep_supplement_max_buildings")
    if max_buildings:
        building_ids = building_ids[: int(max_buildings)]
    logger.info(
        "LANDREG deep supplement: %d buildings active in the last %d months",
        len(building_ids), active_months,
    )
    if not building_ids:
        record_node_execution(
            node_name="fetch_landreg_deep_supplement", node_type="default",
            metadata={"buildings_swept": 0},
        )
        return transactions_df

    headers = get_required_setting(params, "source_b_res", "site", "transaction_headers")
    delay = float(sb_params.get("deep_supplement_request_delay", 0.35))
    session = requests.Session()

    hier_cols = [c for c in (
        "region_name", "subregion", "district", "sm_district",
        "int_district_id", "int_district", "int_sm_district_id", "int_sm_district",
        "estate_id", "estate", "building", "building_first_op_date", "location",
    ) if c in transactions_df.columns]
    hierarchy = (
        transactions_df.dropna(subset=["building_id"])
        .drop_duplicates(subset=["building_id"], keep="last")[["building_id"] + hier_cols]
    )

    source_col = transactions_df["source"] if "source" in transactions_df.columns else pd.Series("", index=transactions_df.index)
    is_landreg = source_col.isin(["LANDREG", "LANDREG_DEEP_PULL"])
    existing_landreg = transactions_df[is_landreg].copy()
    existing_other = transactions_df[~is_landreg].copy()
    existing_landreg["_key"] = existing_landreg.apply(
        lambda r: _landreg_match_key(r["building_id"], r["tx_date"], r.get("floor"), r.get("flat"), r.get("price")),
        axis=1,
    )

    pull_ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")
    new_records = []
    new_keys_seen = set()
    for bid in tqdm(building_ids, desc="LANDREG deep supplement"):
        rows = _fetch_building_deep_pull(session, headers, bid, delay)
        for r in rows:
            price_f = _safe_float(r.get("price"))
            gain_f = _safe_float(r.get("gain"))
            last_tx_date = r.get("last_tx_date")
            last_price = price_f / (1 + gain_f / 100) if (price_f is not None and gain_f is not None) else None
            holding_period = None
            if last_tx_date and r.get("tx_date"):
                try:
                    d1 = pd.Timestamp(r["tx_date"])
                    d0 = pd.Timestamp(last_tx_date)
                    holding_period = f"{(d1 - d0).days / 365.25:.1f}"
                except (ValueError, TypeError):
                    pass
            area_f = _safe_float(r.get("area"))
            net_area_f = _safe_float(r.get("net_area"))
            mkt_type_f = _safe_float(r.get("mkt_type"))
            new_records.append({
                "building_id": r.get("building_id"), "unit": None, "floor": r.get("floor"),
                "floor_level": None, "floor_level_id": None, "flat": r.get("flat") or None,
                "area": int(area_f) if area_f is not None else None,
                "net_area": int(net_area_f) if net_area_f is not None else None,
                "price": price_f, "tags": np.array(["LANDREG_DEEP_PULL"], dtype=object),
                "tx_date": r.get("tx_date"), "tx_type": r.get("tx_type"), "last_tx_date": last_tx_date,
                "holding_period": holding_period, "last_price": last_price,
                "mkt_type": int(mkt_type_f) if mkt_type_f is not None else None,
                "source": "LANDREG_DEEP_PULL", "update_date": pull_ts, "gain": gain_f,
                "transaction_type": r.get("transaction_type"), "url_desc": r.get("url_desc"),
                "unit_price_net": r.get("net_ft_price"),
            })
            new_keys_seen.add(
                _landreg_match_key(r.get("building_id"), r.get("tx_date"), r.get("floor"), r.get("flat"), price_f)
            )

    if not new_records:
        logger.info("LANDREG deep supplement: no rows returned for %d buildings", len(building_ids))
        record_node_execution(
            node_name="fetch_landreg_deep_supplement", node_type="default",
            metadata={"buildings_swept": len(building_ids), "rows_added": 0},
        )
        return transactions_df

    new_df = pd.DataFrame(new_records).merge(hierarchy, on="building_id", how="left")
    existing_landreg_residual = existing_landreg[~existing_landreg["_key"].isin(new_keys_seen)].drop(columns=["_key"])

    final_cols = list(transactions_df.columns)
    new_df = new_df.reindex(columns=final_cols)
    existing_other = existing_other.reindex(columns=final_cols)
    existing_landreg_residual = existing_landreg_residual.reindex(columns=final_cols)
    merged = pd.concat([existing_other, existing_landreg_residual, new_df], ignore_index=True)

    logger.info(
        "LANDREG deep supplement: %d buildings swept, %d rows fetched, %d final rows (was %d)",
        len(building_ids), len(new_df), len(merged), len(transactions_df),
    )
    record_node_execution(
        node_name="fetch_landreg_deep_supplement",
        node_type="default",
        metadata={"buildings_swept": len(building_ids), "rows_added": len(new_df), "final_rows": len(merged)},
    )
    return merged


############# Transaction End #############

############# Data Processing Start #############

def merge_transactions_estates(transactions_df: pd.DataFrame, estates_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge transactions and estates data.
    Handles different possible column names for estate ID.
    """
    import logging
    logger = logging.getLogger(__name__)
    
    # Log available columns for debugging
    logger.info(f"Transactions columns: {list(transactions_df.columns)}")
    logger.info(f"Estates columns: {list(estates_df.columns)}")
    
    # Check if we have estate_id in transactions
    if 'estate_id' not in transactions_df.columns:
        logger.warning("No estate_id column in transactions DataFrame - cannot merge")
        return transactions_df
    
    # Determine the right join column for estates
    if 'estate_id' in estates_df.columns:
        right_on = 'estate_id'
    elif 'id' in estates_df.columns:
        right_on = 'id'
    else:
        # If no clear ID column, try to find one
        id_columns = [col for col in estates_df.columns if 'id' in col.lower()]
        if id_columns:
            right_on = id_columns[0]
            logger.info(f"Using {right_on} as estate ID column")
        else:
            # If still no ID column found, try using estate name for matching
            name_columns = [col for col in estates_df.columns if 'name' in col.lower() and 'estate' in col.lower()]
            if name_columns:
                right_on = name_columns[0]
                logger.warning(f"No ID column found, attempting merge using name column: {right_on}")
                # Try to merge on estate name instead
                return pd.merge(
                    transactions_df,
                    estates_df,
                    left_on='estate',  # Use estate name from transactions
                    right_on=right_on,
                    how='left',
                    suffixes=('_trans', '_estate')
                )
            else:
                raise ValueError("No suitable ID column found in estates DataFrame")
    
    logger.info(f"Merging on: transactions.estate_id = estates.{right_on}")
    
    merged = pd.merge(
        transactions_df,
        estates_df,
        left_on='estate_id',
        right_on=right_on,
        how='left',
        validate='many_to_one',
        suffixes=('_trans', '_estate')
    )
    if len(merged) != len(transactions_df):
        raise ValueError(
            "Estate enrichment changed transaction cardinality: "
            f"{len(transactions_df)} -> {len(merged)}"
        )
    return merged



# Node: source_b_res_final_cleansing



############# Data Processing End #############
