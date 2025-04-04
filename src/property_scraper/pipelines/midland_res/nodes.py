import requests
import pandas as pd
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
from datetime import datetime
from typing import List, Dict, Any

############# Area Code Start #############

def get_district_data(url, headers):
    """
    Fetch district data from Midland API
    
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

def fetch_district_codes():
    """
    Main function to fetch and process district codes
    
    Returns:
        DataFrame: Processed district data
    """
    # API endpoint configuration
    url = "https://data.midland.com.hk/info/v1/region/int_district/int_sm_district?lang=en"
    
    # Configure request headers with provided authentication
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Authorization": "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJndWlkIjoibXItMjAyNC0xMi0wNy0tLVlWN0hKU2QxelRzOHpwVDhJNEdjdGxLcjQ1Z0l4cWhsdVp3SEdvZXVSX1o3RkU2cmh1Q1NjVVpqM1E3SXIzZWVQSmZpMy1JSSIsImF1ZCI6Im15cGFnZWFwcC1tbm5rYiIsInN1YiI6Im1yLTIwMjQtMTItMDctLS1ZVjdISlNkMXpUczh6cFQ4STRHY3RsS3I0NWdJeHFobHVad0hHb2V1Ul9aN0ZFNnJodUNTY1VaajNRN0lyM2VlUEpmaTMtSUkiLCJpYXQiOjE3MzM1NDk0MjUsImV4cCI6MTc2ODEwOTQyNSwiaXNzIjoiZGF0YS5taWRsYW5kLmNvbS5oayJ9.LOOVgc_Nw7OPNnAlB8iC1kRHL0W8UVNVa0GaJYaxTxVZtO33ZbkR64rxMHSifvZOzYr38aJENj-SDIbkq4Y75CxqMPegyBUgHtaub-Fez5qaH2W0Dz71pUdYijDG3rB4Dkbdf8k21QsHerJmOFnpryzTVnZDxv-3g8Lmjz2WUhmrqMamKox3w-T9wRJ4p_wzcJwvXWgtvxkapr3Ep0YSJy3fJsV-Nwm_QiJf2JR0V4rOAu7f-YLMSy7IYje3W-HvVqAZV2cDphg_cYnf6CpirJPu_ix2z6BtIMpYMXeSiZyZtKCHiWFNtUm6QTD2adArWtLl_NvbgcH9mhVYuWi8NcrZBdBh4c72bSNRm104oEbRb9-vb1AylH2oFkEz33xXXEAJRtbQxoQ3qZj_yoDIexrinOSlkJB50fSu98Xizv9eZstnbtzkgVjfKpOAWQFdHKennjN9Azq6yTlejDVspL7A0JsY4ZlO4HQNdkNhiOQDYypHgx8jQMm0B0rbaa0cEz1S0s43Lh01eNVBN9Is35jAWFsJIP-iLvHqXJ9d0pGoHe0N7PQk2dmLo9E5szP0U04MZxt4m9TEpJkn-0uS_ZDSVABlBU2KGIkTmuzm1VltsDhPhoNrbJBJVdxJJdublpDnVFk8aO1gFWKNzptw48ipmLfpRosynC_x3Ud6QMU",
        "Origin": "https://www.midland.com.hk",
        "Referer": "https://www.midland.com.hk/",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
    }
    
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

def load_district_data(district_codes_df: pd.DataFrame) -> pd.DataFrame:
    df = district_codes_df.copy()
    df.drop_duplicates(subset=['subdistrict_code', 'subdistrict_name'], inplace=True)
    return df

@retry(wait=wait_exponential(multiplier=1, min=4, max=10),
      stop=stop_after_attempt(5))
def fetch_estates_data(district_code: str, limit: int = 500) -> List[Dict]:
    base_url = "https://data.midland.com.hk/search/v2/estates"
    results = []
    page = 1
    
    headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0', 
            'authorization': '''Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJndWlkIjoibXItMjAyNC0xMi0wNy0tLVlWN0hKU2QxelRzOHpwVDhJNEdjdGxLcjQ1Z0l4cWhsdVp3SEdvZXVSX1o3RkU2cmh1Q1NjVVpqM1E3SXIzZWVQSmZpMy1JSSIsImF1ZCI6Im15cGFnZWFwcC1tbm5rYiIsInN1YiI6Im1yLTIwMjQtMTItMDctLS1ZVjdISlNkMXpUczh6cFQ4STRHY3RsS3I0NWdJeHFobHVad0hHb2V1Ul9aN0ZFNnJodUNTY1VaajNRN0lyM2VlUEpmaTMtSUkiLCJpYXQiOjE3MzM1NDk0MjUsImV4cCI6MTc2ODEwOTQyNSwiaXNzIjoiZGF0YS5taWRsYW5kLmNvbS5oayJ9.LOOVgc_Nw7OPNnAlB8iC1kRHL0W8UVNVa0GaJYaxTxVZtO33ZbkR64rxMHSifvZOzYr38aJENj-SDIbkq4Y75CxqMPegyBUgHtaub-Fez5qaH2W0Dz71pUdYijDG3rB4Dkbdf8k21QsHerJmOFnpryzTVnZDxv-3g8Lmjz2WUhmrqMamKox3w-T9wRJ4p_wzcJwvXWgtvxkapr3Ep0YSJy3fJsV-Nwm_QiJf2JR0V4rOAu7f-YLMSy7IYje3W-HvVqAZV2cDphg_cYnf6CpirJPu_ix2z6BtIMpYMXeSiZyZtKCHiWFNtUm6QTD2adArWtLl_NvbgcH9mhVYuWi8NcrZBdBh4c72bSNRm104oEbRb9-vb1AylH2oFkEz33xXXEAJRtbQxoQ3qZj_yoDIexrinOSlkJB50fSu98Xizv9eZstnbtzkgVjfKpOAWQFdHKennjN9Azq6yTlejDVspL7A0JsY4ZlO4HQNdkNhiOQDYypHgx8jQMm0B0rbaa0cEz1S0s43Lh01eNVBN9Is35jAWFsJIP-iLvHqXJ9d0pGoHe0N7PQk2dmLo9E5szP0U04MZxt4m9TEpJkn-0uS_ZDSVABlBU2KGIkTmuzm1VltsDhPhoNrbJBJVdxJJdublpDnVFk8aO1gFWKNzptw48ipmLfpRosynC_x3Ud6QMU'''
        }
    
    with tqdm(total=1, desc=f"Processing District {district_code}") as pbar:
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
                    pbar.total = data.get('total_pages', 1)
                    pbar.update(1)
                    page += 1
                else:
                    break
                    
            except RequestException as e:
                print(f"Stopped fetching data at page {page} for district code {district_code} due to error: {e}")
                break
    
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
            amenities = ast.literal_eval(record['amenities'])
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
            print(f"Error parsing market stats: {e}")
            expanded.append(record)
    else:
        expanded.append(record)
    return expanded


def process_estate_data(district_df: pd.DataFrame) -> pd.DataFrame:
    all_estate_data = []
    with tqdm(total=len(district_df), desc="Fetching estate data") as pbar:
        for _, row in district_df.iterrows():
            district_data = fetch_estates_data(row['subdistrict_code'], limit=5000)
            all_estate_data.extend(district_data)
            pbar.update(1)  # Update progress bar after each district is processed
    return pd.DataFrame(all_estate_data)



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

def normalize_all_columns(df: pd.DataFrame) -> pd.DataFrame:
    json_columns = [
        'sm_district', 'region', 'subregion', 'district',
        'combined_district', 'int_district', 'int_sm_district',
        'location', 'developer', 'property_stat', 'market_stat',
        'index_component_estate', 'parent_estate'
    ]
    
    for col in json_columns:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: ast.literal_eval(x) 
                if isinstance(x, str) and x.strip() 
                else (x if isinstance(x, dict) else {})
            )
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
            df[col] = df[col].astype(bool)

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

    df = df.loc[:,~df.columns.duplicated()]
    return df.dropna(axis=1, how='all')

############# Estate End #############

############# Transaction Start #############

def get_soup(url: str, params: Dict = None, headers: Dict = None) -> Dict:
    """
    Make an API request and return the JSON response
    
    Args:
        url: API endpoint URL
        params: Query parameters
        headers: HTTP headers
        
    Returns:
        Dict: JSON response
    """
    if headers is None:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 \
                Satari/537.36 Edg/131.0.0.0', 
            'authorization':'''Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJndWlkIjoibXItMjAyNC0xMi0wNy0tLVlWN0hKU2QxelRzOHpwVDhJNEdjdGxLcjQ1Z0l4cWhsdVp3SEdvZXVSX1o3RkU2cmh1Q1NjVVpqM1E3SXIzZWVQSmZpMy1JSSIsImF1ZCI6Im15cGFnZWFwcC1tbm5rYiIsInN1YiI6Im1yLTIwMjQtMTItMDctLS1ZVjdISlNkMXpUczh6cFQ4STRHY3RsS3I0NWdJeHFobHVad0hHb2V1Ul9aN0ZFNnJodUNTY1VaajNRN0lyM2VlUEpmaTMtSUkiLCJpYXQiOjE3MzM1NDk0MjUsImV4cCI6MTc2ODEwOTQyNSwiaXNzIjoiZGF0YS5taWRsYW5kLmNvbS5oayJ9.LOOVgc_Nw7OPNnAlB8iC1kRHL0W8UVNVa0GaJYaxTxVZtO33ZbkR64rxMHSifvZOzYr38aJENj-SDIbkq4Y75CxqMPegyBUgHtaub-Fez5qaH2W0Dz71pUdYijDG3rB4Dkbdf8k21QsHerJmOFnpryzTVnZDxv-3g8Lmjz2WUhmrqMamKox3w-T9wRJ4p_wzcJwvXWgtvxkapr3Ep0YSJy3fJsV-Nwm_QiJf2JR0V4rOAu7f-YLMSy7IYje3W-HvVqAZV2cDphg_cYnf6CpirJPu_ix2z6BtIMpYMXeSiZyZtKCHiWFNtUm6QTD2adArWtLl_NvbgcH9mhVYuWi8NcrZBdBh4c72bSNRm104oEbRb9-vb1AylH2oFkEz33xXXEAJRtbQxoQ3qZj_yoDIexrinOSlkJB50fSu98Xizv9eZstnbtzkgVjfKpOAWQFdHKennjN9Azq6yTlejDVspL7A0JsY4ZlO4HQNdkNhiOQDYypHgx8jQMm0B0rbaa0cEz1S0s43Lh01eNVBN9Is35jAWFsJIP-iLvHqXJ9d0pGoHe0N7PQk2dmLo9E5szP0U04MZxt4m9TEpJkn-0uS_ZDSVABlBU2KGIkTmuzm1VltsDhPhoNrbJBJVdxJJdublpDnVFk8aO1gFWKNzptw48ipmLfpRosynC_x3Ud6QMU'''
        }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()  # Will raise HTTPError for bad requests
    return response.json()

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
    'location': ['location']
}

def scrape_data(district_ids: List, base_url: str, params: Dict) -> List[Dict]:
    """
    Scrape transaction data for a list of district IDs
    
    Args:
        district_ids: List of district IDs to scrape
        base_url: API endpoint URL
        params: Query parameters
        
    Returns:
        List[Dict]: List of transaction data
    """
    all_data = []
    for district_id in tqdm(district_ids, desc='Scraping district IDs:'):
        params['intsmdist_ids'] = district_id
        page = 1
        while True:
            params['page'] = page
            try:
                data = get_soup(base_url, params=params)
                
                results = data.get('result', [])
                if not results:
                    break
                
                for result in results:
                    row = extract_fields(result, FIELD_MAP)
                    if any(row.values()):
                        all_data.append(row)
                
                page += 1
                time.sleep(3)  # Avoid rate-limiting
                
            except Exception as e:
                print(f"Error occurred while scraping district ID {district_id}, page {page}: {e}")
                break

    return all_data

def fetch_transactions(district_codes_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch transaction data for all districts
    
    Args:
        district_codes_df: DataFrame with district codes
        
    Returns:
        pd.DataFrame: Transaction data
    """
    # Extract district IDs
    district_ids = district_codes_df['subdistrict_code'].dropna().unique().tolist()
    
    # API configuration
    base_url = "https://data.midland.com.hk/search/v2/transactions"
    params = {
        "ad": "true",
        "chart": "true",
        "lang": "en",
        "currency": "HKD",
        "unit": "feet",
        "search_behavior": "normal",
        "tx_date": "3year",
        "limit": 1000  # Number of records per page
    }
    
    # Scrape data
    transactions_data = scrape_data(district_ids, base_url, params)
    
    # Convert to DataFrame
    df = pd.DataFrame(transactions_data)
    
    return df


############# Transaction End #############

############# Data Processing Start #############

def merge_transactions_estates(transactions_df: pd.DataFrame, estates_df: pd.DataFrame) -> pd.DataFrame:
    return pd.merge(
        transactions_df,
        estates_df,
        left_on='estate_id',
        right_on='id',
        how='left',
        suffixes=('_trans', '_estate')
    )

def calculate_price_metrics(merged_df: pd.DataFrame) -> pd.DataFrame:
    merged_df['price_per_sqft'] = merged_df['price'] / merged_df['net_area']
    merged_df['price_variance'] = merged_df['price_per_sqft'] - merged_df['market_stat_net_ft_price']
    return merged_df


############# Data Processing End #############