# src/midland_property/pipelines/midland_transactions/nodes.py
import requests
import json
import pandas as pd
import time
from typing import List, Dict, Any

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
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

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

def scrape_data(district_ids: List, base_url: str, params: Dict, headers: Dict) -> List[Dict]:
    """
    Scrape transaction data for a list of district IDs
    
    Args:
        district_ids: List of district IDs to scrape
        base_url: API endpoint URL
        params: Query parameters
        headers: HTTP headers
        
    Returns:
        List[Dict]: List of transaction data
    """
    all_data = []
    
    # Define a field map for nested fields
    field_map = {
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
    
    for district_id in district_ids:
        params['intsmdist_ids'] = district_id
        page = 1
        
        while True:
            params['page'] = page
            try:
                data = get_soup(base_url, params=params, headers=headers)
                results = data.get('result', [])
                
                if not results:
                    break
                    
                for result in results:
                    row = extract_fields(result, field_map)
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
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
        'authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJndWlkIjoibXItMjAyNC0xMi0wNy0tLVlWN0hKU2QxelRzOHpwVDhJNEdjdGxLcjQ1Z0l4cWhsdVp3SEdvZXVSX1o3RkU2cmh1Q1NjVVpqM1E3SXIzZWVQSmZpMy1JSSIsImF1ZCI6Im15cGFnZWFwcC1tbm5rYiIsInN1YiI6Im1yLTIwMjQtMTItMDctLS1ZVjdISlNkMXpUczh6cFQ4STRHY3RsS3I0NWdJeHFobHVad0hHb2V1Ul9aN0ZFNnJodUNTY1VaajNRN0lyM2VlUEpmaTMtSUkiLCJpYXQiOjE3MzM1NDk0MjUsImV4cCI6MTc2ODEwOTQyNSwiaXNzIjoiZGF0YS5taWRsYW5kLmNvbS5oayJ9.LOOVgc_Nw7OPNnAlB8iC1kRHL0W8UVNVa0GaJYaxTxVZtO33ZbkR64rxMHSifvZOzYr38aJENj-SDIbkq4Y75CxqMPegyBUgHtaub-Fez5qaH2W0Dz71pUdYijDG3rB4Dkbdf8k21QsHerJmOFnpryzTVnZDxv-3g8Lmjz2WUhmrqMamKox3w-T9wRJ4p_wzcJwvXWgtvxkapr3Ep0YSJy3fJsV-Nwm_QiJf2JR0V4rOAu7f-YLMSy7IYje3W-HvVqAZV2cDphg_cYnf6CpirJPu_ix2z6BtIMpYMXeSiZyZtKCHiWFNtUm6QTD2adArWtLl_NvbgcH9mhVYuWi8NcrZBdBh4c72bSNRm104oEbRb9-vb1AylH2oFkEz33xXXEAJRtbQxoQ3qZj_yoDIexrinOSlkJB50fSu98Xizv9eZstnbtzkgVjfKpOAWQFdHKennjN9Azq6yTlejDVspL7A0JsY4ZlO4HQNdkNhiOQDYypHgx8jQMm0B0rbaa0cEz1S0s43Lh01eNVBN9Is35jAWFsJIP-iLvHqXJ9d0pGoHe0N7PQk2dmLo9E5szP0U04MZxt4m9TEpJkn-0uS_ZDSVABlBU2KGIkTmuzm1VltsDhPhoNrbJBJVdxJJdublpDnVFk8aO1gFWKNzptw48ipmLfpRosynC_x3Ud6QMU'
    }
    
    # Scrape data
    transactions_data = scrape_data(district_ids, base_url, params, headers)
    
    # Convert to DataFrame
    df = pd.DataFrame(transactions_data)
    
    return df
