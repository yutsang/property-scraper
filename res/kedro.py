# midland district code reader: midland_res_area_code_reader.ipynb

import requests
import pandas as pd

# API endpoint configuration
url = "https://data.midland.com.hk/info/v1/region/int_district/int_sm_district?lang=en"

# Configure request headers with provided authentication
headers = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "Authorization": "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJndWlkIjoibXItMjAyNC0xMi0wNy0tLVlWN0hKU2QxelRzOHpwVDhJNEdjdGxLcjQ1Z0l4cWhsdVp3SEdvZXVSX1o3RkU2cmh1Q1NjVVpqM1E3SXIzZWVQSmZpMy1JSSIsImF1ZCI6Im15cGFnZWFwcC1tbm5rYiIsInN1YiI6Im1yLTIwMjQtMTItMDctLS1ZVjdISlNkMXpUczh6cFQ4STRHY3RsS3I0NWdJeHFobHVad0hHb2V1Ul9aN0ZFNnJodUNTY1VaajNRN0lyM2VlUEpmaTMtSUkiLCJpYXQiOjE3MzM1NDk0MjUsImV4cCI6MTc2ODEwOTQyNSwiaXNzIjoiZGF0YS5taWRsYW5kLmNvbS5oayJ9.LOOVgc_Nw7OPNnAlB8iC1kRHL0W8UVNVa0GaJYaxTxVZtO33ZbkR64rxMHSifvZOzYr38aJENj-SDIbkq4Y75CxqMPegyBUgHtaub-Fez5qaH2W0Dz71pUdYijDG3rB4Dkbdf8k21QsHerJmOFnpryzTVnZDxv-3g8Lmjz2WUhmrqMamKox3w-T9wRJ4p_wzcJwvXWgtvxkapr3Ep0YSJy3fJsV-Nwm_QiJf2JR0V4rOAu7f-YLMSy7IYje3W-HvVqAZV2cDphg_cYnf6CpirJPu_ix2z6BtIMpYMXeSiZyZtKCHiWFNtUm6QTD2adArWtLl_NvbgcH9mhVYuWi8NcrZBdBh4c72bSNRm104oEbRb9-vb1AylH2oFkEz33xXXEAJRtbQxoQ3qZj_yoDIexrinOSlkJB50fSu98Xizv9eZstnbtzkgVjfKpOAWQFdHKennjN9Azq6yTlejDVspL7A0JsY4ZlO4HQNdkNhiOQDYypHgx8jQMm0B0rbaa0cEz1S0s43Lh01eNVBN9Is35jAWFsJIP-iLvHqXJ9d0pGoHe0N7PQk2dmLo9E5szP0U04MZxt4m9TEpJkn-0uS_ZDSVABlBU2KGIkTmuzm1VltsDhPhoNrbJBJVdxJJdublpDnVFk8aO1gFWKNzptw48ipmLfpRosynC_x3Ud6QMU",
    "Origin": "https://www.midland.com.hk",
    "Priority": "u=1, i",
    "Referer": "https://www.midland.com.hk/",
    "Sec-Ch-Ua": '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"macOS"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
}

# Execute API request
response = requests.get(url, headers=headers)
response.raise_for_status()  # Validate successful response

# Process JSON data structure
def flatten_district_data(api_response):
    """Transform nested JSON structure into flat table format"""
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
    return processed_records

# Create DataFrame from structured data
district_data = flatten_district_data(response.json())
df = pd.DataFrame(district_data)

# Export to Excel
output_file = "midland_res_area_code.xlsx"
df.to_excel(output_file, index=False, engine='openpyxl')

print(f"Successfully exported district data to {output_file}")

# midland transaction: midland_res_trans.ipynb
import json 
import requests
import csv
import time
import os
import pandas as pd 
from tqdm import tqdm
from bs4 import BeautifulSoup
import glob 
from datetime import datetime

# Define a function to handle web requests 
def get_soup(url, params=None):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 \
            Satari/537.36 Edg/131.0.0.0', 
        'authorization':'''Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJndWlkIjoibXItMjAyNC0xMi0wNy0tLVlWN0hKU2QxelRzOHpwVDhJNEdjdGxLcjQ1Z0l4cWhsdVp3SEdvZXVSX1o3RkU2cmh1Q1NjVVpqM1E3SXIzZWVQSmZpMy1JSSIsImF1ZCI6Im15cGFnZWFwcC1tbm5rYiIsInN1YiI6Im1yLTIwMjQtMTItMDctLS1ZVjdISlNkMXpUczh6cFQ4STRHY3RsS3I0NWdJeHFobHVad0hHb2V1Ul9aN0ZFNnJodUNTY1VaajNRN0lyM2VlUEpmaTMtSUkiLCJpYXQiOjE3MzM1NDk0MjUsImV4cCI6MTc2ODEwOTQyNSwiaXNzIjoiZGF0YS5taWRsYW5kLmNvbS5oayJ9.LOOVgc_Nw7OPNnAlB8iC1kRHL0W8UVNVa0GaJYaxTxVZtO33ZbkR64rxMHSifvZOzYr38aJENj-SDIbkq4Y75CxqMPegyBUgHtaub-Fez5qaH2W0Dz71pUdYijDG3rB4Dkbdf8k21QsHerJmOFnpryzTVnZDxv-3g8Lmjz2WUhmrqMamKox3w-T9wRJ4p_wzcJwvXWgtvxkapr3Ep0YSJy3fJsV-Nwm_QiJf2JR0V4rOAu7f-YLMSy7IYje3W-HvVqAZV2cDphg_cYnf6CpirJPu_ix2z6BtIMpYMXeSiZyZtKCHiWFNtUm6QTD2adArWtLl_NvbgcH9mhVYuWi8NcrZBdBh4c72bSNRm104oEbRb9-vb1AylH2oFkEz33xXXEAJRtbQxoQ3qZj_yoDIexrinOSlkJB50fSu98Xizv9eZstnbtzkgVjfKpOAWQFdHKennjN9Azq6yTlejDVspL7A0JsY4ZlO4HQNdkNhiOQDYypHgx8jQMm0B0rbaa0cEz1S0s43Lh01eNVBN9Is35jAWFsJIP-iLvHqXJ9d0pGoHe0N7PQk2dmLo9E5szP0U04MZxt4m9TEpJkn-0uS_ZDSVABlBU2KGIkTmuzm1VltsDhPhoNrbJBJVdxJJdublpDnVFk8aO1gFWKNzptw48ipmLfpRosynC_x3Ud6QMU'''
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status() # Will raise HTTPError for bad requests (4XX or 5XX)
    #return BeautifulSoup(response.text, 'html.parser')
    return response.json()

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
    
# Function to extract fields based on a mapping
def extract_fields(result, field_map):
    row = {}
    for key, path in field_map.items():
        temp = result
        for p in path:
            temp = temp.get(p, None)
            if temp is None:  # Stop if any part of the path is missing
                break
        row[key] = temp
    return row


def scrape_data(district_ids, base_url, params):
    all_data = []
    for district_id in tqdm(district_ids, desc='Scraping district IDs:'):
        params['intsmdist_ids'] = district_id
        page = 1
        while True:
            params['page'] = page
            try:
                data = get_soup(base_url, params)  # Use updated get_soup function
                #print(f"Scraping page {page} for district ID {district_id}")
                
                results = data.get('result', [])
                if not results:
                    #print(f"No more results found for district ID {district_id} on page {page}")
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

def save_to_csv(data, filename):
    if data:
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f'Data saved ({len(data)} records) to {filename}')
        
# Read the district IDs from the file
def read_district_ids_from_excel(filename, column_name):
    df = pd.read_excel(filename, usecols=[column_name])
    return df[column_name].dropna().unique().tolist()

# Script Execution 
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

# Read district IDs from Excel file (ensure file exists)
district_ids = read_district_ids_from_excel('midland_res_area_code.xlsx', 'm_idstrict_code')

# Scrape data and save to CSV
scraped_data = scrape_data(district_ids, base_url, params)
save_to_csv(scraped_data, 'midland_res_transaction_data.csv')

# Midland Estates
import pandas as pd
import requests
import ast
from requests.exceptions import RequestException
from tqdm import tqdm
from collections.abc import MutableMapping
import json
from pandas import json_normalize
import time, random

def load_data(file_path):
    """Load district data from Excel file and drop duplicates"""
    df = pd.read_excel(file_path)
    df.drop_duplicates(subset=['m_district_code', 'm_district'], inplace=True)
    return df

def fetch_estates_data(district_code, limit=500):
    """Fetch estate data from Midland API for a given district code"""
    base_url = "https://data.midland.com.hk/search/v2/estates"
    results = []
    page = 1
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
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0', 
            'authorization': '''Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJndWlkIjoibXItMjAyNC0xMi0wNy0tLVlWN0hKU2QxelRzOHpwVDhJNEdjdGxLcjQ1Z0l4cWhsdVp3SEdvZXVSX1o3RkU2cmh1Q1NjVVpqM1E3SXIzZWVQSmZpMy1JSSIsImF1ZCI6Im15cGFnZWFwcC1tbm5rYiIsInN1YiI6Im1yLTIwMjQtMTItMDctLS1ZVjdISlNkMXpUczh6cFQ4STRHY3RsS3I0NWdJeHFobHVad0hHb2V1Ul9aN0ZFNnJodUNTY1VaajNRN0lyM2VlUEpmaTMtSUkiLCJpYXQiOjE3MzM1NDk0MjUsImV4cCI6MTc2ODEwOTQyNSwiaXNzIjoiZGF0YS5taWRsYW5kLmNvbS5oayJ9.LOOVgc_Nw7OPNnAlB8iC1kRHL0W8UVNVa0GaJYaxTxVZtO33ZbkR64rxMHSifvZOzYr38aJENj-SDIbkq4Y75CxqMPegyBUgHtaub-Fez5qaH2W0Dz71pUdYijDG3rB4Dkbdf8k21QsHerJmOFnpryzTVnZDxv-3g8Lmjz2WUhmrqMamKox3w-T9wRJ4p_wzcJwvXWgtvxkapr3Ep0YSJy3fJsV-Nwm_QiJf2JR0V4rOAu7f-YLMSy7IYje3W-HvVqAZV2cDphg_cYnf6CpirJPu_ix2z6BtIMpYMXeSiZyZtKCHiWFNtUm6QTD2adArWtLl_NvbgcH9mhVYuWi8NcrZBdBh4c72bSNRm104oEbRb9-vb1AylH2oFkEz33xXXEAJRtbQxoQ3qZj_yoDIexrinOSlkJB50fSu98Xizv9eZstnbtzkgVjfKpOAWQFdHKennjN9Azq6yTlejDVspL7A0JsY4ZlO4HQNdkNhiOQDYypHgx8jQMm0B0rbaa0cEz1S0s43Lh01eNVBN9Is35jAWFsJIP-iLvHqXJ9d0pGoHe0N7PQk2dmLo9E5szP0U04MZxt4m9TEpJkn-0uS_ZDSVABlBU2KGIkTmuzm1VltsDhPhoNrbJBJVdxJJdublpDnVFk8aO1gFWKNzptw48ipmLfpRosynC_x3Ud6QMU'''
        }
        
        try: 
            response = requests.get(base_url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            if 'result' in data and data['result']:
                processed_data = process_nested_data(data['result'])
                results.extend(processed_data)
                page += 1
            else:
                break
            
        except RequestException as e:
            print(f"Stopped fetching data at page {page} for district code {district_code} due to error: {e}")
            break
    
    return results

def process_nested_data(data):
    """Process nested structures in the raw API response"""
    processed_records = []
    
    for item in data:
        # Process amenities
        item = process_amenities(item)
        
        # Process market stats
        processed_records.extend(process_market_stats(item))
        
        # Remove original nested fields
        item.pop('market_stat_monthly', None)
        
    return processed_records

def process_amenities(record):
    """Expand amenities data into separate columns"""
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

def process_market_stats(record):
    """Expand market statistics into multiple rows"""
    expanded = []
    monthly_data = record.get('market_stat_monthly')
    
    if monthly_data and isinstance(monthly_data, str):
        try:
            # Clean the string representation
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

def process_estate_data(df):
    """Process estate data for all districts"""
    all_estate_data = []
    for _, row in tqdm(df.iterrows(), total=df.shape[0], desc="Processing districts"):
        district_data = fetch_estates_data(row['m_district_code'], limit=500)
        all_estate_data.extend(district_data)
        time.sleep(random.uniform(1, 3))
        
    return all_estate_data

def deep_flatten_json(df, column):
    """Recursively flatten nested JSON structures in a column"""
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

def normalize_all_columns(df):
    """Comprehensive normalization of all columns"""
    # Handle JSON-like columns
    json_columns = [
        'sm_district', 'region', 'subregion', 'district', 'combined_district',
        'int_district', 'int_sm_district', 'location', 'developer',
        'property_stat', 'market_stat', 'index_component_estate', 'parent_estate'
    ]
    
    for col in json_columns:
        if col in df.columns:
            # Convert string to dict
            df[col] = df[col].apply(
                lambda x: ast.literal_eval(x) 
                if isinstance(x, str) and x.strip() 
                else (x if isinstance(x, dict) else {})
            )
            # Recursively flatten
            flattened = deep_flatten_json(df, col)
            df = pd.concat([df.drop(col, axis=1), flattened], axis=1)
    
    # Convert numeric columns
    numeric_cols = [
        'total_unit_count', 'total_block_count', 'primary_school_net',
        'property_stat_sell_count', 'property_stat_rent_count',
        'market_stat_ft_price', 'market_stat_net_ft_price', 'market_stat_tx_count',
        'market_stat_net_ft_price_chg', 'market_stat_pre_net_ft_price',
        'market_stat_ft_price_chg', 'market_stat_pre_ft_price',
        'market_stat_total_tx_amount', 'monthly_avg_net_ft_price',
        'index_component_estate_net_ft_price', 'index_component_estate_net_ft_price_chg'
    ]
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Convert boolean columns
    bool_cols = ['hos', 'show']
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].astype(bool)
    
    # Convert date columns
    date_cols = ['first_op_date', 'monthly_date']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Clean URL columns
    url_cols = ['url_desc', 'buy_listing_url_desc', 'rent_listing_url_desc', 'photo']
    for col in url_cols:
        if col in df.columns:
            df[col] = df[col].str.replace(r'\\"', '', regex=False)
    
    # Handle hierarchical relationships
    if 'parent_estate_id' in df.columns:
        df['parent_estate_id'] = df['parent_estate_id'].fillna('')
    if 'parent_estate_name' in df.columns:
        df['parent_estate_name'] = df['parent_estate_name'].fillna('')
    
    return df

def main():
    """Main data processing pipeline"""
    # Load and process data
    district_df = load_data("midland_res_area_code.xlsx")
    estate_data = process_estate_data(district_df)
    main_df = pd.DataFrame(estate_data)
    
    if main_df.empty:
        print("No data processed")
        return
    
    # Perform comprehensive normalization
    normalized_df = normalize_all_columns(main_df)
    
    # Final cleanup
    normalized_df = normalized_df.loc[:,~normalized_df.columns.duplicated()]
    normalized_df = normalized_df.dropna(axis=1, how='all')
    
    # Save results
    normalized_df.to_csv('midland_estates.csv', index=False)
    print("Data saved to midland_estates.csv")

if __name__ == "__main__":
    main()
