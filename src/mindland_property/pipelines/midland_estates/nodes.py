# src/midland_property/pipelines/midland_estates/nodes.py
import pandas as pd
import requests
import ast
import json
from typing import List, Dict, Any
import time
import random
from requests.exceptions import RequestException
from pandas import json_normalize

def load_district_data(district_codes_df: pd.DataFrame) -> pd.DataFrame:
    df = district_codes_df.copy()
    df.drop_duplicates(subset=['subdistrict_code', 'subdistrict_name'], inplace=True)
    return df

def fetch_estates_data(district_code: str, limit: int = 500) -> List[Dict]:
    base_url = "https://data.midland.com.hk/search/v2/estates"
    results = []
    page = 1
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
        'authorization': 'Bearer YOUR_BEARER_TOKEN'
    }
    
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

def process_estate_data(district_df: pd.DataFrame) -> List[Dict]:
    all_estate_data = []
    for _, row in district_df.iterrows():
        district_data = fetch_estates_data(row['subdistrict_code'], limit=500)
        all_estate_data.extend(district_data)
        time.sleep(random.uniform(1, 3))
    return all_estate_data

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
