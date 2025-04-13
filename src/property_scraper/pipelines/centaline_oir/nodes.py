import requests
import pandas as pd
from tqdm import tqdm
import time
import random
import os
from typing import Dict, Any
from bs4 import BeautifulSoup
import re

def scrape_building_listings(
    cl_oir_area_code: pd.DataFrame, 
    params: Dict[str, Any])  -> pd.DataFrame:

    # Read district information
    listings_file = params.get('cl_oir_building_listings_file', 'data/02_intermediate/centanet_oir_buildings.parquet')

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
                    params['property_api_url'],
                    headers=params['headers'],
                    cookies=params['cookies'],
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
                            params['property_api_url'],
                            headers=params['headers'],
                            cookies=params['cookies'],
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
        return combined_df

    else:
        #new_df.to_csv(existing_df, index=False)
        return new_df
    
def scrape_building_details(
    building_listing_df: pd.DataFrame,
    params: Dict[str, Any]) -> pd.DataFrame:
    
    # Field mapping configuration
    detail_fields = {
        'District': 'district',
        'Address': 'full_address',
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
        existing_data = pd.read_csv(details_file)
        scraped_ids = set(existing_data['property_id'].astype(str))

    # Prepare filtered input dataframe
    building_listing_df = building_listing_df.copy()
    building_listing_df['propertyID'] = building_listing_df['propertyID'].astype(str)
    todo_df = building_listing_df[~building_listing_df['propertyID'].isin(scraped_ids)]
    
    results = []
    
    for _, row in tqdm(todo_df.iterrows(), total=len(todo_df), desc="Scraping Details"):
        try:
            # URL sanitization
            
            district_slug = re.sub(r'[^a-z0-9]+', '-', row['districtNameEn'].lower()).strip('-')
            building_slug = re.sub(r'[^a-z0-9]+', '-', row['buildingNameEn'].lower()).strip('-')
            
            if row['zoneEn'] is None:
                url = (
                f"https://oir.centanet.com/en/property/office/"
                f"{district_slug}-{building_slug}/detail/{row['propertyID']}/"
            )
            else: 
                zone_slug = re.sub(r'[^a-z0-9]+', '-', row['zoneEn'].lower()).strip('-')
                url = (
                    f"https://oir.centanet.com/en/property/office/"
                    f"{zone_slug}-{district_slug}-{building_slug}/detail/{row['propertyID']}/"
                )

            # Request with timeout and retry
            response = requests.get(url, headers=params['headers'], cookies=params['cookies'], timeout=15)
            if response.status_code != 200:
                continue

            # Parse response
            soup = BeautifulSoup(response.content, 'html.parser')
            container = soup.find('section', class_='property-info')
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
            property_details.update({
                'property_id': row['propertyID'],
                'building_name': row['buildingNameEn'],
                'zone': row['zoneEn'],
                'district': row['districtNameEn']
            })
            
            results.append(property_details)
            time.sleep(random.uniform(1.0, 2.5))

        except Exception as e:
            print(f"Error processing {row['propertyID']}: {str(e)}")
            continue

    # Save results with existing data
    if results:
        new_data = pd.DataFrame(results)
        combined_data = pd.concat([existing_data, new_data], ignore_index=True)
        
        # Remove potential duplicates from new batch
        combined_data = combined_data.drop_duplicates(
            subset=['property_id'], 
            keep='last'
        )
        
        # Save with type conversion for numeric fields
        numeric_cols = ['completion_year', 'total_floors', 'lifts']
        combined_data[numeric_cols] = combined_data[numeric_cols].apply(
            pd.to_numeric, errors='coerce'
        )
        
        #combined_data.to_csv(output_csv, index=False)
    
    return combined_data if results else existing_data



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

def safe_get_value(x, key, default=None):
    if isinstance(x, str):
        try:
            x = ast.literal_eval(x)
        except (SyntaxError, ValueError):
            return default
    if isinstance(x, dict):
        return x.get(key, default)
    return default

def scrape_transaction( 
    area_codes: pd.DataFrame,
    params: Dict[str, Any]
) -> pd.DataFrame:    

    """Kedro node for scraping and cleaning Centanet transaction data.
    
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
    
    # Set default dates
    end_date = params.get("end_date", datetime.date.today().strftime("%Y-%m-%d"))
    start_date = params["start_date"]

    try:
        start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    except Exception as e:
        raise ValueError(f"Invalid date format: {e}") from e

    start_api = start_dt.strftime("%d/%m/%Y")
    end_api = end_dt.strftime("%d/%m/%Y")
    date_range = f"{start_api}-{end_api}"
    date_range_encoded = urllib.parse.quote(date_range)

    session = create_session()
    cookies = get_cookies()
    base_url = params["transaction_api_url"]
    pagesize = params["page_size"]

    all_results = []
    
    for idx, row in tqdm(area_codes.iterrows(), total=area_codes.shape[0], desc="Processing Areas"):
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

        page_index = 1
        area_results = []

        while True:
            # Rotate session every 10 areas
            if idx % 10 == 0:
                cookies = get_cookies()
                session = create_session()

            url = (f"{base_url}?pageindex={page_index}&pagesize={pagesize}"
                   f"&daterang={date_range_encoded}&posttype=B&districtids={code}&lang=EN")
            
            headers = {
                "User-Agent": get_random_user_agent(),
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://oir.centanet.com/",
                "Origin": "https://oir.centanet.com",
                "Connection": "keep-alive"
            }

            try:
                response = session.get(url, headers=headers, cookies=cookies, timeout=20)
                response.raise_for_status()
                json_data = response.json()
                district_meta["requests"] += 1
            except requests.exceptions.RequestException as e:
                print(f"Request failed for {district}: {e}")
                break

            if json_data.get("responseCode") != 1:
                break

            items = json_data.get("data", {}).get("recordList", {}).get("items", [])
            if not items:
                break

            for item in items:
                item["Region"] = region
                item["District"] = district
                item["AreaCode"] = code
                area_results.append(item)
            
            district_meta["pages"] += 1
            district_meta["records"] += len(items)
            page_index += 1
            time.sleep(random.uniform(1, 3))

        if area_results:
            all_results.extend(area_results)
            district_meta["end"] = datetime.datetime.now().isoformat()
        
        time.sleep(random.uniform(3, 5))

    # Clean and transform data
    if not all_results:
        raise ValueError("No data collected from any districts")
    
    df = pd.DataFrame(all_results)
    df_clean = df[['id', 'deptDisplayName', 'centabldg', 'transactionDate', 
                   'transactionType', 'propertyNameCn', 'propertyNameEn', 
                   'propertyUsageDisplayName', 'floor', 'unit', 'isPriceEstimated', 
                   'transactionArea', 'sourceDisplayName', 'priceInfo', 'ibsContractID', 
                   'addressDisplayName', 'Region', 'District', 'AreaCode']].copy()

    # Price info parsing
    price_cols = {
        'price': 'price',
        'pricePostTypeDisplayName': 'price_post_type',
        'avgPrice': 'avg_price',
        'rental': 'rental',
        'rentPostTypeDisplayName': 'rent_post_type', 
        'avgRental': 'avg_rental'
    }
    
    for new_col, key in price_cols.items():
        df_clean[new_col] = df_clean['priceInfo'].apply(
            lambda x: safe_get_value(x, key)
        )

    df_clean.drop(columns=['priceInfo'], inplace=True)

    return df_clean


