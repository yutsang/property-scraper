###########################################

import pandas as pd
import requests
from tqdm import tqdm
import time
import logging
import os
import re
from typing import Dict, List, Optional, Union, Any, Tuple
from datetime import datetime, timedelta, date
from collections import defaultdict

# Import node tracking utilities
from ...utils.node_tracker import should_run_node, record_node_execution
from ...utils.building_supplement import (
    apply_supplemental_matches,
    load_supplemental_building_master,
)
from ...utils.source_runtime import get_required_setting

def scrape_source_b_buildings(
    area_codes: pd.DataFrame,
    params: Dict[str, Any],
    # csv_path: str, = inpuit
    # output_path: str = "source_b_buildings.csv", = output
    log_level: int = logging.INFO,
) -> pd.DataFrame:
    """
    Scrape building information from Source B ICI GraphQL API for all districts
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
    logger = logging.getLogger("source_b_scraper")
    
    # Define property types
    property_types = {
        "mr_ind": "Industrial",
        "mr_comm": "Office",
        "mr_shop": "Shop"
    }
    
    # Define the GraphQL endpoint
    url = params['source_b_commercial']['buildings_url']
    
    # Define the headers
    headers = params['source_b_commercial']['headers']
    
    # Read the CSV file with district IDs
    # Read district information
    listing_file = params['source_b_commercial']['source_b_commercial_building_listings']
    
    try:
        logger.info(f"Successfully loaded {len(area_codes)} districts from area code file.")
    except Exception as e:
        logger.error(f"Failed to load area code CSV file: {str(e)}")
        return pd.DataFrame()
    
    # Filter out the "All Districts" row (ID=0)
    area_codes = area_codes[area_codes['ID'] != 0]
    logger.info(f"Filtered to {len(area_codes)} districts (excluding 'All Districts')")
    
    # Load existing building counts per (district_id, property_type_code)
    # Used to skip combinations where the API count matches the DB count.
    existing_buildings_df = pd.DataFrame()
    db_combo_counts: dict[str, int] = {}   # key: "district_id_sbu" → count

    if os.path.exists(listing_file):
        try:
            existing_buildings_df = pd.read_parquet(listing_file, engine='pyarrow')
            logger.info(f"📊 Loaded {len(existing_buildings_df)} existing buildings")
            if 'district_id' in existing_buildings_df.columns and 'property_type_code' in existing_buildings_df.columns:
                for (did, sbu), grp in existing_buildings_df.groupby(['district_id', 'property_type_code']):
                    db_combo_counts[f"{did}_{sbu}"] = len(grp)
        except Exception as e:
            logger.warning(f"Could not read existing output file: {e}. Starting from scratch.")
    
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

                # After fetching, we compare API count vs DB count.
                # We do the API call first to get the count, then decide.
                combo_key = f"{district_id}_{sbu}"
                
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
                max_retries = params['source_b_commercial']['max_retries']
                request_delay = params['source_b_commercial']['request_delay']
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
                                api_count = len(buildings) if buildings else 0

                                # Skip if count matches DB — no change
                                if api_count == db_combo_counts.get(combo_key, -1):
                                    logger.debug(f"⏭  {property_type}/{district_name_en}: unchanged ({api_count}) — skipping")
                                    success = True
                                    break  # exit retry loop; pbar updated below

                                if buildings:
                                    for building in buildings:
                                        building['district_id'] = district_id
                                        building['district_name_en'] = district_name_en
                                        building['district_name_cn'] = district_name_cn
                                        building['property_type'] = property_type
                                        building['property_type_code'] = sbu
                                        all_buildings.append(building)

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
                if params['source_b_commercial']['save_incremental'] and processed_count % params['source_b_commercial']['incremental_save_frequency'] == 0:
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
        node_name="scrape_source_b_buildings",
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
    text = text.replace("'", "")
    text = text.replace("+", "-")
    return "-".join(text.split()).strip()

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
        base_url = get_required_setting(params, "source_b_commercial", "site", "detail_base_url")
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

                    # District / region hierarchy when exposed in JSON (keys vary by page version)
                    for i in range(1, 5):
                        ak, dk, dk_alt = f'area{i}', f'areaDesc{i}', f'areadesc{i}'
                        if ak in building_data and building_data[ak] is not None:
                            info[f'area{i}'] = building_data[ak]
                        if dk in building_data and building_data[dk] is not None:
                            info[f'area_desc{i}'] = building_data[dk]
                        elif dk_alt in building_data and building_data[dk_alt] is not None:
                            info[f'area_desc{i}'] = building_data[dk_alt]
                    for key in ('district', 'region', 'subDistrict'):
                        node = building_data.get(key)
                        if isinstance(node, dict) and node.get('name'):
                            slot = None
                            for j in range(1, 5):
                                if f'area_desc{j}' not in info:
                                    slot = j
                                    break
                            if slot is not None:
                                code = node.get('code') or node.get('distCode') or node.get('id')
                                info[f'area{slot}'] = code if code is not None else ''
                                info[f'area_desc{slot}'] = node.get('name') or ''
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
    details_file = params.get('source_b_commercial', {}).get('building_details_file', 'data/02_intermediate/midland_ici_building_details.parquet')
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
    saved_count = 0  # track how many results have already been merged to disk
    try:
        with tqdm(total=len(new_listings), desc="Processing buildings") as pbar:
            for _, row in new_listings.iterrows():
                result = scrape_building_info(row)
                if result:
                    results.append(result)
                    if len(results) % 5 == 0 and len(results) > saved_count:
                        # Incremental save: merge new results with existing data
                        incremental_df = pd.concat([existing_df, pd.DataFrame(results)], ignore_index=True)
                        if 'id' in incremental_df.columns:
                            incremental_df = incremental_df.drop_duplicates(subset=['id'], keep='last')
                        incremental_df.to_parquet(details_file, engine='pyarrow')
                        saved_count = len(results)
                pbar.update(1)

    except KeyboardInterrupt:
        logger.warning("User interrupt detected! Saving partial results...")
        if results:
            partial_df = pd.concat([existing_df, pd.DataFrame(results)], ignore_index=True)
            if 'id' in partial_df.columns:
                partial_df = partial_df.drop_duplicates(subset=['id'], keep='last')
            partial_df.to_parquet(details_file, engine='pyarrow')

    finally:
        driver.quit()

    if results:
        final_df = pd.concat([existing_df, pd.DataFrame(results)], ignore_index=True)
        if 'id' in final_df.columns:
            final_df = final_df.drop_duplicates(subset=['id'], keep='last')
        final_df.to_parquet(details_file, engine='pyarrow')
        return final_df

    return existing_df



####################################################################################
# Transaction

import requests
import time
from datetime import datetime
from tqdm import tqdm
import json
import pandas as pd

def sanitize_parquet_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and normalize DataFrame columns for Parquet compatibility"""
    import re
    # Use re.sub via list comprehension - avoids .str accessor which requires
    # the Index dtype to already be string (fails on mixed/int column names)
    df.columns = [re.sub(r'[^a-zA-Z0-9_]', '_', str(col)) for col in df.columns]
    
    # Handle special columns
    if 'floor' in df.columns:
        # Keep "/" in values (e.g. G/F); strip only control characters
        df['floor'] = (
            df['floor']
            .astype(str)
            .str.replace(r'[\x00-\x1F\x7F-\x9F]', '', regex=True)
            .str.strip()
        )
    
    # Convert object columns to string
    obj_cols = df.select_dtypes('object').columns
    df[obj_cols] = df[obj_cols].astype('string')
    
    return df


def _source_b_commercial_tx_date_to_date(tx_raw: Optional[Any]) -> Optional[date]:
    """Calendar date for incremental cutoff. Uses HK local date for ISO datetimes."""
    if tx_raw is None or (isinstance(tx_raw, float) and pd.isna(tx_raw)):
        return None
    s = str(tx_raw).strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        prefix = s[:10]
        try:
            return datetime.fromisoformat(prefix).date()
        except ValueError:
            pass
    try:
        ts = pd.to_datetime(s, utc=True, errors="coerce")
        if pd.isna(ts):
            return None
        if getattr(ts, "tz", None) is None:
            ts = ts.tz_localize("UTC")
        return ts.tz_convert("Asia/Hong_Kong").date()
    except Exception:
        return None


def _source_b_commercial_tx_date_storage(tx_raw: Optional[Any]) -> Optional[str]:
    """Stored tx_date as yyyy-mm-dd 00:00:00 (HK calendar day for ISO timestamps)."""
    d = _source_b_commercial_tx_date_to_date(tx_raw)
    if d is None:
        return None
    return f"{d.isoformat()} 00:00:00"


def _strip_source_b_commercial_floor_label(raw: Optional[Any]) -> Optional[str]:
    """Normalize API floor strings (e.g. **HIGH** band labels)."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    s = str(raw).strip()
    if s.lower() in ("", "nan", "none"):
        return None
    s = s.replace("*", "").strip()
    return s if s else None


def _is_source_b_blank(value: Optional[Any]) -> bool:
    """Treat common empty-like tokens consistently across API and parquet merges."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return True
    return str(value).strip().lower() in ("", "nan", "none", "null", "n/a")


def _clean_source_b_text(value: Optional[Any]) -> Optional[str]:
    """Return stripped text or None when the input is blank-like."""
    if _is_source_b_blank(value):
        return None
    text = str(value).strip()
    return text or None


def _contains_cjk(text: Optional[str]) -> bool:
    """Detect Chinese characters in fields that should be English-only."""
    if not text:
        return False
    return bool(re.search(r"[\u3400-\u4dbf\u4e00-\u9fff]", text))


def _clean_source_b_english(value: Optional[Any]) -> Optional[str]:
    """Discard Chinese fallback text from English output columns."""
    text = _clean_source_b_text(value)
    if not text or _contains_cjk(text):
        return None
    return text


def _pick_floor_flat_raw(
    zh_rec: Dict[str, Any], en_rec: Dict[str, Any]
) -> Tuple[Optional[str], Optional[str]]:
    """Prefer zh-hk floor/flat; fall back to English row when zh is blank."""
    for rec in (zh_rec, en_rec):
        f = _strip_source_b_commercial_floor_label(rec.get("floor"))
        fl = _strip_source_b_commercial_floor_label(rec.get("flat"))
        if f or fl:
            return (rec.get("floor"), rec.get("flat"))
    return (None, None)


def _extract_tx_area_hierarchy(
    zh_rec: Dict[str, Any], en_rec: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Copy optional split-area fragments from transaction JSON.

    ``search/v1/transaction`` always exposes district separately. Some payloads
    also include ``area1``..``area4`` / ``areaDesc1``.. (camelCase) inside the
    nested ``area`` object. Those keys describe split area fragments such as
    ``800`` + ``G/F 2室`` rather than geographic hierarchy, so we normalize them
    to ``area1`` / ``area_desc1`` without backfilling district values.
    """
    out: Dict[str, Any] = {}
    zh_area = zh_rec.get("area") if isinstance(zh_rec.get("area"), dict) else {}
    en_area = en_rec.get("area") if isinstance(en_rec.get("area"), dict) else {}
    for i in range(1, 5):
        ak = f"area{i}"
        dk = f"areaDesc{i}"
        dk_alt = f"areadesc{i}"
        code: Any = None
        desc: Any = None
        # First non-empty wins (zh-hk before en). Newer API payloads keep area1/2
        # under the nested ``area`` object rather than top-level record keys.
        for area_node, rec in ((zh_area, zh_rec), (en_area, en_rec)):
            if code is None:
                for source in (area_node, rec):
                    if isinstance(source, dict) and not _is_source_b_blank(source.get(ak)):
                        code = source.get(ak)
                        break
            if desc is None:
                for source in (area_node, rec):
                    if not isinstance(source, dict):
                        continue
                    for key in (dk, dk_alt):
                        if not _is_source_b_blank(source.get(key)):
                            desc = source.get(key)
                            break
                    if desc is not None:
                        break
        out[ak] = code
        out[f"area_desc{i}"] = desc
    return out


def _derive_floor_flat_from_area_descriptions(
    tx_hierarchy: Dict[str, Any],
) -> Tuple[Optional[str], Optional[str]]:
    """
    Some rows encode floor/unit hints inside ``areaDesc`` fragments, e.g.
    ``G/F D室`` or ``B/F``. Use the first descriptor that looks like a floor or
    unit token to backfill missing ``floor`` / ``flat`` values.
    """
    for idx in range(1, 5):
        desc = _clean_source_b_text(tx_hierarchy.get(f"area_desc{idx}"))
        if not desc:
            continue
        if not re.search(
            r"室|單位|舖|铺|Flat|flat|Shop|WF|舖位|(?:^|\\s)(?:G|LG|UG|B)\\s*/\\s*F|\\d+\\s*/\\s*F|"
            r"地下|地舖|全層|高層|中層|低層|天井",
            desc,
            flags=re.IGNORECASE,
        ):
            continue
        return _split_floor_flat_ici(desc, None)
    return (None, None)


def _build_tx_match_key(rec: Dict[str, Any]) -> Tuple[Any, ...]:
    """Create a stable bilingual alignment key that is safer than page index alone."""
    building = rec.get("building") or {}
    streets = rec.get("streets") or {}
    area = rec.get("area") or {}
    return (
        rec.get("txDate"),
        rec.get("txType"),
        building.get("id"),
        streets.get("id"),
        streets.get("streetno"),
        rec.get("price"),
        rec.get("rent"),
        rec.get("ftPrice"),
        rec.get("ftRent"),
        area.get("value") if isinstance(area, dict) else area,
    )


def _split_floor_flat_ici(
    floor: Optional[str], flat: Optional[str]
) -> Tuple[Optional[str], Optional[str]]:
    """Split combined floor/flat tokens (e.g. 'G/F D室') when only one field is populated."""
    def _clean(x: Optional[str]) -> str:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return ""
        s = str(x).strip()
        if s.lower() in ("", "nan", "none"):
            return ""
        return s.replace("*", "").strip()

    f, fl = _clean(floor), _clean(flat)
    if f and fl:
        return (f, fl)
    combined = f or fl
    if not combined:
        return (None, None)
    # e.g. "G/F D室", "地下 舖位"
    m = re.match(r"^(.+?/\s*F)\s+(\S.+)$", combined, flags=re.IGNORECASE)
    if m and re.search(r"室|單位|舖|铺|Flat|flat|Shop|WF", m.group(2)):
        return (m.group(1).strip(), m.group(2).strip())
    # Chinese band / level + unit in one token (no "/F")
    m2 = re.match(
        r"^(地下|全層|高層|中層|低層|連天台|高|中|低)\s+(\S.+)$",
        combined,
    )
    if m2 and re.search(
        r"室|單位|舖|铺|Flat|flat|Shop|WF|舖位", m2.group(2), flags=re.IGNORECASE
    ):
        return (m2.group(1).strip(), m2.group(2).strip())
    return (f or None, fl or None)


def source_b_commercial_scrape_trans(
    params: Dict[str, Any]) -> pd.DataFrame:
    """
    Scrape transaction data from Source B ICI using NEW API endpoint.
    
    NEW API ENDPOINT (as of Jan 2026):
    - URL: defined in local `source_b_commercial.transaction_url`
    - Requires session cookies from main landing page
    - Returns: ``[{type, count, results: [{txDate, area: {value, area1, areaDesc1, ...}, floor, flat,
      streets: {id, name, streetno}, building: {id, name}, district: {distCode, name},
      ...}]}]``
      Live payloads (2026) expose district separately from optional split-area
      fragments such as ``area1`` / ``areaDesc1``. We keep district in
      ``dist_code`` / ``dist_name_*`` and normalize the camelCase area keys to
      snake_case ``area_desc*`` columns.
    - Bilingual: same page is fetched with ``lang=zh-hk`` and ``lang=en``; rows are
      merged by index for Chinese vs English street/building/district names.
    
    Includes node execution tracking and incremental updates.
    """
    
    # Initialize logging first
    logger = logging.getLogger(__name__)
    
    # Date-based decision: check DB max date vs today (no timer needed)
    output_file = "data/01_raw/midland_ici_trans.parquet"
    all_transactions: List[Dict[str, Any]] = []
    pages_processed = 0
    existing_df: pd.DataFrame = pd.DataFrame()

    def map_ics_type(value: str) -> str:
        if not value:
            return None
        mapping = {
            "COMMERCIAL": "commercial",
            "INDUSTRIAL": "industrial",
            "RETAIL": "retail",
            "SHOPS": "retail",
        }
        return mapping.get(str(value).upper(), str(value).lower())

    def normalize_transaction(zh_rec: Dict[str, Any], en_rec: Dict[str, Any]) -> Dict[str, Any]:
        """Merge zh-hk and en API rows (same index) for bilingual street/district/building names."""
        area = zh_rec.get("area") if isinstance(zh_rec.get("area"), dict) else {}
        district_zh = zh_rec.get("district") or {}
        district_en = en_rec.get("district") or {}
        streets_zh = zh_rec.get("streets") or {}
        streets_en = en_rec.get("streets") or {}
        building_zh = zh_rec.get("building") or {}
        building_en = en_rec.get("building") or {}

        tx_date_normalized = _source_b_commercial_tx_date_storage(zh_rec.get("txDate"))

        floor_raw, flat_raw = _pick_floor_flat_raw(zh_rec, en_rec)
        floor_s, flat_s = _split_floor_flat_ici(
            str(floor_raw) if floor_raw is not None else None,
            str(flat_raw) if flat_raw is not None else None,
        )

        dist_code = district_zh.get("distCode") or district_en.get("distCode")
        dist_name_zh = district_zh.get("name")
        dist_name_en = _clean_source_b_english(district_en.get("name"))

        street_id = streets_zh.get("id") or streets_en.get("id")
        street_name_zh = _clean_source_b_text(streets_zh.get("name"))
        street_name_en = _clean_source_b_english(streets_en.get("name"))

        tx_hierarchy = _extract_tx_area_hierarchy(zh_rec, en_rec)
        if floor_s is None or flat_s is None:
            derived_floor, derived_flat = _derive_floor_flat_from_area_descriptions(tx_hierarchy)
            if floor_s is None:
                floor_s = derived_floor
            if flat_s is None:
                flat_s = derived_flat

        return {
            "tx_date": tx_date_normalized,
            "tx_type": zh_rec.get("txType"),
            "area": area.get("value"),
            "flat": flat_s,
            "floor": floor_s,
            "ft_rent": zh_rec.get("ftRent"),
            "ft_sell": zh_rec.get("ftPrice"),
            "rent": zh_rec.get("rent"),
            "sell": zh_rec.get("price"),
            "price": zh_rec.get("price"),
            "price_per_feet": zh_rec.get("ftPrice") or zh_rec.get("ftRent"),
            "ics_type": map_ics_type(zh_rec.get("sbuOwner")),
            "sbuOwner": zh_rec.get("sbuOwner"),
            "upload_source": zh_rec.get("uploadSource"),
            "dist_code": dist_code,
            "dist_name_zh": _clean_source_b_text(dist_name_zh),
            "dist_name_en": dist_name_en,
            "street_id": str(street_id) if street_id is not None else None,
            "street_name_zh": street_name_zh,
            "street_name_en": street_name_en,
            "streetno": _clean_source_b_text(streets_zh.get("streetno") or streets_en.get("streetno")),
            "building_id": str(building_zh.get("id") or building_en.get("id")) if (building_zh.get("id") or building_en.get("id")) else None,
            "eng_name": _clean_source_b_english(building_en.get("name")),
            "chi_name": _clean_source_b_text(building_zh.get("name") or building_en.get("name")),
            "URL": zh_rec.get("propertyListUrl") or en_rec.get("propertyListUrl"),
            "Name": _clean_source_b_text(zh_rec.get("name")),
            **tx_hierarchy,
        }

    mici_params = params.get("source_b_commercial", {})
    api_url = mici_params["transaction_url"]
    landing_url = mici_params.get(
        "transaction_landing_url",
        get_required_setting(params, "source_b_commercial", "transaction_landing_url"),
    )
    headers_main = mici_params.get("transaction_headers_main", mici_params.get("headers", {}))
    headers_api = mici_params.get("transaction_headers_api", mici_params.get("headers", {}))
    api_params = dict(mici_params.get("transaction_api_params", {}))
    page_size = int(mici_params.get("transaction_page_size", 100))
    api_params["limit"] = page_size
    api_params.setdefault("page", 1)
    lang_zh = mici_params.get("transaction_lang_zh", api_params.get("lang", "zh-hk"))
    lang_en = mici_params.get("transaction_lang_en", "en")

    # Date-based decision: read DB max date → decide start_date.
    # NOTE: we intentionally do NOT return early here even when max_date >= today.
    # We must fetch page 1 first so the gap detection (API total vs our count) can
    # decide whether a full re-scrape is needed.  The early "up-to-date" skip only
    # happens later, after confirming no significant gap exists.
    start_date = params["global"]["start_date"]
    date_columns = ["date", "transaction_date", "tx_date", "Date", "transactionDate"]
    today = datetime.now().date()
    existing_record_count = 0  # track for gap detection later
    _max_date_is_current = False  # True when existing data already reaches today
    if os.path.exists(output_file):
        try:
            existing_df = pd.read_parquet(output_file)
            existing_record_count = len(existing_df)
            for col in date_columns:
                if col in existing_df.columns:
                    parsed = pd.to_datetime(existing_df[col], errors="coerce")
                    if parsed.notna().any():
                        max_date = parsed.max().date()
                        if max_date >= today:
                            _max_date_is_current = True
                            # Don't skip yet — gap detection happens after page-1 fetch
                            logger.info(
                                f"📅 Existing data reaches {max_date}; "
                                f"will verify API count before skipping (existing={existing_record_count:,})"
                            )
                        else:
                            start_date = max_date + timedelta(days=1)
                            logger.info(f"📊 Incremental fetch from {start_date}")
                        break
        except Exception as exc:
            logger.warning(f"Failed to derive incremental start date: {exc}")

    if isinstance(start_date, str):
        start_date = datetime.fromisoformat(start_date).date()
    elif isinstance(start_date, datetime):
        start_date = start_date.date()

    import math
    from concurrent.futures import ThreadPoolExecutor, as_completed

    session = requests.Session()
    max_retries   = int(mici_params.get("max_retries", 5))
    request_delay = float(mici_params.get("request_delay", 0.3))
    # Keep workers low — server 504s at 8; 3 is a safe ceiling
    max_workers   = min(int(mici_params.get("max_workers", 3)), 3)
    # Batch size: how many pages to fire in one wave before pausing
    batch_size    = int(mici_params.get("batch_size", 15))
    inter_batch_delay = float(mici_params.get("inter_batch_delay", 2.0))

    # ── Step 1: Load landing page to acquire session cookies ─────────────
    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(landing_url, headers=headers_main, timeout=20)
            response.raise_for_status()
            break
        except requests.exceptions.RequestException as exc:
            logger.warning(f"Landing page attempt {attempt} failed: {exc}")
            if attempt == max_retries:
                raise
            time.sleep(2 + attempt)

    def _fetch_one_lang(params_copy: Dict[str, Any]) -> Optional[list]:
        for attempt in range(1, max_retries + 1):
            try:
                resp = session.get(
                    api_url, headers=headers_api, params=params_copy, timeout=30
                )
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else 0
                if attempt == max_retries:
                    logger.warning(
                        f"Page {params_copy.get('page')} lang={params_copy.get('lang')} "
                        f"failed after {max_retries} attempts: {exc}"
                    )
                    return None
                if status in (502, 503, 504):
                    wait = (2 ** attempt) * 5
                    logger.debug(
                        f"Page {params_copy.get('page')}: {status} – waiting {wait}s"
                    )
                    time.sleep(wait)
                else:
                    time.sleep(1 + attempt)
            except (requests.exceptions.RequestException, json.JSONDecodeError) as exc:
                if attempt == max_retries:
                    logger.warning(
                        f"Page {params_copy.get('page')} lang={params_copy.get('lang')} "
                        f"failed after {max_retries} attempts: {exc}"
                    )
                    return None
                time.sleep(2 + attempt)
        return None

    def fetch_page_bilingual(page_num: int) -> tuple[int, Optional[list], Optional[list]]:
        """Fetch the same page in zh-hk and en; en is used for English street/building names."""
        params_zh = dict(api_params)
        params_zh["page"] = page_num
        params_zh["lang"] = lang_zh
        params_en = dict(params_zh)
        params_en["lang"] = lang_en
        data_zh = _fetch_one_lang(params_zh)
        data_en = _fetch_one_lang(params_en)
        if data_zh is None:
            return page_num, None, None
        if data_en is None:
            logger.warning(
                f"Page {page_num}: English payload missing — using zh-hk for English name fields"
            )
            data_en = data_zh
        return page_num, data_zh, data_en

    def parse_page(data_zh: Optional[list], data_en: Optional[list]) -> tuple[list, bool]:
        """Parse raw API responses. Returns (records, stop_flag)."""
        if not isinstance(data_zh, list) or not data_zh:
            return [], True
        item_zh = data_zh[0] or {}
        results_zh = item_zh.get("results") or []
        results_en: List[Dict[str, Any]] = []
        if isinstance(data_en, list) and data_en and (data_en[0] or {}).get("results"):
            results_en = (data_en[0] or {}).get("results") or []
        if len(results_en) != len(results_zh) and results_en:
            logger.warning(
                f"zh/en result count mismatch: {len(results_zh)} vs {len(results_en)} — padding en"
            )
        en_by_key: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = defaultdict(list)
        for en_rec in results_en:
            en_by_key[_build_tx_match_key(en_rec)].append(en_rec)
        records: List[Dict[str, Any]] = []
        stop = False
        for i, zh_rec in enumerate(results_zh):
            key = _build_tx_match_key(zh_rec)
            en_matches = en_by_key.get(key) or []
            if en_matches:
                en_rec = en_matches.pop(0)
            elif i < len(results_en) and _build_tx_match_key(results_en[i]) == key:
                en_rec = results_en[i]
            else:
                en_rec = {}
            tx_raw = zh_rec.get("txDate")
            tx_date_obj = _source_b_commercial_tx_date_to_date(tx_raw)
            if tx_date_obj and tx_date_obj < start_date:
                stop = True
                continue
            records.append(normalize_transaction(zh_rec, en_rec))
        return records, stop

    # ── Step 2: Pre-fetch page 1 to discover total_count and total_pages ─
    _, page1_zh, page1_en = fetch_page_bilingual(1)
    page1_data = page1_zh
    if not page1_data or not isinstance(page1_data, list) or not page1_data[0]:
        logger.error("Could not fetch page 1 — aborting transaction scrape")
        trans_df = pd.DataFrame()
    else:
        total_count = (page1_data[0] or {}).get("count", 0) or 0
        total_pages = math.ceil(total_count / page_size) if total_count else 1
        logger.info(f"📊 Total transactions: {total_count:,}  |  Pages: {total_pages}  |  Workers: {max_workers}")

        # Gap detection: if the API total significantly exceeds our existing
        # record count, the incremental start_date may be skipping historical
        # gaps (e.g., missed pages during a previous partial scrape).
        # Reset start_date to a very early date so ALL pages are processed;
        # deduplication at merge time handles any overlap with existing data.
        gap = total_count - existing_record_count
        if existing_record_count > 0 and gap > max(100, existing_record_count * 0.01):
            logger.warning(
                f"⚠️  Gap detected: API total={total_count:,} existing={existing_record_count:,} "
                f"(gap={gap:,}) — resetting start_date to 2000-01-01 for full re-fetch"
            )
            start_date = datetime(2000, 1, 1).date()
            _max_date_is_current = False  # force full scrape even if max_date was today
            # Re-parse page 1 with the updated start_date so records aren't
            # incorrectly filtered out by the old (incremental) cutoff.
            page1_records, page1_stop = parse_page(page1_zh, page1_en)
        elif _max_date_is_current:
            # Data is genuinely up-to-date and no gap — safe to skip.
            logger.info(f"✅ Source B ICI transactions up-to-date (API={total_count:,} = existing={existing_record_count:,}) — skipping")
            return existing_df if existing_record_count > 0 else pd.DataFrame(all_transactions)
        else:
            page1_records, page1_stop = parse_page(page1_zh, page1_en)

        all_transactions.extend(page1_records)

        # ── Step 3: Fetch remaining pages in parallel ─────────────────────
        remaining_pages = range(2, total_pages + 1)
        stop_fetching = page1_stop  # set True once oldest date crosses start_date

        with tqdm(
            total=total_pages,
            desc="Processing Trans",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]"
        ) as pbar:
            pbar.update(1)          # account for page 1 already done
            pbar.set_postfix_str(f"Trans: {len(all_transactions):,}/{total_count:,}")

            if not stop_fetching and remaining_pages:
                page_list = list(remaining_pages)

                # Process in small batches to avoid sustained server hammering.
                # Each batch fires ≤batch_size pages with ≤max_workers threads,
                # then pauses inter_batch_delay seconds before the next wave.
                for batch_start in range(0, len(page_list), batch_size):
                    if stop_fetching:
                        break

                    batch = page_list[batch_start: batch_start + batch_size]

                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        futures = {
                            executor.submit(fetch_page_bilingual, p): p for p in batch
                        }
                        batch_results: Dict[int, Tuple[Optional[list], Optional[list]]] = {}
                        for future in as_completed(futures):
                            page_num, data_zh, data_en = future.result()
                            batch_results[page_num] = (data_zh, data_en)

                    # Process batch in page order so date-cutoff logic stays correct
                    for page_num in sorted(batch_results):
                        data_zh, data_en = batch_results[page_num]
                        if data_zh is None:
                            pbar.update(1)
                            continue
                        records, stop = parse_page(data_zh, data_en)
                        all_transactions.extend(records)
                        if stop:
                            stop_fetching = True
                        pbar.update(1)
                        pbar.set_postfix_str(f"Trans: {len(all_transactions):,}/{total_count:,}")

                    # Polite pause between batches
                    if not stop_fetching and batch_start + batch_size < len(page_list):
                        time.sleep(inter_batch_delay)

        pages_processed = total_pages

    if not all_transactions:
        _existing_file = "data/01_raw/midland_ici_trans.parquet"
        if os.path.exists(_existing_file):
            logger.info("No new transactions found — returning existing data unchanged")
            existing_df = pd.read_parquet(_existing_file, engine='pyarrow')
            record_node_execution(
                node_name="source_b_commercial_scrape_trans",
                node_type="transaction",
                metadata={"records_processed": len(existing_df), "pages_processed": 0,
                          "execution_time": datetime.now().isoformat()}
            )
            return existing_df
        logger.warning("No transactions collected and no existing file — returning empty DataFrame")
        trans_df = pd.DataFrame()
    else:
        trans_df = pd.DataFrame(all_transactions)

    # Add sanitization step (handles empty df gracefully)
    if not trans_df.empty:
        trans_df = sanitize_parquet_columns(trans_df)
    
    # Ensure UTF-8 encoding (use 'replace' instead of 'ignore' to avoid silent data loss)
    trans_df = trans_df.map(
        lambda x: x.encode('utf-8', 'replace').decode('utf-8')
        if isinstance(x, str) else x
    )
    
    # MERGE with existing data (don't overwrite!)
    if not trans_df.empty:
        try:
            output_file = "data/01_raw/midland_ici_trans.parquet"
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            # Load existing data if available
            if os.path.exists(output_file):
                try:
                    existing_df = pd.read_parquet(output_file, engine='pyarrow')
                    logger.info(f"Loading {len(existing_df):,} existing transactions for merge")
                    
                    # Align columns
                    all_cols = sorted(set(existing_df.columns) | set(trans_df.columns))
                    for col in all_cols:
                        if col not in existing_df.columns:
                            existing_df[col] = pd.NA
                        if col not in trans_df.columns:
                            trans_df[col] = pd.NA
                    
                    existing_df = existing_df[all_cols]
                    trans_df = trans_df[all_cols]
                    
                    # Fix data types before concatenation
                    for col in all_cols:
                        # Ensure string columns stay as strings
                        if col in ['building_id', 'dist_code', 'floor', 'flat', 'streetno', 'street_id']:
                            existing_df[col] = existing_df[col].astype(str).replace('nan', pd.NA).replace('None', pd.NA)
                            trans_df[col] = trans_df[col].astype(str).replace('nan', pd.NA).replace('None', pd.NA)
                    
                    # Concatenate
                    combined_df = pd.concat([existing_df, trans_df], ignore_index=True)
                    logger.info(f"Combined: {len(combined_df):,} ({len(existing_df):,} existing + {len(trans_df):,} new)")

                    # Re-sanitize after concat: mixed float+string object columns
                    # (e.g. area1..area4 from older/newer API payloads) must be
                    # coerced back to a single dtype before pyarrow serialization.
                    combined_df = sanitize_parquet_columns(combined_df)
                    
                    # Deduplicate using a broad key that preserves distinct
                    # transactions. ICI records often have NULL floor, so using
                    # only (tx_date, building_id, floor, flat) collapses legitimate
                    # records. Adding tx_type + sell + rent + area gives much
                    # better selectivity without a proper transaction ID.
                    dedup_cols = ['tx_date', 'building_id', 'floor', 'flat',
                                  'tx_type', 'sell', 'rent', 'area']
                    existing_dedup_cols = [col for col in dedup_cols if col in combined_df.columns]
                    
                    if existing_dedup_cols:
                        before_dedup = len(combined_df)
                        combined_df = combined_df.drop_duplicates(subset=existing_dedup_cols, keep='last')
                        logger.info(f"Deduplication: {before_dedup:,} -> {len(combined_df):,} (removed {before_dedup - len(combined_df):,} duplicates)")
                    
                    trans_df = combined_df
                    
                except Exception as e:
                    logger.warning(f"Failed to load existing data for merge: {e}")
                    logger.info("Saving new data only")
            
            # Save merged/new data
            trans_df.to_parquet(output_file, engine='pyarrow', index=False)
            logger.info(f"Saved {len(trans_df):,} total transactions to {output_file}")
            
        except Exception as e:
            logger.error(f"Failed to save transactions to {output_file}: {str(e)}")
    
    # Record node execution
    record_node_execution(
        node_name="source_b_commercial_scrape_trans",
        node_type="transaction",
        metadata={
            "records_processed": len(trans_df),
            "pages_processed": pages_processed,
            "execution_time": datetime.now().isoformat()
        }
    )
    
    return trans_df


def source_b_commercial_join(
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
        transactions (pd.DataFrame): DataFrame containing transaction data (source_b_commercial_trans)
        building_details (pd.DataFrame): DataFrame containing building details (source_b_commercial_building_details)
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
    mici_params = params.get("source_b_commercial", {})
    join_params = mici_params.get("join", {})
    
    left_on = join_params.get('join_left_on', 'building_id')
    right_on = join_params.get('join_right_on', 'id')
    join_type = join_params.get('join_type', 'left')
    supplemental_master = load_supplemental_building_master(
        params,
        domain="commercial",
        source_system="source_b_commercial",
    )
    
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

    # Prefer building-details hierarchy when both sides have area columns (suffix _building).
    # combine_first fills NaN in the caller from the argument, so building_col.combine_first(tx_col)
    # means: use building value when available, fall back to transaction value.
    for i in range(1, 5):
        for col in (f"area{i}", f"area_desc{i}"):
            bcol = f"{col}_building"
            if bcol in joined_df.columns:
                joined_df[bcol] = joined_df[bcol].replace(["", " ", "None", "nan", "NaN", "null"], pd.NA)
                if col in joined_df.columns:
                    joined_df[col] = joined_df[col].replace(["", " ", "None", "nan", "NaN", "null"], pd.NA)
                    # building-details wins; transaction fills remaining gaps
                    joined_df[col] = joined_df[bcol].combine_first(joined_df[col])
                else:
                    joined_df[col] = joined_df[bcol]
                joined_df = joined_df.drop(columns=[bcol])
    
    logger.info(f"Join completed. Result has {len(joined_df)} rows")
    
    # Add a flag to indicate if a transaction has matched building details
    joined_df['has_building_match'] = joined_df[right_on].notnull()
    joined_df['building_match_method'] = joined_df['has_building_match'].map(
        {True: 'native_building_id', False: 'unmatched'}
    )
    joined_df['matched_building_name'] = pd.NA
    if 'Building Name' in joined_df.columns:
        joined_df['matched_building_name'] = joined_df['Building Name'].where(
            joined_df['has_building_match'],
            pd.NA,
        )

    if not supplemental_master.empty:
        unmatched_mask = ~joined_df['has_building_match']
        supplemental_matched, supplemental_unmatched = apply_supplemental_matches(
            unmatched_df=joined_df.loc[unmatched_mask].copy(),
            supplemental_master=supplemental_master,
            source_system="source_b_commercial",
            domain="commercial",
            join_key_col=left_on if left_on in joined_df.columns else None,
            source_name_col='eng_name' if 'eng_name' in joined_df.columns else 'chi_name',
            district_name_col='dist_name_en' if 'dist_name_en' in joined_df.columns else None,
            zone_col=None,
            output_match_method_col='building_match_method',
        )
        if not supplemental_matched.empty:
            supplemental_matched['has_building_match'] = True
            supplemental_matched[right_on] = supplemental_matched.get(
                right_on,
                supplemental_matched.get(left_on),
            )
            if 'completion_year' in supplemental_matched.columns:
                supplemental_matched['Completion'] = supplemental_matched.get(
                    'Completion',
                    pd.Series(pd.NA, index=supplemental_matched.index),
                ).where(
                    supplemental_matched.get(
                        'Completion',
                        pd.Series(pd.NA, index=supplemental_matched.index),
                    ).notna(),
                    supplemental_matched['completion_year'],
                )
            if 'management_company' in supplemental_matched.columns:
                supplemental_matched['Management Company'] = supplemental_matched.get(
                    'Management Company',
                    pd.Series(pd.NA, index=supplemental_matched.index),
                ).where(
                    supplemental_matched.get(
                        'Management Company',
                        pd.Series(pd.NA, index=supplemental_matched.index),
                    ).notna(),
                    supplemental_matched['management_company'],
                )
            if 'url' in supplemental_matched.columns:
                supplemental_matched['URL'] = supplemental_matched.get(
                    'URL',
                    pd.Series(pd.NA, index=supplemental_matched.index),
                ).where(
                    supplemental_matched.get(
                        'URL',
                        pd.Series(pd.NA, index=supplemental_matched.index),
                    ).notna(),
                    supplemental_matched['url'],
                )
            matched_names = supplemental_matched.get('matched_building_name')
            if matched_names is not None and 'Building Name' in supplemental_matched.columns:
                supplemental_matched['Building Name'] = supplemental_matched['Building Name'].where(
                    supplemental_matched['Building Name'].notna(),
                    matched_names,
                )

            joined_df = pd.concat(
                [joined_df.loc[~unmatched_mask], supplemental_matched, supplemental_unmatched],
                ignore_index=True,
            )
    
    matched = joined_df['has_building_match'].sum()
    total = len(joined_df)
    unmatched = total - matched
    match_pct = matched / total * 100 if total else 0
    logger.info(f"Join result: {matched:,} matched, {unmatched:,} unmatched out of {total:,} ({match_pct:.1f}% match rate)")
    if match_pct < 50:
        logger.warning(f"Low match rate ({match_pct:.1f}%) - check if building data is stale or join keys have changed")

    return joined_df




