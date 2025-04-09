# src/kedro_centaline/pipelines/data_processing/nodes.py
import time
import random
import pandas as pd
import numpy as np
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import chromedriver_autoinstaller
import logging
from typing import Dict, Any
from tqdm import tqdm
import configparser
import re
import string
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
import os
import json
from pathlib import Path
from typing import Optional, Set
import pickle
import hashlib
import yaml


# Configure logging
logger = logging.getLogger(__name__)

def generate_session_id(length=10):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

def clean_subdistrict(subdistrict):
    cleaned = re.sub(r'[^A-Za-z0-9]+', '-', subdistrict)
    return cleaned.strip('-').lower()

# Helper functions
# nodes.py (updated ChromeDriver configuration)
def initialize_driver(params: Dict[str, Any]) -> webdriver.Remote:
    """Universal driver initialization with auto-installation"""
    if params.get('use_edge', False):
        return _initialize_edge_driver(params)
    return _initialize_chrome_driver(params)

def _initialize_chrome_driver(params: Dict[str, Any]) -> webdriver.Chrome:
    """Chrome-specific initialization"""
    import chromedriver_autoinstaller
    chromedriver_autoinstaller.install()
    
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    if params.get('headless', True):
        options.add_argument("--headless=new")
    
    # Log suppression
    options.add_argument("--log-level=3")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    
    service = webdriver.ChromeService(
        service_args=['--disable-build-check', '--verbose=0']
    )
    return webdriver.Chrome(service=service, options=options)

def _initialize_edge_driver(params: Dict[str, Any]) -> webdriver.Edge:
    """Edge-specific initialization"""
    import edgedriver_autoinstaller
    edgedriver_autoinstaller.install()
    
    options = webdriver.EdgeOptions()
    options.use_chromium = True
    
    if params.get('headless', True):
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1920,1080")
    
    # Common configurations
    options.add_argument("--log-level=3")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-gpu")
    # Anti-detection configuration  
    options.add_argument("--disable-blink-features=AutomationControlled")  
    #options.add_argument("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.6943.127 Safari/537.36")  
    #options.add_argument(params['user_agent'])
    driver = webdriver.Edge(options=options)

    return driver

def adaptive_wait(driver, selector, timeout=30, poll=3):
    """Hybrid waiting strategy with multiple fallback approaches"""
    end_time = time.time() + timeout
    last_exception = None
    
    while time.time() < end_time:
        try:
            # Try direct element location first
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            if elements:
                return elements
                
            # Fallback to JavaScript DOM query
            elements = driver.execute_script(
                f"return document.querySelectorAll('{selector}')"
            )
            if elements:
                return elements
                
            # Final fallback to WebDriverWait
            return WebDriverWait(driver, timeout).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
            )
            
        except Exception as e:
            last_exception = e
            time.sleep(poll + random.uniform(0, 2))
    
    raise TimeoutException(f"Element not found: {selector}") from last_exception


def random_sleep(min_delay: float, max_delay: float) -> None:
    """Random delay between actions to mimic human behavior"""
    delay = random.uniform(min_delay, max_delay)
    time.sleep(delay)
    
def scroll_down(driver: webdriver.Chrome) -> None:
    """Scroll to bottom of page to trigger lazy loading"""
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(random.uniform(0.5, 1.5))


# Node functions
def scrape_transaction_data(
    area_df: pd.DataFrame,
    params: Dict[str, Any]
) -> pd.DataFrame:
    """Scrape real estate transaction data from Centanet"""
    logger.info("Starting transaction data scraping")
    
    driver = initialize_driver(params)
    base_url = "https://hk.centanet.com/findproperty/en/list/transaction"
    all_data = []
    control_date = pd.to_datetime(params['control_date']).date()
    
    try:
        pbar = tqdm(total=len(area_df), desc="Processing areas", unit="area")
        
        for area_idx, area_row in area_df.iterrows():
            session_id = f"session_{area_idx}_{datetime.now().timestamp()}"
            subdistrict = area_row['Subdistrict'].replace(' ', '-').lower()
            url = f"{base_url}/{subdistrict}_19-{area_row['Code']}?q={session_id}"
            
            driver.get(url)
            random_sleep(params['min_delay'], params['max_delay'])
            
            page = 1
            date_reached = False
            while not date_reached:
                try:
                    WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.bx--structured-list-tbody"))
                    )
                    scroll_down(driver)
                    
                    # Extract table data
                    table_rows = driver.find_elements(By.CSS_SELECTOR, "div.cv-structured-list-item")
                    for table_row in table_rows:
                        cells = table_row.find_elements(By.CSS_SELECTOR, "div.cv-structured-list-data")
                        if len(cells) >= 8:
                            # Date parsing with control date check
                            try:
                                transaction_date = pd.to_datetime(cells[0].text.strip()).date()
                                if transaction_date < control_date:
                                    date_reached = True
                                    break
                            except Exception as e:
                                logger.warning(f"Error parsing date: {str(e)}")
                                continue
                            
                            record = {
                                'date': cells[0].text.strip(),
                                'address': cells[1].text.strip(),
                                'price': cells[3].text.strip(),
                                'price_tag': 'S' if 'tranPrice' in cells[3].text else 'L',
                                'area': cells[5].text.strip(),
                                'ft_price': cells[6].text.strip(),
                                'agency': cells[7].text.strip(),
                                'region': area_row['Region'],
                                'district': area_row['District'],
                                'subdistrict': area_row['Subdistrict'],
                                'code': area_row['Code']
                            }
                            all_data.append(record)
                    
                    if date_reached:
                        logger.info(f"Reached control date {control_date} in {area_row['Subdistrict']}")
                        break
                        
                    # Pagination
                    next_btn = driver.find_element(By.CSS_SELECTOR, "button.btn-next:not([disabled])")
                    driver.execute_script("arguments[0].click();", next_btn)
                    page += 1
                    random_sleep(params['min_delay'], params['max_delay'])
                    
                except Exception as e:
                    logger.debug(f"Finished scraping area {area_row['Subdistrict']} after {page} pages")
                    break
            
            # Update progress bar after each area
            pbar.update(1)
            pbar.set_postfix({
                'current': area_row['Subdistrict'],
                'transactions': len(all_data)
            })
            
        pbar.close()
            
    except Exception as e:
        pbar.close()
        raise e
    finally:
        driver.quit()
    
    return pd.DataFrame(all_data)

def process_transaction_data(
    trans_df: pd.DataFrame,
    params: Dict[str, Any]
) -> pd.DataFrame:
    """Process and structure raw transaction data"""
    logger.info("Processing transaction data")
    
    """Safe numeric conversion with error handling"""
    logger.info("Processing transaction prices")
    
    # Clean price columns with NaN handling
    trans_df['price'] = trans_df['price'].replace('', np.nan)
    trans_df['ft_price'] = trans_df['ft_price'].replace('', np.nan)
    
    # Convert to numeric types
    trans_df['price'] = pd.to_numeric(
        trans_df['price'].str.replace('[^\d]', '', regex=True),
        errors='coerce'
    )
    
    trans_df['ft_price'] = pd.to_numeric(
        trans_df['ft_price'].str.replace('[^\d]', '', regex=True),
        errors='coerce'
    )
    
    # Address parsing
    keywords = {
        'Phase': params['phase_keywords'],
        'Tower/Block': params['block_keywords'],
        'Floor': params['floor_keywords'],
        'Flat': params['flat_keywords']
    }
    
    def parse_address(address: str) -> dict:
        parts = address.split('・')
        result = {'Building': parts[0], 'Phase': None, 'Tower/Block': None, 
                 'Floor': None, 'Flat': None, 'Floor_Type': None}
        
        for part in parts[1:]:
            for key, terms in keywords.items():
                if any(term in part for term in terms):
                    result[key] = part
                    break
            if 'Upper Floor' in part or 'Middle Floor' in part or 'Lower Floor' in part:
                result['Floor_Type'] = part
                
        return result
    
    address_components = trans_df['address'].apply(parse_address).apply(pd.Series)
    return pd.concat([trans_df, address_components], axis=1)

def scrape_estate_listings(area_df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """Scrape estate listings with efficiency by comparing with existing data."""
    driver = initialize_driver(params)
    
    # Load existing estate listings if available
    try:
        listings_file = params.get('estate_listings_file', 'data/02_intermediate/estate_listings.parquet')
        if os.path.exists(listings_file):
            existing_listings = pd.read_parquet(listings_file)
            logger.info(f"Loaded {len(existing_listings)} existing estate listings")
            # Create dictionary of existing estates for fast lookup
            existing_estates = {row['Name']: row for _, row in existing_listings.iterrows()}
        else:
            existing_listings = pd.DataFrame()
            existing_estates = {}
            logger.info("No existing estates found. Will scrape all estates.")
    except Exception as e:
        logger.warning(f"Error loading existing listings: {str(e)}. Will scrape all estates.")
        existing_listings = pd.DataFrame()
        existing_estates = {}
    
    all_scraped_estates = []
    new_or_updated_estates = []
    
    try:
        for _, row in tqdm(area_df.iterrows(), total=len(area_df), desc="Processing areas"):
            subdistrict_part = clean_subdistrict(row['Subdistrict'])
            session_id = generate_session_id()
            url = f"https://hk.centanet.com/findproperty/en/list/estate/{subdistrict_part}_19-{row['Code']}?q={session_id}"
            
            driver.get(url)
            random_sleep(params['min_delay'], params['max_delay'])
            
            # Get total estate count on the page (for verification)
            try:
                estate_count_elem = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.district-select-text"))
                )
                count_match = re.search(r'(\d+)\s+Estate\(s\)', estate_count_elem.text)
                if count_match:
                    expected_count = int(count_match.group(1))
                    logger.info(f"Found {expected_count} estates in {row['Subdistrict']}")
            except Exception:
                expected_count = None
            
            # Extract all estates from all pages
            current_page = 1
            while True:
                try:
                    estate_items = WebDriverWait(driver, 20).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.property-text.flex.def-property-box"))
                    )
                    
                    for item in estate_items:
                        try:
                            estate_name = item.find_element(By.CSS_SELECTOR, "div.main-text").text.strip()
                            all_scraped_estates.append(estate_name)
                            
                            # Check if estate exists and if data has changed
                            estate_changed = False
                            
                            # Extract all relevant data for comparison
                            address = item.find_element(By.CSS_SELECTOR, "div.address.f-middle").text.strip()
                            link = item.get_attribute("href")
                            
                            try:
                                blocks = item.find_element(By.XPATH, 
                                    ".//div[contains(text(), 'No. of Block(s)')]/following-sibling::div").text.strip()
                            except:
                                blocks = ""
                                
                            try:
                                units = item.find_element(By.XPATH, 
                                    ".//div[contains(text(), 'No. of Units')]/following-sibling::div").text.strip()
                            except:
                                units = ""
                                
                            try:
                                unit_rate = item.find_element(By.XPATH, 
                                    ".//div[contains(text(), 'Unit Rate of Saleable Area')]/following-sibling::div").text.strip()
                            except:
                                unit_rate = ""
                                
                            try:
                                mom_change = item.find_element(By.XPATH, 
                                    ".//div[contains(text(), 'MoM')]/following-sibling::div").text.strip()
                            except:
                                mom_change = ""
                                
                            try:
                                for_sale = item.find_element(By.XPATH, 
                                    ".//div[contains(text(), 'For Sale')]/following-sibling::div").text.strip()
                            except:
                                for_sale = ""
                                
                            try:
                                for_rent = item.find_element(By.XPATH, 
                                    ".//div[contains(text(), 'For Rent')]/following-sibling::div").text.strip()
                            except:
                                for_rent = ""
                            
                            # Check if estate exists and has changed
                            if estate_name in existing_estates:
                                existing_estate = existing_estates[estate_name]
                                # Compare key fields to detect changes
                                if (str(existing_estate.get('Address', '')) != address or
                                    str(existing_estate.get('Blocks', '')) != blocks or
                                    str(existing_estate.get('Units', '')) != units or
                                    str(existing_estate.get('UnitRate', '')) != unit_rate or
                                    str(existing_estate.get('ForSale', '')) != for_sale or
                                    str(existing_estate.get('ForRent', '')) != for_rent):
                                    estate_changed = True
                            else:
                                # New estate not in existing data
                                estate_changed = True
                            
                            # Only add to the scraping list if new or changed
                            if estate_changed:
                                estate_data = {
                                    'Name': estate_name,
                                    'Address': address,
                                    'Blocks': blocks,
                                    'Units': units,
                                    'UnitRate': unit_rate,
                                    'MoM': mom_change,
                                    'ForSale': for_sale,
                                    'ForRent': for_rent,
                                    'Link': link,
                                    'Region': row['Region'],
                                    'District': row['District']
                                }
                                new_or_updated_estates.append(estate_data)
                                logger.debug(f"{'Updated' if estate_name in existing_estates else 'New'} estate: {estate_name}")
                            
                        except Exception as e:
                            logger.debug(f"Error processing estate item: {str(e)}")
                    
                    # Check for next page
                    try:
                        next_btn = WebDriverWait(driver, 10).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn-next:not([disabled])"))
                        )
                        driver.execute_script("arguments[0].click();", next_btn)
                        random_sleep(params['min_delay'], params['max_delay'])
                        current_page += 1
                    except:
                        break  # No more pages
                        
                except Exception as e:
                    logger.warning(f"Error processing page {current_page}: {str(e)}")
                    break
        
        # Handle estates that have been removed
        removed_estates = set(existing_estates.keys()) - set(all_scraped_estates)
        if removed_estates:
            logger.info(f"Found {len(removed_estates)} estates that are no longer available")
        
        # Create final DataFrame - keep existing data for unchanged estates
        if new_or_updated_estates:
            new_df = pd.DataFrame(new_or_updated_estates)
            if not existing_listings.empty:
                # Remove estates that no longer exist
                filtered_existing = existing_listings[~existing_listings['Name'].isin(removed_estates)]
                # Remove estates that will be updated
                unchanged_existing = filtered_existing[~filtered_existing['Name'].isin([e['Name'] for e in new_or_updated_estates])]
                # Combine unchanged existing with new/updated
                final_df = pd.concat([unchanged_existing, new_df], ignore_index=True)
            else:
                final_df = new_df
        else:
            # No changes, just remove estates that no longer exist
            if not existing_listings.empty:
                final_df = existing_listings[~existing_listings['Name'].isin(removed_estates)]
            else:
                final_df = pd.DataFrame()
        
        # Save for next time
        final_df.to_parquet(listings_file, index=False)
        
        logger.info(f"Total estates: {len(final_df)}, New/Updated: {len(new_or_updated_estates)}, Removed: {len(removed_estates)}")
        
        return final_df
        
    finally:
        driver.quit()

def safe_get_text(element, selector):
    """Helper function for fault-tolerant text extraction"""
    try:
        return element.find_element(By.CSS_SELECTOR, selector).text.strip()
    except Exception:
        return None

# nodes.py (Kedro compatible version)
def scrape_estate_details(
    listings_df: pd.DataFrame,
    params: Dict[str, Any]
) -> pd.DataFrame:
    """Scrape estate details with incremental updates and change detection."""
    logger = logging.getLogger(__name__)
    
    # Configuration parameters
    details_file = params.get('estate_details_file', 'data/02_intermediate/estate_details_raw.parquet')
    temp_file = params.get('temp_file', 'data/02_intermediate/estate_details_temp.parquet')
    driver = None
    result_df = pd.DataFrame()

    try:
        # Load existing details with column validation
        existing_details = pd.DataFrame()
        if os.path.exists(details_file):
            try:
                existing_details = pd.read_parquet(details_file)
                logger.info(f"Loaded {len(existing_details)} existing estate details")
                
                # Ensure required columns exist
                required_cols = ['link_hash', 'last_updated']
                for col in required_cols:
                    if col not in existing_details.columns:
                        existing_details[col] = pd.NaT if col == 'last_updated' else None
                        logger.warning(f"Added missing column '{col}' to existing details")

            except Exception as e:
                logger.error(f"Error loading existing details: {str(e)}")
                existing_details = pd.DataFrame()

        # Initialize new columns for listings dataframe
        listings_df = listings_df.copy()
        if 'link_hash' not in listings_df.columns:
            listings_df['link_hash'] = listings_df['Link'].apply(
                lambda x: hashlib.sha256(x.encode()).hexdigest()
            )
        
        # Create merged dataframe with fallback values
        merge_df = listings_df.copy()
        if not existing_details.empty:
            try:
                merge_df = pd.merge(
                    listings_df,
                    existing_details[['link_hash', 'last_updated']],
                    on='link_hash',
                    how='left',
                    suffixes=('', '_existing')
                )
            except KeyError as e:
                logger.error(f"Merge failed: {str(e)}")
                merge_df['last_updated'] = pd.NaT
        else:
            merge_df['last_updated'] = pd.NaT

        # Filter based on existence and last update time
        three_months_ago = pd.Timestamp.now() - pd.DateOffset(months=3)
        to_scrape = merge_df[
            (merge_df['last_updated'].isnull()) | 
            (merge_df['last_updated'] < three_months_ago)
        ]

        logger.info(f"Scraping details for {len(to_scrape)}/{len(listings_df)} estates needing updates")

        if to_scrape.empty:
            logger.info("No estate details require updating")
            return existing_details

        # Initialize browser only when needed
        driver = initialize_driver(params)
        result_df = existing_details.copy()

        # Process estates needing updates
        pbar = tqdm(total=len(to_scrape), desc="Processing estate details")
        for idx, row in to_scrape.iterrows():
            try:
                url = row['Link']
                current_hash = row['link_hash']
                
                # Check if already processed in this session
                if current_hash in result_df['link_hash'].values:
                    pbar.update(1)
                    continue

                # Scrape details
                driver.get(url)
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                random_sleep(params['min_delay'], params['max_delay'])

                # Extract detailed information
                detail_data = {
                    'scraped_estate_name': extract_element_text(driver, ".estate-detail-banner-title"),
                    'occupation_permit': extract_element_text(
                        driver, 
                        "//div[contains(text(), 'Date of Occupation Permit')]/following-sibling::div"
                    ),
                    'scraped_blocks': extract_numeric_value(
                        driver, 
                        "//div[contains(text(), 'No. of Block(s)')]/following-sibling::div"
                    ),
                    'scraped_units': extract_numeric_value(
                        driver,
                        "//div[contains(text(), 'No. of Units')]/following-sibling::div"
                    ),
                    'school_net_info': extract_school_net(driver),
                    'estate_detailed_address': extract_element_text(driver, ".estate-detail-banner-position"),
                    'developer': extract_element_text(
                        driver,
                        "//div[contains(.//text(), 'Developer')]/following-sibling::div"
                    ),
                    'last_updated': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'link_hash': current_hash
                }

                # Merge with existing data or add new entry
                new_row = {**row.to_dict(), **detail_data}
                if current_hash in existing_details['link_hash'].values:
                    result_df.loc[result_df['link_hash'] == current_hash] = pd.Series(new_row)
                else:
                    result_df = pd.concat([result_df, pd.DataFrame([new_row])], ignore_index=True)

                # Intermediate save every 10 records
                if idx % 10 == 0:
                    result_df.to_parquet(temp_file, index=False)

                pbar.update(1)
                pbar.set_postfix_str(f"Processed: {new_row.get('scraped_estate_name', 'Unknown')}")

            except Exception as e:
                logger.error(f"Failed to process {url}: {str(e)}")
                continue

        pbar.close()
        
        # Final save and update control date
        result_df = result_df.drop(columns=['link_hash'], errors='ignore')
        result_df.to_parquet(details_file, index=False)
        update_control_date(params)
        return result_df

    except Exception as e:
        logger.error(f"Critical error in scrape_estate_details: {str(e)}")
        raise
    finally:
        if driver:
            driver.quit()

# Helper functions
def extract_element_text(driver, selector: str) -> Optional[str]:
    try:
        elem = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
        ) if '.' in selector else WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, selector))
        )
        return elem.text.strip()
    except:
        return None

def extract_numeric_value(driver, xpath: str) -> Optional[int]:
    try:
        text = extract_element_text(driver, xpath)
        return int(re.search(r'\d+', text).group()) if text else None
    except:
        return None

def extract_school_net(driver) -> Optional[str]:
    try:
        net_div = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(.//text(), 'School Net')]"))
        )
        primary = net_div.find_element(By.XPATH, ".//a[1]").text
        secondary = net_div.find_element(By.XPATH, ".//a[2]").text
        return f"{primary} | {secondary}"
    except:
        return None

def update_control_date(params: Dict[str, Any]) -> None:
    try:
        params_path = "conf/base/parameters.yml"
        with open(params_path, 'r') as file:
            parameters = yaml.safe_load(file)
        
        parameters['centaline_estates'] = datetime.now().strftime("%Y-%m-%d")
        
        with open(params_path, 'w') as file:
            yaml.dump(parameters, file, default_flow_style=False)
        
        logger.info("Successfully updated control date for estate details")
    except Exception as e:
        logger.error(f"Failed to update control date: {str(e)}")


# Updated enrich_estate_data function
def enrich_estate_data(
    listings_df: pd.DataFrame,
    transactions_df: pd.DataFrame
) -> pd.DataFrame:
    """Safe data enrichment with missing column handling"""
    # Ensure required columns exist
    required_cols = ['Name', 'Address', 'Blocks', 'Units', 'Developer']
    for col in required_cols:
        if col not in listings_df.columns:
            listings_df[col] = None
    
    # Prepare listings data
    listings_clean = (
        listings_df
        .rename(columns={'Name': 'Estate_Building'})
        .drop_duplicates('Estate_Building')
        [['Estate_Building', 'Address', 'Blocks', 'Units', 'Developer']]
    )
    
    # Merge with transaction data
    return pd.merge(
        transactions_df,
        listings_clean,
        left_on='Building',
        right_on='Estate_Building',
        how='left',
        suffixes=('', '_estate')
    )



