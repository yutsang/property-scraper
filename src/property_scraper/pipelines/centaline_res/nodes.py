# src/kedro_centaline/pipelines/data_processing/nodes.py
import time
import random
import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import chromedriver_autoinstaller
import logging
from typing import Dict, Any, List
from tqdm import tqdm
import configparser
import re
import string
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, NoSuchElementException
import os
import json
from pathlib import Path
from typing import Optional, Set
from typing import Dict, Any, List, Tuple
import pickle
import hashlib
import yaml
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from difflib import SequenceMatcher
import sys

# Import node tracking utilities
from ...utils.node_tracker import should_run_node, record_node_execution


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
    if params['global'].get('use_edge', False):
        return _initialize_edge_driver(params['global'])
    return _initialize_chrome_driver(params['global'])

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

########################## scrape transaction data ##########################
    
# Below is the updated version of the `scrape_transaction_data` function that includes robust error handling, pagination, and control date checks.

def scrape_transaction_data(
    area_df: pd.DataFrame,  
    params: Dict[str, Any]
) -> pd.DataFrame:
    """
    Enhanced Kedro-compatible transaction data scraper with incremental updating.
    Includes node execution tracking to avoid re-running on the same day.
    """
    
    # Check if node should be run based on max date in dataset
    node_name = "transaction_data_scraper"
    transaction_file = params['centaline_res'].get('res_trans_path', 'data/01_raw/centaline_res_trans_lv_0.parquet')
    tracking_params = params.get('node_tracking', {})
    
    if not should_run_node(node_name, "transaction", tracking_params, transaction_file):
        logger.info(f"Node '{node_name}' - dataset is up to date - returning existing data")
        # Return existing data if available
        if os.path.exists(transaction_file):
            try:
                return pd.read_parquet(transaction_file)
            except Exception as e:
                logger.warning(f"Failed to load existing transaction data: {e}")
                return pd.DataFrame()
        return pd.DataFrame()
    
    # ============ DEFINE NESTED FUNCTIONS FIRST ============
    def parse_date_from_string(date_str):
        """Enhanced date parsing with multiple format support"""
        if not date_str or pd.isna(date_str):
            return None
        date_str = str(date_str).strip()
        date_formats = ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y%m%d', '%d-%m-%Y']
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except (ValueError, TypeError):
                continue
        return None

    def smart_sleep():
        """Unified sleep function with random timing"""
        sleep_time = random.uniform(params['global']['min_delay'], params['global']['max_delay'])
        time.sleep(sleep_time)

    def initialize_driver():
        """Setup Chrome driver with anti-detection configuration"""
        chromedriver_autoinstaller.install()
        options = webdriver.ChromeOptions()
        if params['global'].get('headless', True):
            options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        options.add_argument(f"--user-agent={params['global'].get('user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')}")
        options.add_argument("--window-size=1920,1080")
        driver = webdriver.Chrome(options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        return driver

    def enhanced_scroll_down(driver):
        """Enhanced scrolling strategy for dynamic content loading"""
        last_height = driver.execute_script("return document.body.scrollHeight")
        scroll_attempts = 0
        while scroll_attempts < 3:
            scroll_distance = random.randint(300, 800)
            driver.execute_script(f"window.scrollBy(0, {scroll_distance})")
            smart_sleep()
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
            scroll_attempts += 1

    def extract_table_data(driver):
        """Extract transaction data from the structured table layout."""
        table_data = []
        try:
            enhanced_scroll_down(driver)

            # Find all transaction rows (desktop table format)
            rows = driver.find_elements(By.CSS_SELECTOR, "tr.cv-structured-list-item")
            
            for row in rows:
                try:
                    # Get all cells in the row
                    cells = row.find_elements(By.CSS_SELECTOR, "td.cv-structured-list-data")
                    
                    if len(cells) >= 5:  # Ensure we have enough cells (minimum 5 for basic data)
                        # Extract date from the first cell
                        try:
                            date_element = cells[0].find_element(By.CSS_SELECTOR, ".info-date span")
                            date_text = date_element.text.strip()
                        except:
                            # Fallback: use cell text directly
                            date_text = cells[0].text.strip()
                        
                        # Extract address from the second cell
                        try:
                            address_element = cells[1].find_element(By.CSS_SELECTOR, ".addr")
                            address_text = address_element.text.strip()
                        except:
                            # Fallback: use cell text directly
                            address_text = cells[1].text.strip()
                        
                        # For desktop table format, title-lg is not available, so we'll use the address
                        title_lg_text = address_text
                        
                        # Extract rooms from the third cell
                        rooms_text = cells[2].text.strip()
                        
                        # Determine transaction type and extract price from the fourth cell
                        transaction_type = "SALE"
                        price_text = cells[3].text.strip()
                        
                        # Check if it's a rent transaction (usually contains "租" or "$" with smaller amounts)
                        if "租" in price_text or (price_text.startswith("$") and any(char.isdigit() for char in price_text)):
                            # Additional check for rent vs sale based on price format
                            if price_text.startswith("$") and len(price_text) < 10:  # Likely rent
                                transaction_type = "RENT"
                        
                        # Extract area from the fifth cell
                        area_text = cells[4].text.strip()
                        
                        # Initialize optional fields
                        ft_price_text = ""
                        changes_text = ""
                        agency_text = ""
                        
                        # Extract additional data if available
                        if len(cells) >= 6:
                            ft_price_text = cells[5].text.strip()
                        if len(cells) >= 7:
                            try:
                                changes_element = cells[6].find_element(By.CSS_SELECTOR, ".riseBox span")
                                changes_text = changes_element.text.strip()
                            except:
                                changes_text = cells[6].text.strip()
                        if len(cells) >= 8:
                            try:
                                agency_element = cells[7].find_element(By.CSS_SELECTOR, ".label")
                                agency_text = agency_element.text.strip()
                            except:
                                agency_text = cells[7].text.strip()
                        
                        record = {
                            'date': date_text,
                            'address': address_text,
                            'title_lg': title_lg_text,  # Address with native separators
                            'rooms': rooms_text,
                            'price': price_text,
                            'area': area_text,
                            'ft_price': ft_price_text,
                            'changes': changes_text,
                            'agency': agency_text,
                            'transaction_type': transaction_type,
                        }
                        table_data.append(record)
                        
                except Exception as e:
                    logger.debug(f"Error processing row: {str(e)}")
                    continue
                    
            # Extract title-lg information from mobile card format
            mobile_cards = driver.find_elements(By.CSS_SELECTOR, ".transactions-content")
            
            # Create a list of title-lg values from mobile cards
            title_lg_values = []
            for card in mobile_cards:
                try:
                    # First try: .text01 .title-lg (the correct structure)
                    text01_elements = card.find_elements(By.CSS_SELECTOR, ".text01")
                    if text01_elements:
                        title_lg_elements = text01_elements[0].find_elements(By.CSS_SELECTOR, ".title-lg")
                        if title_lg_elements:
                            title_lg_text = title_lg_elements[0].text.strip()
                            if title_lg_text:
                                title_lg_values.append(title_lg_text)
                                continue
                    
                    # Fallback: direct .title-lg
                    title_lg_elements = card.find_elements(By.CSS_SELECTOR, ".title-lg")
                    if title_lg_elements:
                        title_lg_text = title_lg_elements[0].text.strip()
                        if title_lg_text:
                            title_lg_values.append(title_lg_text)
                except Exception as e:
                    logger.debug(f"Error processing mobile card: {str(e)}")
                    continue
            
            # Enrich table data with title-lg information
            # Since desktop table addresses are often empty, we'll use the mobile card title-lg values
            # and assign them sequentially to table records
            for i, record in enumerate(table_data):
                if i < len(title_lg_values):
                    record['title_lg'] = title_lg_values[i]
                else:
                    # If we run out of title-lg values, use the address as fallback
                    record['title_lg'] = record['address'] if record['address'] else ''
            
            return table_data
        except Exception as e:
            logger.error(f"Error extracting table data: {str(e)}")
            return []

    def check_for_next_page(driver):
        """Check if next page button exists and is clickable"""
        try:
            next_selectors = [
                "button.btn-next:not(.disabled):not([disabled])",
                "a.pagination-next:not(.disabled)"
            ]
            
            for selector in next_selectors:
                try:
                    next_buttons = driver.find_elements(By.CSS_SELECTOR, selector)
                    if next_buttons and next_buttons[0].is_displayed() and next_buttons[0].is_enabled():
                        return next_buttons[0]
                except:
                    continue
            return None
        except:
            return None

    def go_to_next_page(driver):
        """Navigate to next page with enhanced verification"""
        try:
            next_button = check_for_next_page(driver)
            if next_button:
                driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", next_button)
                smart_sleep()
                driver.execute_script("arguments[0].click();", next_button)
                smart_sleep()
                smart_sleep()  # Extra wait for page load
                return True
            return False
        except:
            return False

    # ============ INCREMENTAL UPDATE LOGIC ============
    transaction_file = "data/01_raw/centaline_res_trans_lv_0.parquet"
    existing_data = pd.DataFrame()
    
    if os.path.exists(transaction_file):
        try:
            existing_data = pd.read_parquet(transaction_file)
            logger.info(f"Loaded {len(existing_data)} existing transactions")
            
            if 'date' in existing_data.columns and not existing_data.empty:
                existing_data_temp = existing_data.copy()
                existing_data_temp['parsed_date'] = existing_data_temp['date'].apply(
                    lambda x: parse_date_from_string(x) if pd.notna(x) else None
                )
                
                valid_dates = existing_data_temp['parsed_date'].dropna()
                if not valid_dates.empty:
                    max_date = valid_dates.max()
                    control_date = max_date + timedelta(days=1)
                    logger.info(f"Using incremental control date: {control_date}")
                else:
                    control_date = pd.to_datetime(params.get('centaline_res', {}).get('control_date', params['global']['start_date'])).date()
                    logger.info(f"No valid dates found, using parameter control date: {control_date}")
            else:
                control_date = pd.to_datetime(params.get('centaline_res', {}).get('control_date', params['global']['start_date'])).date()
                logger.info(f"No date column found, using parameter control date: {control_date}")
                
        except Exception as e:
            logger.error(f"Error loading existing data: {str(e)}")
            existing_data = pd.DataFrame()
            control_date = pd.to_datetime(params.get('centaline_res', {}).get('control_date', params['global']['start_date'])).date()
    else:
        logger.info("No existing transaction file found, starting fresh")
        control_date = pd.to_datetime(params.get('centaline_res', {}).get('control_date', params['global']['start_date'])).date()
    
    # Set end date to today
    end_date = datetime.now().date()
    logger.info(f"Scraping transactions from {control_date} to {end_date}")
    
    # If control date is already today or later, return existing data
    if control_date >= end_date:
        logger.info("No new transactions to scrape - control date is current")
        return existing_data if not existing_data.empty else pd.DataFrame()

    # ============ MAIN SCRAPING LOGIC ============
    driver = initialize_driver()
    base_url = "https://hk.centanet.com/findproperty/en/list/transaction"
    all_data = []

    try:
        with tqdm(total=len(area_df), desc="Processing areas", unit="area") as pbar:
            for area_idx, area_row in area_df.iterrows():
                subdistrict = area_row['Subdistrict'].replace(' ', '-').lower()
                session_id = f"session_{area_idx}_{int(datetime.now().timestamp())}"
                url = f"{base_url}/{subdistrict}_19-{area_row['Code']}?q={session_id}"
                
                max_retries = params['global'].get('max_retries', 3)
                for attempt in range(max_retries):
                    try:
                        driver.get(url)
                        smart_sleep()
                        
                        page = 1
                        date_reached = False
                        max_pages = params['global'].get('max_pages_per_area', 50)
                        
                        while not date_reached and page <= max_pages:
                            page_data = extract_table_data(driver)
                            
                            for record in page_data:
                                # Check control date
                                try:
                                    transaction_date = parse_date_from_string(record['date'])
                                    if transaction_date and transaction_date < control_date:
                                        date_reached = True
                                        break
                                except:
                                    continue
                                
                                # Add area metadata - **FIXED TIMESTAMP FORMAT**
                                record.update({
                                    'region': area_row['Region'],
                                    'district': area_row['District'],
                                    'subdistrict': area_row['Subdistrict'],
                                    'code': area_row['Code'],
                                    'scrape_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # STRING FORMAT
                                })
                                all_data.append(record)
                            
                            if date_reached:
                                break
                                
                            if not go_to_next_page(driver):
                                break
                            page += 1
                        
                        break  # Success, exit retry loop
                    except Exception:
                        if attempt == max_retries - 1:
                            continue
                        time.sleep(2 ** attempt)
                
                pbar.update(1)
                pbar.set_postfix({
                    'current': area_row['Subdistrict'][:15],
                    'transactions': len(all_data)
                })
                
    finally:
        driver.quit()

    # ============ COMBINE AND CLEAN DATA ============
    new_data_df = pd.DataFrame(all_data)
    
    if not existing_data.empty and not new_data_df.empty:
        logger.info(f"Combining {len(existing_data)} existing and {len(new_data_df)} new transactions")
        combined_df = pd.concat([existing_data, new_data_df], ignore_index=True)
    elif not new_data_df.empty:
        logger.info(f"Using {len(new_data_df)} new transactions (no existing data)")
        combined_df = new_data_df
    elif not existing_data.empty:
        logger.info(f"No new transactions found, returning {len(existing_data)} existing transactions")
        combined_df = existing_data
    else:
        logger.info("No transactions found")
        combined_df = pd.DataFrame()

    # Deduplicate and clean data types
    if not combined_df.empty:
        # **CRITICAL FIX: Normalize timestamp column for consistent parquet serialization**
        if 'scrape_timestamp' in combined_df.columns:
            combined_df['scrape_timestamp'] = pd.to_datetime(
                combined_df['scrape_timestamp'], errors='coerce'
            ).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Deduplicate based on key columns
        dedup_columns = ['date', 'address', 'price', 'area', 'region', 'district', 'subdistrict']
        dedup_columns = [col for col in dedup_columns if col in combined_df.columns]
        
        if dedup_columns:
            before_dedup = len(combined_df)
            combined_df = combined_df.drop_duplicates(subset=dedup_columns, keep='last')
            after_dedup = len(combined_df)
            
            if before_dedup != after_dedup:
                logger.info(f"Removed {before_dedup - after_dedup} duplicate transactions")

    logger.info(f"Final dataset contains {len(combined_df)} transactions")
    
    # Record node execution
    record_node_execution(
        node_name="transaction_data_scraper",
        node_type="transaction",
        metadata={
            "records_processed": len(combined_df),
            "areas_processed": len(area_df),
            "execution_time": datetime.now().isoformat()
        }
    )
    
    return combined_df

    


########################## scrape transaction data ##########################

'''def process_transaction_data(
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
    return pd.concat([trans_df, address_components], axis=1)'''
    
def process_transaction_data(
    trans_df: pd.DataFrame,
    params: Dict[str, Any]
) -> pd.DataFrame:
    """
    Process transaction data by filtering out invalid records and cleaning the data
    """
    logger.info(f"🔄 Processing transaction data: {len(trans_df)} records")
    
    # Create a copy to avoid modifying original
    processed_df = trans_df.copy()
    
    # Filter out rows with empty or invalid dates
    initial_count = len(processed_df)
    
    # Remove rows where date is empty, None, or invalid
    processed_df = processed_df.dropna(subset=['date'])
    processed_df = processed_df[processed_df['date'] != '']
    processed_df = processed_df[processed_df['date'].str.strip() != '']
    
    # Also filter out rows where address is empty (these are likely failed scrapes)
    processed_df = processed_df.dropna(subset=['address'])
    processed_df = processed_df[processed_df['address'] != '']
    processed_df = processed_df[processed_df['address'].str.strip() != '']
    
    # Remove rows where price is empty (these are likely incomplete records)
    processed_df = processed_df.dropna(subset=['price'])
    processed_df = processed_df[processed_df['price'] != '']
    processed_df = processed_df[processed_df['price'].str.strip() != '']
    
    final_count = len(processed_df)
    removed_count = initial_count - final_count
    
    logger.info(f"📊 Data cleaning results:")
    logger.info(f"  - Initial records: {initial_count:,}")
    logger.info(f"  - Valid records: {final_count:,}")
    logger.info(f"  - Removed invalid records: {removed_count:,}")
    logger.info(f"  - Success rate: {(final_count/initial_count)*100:.1f}%")
    
    # Additional data cleaning
    if len(processed_df) > 0:
        # Clean up date format
        processed_df['date'] = processed_df['date'].str.strip()
        
        # Clean up address format
        processed_df['address'] = processed_df['address'].str.strip()
        
        # Clean up price format
        processed_df['price'] = processed_df['price'].str.strip()
        
        # Ensure title_lg is populated (use address if title_lg is empty)
        processed_df['title_lg'] = processed_df['title_lg'].fillna(processed_df['address'])
        # Replace empty strings with address values
        mask = (processed_df['title_lg'] == '') | (processed_df['title_lg'].isna())
        processed_df.loc[mask, 'title_lg'] = processed_df.loc[mask, 'address']
        
        logger.info(f"✅ Data processing completed successfully")
    
    return processed_df


def scrape_estate_listings(area_df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """Robust estate scraper with actual data row counting for each district
    Includes node execution tracking to avoid re-running within 7 days.
    """
    
    # Check if node should be run based on last execution date
    node_name = "estate_listing_scraper"
    if not should_run_node(node_name, "estate"):
        logger.info(f"Node '{node_name}' last run within 7 days - returning existing data")
        # Return existing data if available
        listings_file = params.get('estate_listings_file', 'data/01_raw/centaline_estate_lv_1.parquet')
        if os.path.exists(listings_file):
            try:
                return pd.read_parquet(listings_file)
            except Exception as e:
                logger.warning(f"Failed to load existing estate listings: {e}")
                return pd.DataFrame()
        return pd.DataFrame()
    from tqdm.auto import tqdm
    from datetime import datetime
    
    driver = initialize_driver(params)
    required_columns = [
        'Name', 'Address', 'Blocks', 'Units', 'UnitRate',
        'MoM', 'ForSale', 'ForRent', 'Link', 'Region',
        'District', 'Subdistrict', 'Code', 'LastScraped'
    ]
    
    # Initialize data structures
    district_changes = []
    zero_count_districts = []
    existing_listings = pd.DataFrame(columns=required_columns)
    district_counts = {}
    
    try:
        listings_file = params.get('estate_listings_file', 'data/01_raw/centaline_estate_lv_1.parquet')
        if os.path.exists(listings_file):
            try:
                existing_listings = pd.read_parquet(listings_file)
                logger.info(f"📊 Loaded {len(existing_listings)} existing estate listings")
                
                # Clean and standardize data
                existing_listings['Subdistrict'] = existing_listings['Subdistrict'].str.strip()
                existing_listings['Code'] = existing_listings['Code'].astype(str).str.strip()
                
                # Create district count map based on actual rows in data
                district_counts = existing_listings.groupby(['Subdistrict', 'Code']).size().to_dict()
                logger.info(f"🏘️  Will check {len(area_df)} districts for updates...")
            except Exception as e:
                logger.error(f"Data loading failed: {str(e)}")
                existing_listings = pd.DataFrame(columns=required_columns)
    except Exception as e:
            logger.error(f"Initialization error: {str(e)}")
    
    new_or_updated_estates = []
    
    try:
        with tqdm(area_df.iterrows(), total=len(area_df), desc="Processing districts") as district_iter:
            for _, row in district_iter:
                subdistrict = str(row['Subdistrict']).strip()
                code = str(row['Code']).strip()
                district_key = (subdistrict, code)
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                try:
                    # Generate session URL
                    subdistrict_clean = clean_subdistrict(subdistrict)
                    session_id = generate_session_id()
                    url = f"https://hk.centanet.com/findproperty/en/list/estate/{subdistrict_clean}_19-{code}?q={session_id}"
                    
                    # Navigate and get website count
                    driver.get(url)
                    random_sleep(params['global']['min_delay'], params['global']['max_delay'])
                    
                    # Check if estate listings exist (ignore broken count element)
                    try:
                        # Wait for estate listings to load
                        WebDriverWait(driver, 15).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "a.property-text.flex.def-property-box"))
                        )
                        # Count actual estate listings on page
                        soup = BeautifulSoup(driver.page_source, 'html.parser')
                        estate_items = soup.select("a.property-text.flex.def-property-box")
                        current_count = len(estate_items)
                        
                        logger.debug(f"Found {current_count} estate listings for {subdistrict}")
                        
                    except Exception as e:
                        logger.debug(f"No estate listings found for {subdistrict}: {str(e)}")
                        current_count = 0
                    
                    # Get previous count from actual data rows
                    previous_count = district_counts.get(district_key, 0)
                    
                    # Handle zero-count districts
                    if current_count == 0:
                        zero_count_districts.append(district_key)
                        logger.debug(f"Skipping zero-count district: {subdistrict} ({code})")
                        continue
                    
                    # Skip unchanged districts (only if we have previous data)
                    if previous_count > 0 and current_count == previous_count:
                        logger.debug(f"Skipping unchanged district: {subdistrict} ({code}) [{current_count}]")
                        continue
                    
                    # Track changed districts and start scraping
                    district_changes.append({
                        'district': subdistrict,
                        'code': code,
                        'previous': previous_count,
                        'current': current_count,
                        'timestamp': current_time
                    })
                    
                    # **UPDATED**: Initialize district-specific tracking
                    district_estates = []
                    current_page = 1
                    
                    # Full scraping process with row counting
                    while True:
                        try:
                            WebDriverWait(driver, 20).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "a.property-text.flex.def-property-box"))
                            )
                            
                            soup = BeautifulSoup(driver.page_source, 'html.parser')
                            estate_items = soup.select("a.property-text.flex.def-property-box")
                            
                            # Process each estate on current page
                            for item in estate_items:
                                try:
                                    estate_data = process_estate_item(item, row)
                                    district_estates.append(estate_data)
                                except Exception as e:
                                    logger.error(f"Error processing estate: {str(e)}")
                                    continue
                            
                            # Pagination handling
                            try:
                                next_btn = driver.find_element(By.CSS_SELECTOR, "button.btn-next:not([disabled])")
                                driver.execute_script("arguments[0].click();", next_btn)
                                random_sleep(params['global']['min_delay'], params['global']['max_delay'])
                                current_page += 1
                            except NoSuchElementException:
                                break
                                
                        except TimeoutException:
                            logger.warning(f"Timeout in {subdistrict} page {current_page}")
                            break
                        except Exception as e:
                            logger.error(f"Critical error: {str(e)}")
                            break
                    
                    # **UPDATED**: Count actual rows scraped for this district
                    actual_scraped_count = len(district_estates)
                    
                    # Add district estates to main collection
                    new_or_updated_estates.extend(district_estates)
                    
                    # **ENHANCED LOGGING**: Clear and concise district completion
                    pages_scraped = current_page - 1 if 'current_page' in locals() else 1
                    logger.info(f"✅ {subdistrict} ({code}): {actual_scraped_count} estates scraped from {pages_scraped} pages")
                    
                    # **UPDATE PROGRESS BAR**: Show current district and totals
                    district_iter.set_postfix({
                        'district': subdistrict[:12],
                        'total_estates': len(new_or_updated_estates)
                    })
                    
                except Exception as e:
                    logger.error(f"District processing failed: {subdistrict} - {str(e)}")
                    continue
        
        # **UPDATED**: Final data consolidation with actual row counting
        if new_or_updated_estates:
            new_df = pd.DataFrame(new_or_updated_estates)
            logger.info(f"Processing {len(new_df)} new/updated estates")
            
            # Remove old data for updated districts to prevent duplicates
            updated_districts = set((estate['Subdistrict'], estate['Code']) for estate in new_or_updated_estates)
            existing_to_keep = existing_listings[
                ~existing_listings[['Subdistrict', 'Code']].apply(tuple, axis=1).isin(updated_districts)
            ]
            
            logger.info(f"Keeping {len(existing_to_keep)} existing estates from unchanged districts")
            final_df = pd.concat([existing_to_keep, new_df], ignore_index=True)
        else:
            logger.info("No new/updated estates found - preserving all existing data")
            final_df = existing_listings.copy()
        
        # **UPDATED**: Generate final report with actual row counts
        logger.info("\n" + "="*60)
        logger.info("ESTATE SCRAPING COMPLETION REPORT")
        logger.info("="*60)
        logger.info(f"Total districts processed: {len(area_df)}")
        logger.info(f"Changed districts: {len(district_changes)}")
        logger.info(f"Zero-count districts: {len(zero_count_districts)}")
        logger.info(f"Total new estates scraped: {len(new_or_updated_estates)}")
        
        if district_changes:
            logger.info("\nDETAILED SCRAPING RESULTS:")
            total_website_count = 0
            total_actual_rows = 0
            
            for change in district_changes:
                # **UPDATED**: Count actual rows in final dataset for this district
                district_rows = len([e for e in new_or_updated_estates 
                                   if e['Subdistrict'] == change['district'] 
                                   and e['Code'] == change['code']])
                
                total_website_count += change['current']
                total_actual_rows += district_rows
                
                rate = (district_rows / change['current'] * 100) if change['current'] > 0 else 0
                logger.info(f"  {change['district']} ({change['code']}): "
                          f"Website={change['current']} | Actual_Rows={district_rows} | Rate={rate:.1f}%")
            
            overall_rate = (total_actual_rows / total_website_count * 100) if total_website_count > 0 else 0
            logger.info(f"\nOVERALL SUMMARY:")
            logger.info(f"  Total expected (from websites): {total_website_count}")
            logger.info(f"  Total actual rows scraped: {total_actual_rows}")
            logger.info(f"  Overall completion rate: {overall_rate:.1f}%")
        
        # **UPDATED**: Final dataset statistics with row counts by district
        if not final_df.empty:
            final_district_counts = final_df.groupby(['Subdistrict', 'Code']).size()
            logger.info(f"\nFINAL DATASET STATISTICS:")
            logger.info(f"  Total estates in dataset: {len(final_df)}")
            logger.info(f"  Districts represented: {len(final_district_counts)}")
            
            # Show sample of district row counts
            logger.info(f"  Sample district row counts:")
            for (district, code), count in final_district_counts.head(10).items():
                logger.info(f"    {district} ({code}): {count} rows")
        
        # Safety deduplication
        before_dedup = len(final_df)
        final_df = final_df.drop_duplicates(
            subset=['Name', 'Address', 'Region', 'District', 'Subdistrict', 'Code'],
            keep='last'
        )
        after_dedup = len(final_df)
        if before_dedup != after_dedup:
            logger.info(f"Removed {before_dedup - after_dedup} duplicate estates")
        
        # Save data
        final_df.to_parquet(listings_file, index=False)
        logger.info(f"\nSaved {len(final_df)} total estates to {listings_file}")
        
        # Record node execution
        record_node_execution(
            node_name="estate_listing_scraper",
            node_type="estate",
            metadata={
                "estates_processed": len(final_df),
                "districts_processed": len(area_df),
                "execution_time": datetime.now().isoformat()
            }
        )
        
        return final_df[final_df['Name'].notnull()]
        
    except Exception as e:
        logger.error(f"Data consolidation failed: {str(e)}")
        return existing_listings[existing_listings['Name'].notnull()]
        
    finally:
        driver.quit()

        
def log_district_completion(subdistrict, code, current_count, final_df):
    """Log district completion with actual data count"""
    # Count actual rows for this district in the final dataset
    actual_scraped_count = len(final_df[
        (final_df['Subdistrict'] == subdistrict) & 
        (final_df['Code'] == code)
    ])
    
    completion_rate = (actual_scraped_count / current_count * 100) if current_count > 0 else 0
    
    logger.info(f"Processing changed district: {subdistrict} ({code}) "
              f"Website: {current_count} | "
              f"Actual in dataset: {actual_scraped_count} | "
              f"Rate: {completion_rate:.1f}%")


def estate_changed(existing: pd.Series, new: dict) -> bool:
    """Safe comparison of individual values"""
    comparison_fields = ['Address', 'Blocks', 'Units', 'UnitRate', 'ForSale', 'ForRent']
    return any(
        str(existing.get(field, '')) != str(new.get(field, ''))
        for field in comparison_fields
    )



def process_estate_item(item, district_row) -> dict:
    """Extract structured data from individual estate elements."""
    return {
        'Name': item.select_one("div.main-text").get_text(strip=True),
        'Address': item.select_one("div.address.f-middle").get_text(strip=True),
        'Blocks': safe_extract(item, "div:-soup-contains('No. of Block(s)') + div"),
        'Units': safe_extract(item, "div:-soup-contains('No. of Units') + div"),
        'UnitRate': safe_extract(item, "div:-soup-contains('Unit Rate of Saleable Area') + div"),
        'MoM': safe_extract(item, "div:-soup-contains('MoM') + div"),
        'ForSale': safe_extract(item, "div:-soup-contains('For Sale') + div"),
        'ForRent': safe_extract(item, "div:-soup-contains('For Rent') + div"),
        'Link': item.get('href'),
        'Region': district_row['Region'],
        'District': district_row['District'],
        'Subdistrict': district_row['Subdistrict'],
        'Code': district_row['Code'],
        'LastScraped': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

def safe_extract(item, selector):
    """Safe element text extraction with error handling."""
    element = item.select_one(selector)
    return element.get_text(strip=True) if element else ""

def estate_changed(existing, new) -> bool:
    """Compare key fields for change detection."""
    return any(
        str(existing.get(field, '')) != str(new.get(field, ''))
        for field in ['Address', 'Blocks', 'Units', 'UnitRate', 'ForSale', 'ForRent']
    )


def safe_get_text(element, selector):
    """Helper function for fault-tolerant text extraction"""
    try:
        return element.find_element(By.CSS_SELECTOR, selector).text.strip()
    except Exception:
        return None

# nodes.py (Kedro compatible version)
def scrape_estate_details(listings_df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """Scrape detailed estate information with full data fields using incremental updates."""
    logger = logging.getLogger(__name__)
    
    # Check if node should be run based on last execution date
    node_name = "estate_detail_scraper"
    tracking_params = params.get('node_tracking', {})
    if not should_run_node(node_name, "estate", tracking_params):
        logger.info(f"Node '{node_name}' last run within configured days - returning existing data")
        # Return existing data if available
        details_file = params.get('estate_details_file', 'data/01_raw/centaline_estate_lv_2.parquet')
        if os.path.exists(details_file):
            try:
                return pd.read_parquet(details_file)
            except Exception as e:
                logger.warning(f"Failed to load existing estate details: {e}")
                return pd.DataFrame()
        return pd.DataFrame()
    
    details_file = params.get('estate_details_file', 'data/01_raw/centaline_estate_lv_2.parquet')
    print("Details File:", details_file)
    
    # Load existing details
    existing_details = pd.DataFrame()
    if os.path.exists(details_file):
        #print("inside!!inside!!inside!!inside!!")
        existing_details = pd.read_parquet(details_file)
        #print('Checkpoint:', existing_details['Name'])
        logger.info(f"Loaded {len(existing_details)} existing estate details")
    
    # Get existing links and filter new listings
    existing_items = set(existing_details['Name']) if 'Name' in existing_details.columns else set()
    new_listings = listings_df[~listings_df['Name'].isin(existing_items)]
    
    if new_listings.empty:
        logger.info("No new estates to scrape")
        return existing_details

    driver = initialize_driver(params)
    new_details = []

    try:
        for _, row in tqdm(new_listings.iterrows(), total=len(new_listings), desc="Scraping new estates"):
            try:
                driver.get(row['Link'])
                random_sleep(params['global']['min_delay'], params['global']['max_delay'])
                
                # Extract core information
                detail_data = {
                    'scraped_estate_name': WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".estate-detail-banner-title"))
                    ).text.strip(),
                    'occupation_permit': extract_element_text(
                        driver, 
                        "//div[contains(text(), 'Date of Occupation Permit')]/following-sibling::div"
                    ),
                    'scraped_blocks': extract_element_text(
                        driver,
                        "//div[contains(text(), 'No. of Block(s)')]/following-sibling::div"
                    ),
                    # New fields
                    'school_net_info': None,
                    'estate_detailed_address': None,
                    'developer': None,
                    'Link': row['Link'],
                    'Region': row['Region'],
                    'District': row['District']
                }

                # Extract School Net information
                try:
                    items_divs = driver.find_elements(By.CLASS_NAME, "item")
                    for div in items_divs:
                        try:
                            label = div.find_element(By.CLASS_NAME, "label-item-left").text.strip()
                            if "School Net" in label:
                                links = div.find_elements(By.TAG_NAME, "a")
                                if len(links) >= 2:
                                    primary = links[0].text.strip()
                                    secondary = links[1].text.strip()
                                    detail_data['school_net_info'] = f"{primary} | {secondary}"
                                break
                        except Exception:
                            continue
                except Exception as e:
                    logger.debug(f"School net extraction failed: {str(e)}")

                # Extract Detailed Address
                try:
                    address_elem = driver.find_element(By.CLASS_NAME, "estate-detail-banner-position")
                    detail_data['estate_detailed_address'] = address_elem.text.strip()
                except Exception as e:
                    logger.debug(f"Address extraction failed: {str(e)}")

                # Extract Developer
                try:
                    items = driver.find_elements(By.CLASS_NAME, "item")
                    for item in items:
                        try:
                            label = item.find_element(By.CLASS_NAME, "label-item-left").text.strip()
                            if "Developer" in label:
                                developer = item.find_element(By.CLASS_NAME, "label-item-right").text.strip()
                                detail_data['developer'] = developer
                                break
                        except Exception:
                            continue
                except Exception as e:
                    logger.debug(f"Developer extraction failed: {str(e)}")

                new_details.append(detail_data)
                
            except Exception as e:
                logger.error(f"Failed to process {row['Link']}: {str(e)}")
                continue

        # Merge and save results
        if new_details:
            new_df = pd.DataFrame(new_details)
            updated_df = pd.concat([existing_details, new_df], ignore_index=True)
            updated_df.to_parquet(details_file, index=False)
            logger.info(f"Added {len(new_df)} new estate details")
            
            # Record node execution
            record_node_execution(
                node_name="estate_detail_scraper",
                node_type="estate",
                metadata={
                    "estates_processed": len(updated_df),
                    "new_details_scraped": len(new_df),
                    "execution_time": datetime.now().isoformat()
                }
            )
            
            return updated_df[updated_df['Name'].notnull()]
        
        # Record node execution (no new data)
        record_node_execution(
            node_name="estate_detail_scraper",
            node_type="estate",
            metadata={
                "estates_processed": len(existing_details),
                "new_details_scraped": 0,
                "execution_time": datetime.now().isoformat()
            }
        )
        
        return existing_details[existing_details['Name'].notnull()]

    finally:
        # Robust driver termination with exception handling
        if driver is not None:
            try:
                driver.quit()
            except Exception as e:
                logger.debug(f"Driver termination exception: {str(e)}")

def extract_element_text(driver, xpath: str) -> Optional[str]:
    """Helper function to safely extract text from elements."""
    try:
        element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        return element.text.strip()
    except:
        return None




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
'''def enrich_estate_data(
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
    )'''

def enrich_estate_data(
    estate_details_df: pd.DataFrame,
    transactions_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Simplified estate data enrichment - building matching now handled by centralized buildings pipeline.
    This function now only adds basic building information without complex matching logic.
    """
    logger.info("🏗️ Starting simplified estate data enrichment (building matching moved to centralized pipeline)")
    
    # Load existing data if available
    output_file = "data/02_intermediate/centaline_res_base.parquet"
    existing_enriched = pd.DataFrame()
    
    try:
        if os.path.exists(output_file):
            existing_enriched = pd.read_parquet(output_file)
            logger.info(f"✅ Found existing enriched data: {len(existing_enriched)} records")
        else:
            logger.info("📂 No existing enriched data found - starting fresh")
    except Exception as e:
        logger.warning(f"⚠️ Warning: Error loading existing file: {str(e)}")
        existing_enriched = pd.DataFrame()

    # Simple estate name extraction (without complex matching)
    def extract_estate_name_from_address(address: str) -> str:
        """Extract basic estate name from transaction address"""
        try:
            if pd.isna(address) or address is None:
                return ""
            
            address = str(address).strip()
            if not address:
                return ""
            
            # Simple extraction: take first part before common separators
            stop_indicators = [
                'Tower', 'Block', 'Upper', 'Lower', 'Middle', 'Floor',
                '/F', 'LG', 'UG', 'Flat', 'No.', 'G/F', 'B/M', 'U/L',
                'Carpark', 'Site'
            ]
            
            words = address.split()
            estate_words = []
            
            for i, word in enumerate(words):
                if any(indicator in word for indicator in stop_indicators):
                    break
                if re.match(r'\d+/F', word):
                    break
                estate_words.append(word)
            
            result = ' '.join(estate_words)
            result = re.sub(r'[,\(\)\[\]]+$', '', result).strip()
            return result
            
        except Exception as e:
            logger.debug(f"Error extracting estate name from '{address}': {str(e)}")
            return ""

    # Process transactions
    logger.info("🔍 Extracting basic estate names from transaction addresses...")
    
    try:
        transactions_copy = transactions_df.copy()
        
        # Extract estate names
        transactions_copy['estate_name'] = transactions_copy['address'].apply(
            lambda x: extract_estate_name_from_address(x) if pd.notna(x) else ""
        )
        
        # Add basic building information columns
        transactions_copy['building_name'] = transactions_copy['estate_name']
        
        # Initialize region/district/subdistrict columns (will be populated from estate details)
        transactions_copy['region'] = None
        transactions_copy['district'] = None
        transactions_copy['subdistrict'] = None
        transactions_copy['code'] = None
        
        # Add processing metadata
        transactions_copy['processing_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        transactions_copy['source'] = 'centaline_res'
        transactions_copy['property_type'] = 'residential'
        
        # Fix date format (remove the incorrect year column creation)
        if 'date' in transactions_copy.columns:
            # Convert date to dd/mm/yyyy format
            try:
                date_dt = pd.to_datetime(transactions_copy['date'], errors='coerce')
                transactions_copy['date'] = date_dt.dt.strftime('%d/%m/%Y')
                logger.info("✅ Fixed date format to dd/mm/yyyy")
            except Exception as e:
                logger.warning(f"⚠️ Could not fix date format: {e}")
        
        # Add building completion year and age from estate details
        if not estate_details_df.empty:
            try:
                # Extract completion year from Occupation Permit column
                estate_details_copy = estate_details_df.copy()
                
                # Function to extract year from occupation permit string
                def extract_completion_year(permit_str):
                    if pd.isna(permit_str) or permit_str == 'None':
                        return None
                    try:
                        # Handle formats like "2004/11", "2000/2", "1986/2"
                        if '/' in str(permit_str):
                            year_part = str(permit_str).split('/')[0]
                            if year_part.isdigit() and len(year_part) == 4:
                                return int(year_part)
                    except:
                        pass
                    return None
                
                # Extract completion year from Occupation Permit
                estate_details_copy['completion_year'] = estate_details_copy['Occupation Permit'].apply(extract_completion_year)
                
                # Join with estate details to get completion year
                transactions_copy = transactions_copy.merge(
                    estate_details_copy[['Scraped Estate Name', 'completion_year']],
                    left_on='estate_name',
                    right_on='Scraped Estate Name',
                    how='left'
                )
                
                # Drop the duplicate column
                if 'Scraped Estate Name' in transactions_copy.columns:
                    transactions_copy = transactions_copy.drop('Scraped Estate Name', axis=1)
                
                # Calculate building age and update year column
                if 'completion_year' in transactions_copy.columns:
                    current_year = datetime.now().year
                    transactions_copy['age'] = transactions_copy['completion_year'].apply(
                        lambda x: max(0, current_year - x) if pd.notna(x) else None
                    )
                    
                    # Replace the year column with completion_year (this is what we want)
                    transactions_copy['year'] = transactions_copy['completion_year']
                    
                    # Drop the completion_year column since we're using year
                    transactions_copy = transactions_copy.drop('completion_year', axis=1)
                    
                    logger.info(f"✅ Added building completion year and age from {estate_details_copy['completion_year'].notna().sum()} estates")
                    logger.info(f"✅ Updated year column to contain completion years")
                else:
                    logger.warning("⚠️ No completion year data found in estate details")
                
                # Add region, district, subdistrict, code, and developer from estate details
                try:
                    # Join with estate details to get location information and developer
                    location_join = transactions_copy.merge(
                        estate_details_copy[['Scraped Estate Name', 'Region', 'District', 'Subdistrict', 'Code', 'Developer']],
                        left_on='estate_name',
                        right_on='Scraped Estate Name',
                        how='left'
                    )
                    
                    # Update the location columns
                    transactions_copy['region'] = location_join['Region']
                    transactions_copy['district'] = location_join['District']
                    transactions_copy['subdistrict'] = location_join['Subdistrict']
                    transactions_copy['code'] = location_join['Code']
                    transactions_copy['developer'] = location_join['Developer']
                    
                    # Count records with location data
                    records_with_location = transactions_copy['region'].notna().sum()
                    records_with_developer = transactions_copy['developer'].notna().sum()
                    logger.info(f"✅ Added location data to {records_with_location} records")
                    logger.info(f"✅ Added developer data to {records_with_developer} records")
                except Exception as e:
                    logger.warning(f"⚠️ Could not add location and developer data: {e}")
            except Exception as e:
                logger.warning(f"⚠️ Could not add building completion year: {e}")
        
        logger.info(f"📊 Processed {len(transactions_copy)} transactions")
        
    except Exception as e:
        logger.error(f"⚠️ Error in estate name extraction: {str(e)}")
        transactions_copy = transactions_df.copy()
        transactions_copy['estate_name'] = ""
        transactions_copy['building_name'] = ""
        transactions_copy['district'] = ""
        transactions_copy['region'] = ""
        transactions_copy['subdistrict'] = ""
        transactions_copy['processing_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        transactions_copy['source'] = 'centaline_res'
        transactions_copy['property_type'] = 'residential'

    # Combine with existing data
    try:
        if not existing_enriched.empty:
            # Ensure existing data has developer column if it doesn't exist
            if 'developer' not in existing_enriched.columns and 'developer' in transactions_copy.columns:
                existing_enriched['developer'] = None
                logger.info("✅ Added developer column to existing data")
            
            # Use pandas concat to handle different column sets
            final_df = pd.concat([existing_enriched, transactions_copy], ignore_index=True, sort=False)
            logger.info(f"📊 Combined {len(existing_enriched)} existing + {len(transactions_copy)} new = {len(final_df)} total records")
        else:
            final_df = transactions_copy
            logger.info(f"📊 Created new dataset with {len(final_df)} records")
            
    except Exception as e:
        logger.error(f"⚠️ Error in combining data: {str(e)}")
        final_df = transactions_copy

    # Generate statistics
    try:
        logger.info("\n📈 Simplified Estate Enrichment Statistics:")
        total_processed = len(transactions_copy)
        estate_names_extracted = len(transactions_copy[transactions_copy['estate_name'].str.len() > 0])
        
        logger.info(f"   - Transactions processed: {total_processed:,}")
        logger.info(f"   - Estate names extracted: {estate_names_extracted:,} ({estate_names_extracted/total_processed*100:.1f}%)")
        logger.info(f"   - Note: Building matching now handled by centralized buildings pipeline")
            
    except Exception as e:
        logger.error(f"⚠️ Error generating statistics: {str(e)}")

    # Ensure data types are consistent before saving
    try:
        # Convert age column to float64 if it exists
        if 'age' in final_df.columns:
            final_df['age'] = pd.to_numeric(final_df['age'], errors='coerce').astype('float64')
            logger.info("✅ Fixed age column data type to float64")
        
        # Convert year column to Int64 if it exists
        if 'year' in final_df.columns:
            final_df['year'] = pd.to_numeric(final_df['year'], errors='coerce').astype('Int64')
            logger.info("✅ Fixed year column data type to Int64")
            
    except Exception as e:
        logger.warning(f"⚠️ Error fixing data types: {e}")
    
    logger.info("✅ Simplified estate data enrichment completed successfully!")
    logger.info("📝 Note: Complex building matching has been moved to the centralized buildings pipeline")
    
    return final_df



