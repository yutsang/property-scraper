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
from typing import Dict, Any, List, Tuple
from tqdm import tqdm
import configparser
import re
import string
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, NoSuchElementException
import os
import json
from pathlib import Path
from typing import Optional, Set
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
        """Enhanced date parsing with ISO format support for JavaScript extraction"""
        if not date_str or pd.isna(date_str):
            return None
        date_str = str(date_str).strip()
        
        # Try pandas first (handles ISO: "2026-01-14T00:00:00" and other formats)
        try:
            parsed = pd.to_datetime(date_str, errors='coerce')
            if pd.notna(parsed):
                return parsed.date()
        except:
            pass
        
        # Fallback to manual parsing
        date_formats = [
            '%Y-%m-%dT%H:%M:%S',  # ISO from JavaScript
            '%Y-%m-%d',
            '%d/%m/%Y',
            '%m/%d/%Y',
            '%Y%m%d',
            '%d-%m-%Y'
        ]
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

    def extract_nuxt_transactions(driver):
        """
        IMPROVED: Extract transaction data from window.__NUXT__ JavaScript object.
        Gets ALL data: gArea, nArea, gUnitPrice, nUnitPrice, region, district, building codes.
        Much more reliable and complete than HTML scraping.
        """
        try:
            # Execute JavaScript to get the __NUXT__ object
            nuxt_data = driver.execute_script("return window.__NUXT__;")
            
            if not nuxt_data:
                logger.warning("window.__NUXT__ is empty")
                return []
            
            # Navigate to transactions: state.transaction.transactionList.data
            transactions = nuxt_data.get('state', {}).get('transaction', {}).get('transactionList', {}).get('data', [])
            
            if not transactions:
                logger.debug("No transactions found in __NUXT__ object")
                return []
            
            # Parse each transaction
            parsed_transactions = []
            
            for txn in transactions:
                try:
                    # Extract nested data
                    scope = txn.get('scope', {})
                    display_text = txn.get('displayText', {}).get('addr', {})
                    
                    # Extract building information
                    big_estate = txn.get('bigEstateName', '').strip()  # Main estate name (e.g. "Residence Bel-Air")
                    estate = txn.get('estateName', '').strip()  # Phase/sub-estate (e.g. "Phase 2 South Tower")
                    building = txn.get('buildingName', '').strip()  # Tower/Block (e.g. "Tower 1")
                    formatted_address = display_text.get('line1', '').strip()
                    
                    # Build full name with priority: big_estate > estate > building
                    # For phased developments: "Estate Name Phase X Tower Y"
                    # For simple buildings: "Building Name"
                    if big_estate and estate and building:
                        # Full hierarchy: "Residence Bel-Air Phase 2 South Tower Tower 1"
                        full_name = f"{big_estate} {estate} {building}"
                    elif big_estate and estate:
                        # Main estate + phase: "Residence Bel-Air Phase 2 South Tower"
                        full_name = f"{big_estate} {estate}"
                    elif big_estate and building:
                        # Main estate + building: "Residence Bel-Air Tower 1"
                        full_name = f"{big_estate} {building}"
                    elif big_estate:
                        # Just main estate: "Residence Bel-Air"
                        full_name = big_estate
                    elif estate and building:
                        # Phase + building: "Phase 2 South Tower Tower 1"
                        full_name = f"{estate} {building}"
                    elif building:
                        full_name = building
                    elif estate:
                        full_name = estate
                    elif formatted_address:
                        # Extract building name from formatted address
                        # Format: "Building Name Floor Flat" - take first part before Floor
                        parts = formatted_address.split()
                        name_parts = []
                        for part in parts:
                            # Stop at floor indicators
                            if part in ['Upper', 'Middle', 'Lower', 'High', 'Mid', 'Low'] or 'Floor' in part or '/F' in part:
                                break
                            name_parts.append(part)
                        full_name = ' '.join(name_parts) if name_parts else formatted_address
                    else:
                        full_name = ''
                    
                    # Extract completion year and convert to number
                    completion_year_str = txn.get('opYear', '')
                    completion_year = None
                    if completion_year_str:
                        import re
                        # Extract 4-digit year from strings like "2021年" or "1983年"
                        year_match = re.search(r'(\d{4})', completion_year_str)
                        if year_match:
                            try:
                                completion_year = int(year_match.group(1))
                            except:
                                pass
                    
                    # Calculate age with month precision if available
                    from datetime import datetime
                    current_year = datetime.now().year
                    current_month = datetime.now().month
                    age = None
                    if completion_year:
                        # Calculate age based on year and month if available
                        # If building completed in December 2023 and now is January 2026:
                        # Age = (2026 - 2023) = 3 years (simple)
                        # Or with month: if completed Dec 2023, now Jan 2026 = ~2 years 1 month
                        # For simplicity, use year-based age (more accurate would need completion month)
                        age = current_year - completion_year
                        if age < 0:
                            age = None
                    
                    # Extract area with multiple fallbacks: prefer nArea (net), then gArea (gross), then any other area field
                    n_area = txn.get('nArea')
                    g_area = txn.get('gArea')
                    
                    # Try additional area fields that might exist
                    salable_area = txn.get('salableArea')
                    floor_area = txn.get('floorArea')
                    build_area = txn.get('buildUpArea')
                    usable_area = txn.get('usableArea')
                    
                    # Use first available area value
                    area_value = (n_area if n_area is not None 
                                 else g_area if g_area is not None
                                 else salable_area if salable_area is not None
                                 else floor_area if floor_area is not None
                                 else build_area if build_area is not None
                                 else usable_area)
                    
                    # Log if no area found for non-carpark properties
                    if area_value is None:
                        property_name = full_name if full_name else ''
                        if 'carpark' not in property_name.lower():
                            logger.debug(f"No area found for: {property_name} (ID: {txn.get('id')})")
                    
                    # Extract unit price with same fallback logic
                    n_unit_price = txn.get('nUnitPrice')
                    g_unit_price = txn.get('gUnitPrice')
                    ft_price_value = n_unit_price if n_unit_price is not None else g_unit_price
                    
                    # Detect if this is a carpark transaction
                    is_carpark = False
                    if full_name and 'carpark' in full_name.lower():
                        is_carpark = True
                    elif building and 'carpark' in building.lower():
                        is_carpark = True
                    
                    # Set property type based on carpark detection
                    property_type = 'Carpark' if is_carpark else 'residential'
                    
                    record = {
                        # Match your required column order exactly
                        'date': txn.get('insDate', ''),
                        'date_original': txn.get('insDate', ''),  # Keep original date for fallback sorting
                        'region': scope.get('terr', ''),
                        'district': scope.get('db', ''),
                        'subdistrict': scope.get('hma', ''),
                        'Name': full_name if full_name else None,  # Use None instead of empty string
                        'Tower': building if building else (estate if estate and not building else None),
                        'Floor': txn.get('yAxis', ''),
                        'Flat': txn.get('xAxis', ''),
                        'transaction_type': 'SALE' if txn.get('postType') == 'S' else 'RENT' if txn.get('postType') == 'R' else '',
                        'area': area_value,  # Use nArea with gArea fallback
                        'price': txn.get('transactionPrice'),
                        'ft_price': ft_price_value,  # Use nUnitPrice with gUnitPrice fallback
                        'source': 'centaline_res',
                        'property_type': property_type,  # 'Carpark' or 'residential'
                        'address': formatted_address,  # KEEP the full formatted address from JavaScript
                        'street_address': txn.get('address', ''),
                        'building_code': txn.get('typeCode', ''),
                        'g_area': g_area,  # Keep original gross area
                        'g_unit_price': g_unit_price,  # Keep original gross unit price
                        'completion_year': completion_year,
                        'age': age,  # Age calculated with current year
                        'estate_type': txn.get('estateType', ''),
                        'transaction_url': txn.get('detailUrl', ''),
                        'transaction_id': txn.get('id', ''),
                        'title_lg': display_text.get('line5', ''),
                        'rooms': txn.get('bedroomCount'),
                        'direction': txn.get('direction', ''),
                        'estate_name': estate if estate else None,
                        'building_name': building if building else None,
                    }
                    
                    parsed_transactions.append(record)
                    
                except Exception as e:
                    logger.debug(f"Error parsing transaction: {e}")
                    continue
            
            return parsed_transactions
            
        except Exception as e:
            logger.error(f"Error extracting __NUXT__ data: {e}")
            return []
    
    def extract_combined_data(driver):
        """
        Extract transaction data from BOTH JavaScript __NUXT__ and HTML table.
        - JavaScript provides: metadata (building codes, dates, etc.)
        - HTML table provides: VISIBLE area, price, ft_price (always shown on list page)
        - Uses HTML as FALLBACK for missing JavaScript data
        Returns merged data with all available fields.
        """
        try:
            # Extract from JavaScript (most complete for metadata)
            js_data = extract_nuxt_transactions(driver)
            
            # Extract from HTML table (has visible area/price that might be missing from JS)
            try:
                html_data = extract_table_data(driver)
            except Exception as e:
                logger.warning(f"HTML table extraction failed: {e}")
                html_data = []
            
            if not js_data and not html_data:
                logger.debug("No data from either source")
                return []
            
            # If we have both sources, merge them
            if js_data and html_data:
                # Helper function to parse HTML values to numeric
                def parse_html_area(area_text):
                    """Parse area from HTML like '401呎' to 401.0"""
                    if not area_text or area_text == '--':
                        return None
                    import re
                    area_clean = re.sub(r'[^\d,.]', '', str(area_text))
                    area_clean = area_clean.replace(',', '')
                    return float(area_clean) if area_clean else None
                
                def parse_html_price(price_text):
                    """Parse price from HTML like '$545.5萬' to 5455000.0"""
                    if not price_text or price_text == '--':
                        return None
                    import re
                    price_str = str(price_text).replace('$', '').replace(',', '')
                    # Handle millions (萬 = 10,000)
                    if '萬' in price_str:
                        number = re.findall(r'[\d.]+', price_str)
                        if number:
                            return float(number[0]) * 10000
                    # Handle billions (億)
                    elif '億' in price_str:
                        number = re.findall(r'[\d.]+', price_str)
                        if number:
                            return float(number[0]) * 100000000
                    # Regular number
                    else:
                        number = re.findall(r'[\d.]+', price_str)
                        if number:
                            return float(number[0])
                    return None
                
                def parse_html_ft_price(ft_price_text):
                    """Parse ft_price from HTML like '@$13,603' to 13603.0"""
                    if not ft_price_text or ft_price_text == '--':
                        return None
                    import re
                    price_clean = str(ft_price_text).replace('@', '').replace('$', '').replace(',', '')
                    numbers = re.findall(r'[\d.]+', price_clean)
                    return float(numbers[0]) if numbers else None
                
                # Merge by position/index
                merged_data = []
                area_fallback_count = 0
                
                for i, js_rec in enumerate(js_data):
                    # Use HTML data at same index if available
                    if i < len(html_data):
                        html_rec = html_data[i]
                        
                        # Parse HTML values and use as FALLBACK when JS is missing
                        if js_rec.get('area') is None and html_rec.get('area'):
                            parsed_area = parse_html_area(html_rec.get('area'))
                            if parsed_area:
                                js_rec['area'] = parsed_area
                                area_fallback_count += 1
                                logger.info(f"   ✓ Used HTML area fallback for record #{i+1}: {html_rec.get('area')} → {parsed_area} ({js_rec.get('Name', 'unknown')})")
                        
                        if js_rec.get('price') is None and html_rec.get('price'):
                            parsed_price = parse_html_price(html_rec.get('price'))
                            if parsed_price:
                                js_rec['price'] = parsed_price
                                logger.debug(f"Used HTML price fallback: {parsed_price}")
                        
                        if js_rec.get('ft_price') is None and html_rec.get('ft_price'):
                            parsed_ft_price = parse_html_ft_price(html_rec.get('ft_price'))
                            if parsed_ft_price:
                                js_rec['ft_price'] = parsed_ft_price
                                logger.debug(f"Used HTML ft_price fallback: {parsed_ft_price}")
                    
                    merged_data.append(js_rec)
                
                #logger.info(f"   ✅ Merged {len(merged_data)} records from JS and HTML table")
                if area_fallback_count > 0:
                    logger.info(f"   📊 Used HTML fallback for {area_fallback_count} area values")
                return merged_data
            
            # If only one source has data, use it
            elif js_data:
                logger.debug("Using JavaScript data only (no HTML data)")
                return js_data
            else:
                logger.debug("Using HTML data only (no JavaScript data)")
                return html_data
            
        except Exception as e:
            logger.error(f"Error in extract_combined_data: {e}")
            return []
    
    def extract_table_data(driver):
        """Extract visible data from HTML table on the transaction list page."""
        table_data = []
        try:
            enhanced_scroll_down(driver)

            # Find all transaction rows (desktop table format)
            rows = driver.find_elements(By.CSS_SELECTOR, "tr.cv-structured-list-item")
            logger.debug(f"   Found {len(rows)} table rows for HTML extraction")
            
            for row in rows:
                try:
                    # Get all cells in the row
                    cells = row.find_elements(By.CSS_SELECTOR, "td.cv-structured-list-data")
                    
                    if len(cells) >= 6:  # Ensure we have enough cells (minimum 6 for basic data with title_lg)
                        # IMPORTANT: Extract transaction URL for later duplicate estate matching
                        transaction_url = ""
                        try:
                            # Try to find a link in the row that leads to transaction details
                            link_element = row.find_element(By.CSS_SELECTOR, "a[href*='/transaction/'], a.transaction-link")
                            transaction_url = link_element.get_attribute('href')
                        except:
                            # Try alternative: check if row itself is clickable
                            try:
                                transaction_url = row.get_attribute('data-href')
                            except:
                                pass
                        
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

                        # Extract title_lg from the third cell (ACTUAL TITLE_LG COLUMN)
                        title_lg_text = cells[2].text.strip()

                        # Extract rooms from the fourth cell
                        rooms_text = cells[3].text.strip()

                        # Determine transaction type and extract price from the FIFTH cell
                        transaction_type = "SALE"
                        price_text = cells[4].text.strip()

                        # Check if it's a rent transaction
                        # Priority 1: If price contains "租" (Chinese character for rent), it's definitely a rent transaction
                        if "租" in price_text:
                            transaction_type = "RENT"
                        # Priority 2: If price starts with "$" and is short (likely rent amount), it's a rent transaction
                        elif price_text.startswith("$") and len(price_text) < 10 and any(char.isdigit() for char in price_text):
                            transaction_type = "RENT"

                        # Extract area from the SIXTH cell (面積實)
                        area_text = cells[5].text.strip()
                        
                        # Debug: log if area is found
                        if area_text and area_text != '--':
                            logger.debug(f"   HTML area extracted: {area_text}")

                        # Initialize optional fields
                        ft_price_text = ""
                        changes_text = ""

                        # Extract additional data if available
                        if len(cells) >= 7:
                            ft_price_text = cells[6].text.strip()
                        if len(cells) >= 8:
                            try:
                                changes_element = cells[7].find_element(By.CSS_SELECTOR, ".riseBox span")
                                changes_text = changes_element.text.strip()
                            except:
                                changes_text = cells[7].text.strip()
                        
                        record = {
                            'date': date_text,
                            'address': address_text,
                            'title_lg': title_lg_text,  # Address with native separators
                            'rooms': rooms_text,
                            'price': price_text,
                            'area': area_text,
                            'ft_price': ft_price_text,
                            'changes': changes_text,
                            'transaction_type': transaction_type,
                            'transaction_url': transaction_url,  # NEW: URL for duplicate matching
                        }
                        table_data.append(record)
                        
                except Exception as e:
                    logger.debug(f"Error processing row: {str(e)}")
                    continue
                    
            # Extract title-lg and price information from mobile card format
            mobile_cards = driver.find_elements(By.CSS_SELECTOR, ".transactions-content")
            
            # Create lists for title-lg and price values from mobile cards
            title_lg_values = []
            price_values = []
            
            for card in mobile_cards:
                try:
                    # Extract title-lg with multiple approaches
                    title_lg_text = None
                    
                    # Method 1: Try .text01 .title-lg
                    text01_elements = card.find_elements(By.CSS_SELECTOR, ".text01")
                    if text01_elements:
                        title_lg_elements = text01_elements[0].find_elements(By.CSS_SELECTOR, ".title-lg")
                        if title_lg_elements:
                            title_lg_text = title_lg_elements[0].text.strip()
                    
                    # Method 2: Try direct .title-lg
                    if not title_lg_text:
                        title_lg_elements = card.find_elements(By.CSS_SELECTOR, ".title-lg")
                        if title_lg_elements:
                            title_lg_text = title_lg_elements[0].text.strip()
                    
                    # Method 3: Try to extract from the first line of card text
                    if not title_lg_text:
                        card_text = card.text.strip()
                        lines = card_text.split('\n')
                        if lines:
                            # The first line usually contains the property name
                            first_line = lines[0].strip()
                            if first_line and len(first_line) > 5:  # Reasonable length for a property name
                                title_lg_text = first_line
                    
                    if title_lg_text:
                        title_lg_values.append(title_lg_text)
                    
                    # Extract price from mobile card
                    price_text = None
                    try:
                        # Look for price in content-price section with multiple selectors
                        price_selectors = [
                            ".content-price .saleprice span",
                            ".content-price .saleprice",
                            ".content-price span",
                            ".content-price",
                            ".saleprice span",
                            ".saleprice"
                        ]
                        
                        for selector in price_selectors:
                            price_elements = card.find_elements(By.CSS_SELECTOR, selector)
                            if price_elements:
                                temp_price = price_elements[0].text.strip()
                                # Check if it looks like a price (contains $ and numbers)
                                if temp_price and '$' in temp_price and any(char.isdigit() for char in temp_price):
                                    # Prefer the largest amount (likely the main price, not per sq ft)
                                    # Look for patterns like $X,XXX or $X.XM which are likely main prices
                                    if 'M' in temp_price or (',' in temp_price and len(temp_price) > 6):
                                        price_text = temp_price
                                        break
                                    elif not price_text:
                                        price_text = temp_price
                        
                        # If still no price, try to extract from the entire card text
                        if not price_text:
                            card_text = card.text
                            # Look for price patterns like $X.XM, $X,XXX, etc.
                            import re
                            price_patterns = [
                                r'\$\d+\.?\d*M',  # $1.2M, $2M
                                r'\$\d{1,3}(?:,\d{3})*',  # $1,234, $12,345
                                r'\$\d+',  # $1234
                            ]
                            for pattern in price_patterns:
                                matches = re.findall(pattern, card_text)
                                if matches:
                                    price_text = matches[0]
                                    break
                    except Exception as e:
                        logger.debug(f"Error extracting price from mobile card: {e}")
                    
                    price_values.append(price_text if price_text else "")
                    
                except Exception as e:
                    logger.debug(f"Error processing mobile card: {str(e)}")
                    title_lg_values.append("")
                    price_values.append("")
                    continue
            

            
            # Enrich table data with title-lg and price information
            # Map title-lg and price values to table records based on matching addresses
            for i, record in enumerate(table_data):
                record_address = record['address'].lower()
                matched_title_lg = None
                matched_price = None
                
                # Try to find matching title-lg and price based on address similarity
                for j, title_lg in enumerate(title_lg_values):
                    title_lg_lower = title_lg.lower()
                    record_address_lower = record_address.lower()
                    
                    # Method 1: Check if the title-lg contains key parts of the address
                    if any(part in title_lg_lower for part in record_address_lower.split() if len(part) > 2):
                        matched_title_lg = title_lg
                        if j < len(price_values):
                            matched_price = price_values[j]
                        break
                    
                    # Method 2: Check if address contains key parts of title-lg
                    elif any(part in record_address_lower for part in title_lg_lower.split() if len(part) > 2):
                        matched_title_lg = title_lg
                        if j < len(price_values):
                            matched_price = price_values[j]
                        break
                
                # If no match found, use the first available title-lg and price or fallback
                if matched_title_lg:
                    record['title_lg'] = matched_title_lg
                    if matched_price and (not record['price'] or record['price'].strip() == ''):
                        record['price'] = matched_price
                elif title_lg_values:
                    record['title_lg'] = title_lg_values[0]  # Use first available
                    if price_values and (not record['price'] or record['price'].strip() == ''):
                        record['price'] = price_values[0]
                    title_lg_values = title_lg_values[1:]  # Remove used value
                    price_values = price_values[1:]  # Remove used value
                else:
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
                    logger.info(f"✅ Using incremental control date: {control_date} (max existing: {max_date})")
                else:
                    control_date = pd.to_datetime(params.get('centaline_res', {}).get('control_date', params['global']['start_date'])).date()
                    logger.info(f"⚠️ No valid dates found in existing data, using parameter control date: {control_date}")
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

    # ============ MAIN SCRAPING LOGIC WITH MULTI-THREADING ============
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    
    # Thread-local storage for drivers
    thread_local = threading.local()
    
    def get_thread_driver():
        """Get or create driver for current thread"""
        if not hasattr(thread_local, 'driver'):
            thread_local.driver = initialize_driver()
        return thread_local.driver
    
    def scrape_area_transactions(area_row):
        """Scrape transactions for a single area (thread-safe)"""
        driver = get_thread_driver()
        base_url = "https://hk.centanet.com/findproperty/en/list/transaction"
        area_data = []
        
        try:
            subdistrict = area_row['Subdistrict'].replace(' ', '-').lower()
            session_id = f"session_{int(datetime.now().timestamp())}_{threading.get_ident()}"
            url = f"{base_url}/{subdistrict}_19-{area_row['Code']}?q={session_id}"
            
            driver.get(url)
            smart_sleep()
            
            page = 1
            date_reached = False
            max_pages = params['global'].get('max_pages_per_area', 50)
            
            while not date_reached and page <= max_pages:
                # Extract from BOTH JavaScript and HTML table (HTML as fallback for missing area/price)
                page_data = extract_combined_data(driver)
                            
                for record in page_data:
                    # Check control date
                    try:
                        transaction_date = parse_date_from_string(record['date'])
                        if transaction_date and transaction_date < control_date:
                            date_reached = True
                            break
                    except:
                        pass
                    
                    # Add area_code for reference
                    record['area_code'] = area_row['Code']
                    record['scrape_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    area_data.append(record)
                
                if date_reached:
                    break
                    
                if not go_to_next_page(driver):
                    break
                page += 1
            
            return {'success': True, 'data': area_data, 'area': area_row['Subdistrict']}
            
        except Exception as e:
            logger.debug(f"Error scraping {area_row['Subdistrict']}: {e}")
            return {'success': False, 'data': [], 'area': area_row['Subdistrict'], 'error': str(e)}
    
    # Execute scraping with thread pool
    max_threads = params.get('global', {}).get('max_threads', 3)
    logger.info(f"🚀 Using {max_threads} parallel threads for transaction scraping")
    
    all_data = []
    failed_areas = []
    
    from tqdm.auto import tqdm
    
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        # Submit all area scraping tasks
        future_to_area = {executor.submit(scrape_area_transactions, row): row for _, row in area_df.iterrows()}
        
        # Process completed tasks with progress bar
        with tqdm(total=len(future_to_area), desc=f"Scraping areas ({max_threads} threads)") as pbar:
            for future in as_completed(future_to_area):
                result = future.result()
                if result['success']:
                    all_data.extend(result['data'])
                    pbar.set_postfix({'area': result['area'][:15], 'total': len(all_data)})
                else:
                    failed_areas.append(result)
                pbar.update(1)
    
    logger.info(f"✅ Successfully scraped {len(all_data)} transactions from {len(future_to_area) - len(failed_areas)} areas")
    if failed_areas:
        logger.warning(f"⚠️ Failed to scrape {len(failed_areas)} areas")

    # ============ COMBINE AND CLEAN DATA ============
    new_data_df = pd.DataFrame(all_data)
    
    # Log data quality
    if not new_data_df.empty:
        logger.info(f"📊 New data quality:")
        logger.info(f"  Total records: {len(new_data_df)}")
        logger.info(f"  With region: {new_data_df['region'].notna().sum()} ({new_data_df['region'].notna().sum()/len(new_data_df)*100:.1f}%)")
        logger.info(f"  With district: {new_data_df['district'].notna().sum()} ({new_data_df['district'].notna().sum()/len(new_data_df)*100:.1f}%)")
        logger.info(f"  With building_code: {new_data_df['building_code'].notna().sum()} ({new_data_df['building_code'].notna().sum()/len(new_data_df)*100:.1f}%)")
        
        # Transaction type breakdown
        if 'transaction_type' in new_data_df.columns:
            type_counts = new_data_df['transaction_type'].value_counts()
            logger.info(f"  Transaction types:")
            for ttype, count in type_counts.items():
                logger.info(f"    {ttype}: {count} ({count/len(new_data_df)*100:.1f}%)")
    
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
        # Normalize timestamp
        if 'scrape_timestamp' in combined_df.columns:
            combined_df['scrape_timestamp'] = pd.to_datetime(
                combined_df['scrape_timestamp'], errors='coerce'
            ).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Fix data types for parquet compatibility
        # Fix rooms column - convert to Int64 (nullable integer)
        if 'rooms' in combined_df.columns:
            combined_df['rooms'] = pd.to_numeric(combined_df['rooms'], errors='coerce').astype('Int64')
        
        # Fix completion_year - ensure it's Int64
        if 'completion_year' in combined_df.columns:
            combined_df['completion_year'] = pd.to_numeric(combined_df['completion_year'], errors='coerce').astype('Int64')
        
        # Fix age - ensure it's Int64
        if 'age' in combined_df.columns:
            combined_df['age'] = pd.to_numeric(combined_df['age'], errors='coerce').astype('Int64')
        
        # Fix price columns - ensure numeric
        for col in ['price', 'area', 'ft_price', 'g_area', 'g_unit_price', 'n_area', 'n_unit_price']:
            if col in combined_df.columns:
                combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')
        
        # Deduplicate based on transaction_id (if available) or key columns
        if 'transaction_id' in combined_df.columns:
            before_dedup = len(combined_df)
            combined_df = combined_df.drop_duplicates(subset=['transaction_id'], keep='last')
            after_dedup = len(combined_df)
            if before_dedup != after_dedup:
                logger.info(f"Removed {before_dedup - after_dedup} duplicate transactions by ID")
        else:
            # Fallback dedup
            dedup_columns = ['date', 'address', 'price', 'area']
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
            "failed_areas": len(failed_areas),
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
    Process transaction data - UPDATED for JavaScript extraction.
    Data from JavaScript is already clean, just do minimal validation.
    """
    logger.info(f"🔄 Processing transaction data: {len(trans_df)} records")
    
    # Create a copy to avoid modifying original
    processed_df = trans_df.copy()
    
    # Filter out rows with empty or invalid dates
    initial_count = len(processed_df)
    
    # Remove rows where date is empty, None, or invalid
    if 'date' in processed_df.columns:
        processed_df = processed_df.dropna(subset=['date'])
        processed_df = processed_df[processed_df['date'] != '']
        if processed_df['date'].dtype == 'object':
            processed_df = processed_df[processed_df['date'].astype(str).str.strip() != '']
    
    # Check Name column instead of address (JavaScript extraction uses 'Name' not 'address')
    if 'Name' in processed_df.columns:
        processed_df = processed_df.dropna(subset=['Name'])
        processed_df = processed_df[processed_df['Name'] != '']
        if processed_df['Name'].dtype == 'object':
            processed_df = processed_df[processed_df['Name'].astype(str).str.strip() != '']
    
    # Remove rows where price is empty (these are likely incomplete records)
    if 'price' in processed_df.columns:
        processed_df = processed_df.dropna(subset=['price'])
        processed_df = processed_df[processed_df['price'] != 0]
    
    final_count = len(processed_df)
    removed_count = initial_count - final_count
    
    logger.info(f"📊 Data cleaning results:")
    logger.info(f"  - Initial records: {initial_count:,}")
    logger.info(f"  - Valid records: {final_count:,}")
    logger.info(f"  - Removed invalid records: {removed_count:,}")
    if initial_count > 0:
        logger.info(f"  - Success rate: {(final_count/initial_count)*100:.1f}%")
    else:
        logger.info(f"  - Success rate: N/A (no initial records)")
    
    # Additional data cleaning (minimal - JavaScript data is already clean)
    if len(processed_df) > 0:
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
        'MoM', 'ForSale', 'ForRent', 'Link', 'EstateCode', 'Region',
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
        
        # Drop rows where Name is None or empty
        final_df = final_df[final_df['Name'].notnull() & (final_df['Name'].str.strip() != '')]
        logger.info(f"📋 Final dataset after dropping None names: {len(final_df)} estates")
        
        return final_df
        
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
    link = item.get('href', '')
    
    # Extract estate code from URL by splitting on last "/"
    estate_code = ''
    if link:
        try:
            estate_code = link.rstrip('/').split('/')[-1]
        except:
            estate_code = ''
    
    return {
        'Name': item.select_one("div.main-text").get_text(strip=True),
        'Address': item.select_one("div.address.f-middle").get_text(strip=True),
        'Blocks': safe_extract(item, "div:-soup-contains('No. of Block(s)') + div"),
        'Units': safe_extract(item, "div:-soup-contains('No. of Units') + div"),
        'UnitRate': safe_extract(item, "div:-soup-contains('Unit Rate of Saleable Area') + div"),
        'MoM': safe_extract(item, "div:-soup-contains('MoM') + div"),
        'ForSale': safe_extract(item, "div:-soup-contains('For Sale') + div"),
        'ForRent': safe_extract(item, "div:-soup-contains('For Rent') + div"),
        'Link': link,
        'EstateCode': estate_code,
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

# nodes.py (Kedro compatible version with multi-threading and gap-filling)
def scrape_estate_details(listings_df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """
    Scrape detailed estate information with multi-threading, incremental updates, and gap-filling.
    - Uses multiple threads for parallel scraping
    - Re-scrapes rows with missing critical data
    - Resilient to failures with automatic retry on next run
    """
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
    logger.info(f"Details File: {details_file}")
    
    # Load existing details
    existing_details = pd.DataFrame()
    if os.path.exists(details_file):
        existing_details = pd.read_parquet(details_file)
        logger.info(f"📊 Loaded {len(existing_details)} existing estate details")
        
        # Check for incomplete records (gap-filling mechanism)
        critical_fields = ['scraped_estate_name', 'occupation_permit', 'estate_code']
        if not existing_details.empty:
            incomplete_mask = existing_details[critical_fields].isna().any(axis=1)
            incomplete_count = incomplete_mask.sum()
            if incomplete_count > 0:
                logger.info(f"🔧 Found {incomplete_count} incomplete records - will re-scrape them")
                # Add incomplete records back to scraping queue
                incomplete_names = set(existing_details[incomplete_mask]['Name'].tolist())
    else:
        incomplete_names = set()
    
    # Determine which estates need scraping
    # 1. New estates not in existing data
    # 2. Incomplete estates (missing critical fields)
    existing_complete_names = set()
    if not existing_details.empty and 'Name' in existing_details.columns:
        # Only consider estates complete if they have all critical fields
        critical_fields = ['scraped_estate_name', 'occupation_permit', 'estate_code']
        complete_mask = existing_details[critical_fields].notna().all(axis=1)
        existing_complete_names = set(existing_details[complete_mask]['Name'].tolist())
        logger.info(f"✓ {len(existing_complete_names)} estates are complete")
    
    estates_to_scrape = listings_df[~listings_df['Name'].isin(existing_complete_names)]
    
    if estates_to_scrape.empty:
        logger.info("✅ No estates to scrape - all complete")
        return existing_details
    
    logger.info(f"🎯 Will scrape {len(estates_to_scrape)} estates ({len(estates_to_scrape) - len(incomplete_names if 'incomplete_names' in locals() else [])} new, {len(incomplete_names if 'incomplete_names' in locals() else [])} retry)")
    
    # Multi-threading setup
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    
    # Get thread count from params or use default
    max_threads = params.get('global', {}).get('max_threads', 5)
    logger.info(f"🚀 Using {max_threads} parallel threads for scraping")
    
    # Thread-local storage for drivers
    thread_local = threading.local()
    
    def get_thread_driver():
        """Get or create driver for current thread"""
        if not hasattr(thread_local, 'driver'):
            thread_local.driver = initialize_driver(params)
        return thread_local.driver
    
    def scrape_single_estate(row):
        """Scrape a single estate (thread-safe)"""
        driver = get_thread_driver()
        
        try:
            driver.get(row['Link'])
            random_sleep(params['global']['min_delay'], params['global']['max_delay'])
            
            # Extract estate code from URL
            estate_code = ''
            try:
                estate_code = row['Link'].rstrip('/').split('/')[-1]
            except:
                estate_code = ''
            
            # Extract core information
            detail_data = {
                'Name': row['Name'],
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
                'chinese_name': None,
                'school_net_info': None,
                'estate_detailed_address': None,
                'developer': None,
                'estate_code': estate_code,
                'Link': row['Link'],
                'Region': row['Region'],
                'District': row['District']
            }
            
            # Extract Chinese name
            try:
                chinese_name_elem = driver.find_element(By.CSS_SELECTOR, ".estate-detail-banner-title-cn, .chinese-name, h1.cn, .title-cn")
                detail_data['chinese_name'] = chinese_name_elem.text.strip()
            except:
                try:
                    import re
                    chinese_pattern = re.compile(r'[\u4e00-\u9fff]+')
                    title_section = driver.find_element(By.CSS_SELECTOR, ".estate-detail-banner")
                    chinese_matches = chinese_pattern.findall(title_section.text)
                    if chinese_matches:
                        detail_data['chinese_name'] = ''.join(chinese_matches[:10])
                except:
                    pass
            
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
                    except:
                        continue
            except:
                pass
            
            # Extract Detailed Address
            try:
                address_elem = driver.find_element(By.CLASS_NAME, "estate-detail-banner-position")
                detail_data['estate_detailed_address'] = address_elem.text.strip()
            except:
                pass
            
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
                    except:
                        continue
            except:
                pass
            
            return {'success': True, 'data': detail_data}
            
        except Exception as e:
            logger.debug(f"Failed to scrape {row.get('Name', 'Unknown')}: {str(e)}")
            return {'success': False, 'name': row.get('Name', 'Unknown'), 'link': row.get('Link', ''), 'error': str(e)}
    
    # Execute scraping with thread pool
    new_details = []
    failed_estates = []
    
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        # Submit all tasks
        future_to_estate = {executor.submit(scrape_single_estate, row): row for _, row in estates_to_scrape.iterrows()}
        
        # Process completed tasks with progress bar
        from tqdm.auto import tqdm
        with tqdm(total=len(future_to_estate), desc=f"Scraping estates ({max_threads} threads)") as pbar:
            for future in as_completed(future_to_estate):
                result = future.result()
                if result['success']:
                    new_details.append(result['data'])
                else:
                    failed_estates.append(result)
                pbar.update(1)
    
    # Close all thread-local drivers
    logger.info("🔒 Closing all thread drivers...")
    # Note: ThreadPoolExecutor will clean up threads, drivers will be garbage collected
    
    # Report results
    logger.info(f"✅ Successfully scraped: {len(new_details)} estates")
    if failed_estates:
        logger.warning(f"⚠️  Failed to scrape: {len(failed_estates)} estates")
        logger.info("   These will be retried on next run (gap-filling mechanism)")
    
    # Merge and save results
    try:
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
        
        return existing_details
    
    except Exception as e:
        logger.error(f"Fatal error in scrape_estate_details: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return existing_details if not existing_details.empty else pd.DataFrame()

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

def create_same_name_list(estate_details_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a list of estates with duplicate names based on Name + Region + District + Subdistrict.
    Returns DataFrame with duplicate estates and their codes.
    """
    logger.info("🔍 Creating same_name list for duplicate estates...")
    
    if estate_details_df.empty:
        return pd.DataFrame()
    
    # Group by Name, Region, District, Subdistrict (3 layers of region)
    grouped = estate_details_df.groupby(['Scraped Estate Name', 'Region', 'District', 'Subdistrict']).size()
    
    # Find groups with more than one estate
    duplicates = grouped[grouped > 1]
    
    if len(duplicates) == 0:
        logger.info("✅ No duplicate estate names found")
        return pd.DataFrame()
    
    # Get full details of duplicate estates
    duplicate_keys = duplicates.index.tolist()
    same_name_list = []
    
    for name, region, district, subdistrict in duplicate_keys:
        mask = (
            (estate_details_df['Scraped Estate Name'] == name) &
            (estate_details_df['Region'] == region) &
            (estate_details_df['District'] == district) &
            (estate_details_df['Subdistrict'] == subdistrict)
        )
        duplicate_estates = estate_details_df[mask]
        same_name_list.append(duplicate_estates)
    
    same_name_df = pd.concat(same_name_list, ignore_index=True) if same_name_list else pd.DataFrame()
    
    logger.info(f"📋 Found {len(duplicates)} groups with duplicate names, totaling {len(same_name_df)} estates")
    logger.info(f"   Duplicate names: {duplicates.head(10).to_dict()}")
    
    return same_name_df


def match_transaction_to_duplicate_estates(transaction_link: str, estate_codes: list, driver) -> str:
    """
    Visit transaction detail page and find which estate code appears in the HTML.
    Returns the matching estate code or empty string if none found.
    """
    if not transaction_link or not estate_codes:
        return ''
    
    try:
        driver.get(transaction_link)
        time.sleep(2)  # Wait for page to load
        
        # Get page HTML
        page_html = driver.page_source.lower()
        
        # Check which estate code appears in the HTML
        for code in estate_codes:
            if code.lower() in page_html:
                return code
        
        return ''
    except Exception as e:
        logger.debug(f"Error checking transaction HTML: {str(e)}")
        return ''


def refine_duplicate_estate_matching(
    transactions_df: pd.DataFrame,
    estate_details_df: pd.DataFrame,
    params: Dict[str, Any]
) -> pd.DataFrame:
    """
    OPTIONAL NODE: Refine estate matching for duplicate estate names by checking transaction HTML.
    This node checks the HTML of transaction detail pages to find which estate code appears,
    allowing precise matching when multiple estates share the same name.
    
    Only processes transactions where:
    - Estate name appears in multiple estates (same name, region, district, subdistrict)
    - Transaction needs more precise estate matching
    
    Args:
        transactions_df: Transaction data with estate_name column
        estate_details_df: Estate details with estate_code column
        params: Pipeline parameters
        
    Returns:
        Updated transactions DataFrame with refined estate_code matches
    """
    logger.info("🔍 Starting optional duplicate estate matching refinement...")
    
    # Create same_name list
    same_name_df = create_same_name_list(estate_details_df)
    
    if same_name_df.empty:
        logger.info("✅ No duplicate estates found - skipping refinement")
        return transactions_df
    
    duplicate_estate_names = set(same_name_df['Scraped Estate Name'].unique())
    
    # Find transactions that need refinement
    needs_refinement = transactions_df[
        transactions_df['estate_name'].isin(duplicate_estate_names) &
        transactions_df['estate_code'].isna()  # Only refine if estate_code is not set
    ]
    
    if needs_refinement.empty:
        logger.info("✅ No transactions need refinement")
        return transactions_df
    
    logger.info(f"🔧 Refining {len(needs_refinement)} transactions with duplicate estate names...")
    
    # Initialize driver
    driver = initialize_driver(params)
    transactions_copy = transactions_df.copy()
    
    try:
        # Group duplicate estates by name for faster lookup
        duplicate_estates_grouped = same_name_df.groupby('Scraped Estate Name')
        
        refined_count = 0
        from tqdm.auto import tqdm
        
        for idx, row in tqdm(needs_refinement.iterrows(), total=len(needs_refinement), desc="Refining duplicates"):
            estate_name = row['estate_name']
            
            # Get all possible estate codes for this name
            if estate_name in duplicate_estates_grouped.groups:
                possible_estates = duplicate_estates_grouped.get_group(estate_name)
                estate_codes = possible_estates['estate_code'].dropna().tolist()
                
                if estate_codes:
                    # Check transaction HTML to find matching estate code
                    # First, construct transaction detail link if not available
                    transaction_link = row.get('transaction_detail_link', '')
                    
                    if not transaction_link and 'address' in row:
                        # Transaction links might need to be constructed from transaction data
                        # This depends on the website structure
                        logger.debug(f"No transaction detail link available for {estate_name}")
                        continue
                    
                    matched_code = match_transaction_to_duplicate_estates(
                        transaction_link,
                        estate_codes,
                        driver
                    )
                    
                    if matched_code:
                        transactions_copy.at[idx, 'estate_code'] = matched_code
                        refined_count += 1
                        logger.debug(f"Matched {estate_name} to estate code {matched_code}")
        
        logger.info(f"✅ Refined {refined_count} transactions with precise estate code matching")
        
        return transactions_copy
        
    except Exception as e:
        logger.error(f"Error in duplicate estate refinement: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return transactions_df
    finally:
        driver.quit()


def enrich_estate_data(
    estate_details_df: pd.DataFrame,
    transactions_df: pd.DataFrame,
    params: Dict[str, Any] = None
) -> pd.DataFrame:
    """
    Enhanced estate data enrichment with 3-layer region matching and duplicate estate handling.
    Matches transactions to estates using Name + Region + District + Subdistrict.
    For duplicate estates, checks transaction HTML to find correct match.
    """
    logger.info("🏗️ Starting enhanced estate data enrichment with 3-layer region matching")
    
    # Set default params if not provided
    if params is None:
        params = {}
    
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

    # Process transactions - NEW APPROACH: Use building_code for matching
    logger.info("🔍 Starting two-step estate enrichment (building_code -> name)...")
    
    try:
        transactions_copy = transactions_df.copy()
        
        # Transactions from JavaScript already have:
        # - region, district, subdistrict (100% complete)
        # - building_code (for matching)
        # - estate_name, building_name
        # We just need to enrich with estate details
        
        logger.info(f"📊 Transaction data quality:")
        logger.info(f"  Total transactions: {len(transactions_copy)}")
        if 'region' in transactions_copy.columns:
            logger.info(f"  With region: {transactions_copy['region'].notna().sum()} ({transactions_copy['region'].notna().sum()/len(transactions_copy)*100:.1f}%)")
        if 'building_code' in transactions_copy.columns:
            logger.info(f"  With building_code: {transactions_copy['building_code'].notna().sum()} ({transactions_copy['building_code'].notna().sum()/len(transactions_copy)*100:.1f}%)")
        
        # Initialize enrichment columns
        transactions_copy['matched_estate_name'] = ''
        transactions_copy['estate_region'] = ''
        transactions_copy['estate_district'] = ''
        transactions_copy['estate_subdistrict'] = ''
        transactions_copy['estate_blocks'] = ''
        transactions_copy['estate_units'] = ''
        transactions_copy['match_method'] = 'no_match'
        
        # Add processing metadata (if not already present)
        if 'processing_timestamp' not in transactions_copy.columns:
            transactions_copy['processing_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if 'source' not in transactions_copy.columns:
            transactions_copy['source'] = 'centaline_res'
        if 'property_type' not in transactions_copy.columns:
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

                # Deduplicate estate details before joining to prevent duplicates
                estate_completion_unique = estate_details_copy.drop_duplicates(subset='Scraped Estate Name', keep='first')

                # Join with estate details to get completion year
                transactions_copy = transactions_copy.merge(
                    estate_completion_unique[['Scraped Estate Name', 'completion_year']],
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
                
                # TWO-STEP MAPPING: building_code -> name
                try:
                    logger.info(f"🔍 Enriching {len(transactions_copy)} transactions with estate details...")
                    logger.info(f"   Using two-step matching: building_code (primary) -> name (fallback)")
                    
                    # Prepare estate details and create lookup maps
                    # Extract estate codes from estate listings (level 1)
                    estate_listings = pd.DataFrame()
                    try:
                        estate_file = 'data/01_raw/centaline_estate_lv_1.parquet'
                        if os.path.exists(estate_file):
                            estate_listings = pd.read_parquet(estate_file)
                            if 'EstateCode' not in estate_listings.columns and 'Link' in estate_listings.columns:
                                estate_listings['EstateCode'] = estate_listings['Link'].apply(
                                    lambda x: x.rstrip('/').split('/')[-1] if pd.notna(x) else ''
                                )
                            logger.info(f"   Loaded {len(estate_listings)} estates for matching")
                    except Exception as e:
                        logger.warning(f"Could not load estate listings: {e}")
                    
                    # Create building code -> estate mapping
                    building_code_map = {}
                    if not estate_listings.empty:
                        for _, estate in estate_listings.iterrows():
                            code = estate.get('EstateCode', '')
                            if code:
                                building_code_map[code] = estate.to_dict()
                    
                    # Create name -> estate mapping (fallback)
                    name_map = {}
                    if not estate_listings.empty:
                        for _, estate in estate_listings.iterrows():
                            name = estate.get('Name', '')
                            if name:
                                if name not in name_map:
                                    name_map[name] = []
                                name_map[name].append(estate.to_dict())
                    
                    logger.info(f"   Building code map: {len(building_code_map)} codes")
                    logger.info(f"   Name map: {len(name_map)} names")
                    
                    # Process each transaction with two-step matching
                    logger.info("   Starting two-step enrichment...")
                    
                    matched_by_code = 0
                    matched_by_name = 0
                    no_match = 0
                    
                    from tqdm.auto import tqdm
                    
                    for idx, row in tqdm(transactions_copy.iterrows(), total=len(transactions_copy), desc="Enriching"):
                        building_code = row.get('building_code', '')
                        txn_name = row.get('Name', '')
                        
                        estate_info = None
                        
                        # Step 1: Match by building_code (PRIMARY)
                        if building_code and building_code in building_code_map:
                            estate_info = building_code_map[building_code]
                            matched_by_code += 1
                            transactions_copy.at[idx, 'match_method'] = 'building_code'
                        
                        # Step 2: Fallback to name matching
                        elif txn_name and txn_name in name_map:
                            estate_info = name_map[txn_name][0]
                            matched_by_name += 1
                            transactions_copy.at[idx, 'match_method'] = 'name'
                        else:
                            no_match += 1
                        
                        # Enrich with estate details if matched
                        if estate_info:
                            transactions_copy.at[idx, 'matched_estate_name'] = estate_info.get('Name', '')
                            transactions_copy.at[idx, 'estate_region'] = estate_info.get('Region', '')
                            transactions_copy.at[idx, 'estate_district'] = estate_info.get('District', '')
                            transactions_copy.at[idx, 'estate_subdistrict'] = estate_info.get('Subdistrict', '')
                            transactions_copy.at[idx, 'estate_blocks'] = str(estate_info.get('Blocks', ''))
                            transactions_copy.at[idx, 'estate_units'] = str(estate_info.get('Units', ''))
                            
                            # Add additional building info from estate details
                            transactions_copy.at[idx, 'estate_full_address'] = estate_info.get('estate_detailed_address', '')
                            transactions_copy.at[idx, 'developer'] = estate_info.get('developer', '')
                            transactions_copy.at[idx, 'estate_chinese_name'] = estate_info.get('chinese_name', '')
                            
                            # Add additional building info from estate details
                            if 'estate_detailed_address' in estate_info:
                                transactions_copy.at[idx, 'estate_full_address'] = estate_info.get('estate_detailed_address', '')
                            if 'developer' in estate_info:
                                transactions_copy.at[idx, 'developer'] = estate_info.get('developer', '')
                            if 'chinese_name' in estate_info:
                                transactions_copy.at[idx, 'estate_chinese_name'] = estate_info.get('chinese_name', '')
                    
                    logger.info(f"\n   📊 Enrichment Statistics:")
                    logger.info(f"      Matched via building code: {matched_by_code:,} ({matched_by_code/len(transactions_copy)*100:.1f}%)")
                    logger.info(f"      Matched via name: {matched_by_name:,} ({matched_by_name/len(transactions_copy)*100:.1f}%)")
                    logger.info(f"      No match (already complete): {no_match:,} ({no_match/len(transactions_copy)*100:.1f}%)")
                    logger.info("✅ Estate enrichment complete using building_code and name matching")
                    
                except Exception as e:
                    logger.warning(f"⚠️ Could not add location and developer data: {e}")
                    import traceback
                    logger.warning(traceback.format_exc())
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

    # Combine with existing data but remove duplicates
    # This allows incremental updates without duplication
    try:
        if not existing_enriched.empty:
            logger.info(f"📊 Found {len(existing_enriched)} existing records")
            
            # Combine existing and new data
            combined = pd.concat([existing_enriched, transactions_copy], ignore_index=True, sort=False)
            logger.info(f"📊 Combined to {len(combined)} total records (before dedup)")
            
            # Remove duplicates based on transaction_id (keep most recent = last occurrence)
            if 'transaction_id' in combined.columns:
                before_dedup = len(combined)
                final_df = combined.drop_duplicates(subset=['transaction_id'], keep='last')
                after_dedup = len(final_df)
                
                if before_dedup != after_dedup:
                    logger.info(f"🗑️  Removed {before_dedup - after_dedup:,} duplicate transactions")
                logger.info(f"✅ Final clean dataset: {len(final_df):,} unique transactions")
            else:
                logger.warning("⚠️  No transaction_id column - cannot deduplicate")
                final_df = combined
        else:
            final_df = transactions_copy
            logger.info(f"📊 Using new transaction data: {len(final_df)} records (no existing data)")
            
    except Exception as e:
        logger.error(f"⚠️ Error in combining data: {str(e)}")
        final_df = transactions_copy
        logger.info(f"📊 Falling back to new data only: {len(final_df)} records")

    # Fix all data types for parquet compatibility
    try:
        logger.info("\n🔧 Fixing data types for parquet compatibility...")
        
        # Integer columns (nullable)
        int_cols = ['rooms', 'completion_year', 'age']
        for col in int_cols:
            if col in final_df.columns:
                final_df[col] = pd.to_numeric(final_df[col], errors='coerce').astype('Int64')
        
        # Float columns
        float_cols = ['area', 'price', 'ft_price', 'g_area', 'g_unit_price']
        for col in float_cols:
            if col in final_df.columns:
                final_df[col] = pd.to_numeric(final_df[col], errors='coerce').astype('float64')
        
        # String columns - ensure they're strings
        str_cols = ['date', 'region', 'district', 'subdistrict', 'Name', 'Tower', 'Floor', 'Flat',
                    'transaction_type', 'source', 'property_type', 'street_address', 'building_code',
                    'estate_type', 'transaction_url', 'transaction_id', 'title_lg', 'direction',
                    'estate_name', 'building_name', 'matched_estate_name', 'estate_region',
                    'estate_district', 'estate_subdistrict', 'estate_blocks', 'estate_units', 'match_method']
        for col in str_cols:
            if col in final_df.columns:
                final_df[col] = final_df[col].astype(str).replace('nan', '').replace('<NA>', '')
        
        logger.info(f"✅ Data types fixed")
        
    except Exception as e:
        logger.error(f"⚠️ Error fixing data types: {e}")
    
    # Set final column order as requested
    try:
        logger.info("\n📋 Setting final column order...")
        
        # Exact column order as requested
        requested_columns = [
            'date', 'region', 'district', 'subdistrict', 'Name', 'Tower', 'Floor', 'Flat',
            'transaction_type', 'area', 'price', 'ft_price', 'source', 'property_type',
            'street_address', 'building_code', 'g_area', 'g_unit_price', 'completion_year',
            'age', 'estate_type', 'transaction_url', 'transaction_id', 'title_lg',
            'matched_estate_name', 'estate_region', 'estate_district', 'estate_subdistrict',
            'estate_blocks', 'estate_units', 'match_method'
        ]
        
        # Add any remaining columns
        remaining = [col for col in final_df.columns if col not in requested_columns]
        all_columns = requested_columns + remaining
        
        # Reorder (only existing columns)
        existing_columns = [col for col in all_columns if col in final_df.columns]
        final_df = final_df[existing_columns]
        
        logger.info(f"✅ Column order set: {len(existing_columns)} columns")
        
    except Exception as e:
        logger.error(f"⚠️ Error setting column order: {e}")
    
    # Generate statistics
    try:
        logger.info("\n📈 Enrichment Statistics:")
        
        if 'match_method' in final_df.columns:
            match_counts = final_df['match_method'].value_counts()
            logger.info(f"   Matching results:")
            for method, count in match_counts.items():
                logger.info(f"     {method}: {count:,} ({count/len(final_df)*100:.1f}%)")
        
        if 'transaction_type' in final_df.columns:
            type_counts = final_df['transaction_type'].value_counts()
            logger.info(f"   Transaction types:")
            for ttype, count in type_counts.items():
                logger.info(f"     {ttype}: {count:,} ({count/len(final_df)*100:.1f}%)")
        
        logger.info(f"   Total records: {len(final_df):,}")
            
    except Exception as e:
        logger.error(f"⚠️ Error generating statistics: {e}")

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
    
    # ============ LOAD AND MERGE OLD DATA ============
    logger.info("📚 Loading and merging old Centaline Residential data...")
    try:
        old_data_file = "./centaline_res.parquet"
        if os.path.exists(old_data_file):
            old_data = pd.read_parquet(old_data_file)
            logger.info(f"📖 Loaded {len(old_data)} records from old data file")
            
            # Convert date format to match current data (dd/mm/yyyy)
            if 'date' in old_data.columns:
                try:
                    # Convert from datetime to dd/mm/yyyy format
                    old_data['date'] = pd.to_datetime(old_data['date'], errors='coerce').dt.strftime('%d/%m/%Y')
                    logger.info("✅ Converted old data date format to dd/mm/yyyy")
                except Exception as e:
                    logger.warning(f"⚠️ Error converting old data date format: {e}")
            
            # Standardize column names to match current format
            column_mapping = {
                'Name': 'estate_name',
                'Address': 'address',
                'Blocks': 'building_name',
                'Units': 'flat',
                'Unit Rate': 'ft_price',
                'Trans Record': 'transaction_type',
                'Occupation Permit': 'occupation_permit',
                'School Net Info': 'school_net_info',
                'Developer': 'developer'
            }
            
            # Rename columns that exist in old data
            for old_col, new_col in column_mapping.items():
                if old_col in old_data.columns:
                    old_data = old_data.rename(columns={old_col: new_col})
            
            # Add missing columns to match current format
            missing_columns = ['title_lg', 'property_name', 'Tower', 'Floor', 'Flat', 'Type', 'Carpark_Floor', 'Carpark_Number']
            for col in missing_columns:
                if col not in old_data.columns:
                    old_data[col] = None
            
            # Filter old data to only include records older than 3 years from current date
            current_date = pd.Timestamp.now()
            cutoff_date = current_date - pd.DateOffset(years=3)
            
            # Convert old data dates to datetime for filtering
            old_data_dates = pd.to_datetime(old_data['date'], format='%d/%m/%Y', errors='coerce')
            old_data_filtered = old_data[old_data_dates < cutoff_date].copy()
            
            logger.info(f"📅 Filtered to {len(old_data_filtered)} records older than 3 years (before {cutoff_date.strftime('%d/%m/%Y')})")
            
            # Merge old data with current data
            if not old_data_filtered.empty:
                # Ensure both datasets have the same columns
                common_columns = list(set(final_df.columns) & set(old_data_filtered.columns))
                final_df_common = final_df[common_columns]
                old_data_common = old_data_filtered[common_columns]
                
                # Reset indices to avoid conflicts and ensure unique indices
                final_df_common = final_df_common.reset_index(drop=True)
                old_data_common = old_data_common.reset_index(drop=True)
                
                # Ensure unique indices by adding offset to old data
                old_data_common.index = old_data_common.index + len(final_df_common)
                
                # Concatenate old and new data
                final_df = pd.concat([old_data_common, final_df_common], ignore_index=True, sort=False)
                logger.info(f"📊 Merged {len(old_data_common)} old + {len(final_df_common)} new = {len(final_df)} total records")
            else:
                logger.info("📊 No old data to merge (all data is within 3 years)")
                
        else:
            logger.info("📚 No old data file found at ./centaline_res.parquet")
            
    except Exception as e:
        logger.error(f"⚠️ Error loading and merging old data: {e}")
    
    logger.info("✅ Simplified estate data enrichment completed successfully!")
    logger.info("📝 Note: Complex building matching has been moved to the centralized buildings pipeline")
    
    return final_df



