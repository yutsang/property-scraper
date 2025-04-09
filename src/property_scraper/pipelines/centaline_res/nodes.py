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
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
import os
import json
from pathlib import Path
from typing import Optional, Set
import pickle
import hashlib

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
    options.add_experimental_option("excludeSwitches", ["enable-automation"])  
    options.add_argument("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.6943.127 Safari/537.36")  
    
    driver = webdriver.Edge(options=options)
    
     # Evasion scripts
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            })
        """
    })

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
    time.sleep(2)


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

    driver = initialize_driver(params)
    existing_links = load_existing_links(params)
    new_data = []
    
    try:
        if params.get('area_limit'):
            area_df = area_df.head(params['area_limit'])
            
        for _, row in tqdm(area_df[:10].iterrows(), total=len(area_df)):
            subdistrict_part = clean_subdistrict(row['Subdistrict'])
            session_id = generate_session_id()
            url = f"https://hk.centanet.com/findproperty/en/list/estate/{subdistrict_part}_19-{row['Code']}?q={session_id}"
            
            driver.get(url)
            random_sleep(params['min_delay'], params['max_delay'])
            
            current_page = 1
            while True:
                estate_items = WebDriverWait(driver, 20).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.property-text.flex.def-property-box"))
                )
                
                for item in estate_items:
                    try:
                        link = item.get_attribute("href")
                        if link in existing_links:
                            continue
                            
                        # Original data extraction logic
                        entry = [
                            item.find_element(By.CSS_SELECTOR, "div.main-text").text.strip(),
                            item.find_element(By.CSS_SELECTOR, "div.address.f-middle").text.strip(),
                            item.find_element(By.XPATH, ".//div[contains(text(), 'No. of Block(s)')]/following-sibling::div").text.strip(),
                            item.find_element(By.XPATH, ".//div[contains(text(), 'No. of Units')]/following-sibling::div").text.strip(),
                            item.find_element(By.XPATH, ".//div[contains(text(), 'Unit Rate of Saleable Area')]/following-sibling::div").text.strip(),
                            link,
                            row['Region'],
                            row['District']
                        ]
                        new_data.append(entry)
                        existing_links.add(link)
                        
                    except Exception as e:
                        logger.debug(f"Skipping item: {str(e)}")
                        
                try:
                    next_btn = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn-next:not([disabled])"))
                    )
                    driver.execute_script("arguments[0].click();", next_btn)
                    random_sleep(params['min_delay'], params['max_delay'])
                    current_page += 1
                except Exception:
                    break
                    
    finally:
        driver.quit()
        save_existing_links(existing_links, params)
        save_checkpoint(new_data, params, final=True)
        
    return pd.DataFrame(new_data, columns=COLUMN_NAMES)

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
    """Kedro-compatible estate detail scraper maintaining original logic"""
    logger = logging.getLogger(__name__)
    driver = initialize_driver(params)
    enriched_data = []

    # Ensure temp_file parameter exists
    temp_file = params.get('temp_file', 'data/02_intermediate/estate_details_temp.csv')

    try:
        # Create copy of input DataFrame to preserve original data
        result_df = listings_df.copy()
        
        # Add new columns if they don't exist
        new_columns = [
            'scraped_estate_name', 'occupation_permit', 'scraped_blocks',
            'scraped_units', 'school_net_info', 'estate_detailed_address', 'developer'
        ]
        
        for col in new_columns:
            if col not in result_df.columns:
                result_df[col] = None

        # Initialize progress bar
        pbar = tqdm(total=len(result_df), desc="Processing estate details")
        
        for idx, row in result_df[:10].iterrows():
            url = row['Link']
            try:
                driver.get(url)
                random_sleep(params['min_delay'], params['max_delay'])
                
                # Scroll to load dynamic content
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)

                # --- Original scraping logic preserved ---
                # Estate Name
                try:
                    name_elem = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "estate-detail-banner-title"))
                    )
                    result_df.at[idx, 'scraped_estate_name'] = name_elem.text.strip()
                except Exception as e:
                    logger.debug(f"Name not found: {url}")

                # Occupation Permit
                try:
                    permit_elem = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, 
                            "//div[contains(text(), 'Date of Occupation Permit')]/following-sibling::div"))
                    )
                    result_df.at[idx, 'occupation_permit'] = permit_elem.text.strip()
                except Exception:
                    pass

                # Blocks and Units
                try:
                    table_items = driver.find_elements(By.CLASS_NAME, "table-item")
                    for item in table_items:
                        try:
                            title = item.find_element(By.CLASS_NAME, "table-item-title").text
                            value = item.find_element(By.CLASS_NAME, "table-item-text").text
                            
                            if "No. of Block(s)" in title:
                                result_df.at[idx, 'scraped_blocks'] = int(value.split()[0])
                            elif "No. of Units" in title:
                                result_df.at[idx, 'scraped_units'] = int(value)
                        except Exception:
                            continue
                except Exception:
                    pass

                # School Net Information
                try:
                    net_div = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH,
                            "//div[contains(.//text(), 'School Net')]"))
                    )
                    primary = net_div.find_element(By.XPATH, ".//a[1]").text
                    secondary = net_div.find_element(By.XPATH, ".//a[2]").text
                    result_df.at[idx, 'school_net_info'] = f"{primary} | {secondary}"
                except Exception:
                    pass

                # Detailed Address
                try:
                    address_elem = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "estate-detail-banner-position"))
                    )
                    result_df.at[idx, 'estate_detailed_address'] = address_elem.text.strip()
                except Exception:
                    pass

                # Developer
                try:
                    dev_div = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH,
                            "//div[contains(.//text(), 'Developer')]"))
                    )
                    result_df.at[idx, 'developer'] = dev_div.find_element(
                        By.CLASS_NAME, "label-item-right").text.strip()
                except Exception:
                    pass

                # Update progress
                pbar.update(1)
                pbar.set_postfix_str(f"Processed: {result_df.at[idx, 'scraped_estate_name']}")

                # Intermediate save every 10 records
                if idx % 10 == 0:
                    result_df.to_csv(params['temp_file'], index=False)

            except Exception as e:
                logger.error(f"Failed to process {url}: {str(e)}")
                continue

        pbar.close()

        # Update the Date 
        from datetime import datetime
        import yaml, os

        # Get today's date
        today_date = datetime.now().strftime("%Y-%m-%d")

        #Load the parameters from the config file
        params_path = "conf/base/parameters.yml"
        with open(params_path, 'r') as file:
            parameters = yaml.safe_load(file)

        # Uodate the date in the parameters
        parameters['Control_date']['centaline_estates'] = today_date

        # Save updated parameters
        with open(params_path, 'w') as file:
            yaml.dump(parameters, file, default_flow_style=False)
            
        logger.info(f"Successfully updated centaline_estates control date to {today_date}")
    
        return result_df
    
    except Exception as e:
        logger.error(f"Failed to update control date: {str(e)}")
        raise

    finally:
        driver.quit()


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



