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
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, NoSuchElementException
import os
import json
from pathlib import Path
from typing import Optional, Set
import pickle
import hashlib
import yaml
from bs4 import BeautifulSoup

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
                        #logger.info(f"Reached control date {control_date} in {area_row['Subdistrict']}")
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
    """Robust estate scraper with district change tracking and zero-count handling"""
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
        listings_file = params.get('estate_listings_file', 'data/02_intermediate/centaline_estate_lv_1.parquet')
        if os.path.exists(listings_file):
            try:
                existing_listings = pd.read_parquet(listings_file)
                logger.info(f"Loaded {len(existing_listings)} existing listings")
                
                # Clean and standardize data
                existing_listings['Subdistrict'] = existing_listings['Subdistrict'].str.strip()
                existing_listings['Code'] = existing_listings['Code'].astype(str).str.strip()
                
                # Create district count map
                district_counts = existing_listings.groupby(['Subdistrict', 'Code']).size().to_dict()
                logger.info(f"Loaded counts for {len(district_counts)} districts")

            except Exception as e:
                logger.error(f"Data loading failed: {str(e)}")
                existing_listings = pd.DataFrame(columns=required_columns)

    except Exception as e:
        logger.error(f"Initialization error: {str(e)}")

    all_scraped_estates = []
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
                    
                    # Navigate and get count
                    driver.get(url)
                    random_sleep(params['min_delay'], params['max_delay'])
                    
                    # Robust count detection
                    current_count = 0
                    try:
                        count_element = WebDriverWait(driver, 15).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "h2.center span.text-red"))
                        )
                        count_text = count_element.text.strip().replace(',', '')
                        current_count = int(count_text) if count_text.isdigit() else 0
                    except Exception as e:
                        logger.warning(f"Count check failed for {subdistrict}: {str(e)}")
                    
                    previous_count = district_counts.get(district_key, 0)
                    status = "UNCHANGED"

                    # Handle zero-count districts
                    if current_count == 0 and previous_count == 0:
                        zero_count_districts.append(district_key)
                        status = "ZERO"
                        logger.debug(f"Skipping zero-count district: {subdistrict} ({code})")
                        continue
                    
                    # Skip unchanged districts
                    if current_count == previous_count:
                        status = "SKIPPED"
                        #logger.info(f"Skipping unchanged district: {subdistrict} ({code}) [{current_count}]")
                        continue

                    # Track changed districts
                    if current_count != previous_count:
                        status = "MODIFIED"
                        district_changes.append({
                            'district': subdistrict,
                            'code': code,
                            'previous': previous_count,
                            'current': current_count,
                            'timestamp': current_time
                        })
                        logger.info(f"Processing changed district: {subdistrict} ({code}) {previous_count}→{current_count}")

                    # Full scraping process
                    current_page = 1
                    scraped_estate_names = []
                    
                    with tqdm(desc="Scraping pages", leave=False) as page_bar:
                        while True:
                            try:
                                WebDriverWait(driver, 20).until(
                                    EC.presence_of_element_located((By.CSS_SELECTOR, "a.property-text.flex.def-property-box"))
                                )
                                soup = BeautifulSoup(driver.page_source, 'html.parser')
                                estate_items = soup.select("a.property-text.flex.def-property-box")
                                
                                with tqdm(estate_items, desc="Processing estates", leave=False) as estate_iter:
                                    for item in estate_iter:
                                        try:
                                            estate_data = process_estate_item(item, row)
                                            scraped_estate_names.append(estate_data['Name'])
                                            
                                            existing_entry = existing_listings.get(estate_data['Name'], pd.Series(dtype=object))
                                            if existing_entry.empty or estate_changed(existing_entry, estate_data):
                                                new_or_updated_estates.append(estate_data)
                                        except Exception as e:
                                            logger.error(f"Error processing estate: {str(e)}")
                                
                                page_bar.update(1)
                                
                                # Pagination handling
                                try:
                                    next_btn = driver.find_element(By.CSS_SELECTOR, "button.btn-next:not([disabled])")
                                    driver.execute_script("arguments[0].click();", next_btn)
                                    random_sleep(params['min_delay'], params['max_delay'])
                                    current_page += 1
                                except NoSuchElementException:
                                    break
                                    
                            except TimeoutException:
                                logger.warning(f"Timeout in {subdistrict} page {current_page}")
                                break
                            except Exception as e:
                                logger.error(f"Critical error: {str(e)}")
                                break

                    all_scraped_estates.extend(scraped_estate_names)

                except Exception as e:
                    logger.error(f"District processing failed: {subdistrict} - {str(e)}")
                    continue

        # Generate final report
        logger.info("\n=== District Change Report ===")
        logger.info(f"Total districts processed: {len(area_df)}")
        logger.info(f"Changed districts: {len(district_changes)}")
        logger.info(f"Unchanged districts: {len(area_df) - len(district_changes) - len(zero_count_districts)}")
        logger.info(f"Persistent zero-count districts: {len(zero_count_districts)}")
        
        if district_changes:
            logger.info("\nModified Districts (Previous → Current):")
            for change in district_changes:
                logger.info(f"  - {change['district']} ({change['code']}): {change['previous']} → {change['current']}")
        
        if zero_count_districts:
            logger.info("\nZero-count Districts (Unchanged):")
            for district in zero_count_districts[:5]:  # Show first 5 as sample
                logger.info(f"  - {district[0]} ({district[1]})")

        # Data consolidation
        final_df = pd.concat([
            existing_listings[existing_listings['Name'].isin(all_scraped_estates)],
            pd.DataFrame(new_or_updated_estates)
        ], ignore_index=True).reindex(columns=required_columns)

        final_df = final_df[final_df['Name'].isin(all_scraped_estates)]
        final_df.to_parquet(listings_file, index=False)
        
        logger.info(f"\nFinal counts: {len(final_df)} total estates ({len(new_or_updated_estates)} new/updated)")
        
        return final_df[final_df['Name'].notnull()]

    finally:
        driver.quit()

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
    details_file = params.get('estate_details_file', 'data/02_intermediate/centaline_estate_lv_2.parquet')
    print("Details File:", details_file)
    
    # Load existing details
    existing_details = pd.DataFrame()
    if os.path.exists(details_file):
        print("inside!!inside!!inside!!inside!!")
        existing_details = pd.read_parquet(details_file)
        print('Checkpoint:', existing_details['Name'])
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
                random_sleep(params['min_delay'], params['max_delay'])
                
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
            return updated_df[updated_df['Name'].notnull()]
        
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



