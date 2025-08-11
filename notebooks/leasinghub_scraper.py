import pandas as pd
import time
import json
import re
import random
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from tqdm import tqdm

def setup_undetected_driver():
    """Setup undetected ChromeDriver to bypass Cloudflare."""
    options = uc.ChromeOptions()
    
    # Basic options for better performance
    options.add_argument('--no-first-run')
    options.add_argument('--no-default-browser-check')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-software-rasterizer')
    
    # Non-headless mode for better Cloudflare compatibility
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-blink-features=AutomationControlled')
    
    # Create undetected driver with correct Chrome version
    driver = uc.Chrome(options=options, version_main=137)
    
    return driver

def wait_for_cloudflare_check(driver, max_wait=5):
    """Wait for Cloudflare security check to complete."""
    start_time = time.time()
    cloudflare_detected = False
    
    while time.time() - start_time < max_wait:
        try:
            page_source = driver.page_source.lower()
            current_url = driver.current_url.lower()
            
            # Check for Cloudflare indicators
            if any(indicator in page_source for indicator in [
                'checking your browser',
                'cloudflare',
                'security check',
                'ray id',
                'please wait',
                'just a moment',
                'challenge'
            ]) or 'cloudflare' in current_url:
                cloudflare_detected = True
                time.sleep(0.5)
                continue
            
            # If we had Cloudflare but now don't, wait a bit more to be sure
            if cloudflare_detected:
                time.sleep(1)
                cloudflare_detected = False
                continue
            
            # Check if we have building content and correct URL
            if (('building' in page_source or 'office' in page_source or 'leasinghub' in page_source) 
                and 'leasinghub.com' in current_url):
                return True
            
            time.sleep(0.2)
        except Exception as e:
            # If we can't check, assume it's working
            return True
    
    return False

def is_valid_building_link(url, text):
    """Check if a link is a valid building (not for lease/sale actions)."""
    if not url or not text:
        return False
    
    # Exclude action links
    exclude_patterns = [
        '/office/rent/',
        '/office/sale/', 
        '/lease/',
        '/sale/',
        'for-lease',
        'for-sale'
    ]
    
    if any(pattern in url.lower() for pattern in exclude_patterns):
        return False
    
    # Exclude action text
    exclude_text = [
        'for lease',
        'for sale',
        'lease',
        'sale',
        'rent',
        'buy'
    ]
    
    text_lower = text.lower().strip()
    if any(exclude in text_lower for exclude in exclude_text):
        return False
    
    # Must be a building URL pattern
    if '/building/' not in url:
        return False
    
    # Text should not be just numbers or too short
    if len(text.strip()) < 3 or text.strip().isdigit():
        return False
    
    return True

def extract_buildings_from_html(html):
    """Extract building data from HTML with improved filtering."""
    soup = BeautifulSoup(html, 'html.parser')
    buildings = []
    
    # Strategy 1: Look for building links directly with validation
    building_links = soup.select('a[href*="/building/"]')
    
    for link in building_links:
        url = link.get('href')
        text = link.get_text(strip=True)
        
        if is_valid_building_link(url, text):
            building_data = {
                'name': text,
                'url': url,
                'source': 'direct_link'
            }
            buildings.append(building_data)
    
    # Strategy 2: Look for building containers/cards
    if len(buildings) < 10:  # If we didn't find many valid buildings
        selectors_to_try = [
            '.building-item, .property-item',
            '.building-card, .property-card', 
            '.listing-item, .search-result',
            '[data-building], [data-property]',
            '.card:has(a[href*="building"])',
            'div:has(h3):has(a[href*="building"])'
        ]
        
        for selector in selectors_to_try:
            try:
                cards = soup.select(selector)
                if cards:
                    for card in cards:
                        # Look for the main building link in the card
                        building_link = card.select_one('a[href*="/building/"]:not([href*="/office/"])')
                        if building_link:
                            url = building_link.get('href')
                            
                            # Try to get building name from various elements
                            name_elem = (card.select_one('h1, h2, h3, .title, .name, .building-name') or 
                                       card.select_one('.card-title, .property-title') or
                                       building_link)
                            
                            if name_elem:
                                text = name_elem.get_text(strip=True)
                                if is_valid_building_link(url, text):
                                    building_data = {
                                        'name': text,
                                        'url': url,
                                        'source': f'card_{selector}'
                                    }
                                    buildings.append(building_data)
                    if buildings:  # If we found buildings with this selector, stop trying others
                        break
            except Exception as e:
                continue
    
    # Remove duplicates based on URL
    seen_urls = set()
    unique_buildings = []
    for building in buildings:
        if building['url'] not in seen_urls:
            seen_urls.add(building['url'])
            unique_buildings.append(building)
    
    # Filter out any remaining invalid entries
    valid_buildings = [b for b in unique_buildings if is_valid_building_link(b['url'], b['name'])]
    
    return valid_buildings

def get_total_count_from_page(html):
    """Extract total building count from the page with better patterns."""
    soup = BeautifulSoup(html, 'html.parser')
    text = soup.get_text()
    
    # More comprehensive patterns for total counts
    patterns = [
        r'showing\s+\d+\s*-\s*\d+\s+of\s+(\d{1,5})',  # "showing 1-20 of 1679"
        r'(\d{1,5})\s*(?:buildings?|results?|properties?|listings?)\s*(?:found|total)',
        r'(?:total|found):\s*(\d{1,5})',
        r'(\d{1,5})\s*(?:total|found|results?)',
        r'of\s+(\d{1,5})\s+(?:buildings?|results?|properties?)',
        r'results?\s*\(\s*(\d{1,5})\s*\)',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            try:
                count = int(match.replace(',', ''))
                if 100 <= count <= 50000:  # Reasonable range for buildings
                    return count
            except ValueError:
                continue
    
    return None

def scrape_page_with_cloudflare_bypass(driver, page_num=1, pbar=None):
    """Scrape a single page with Cloudflare bypass."""
    try:
        # More URL patterns to try
        urls_to_try = [
            f"https://www.leasinghub.com/office/buildings?limitstart={(page_num-1)*20}",
            f"https://www.leasinghub.com/office/buildings?start={(page_num-1)*20}",
            f"https://www.leasinghub.com/office/buildings?page={page_num}",
            f"https://www.leasinghub.com/office/buildings?offset={(page_num-1)*20}",
        ]
        
        for attempt, url in enumerate(urls_to_try):
            if pbar:
                pbar.set_postfix_str(f"Page {page_num}: Trying URL {attempt + 1}/4")
            
            driver.get(url)
            
            # Wait for Cloudflare check
            if pbar:
                pbar.set_postfix_str(f"Page {page_num}: Checking Cloudflare...")
            
            if not wait_for_cloudflare_check(driver):
                continue
            
            if pbar:
                pbar.set_postfix_str(f"Page {page_num}: Loading content...")
            
            # Minimal wait for content to load
            time.sleep(random.uniform(1, 2))
            
            # Quick scroll to load any lazy content (with error handling)
            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(0.5)
            except Exception as scroll_error:
                # Continue without scrolling
                pass
            
            if pbar:
                pbar.set_postfix_str(f"Page {page_num}: Extracting buildings...")
            
            # Get page HTML
            html = driver.page_source
            
            # Extract buildings
            buildings = extract_buildings_from_html(html)
            
            # Get total count on first page
            total_count = None
            if page_num == 1:
                total_count = get_total_count_from_page(html)
            
            if buildings:
                if pbar:
                    pbar.set_postfix_str(f"Page {page_num}: Found {len(buildings)} buildings")
                return buildings, total_count
            else:
                if pbar:
                    pbar.set_postfix_str(f"Page {page_num}: No buildings found, trying next URL")
                
        if pbar:
            pbar.set_postfix_str(f"Page {page_num}: Failed all URLs")
        return [], None
        
    except Exception as e:
        if pbar:
            pbar.set_postfix_str(f"Page {page_num}: Error - {str(e)[:50]}")
        return [], None

def scrape_all_buildings_cloudflare():
    """Main function to scrape all buildings with Cloudflare bypass."""
    print("🏢 LeasingHub Enhanced Building Scraper")
    print("🚀 Starting enhanced Cloudflare-bypassing scraper...")
    
    driver = None
    all_buildings = []
    total_count = None
    
    try:
        print("🔧 Initializing headless browser...")
        driver = setup_undetected_driver()
        print("✅ Browser initialized successfully!")
        
        page = 1
        consecutive_empty = 0
        max_consecutive_empty = 5  # Allow more empty pages before stopping
        max_pages = 200  # Increased page limit
        
        # Initialize progress bar with max pages, will update after first page
        pbar = tqdm(total=max_pages, desc="Initializing...", unit="page")
        
        while page <= max_pages:
            buildings, page_total = scrape_page_with_cloudflare_bypass(driver, page, pbar)
            
            # Store total from first page and update progress bar
            if page == 1 and page_total:
                total_count = page_total
                estimated_pages = (total_count + 19) // 20  # 20 buildings per page
                actual_pages = min(estimated_pages, max_pages)
                
                # Update progress bar description and total
                pbar.total = actual_pages
                pbar.set_description(f"Scraping {total_count} buildings ({actual_pages} pages)")
                pbar.refresh()  # Refresh to show updated info
            
            if not buildings:
                consecutive_empty += 1
                pbar.set_postfix_str(f"Page {page}: Empty (#{consecutive_empty})")
                
                if consecutive_empty >= max_consecutive_empty:
                    pbar.set_postfix_str(f"Stopped: {consecutive_empty} consecutive empty pages")
                    break
            else:
                consecutive_empty = 0
                all_buildings.extend(buildings)
                
                completion = f"{len(all_buildings)}"
                if total_count:
                    completion += f"/{total_count} ({len(all_buildings)/total_count*100:.1f}%)"
                
                pbar.set_postfix_str(f"Page {page}: {len(buildings)} buildings | Total: {completion}")
                
                # Check if we've reached the expected total (with some buffer)
                if total_count and len(all_buildings) >= total_count * 0.95:
                    # Continue for a few more pages to ensure completeness
                    if len(all_buildings) >= total_count * 1.05:
                        pbar.set_postfix_str(f"Completed: Exceeded expected total")
                        break
            
            # Update progress for all pages
            pbar.update(1)
            page += 1
            
            # Minimal delay between pages (just enough to avoid blocking)
            delay = random.uniform(1, 3)
            time.sleep(delay)
        
        pbar.close()
                
    except Exception as e:
        print(f"❌ Critical error: {e}")
    finally:
        if driver:
            driver.quit()
            print("🔒 Browser closed")
    
    return all_buildings, total_count

def main():
    print("=" * 60)
    
    buildings, total_count = scrape_all_buildings_cloudflare()
    
    if not buildings:
        print("❌ No buildings were scraped")
        return
    
    # Create DataFrame
    df = pd.DataFrame(buildings)
    
    # Additional data cleaning
    print(f"\n🧹 Cleaning data...")
    
    # Remove any remaining invalid entries
    original_count = len(df)
    df = df[df['name'].str.len() >= 3]  # Remove very short names
    df = df[~df['name'].str.lower().str.contains('lease|sale|rent|buy', na=False)]  # Remove action words
    df = df[df['url'].str.contains('/building/', na=False)]  # Ensure building URLs
    df = df.drop_duplicates(subset=['url'])  # Remove URL duplicates
    df = df.drop_duplicates(subset=['name'])  # Remove name duplicates
    
    cleaned_count = len(df)
    print(f"📊 Removed {original_count - cleaned_count} invalid/duplicate entries")
    
    # Sort by name for better organization
    df = df.sort_values('name').reset_index(drop=True)
    
    # Display results
    print(f"\n{'='*50}")
    print(f"🎉 SCRAPING COMPLETED!")
    print(f"📊 Valid buildings scraped: {len(df)}")
    if total_count:
        completion_rate = (len(df) / total_count) * 100
        print(f"📈 Expected total: {total_count}")
        print(f"📉 Completion rate: {completion_rate:.1f}%")
    print(f"{'='*50}")
    
    # Show sample data
    print(f"\n📋 Sample data (first 10):")
    print(df.head(10))
    
    print(f"\n📋 Sample data (last 10):")
    print(df.tail(10))
    
    print(f"\n📝 Columns: {list(df.columns)}")
    
    # Save to CSV
    output_file = 'notebooks/leasinghub_buildings_clean.csv'
    df.to_csv(output_file, index=False)
    print(f"\n💾 Clean data saved to: {output_file}")
    
    # Basic statistics
    print(f"\n📈 Statistics:")
    print(f"   • Total unique buildings: {len(df)}")
    print(f"   • Average name length: {df['name'].str.len().mean():.1f} characters")
    chinese_pattern = r'[\u4e00-\u9fff]'
    chinese_count = df['name'].str.contains(chinese_pattern).sum()
    print(f"   • Buildings with Chinese characters: {chinese_count}")
    print(f"   • Data sources: {df['source'].value_counts().to_dict()}")

if __name__ == "__main__":
    main() 