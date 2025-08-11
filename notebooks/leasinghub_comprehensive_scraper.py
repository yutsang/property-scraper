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
import os

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
        '/shop/rent/',
        '/shop/sale/',
        '/industrial/rent/',
        '/industrial/sale/',
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

def extract_buildings_from_html(html, property_type):
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
                'property_type': property_type,
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
                                        'property_type': property_type,
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
        r'(\d{1,5})\s*buildings?',
        r'(\d{1,5})\s*results?',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            try:
                count = int(match.replace(',', ''))
                if 10 <= count <= 50000:  # Reasonable range for buildings
                    return count
            except ValueError:
                continue
    
    return None

def scrape_property_type_buildings(driver, property_type, max_pages=200):
    """Scrape all buildings for a specific property type."""
    print(f"\n🏢 Scraping {property_type.upper()} buildings...")
    print(f"🔧 Setting up for {property_type} scraping...")
    
    all_buildings = []
    total_count = None
    page = 1
    consecutive_empty = 0
    max_consecutive_empty = 5
    
    # Property type URL mapping
    base_urls = {
        'office': 'https://www.leasinghub.com/office/buildings',
        'shop': 'https://www.leasinghub.com/shop/buildings', 
        'industrial': 'https://www.leasinghub.com/industrial/buildings'
    }
    
    base_url = base_urls.get(property_type)
    if not base_url:
        print(f"❌ Unknown property type: {property_type}")
        return []
    
    print(f"🌐 Target URL: {base_url}")
    
    # Initialize progress bar
    pbar = tqdm(total=max_pages, desc=f"{property_type.title()} Buildings", unit="page")
    
    try:
        while page <= max_pages:
            # Try different URL patterns
            urls_to_try = [
                f"{base_url}?limitstart={(page-1)*20}",
                f"{base_url}?start={(page-1)*20}",
                f"{base_url}?page={page}",
                f"{base_url}?offset={(page-1)*20}",
            ]
            
            buildings_found = False
            
            for attempt, url in enumerate(urls_to_try):
                try:
                    pbar.set_postfix_str(f"page={page} | trans=loading")
                    driver.get(url)
                    
                    # Wait for Cloudflare check
                    if not wait_for_cloudflare_check(driver):
                        continue
                    
                    # Wait for content to load
                    pbar.set_postfix_str(f"page={page} | trans=extracting")
                    time.sleep(random.uniform(1, 2))
                    
                    # Quick scroll to load any lazy content
                    try:
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(0.5)
                    except:
                        pass
                    
                    # Get page HTML
                    html = driver.page_source
                    
                    # Extract buildings
                    buildings = extract_buildings_from_html(html, property_type)
                    
                    # Get total count on first page
                    if page == 1:
                        total_count = get_total_count_from_page(html)
                        if total_count:
                            estimated_pages = (total_count + 19) // 20
                            actual_pages = min(estimated_pages, max_pages)
                            pbar.total = actual_pages
                            pbar.set_description(f"{property_type.title()} ({total_count} total)")
                            pbar.refresh()
                    
                    if buildings:
                        all_buildings.extend(buildings)
                        buildings_found = True
                        consecutive_empty = 0
                        
                        pbar.set_postfix_str(f"page={page} | found={len(buildings)} | total={len(all_buildings)}")
                        break
                    
                except Exception as e:
                    continue
            
            if not buildings_found:
                consecutive_empty += 1
                pbar.set_postfix_str(f"page={page} | empty={consecutive_empty}")
                
                if consecutive_empty >= max_consecutive_empty:
                    pbar.set_postfix_str(f"stopped | total={len(all_buildings)}")
                    break
            
            # Check if we've reached the expected total
            if total_count and len(all_buildings) >= total_count:
                pbar.set_postfix_str(f"completed | total={len(all_buildings)}")
                break
            
            pbar.update(1)
            page += 1
            
            # Delay between pages
            time.sleep(random.uniform(1, 3))
            
    except Exception as e:
        print(f"❌ Error scraping {property_type}: {e}")
    finally:
        pbar.close()
    
    print(f"✅ {property_type.title()}: Found {len(all_buildings)} buildings")
    return all_buildings

def extract_building_details(html, url):
    """Extract all possible building details from a building page."""
    soup = BeautifulSoup(html, 'html.parser')
    details = {'url': url}
    text = soup.get_text().lower()
    
    # Building name
    title = soup.select_one('h1, .title, .building-title, .page-title')
    details['building_name'] = title.get_text(strip=True) if title else ''
    
    # Address
    for addr_selector in ['.address', '.location', '[class*="address"]', '.building-address']:
        addr = soup.select_one(addr_selector)
        if addr and len(addr.get_text(strip=True)) > 5:
            details['address'] = addr.get_text(strip=True)
            break
    
    # District
    for dist_selector in ['.district', '.area', '[class*="district"]', '.building-district']:
        dist = soup.select_one(dist_selector)
        if dist:
            details['district'] = dist.get_text(strip=True)
            break
    
    # Year built
    year_match = re.search(r'(?:built|建於|built in|completed).*?(\d{4})', text)
    if year_match:
        details['year_built'] = year_match.group(1)
    
    # Floor count
    floor_match = re.search(r'(\d+)\s*(?:floors?|樓|storeys?)', text)
    if floor_match:
        details['total_floors'] = floor_match.group(1)
    
    # Building grade
    grade_match = re.search(r'(?:grade|class)\s*([a-c])|[甲乙丙]級', text)
    if grade_match:
        details['building_grade'] = grade_match.group(0)
    
    # Pricing information
    prices = []
    for price_elem in soup.select('.price, .rent, .rate, [class*="price"], [class*="rent"]'):
        price_text = price_elem.get_text(strip=True)
        if re.search(r'[\$￥]\s*\d+|hk\$|\d+\s*psf|per sq|rent', price_text.lower()):
            prices.append(price_text)
    if prices:
        details['pricing_info'] = ' | '.join(prices[:3])
    
    # Available floors/units
    floors = []
    for floor_elem in soup.select('.floor, .unit, [class*="floor"], [class*="unit"]'):
        floor_text = floor_elem.get_text(strip=True)
        if re.search(r'floor|樓|unit|室|available', floor_text.lower()) and len(floor_text) < 100:
            floors.append(floor_text)
    if floors:
        details['available_floors'] = ' | '.join(floors[:3])
    
    # Amenities
    amenities = []
    amenity_words = ['parking', 'elevator', 'lift', 'aircon', 'security', 'lobby', 'reception', 
                     '停車', '電梯', '冷氣', '保安', 'wifi', 'pantry', 'meeting room']
    for word in amenity_words:
        if word in text:
            amenities.append(word)
    if amenities:
        details['amenities'] = ', '.join(list(set(amenities))[:8])  # Remove duplicates
    
    # Building area
    area_match = re.search(r'(\d+,?\d*)\s*(?:sq\.?\s*ft|平方呎|sqft|square feet)', text)
    if area_match:
        details['building_area'] = area_match.group(1)
    
    # Property type from content
    for prop_type in ['office', 'retail', 'industrial', 'warehouse', 'shop', 'commercial',
                      '寫字樓', '商舖', '工業', '零售', '辦公室']:
        if prop_type in text:
            details['detected_property_type'] = prop_type
            break
    
    # Contact information
    phone_match = re.search(r'\b\d{4}[\s-]?\d{4}\b', soup.get_text())
    if phone_match:
        details['contact_phone'] = phone_match.group(0)
    
    email_match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', soup.get_text())
    if email_match:
        details['contact_email'] = email_match.group(0)
    
    # Transportation
    transport_keywords = ['mtr', 'station', '地鐵', '站', 'bus', 'transport', 'exit']
    for transport_word in transport_keywords:
        if transport_word in text:
            idx = text.find(transport_word)
            if idx > -1:
                context = text[max(0, idx-50):idx+50].strip()[:150]
                details['transport_info'] = context
                break
    
    # Building description
    desc_selectors = ['.description', '.building-description', '[class*="desc"]', '.details']
    for selector in desc_selectors:
        desc_elem = soup.select_one(selector)
        if desc_elem:
            desc_text = desc_elem.get_text(strip=True)
            if len(desc_text) > 50:
                details['description'] = desc_text[:500]  # Limit length
                break
    
    # Images count
    images = soup.select('img[src]')
    details['image_count'] = len([img for img in images if img.get('src') and 'building' in img.get('src', '').lower()])
    
    # Page content metrics
    details['full_text_length'] = len(soup.get_text())
    details['html_size'] = len(html)
    
    return details

def scrape_building_details(buildings_df, start_idx=0, batch_size=None):
    """Scrape detailed information for each building."""
    print(f"\n🔍 Scraping building details...")
    
    # Apply batch filtering
    if batch_size:
        buildings_df = buildings_df.iloc[start_idx:start_idx + batch_size]
    else:
        buildings_df = buildings_df.iloc[start_idx:]
    
    print(f"📊 Processing {len(buildings_df)} buildings (starting from index {start_idx})")
    
    driver = setup_undetected_driver()
    results = []
    base_url = "https://www.leasinghub.com"
    
    try:
        with tqdm(total=len(buildings_df), desc="Processing Details", unit="building") as pbar:
            for idx, row in buildings_df.iterrows():
                try:
                    url = base_url + row['url']
                    name = row['name'][:20]
                    prop_type = row.get('property_type', 'unknown')
                    
                    pbar.set_postfix_str(f"type={prop_type} | trans=starting | name={name[:15]}...")
                    
                    pbar.set_postfix_str(f"type={prop_type} | trans=loading | name={name[:15]}")
                    driver.get(url)
                    
                    # Wait for page to load
                    pbar.set_postfix_str(f"type={prop_type} | trans=cloudflare | name={name[:15]}")
                    if not wait_for_cloudflare_check(driver):
                        time.sleep(2)  # Additional wait if cloudflare detected
                    
                    pbar.set_postfix_str(f"type={prop_type} | trans=waiting | name={name[:15]}")
                    time.sleep(random.uniform(1, 2))
                    
                    # Scroll to load dynamic content
                    pbar.set_postfix_str(f"type={prop_type} | trans=scrolling | name={name[:15]}")
                    try:
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(0.5)
                        driver.execute_script("window.scrollTo(0, 0);")
                        time.sleep(0.3)
                    except:
                        pass
                    
                    # Extract details
                    pbar.set_postfix_str(f"type={prop_type} | trans=extracting | name={name[:15]}")
                    details = extract_building_details(driver.page_source, row['url'])
                    details['original_name'] = row['name']
                    details['property_type'] = row.get('property_type', 'unknown')
                    details['source'] = row.get('source', 'unknown')
                    details['scrape_index'] = idx
                    details['scrape_timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S')
                    
                    results.append(details)
                    pbar.set_postfix_str(f"type={prop_type} | trans=success | name={name[:15]} | total={len(results)}")
                    pbar.update(1)
                    
                    # Save progress every 25 buildings
                    if len(results) % 25 == 0:
                        temp_df = pd.DataFrame(results)
                        temp_df.to_csv(f'temp_details_{start_idx}_{len(results)}.csv', index=False)
                        print(f"\n💾 Progress saved: {len(results)} buildings processed")
                    
                    # Random delay to avoid rate limiting
                    time.sleep(random.uniform(1.5, 3.0))
                    
                except Exception as e:
                    prop_type = row.get('property_type', 'unknown')
                    name = row['name'][:20]
                    error_record = {
                        'url': row['url'],
                        'original_name': row['name'],
                        'property_type': prop_type,
                        'scrape_index': idx,
                        'error': str(e)[:200],
                        'scrape_timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
                    }
                    results.append(error_record)
                    pbar.set_postfix_str(f"type={prop_type} | trans=error | name={name[:15]} | error={str(e)[:15]}")
                    pbar.update(1)
                    
    finally:
        driver.quit()
        print("🔒 Browser closed")
    
    return results

def main():
    """Main function to run the comprehensive scraper for all 3 property types."""
    print("🏢 LeasingHub Comprehensive Building Scraper")
    print("=" * 60)
    print("🚀 Scraping all 3 property types: office, shop, industrial")
    print("🔧 Initializing Chrome browser...")
    print("⏳ This may take 10-30 seconds for first-time setup...")
    
    all_buildings = []
    property_types = ['office', 'shop', 'industrial']
    
    driver = setup_undetected_driver()
    print("✅ Browser ready! Starting to scrape...")
    
    try:
        for property_type in property_types:
            print(f"\n🏢 Scraping {property_type.upper()} buildings...")
            buildings = scrape_property_type_buildings(driver, property_type)
            all_buildings.extend(buildings)
            
            # Save individual property type results
            if buildings:
                property_df = pd.DataFrame(buildings)
                property_df.to_csv(f'leasinghub_{property_type}_buildings.csv', index=False)
                print(f"💾 Saved {property_type} buildings to: leasinghub_{property_type}_buildings.csv")
            
            # Short break between property types
            time.sleep(random.uniform(3, 5))
            
    finally:
        driver.quit()
    
    if all_buildings:
        # Combine and clean all buildings
        all_df = pd.DataFrame(all_buildings)
        
        # Remove duplicates
        original_count = len(all_df)
        all_df = all_df.drop_duplicates(subset=['url'])
        all_df = all_df.drop_duplicates(subset=['name'])
        
        # Additional cleaning
        all_df = all_df[all_df['name'].str.len() >= 3]
        all_df = all_df[~all_df['name'].str.lower().str.contains('lease|sale|rent|buy', na=False)]
        
        cleaned_count = len(all_df)
        print(f"\n🧹 Removed {original_count - cleaned_count} duplicates/invalid entries")
        
        # Sort and save
        all_df = all_df.sort_values(['property_type', 'name']).reset_index(drop=True)
        all_df.to_csv('leasinghub_all_buildings.csv', index=False)
        
        print(f"\n🎉 BUILDING LIST SCRAPING COMPLETED!")
        print(f"📊 Total unique buildings: {len(all_df)}")
        print(f"📈 By property type:")
        for ptype in all_df['property_type'].value_counts().items():
            print(f"   • {ptype[0].title()}: {ptype[1]} buildings")
        print(f"💾 Combined results saved to: leasinghub_all_buildings.csv")
        
        # Show sample
        print(f"\n📋 Sample data:")
        print(all_df.head(10)[['name', 'property_type', 'url']])
    else:
        print("❌ No buildings were scraped!")

if __name__ == "__main__":
    main() 