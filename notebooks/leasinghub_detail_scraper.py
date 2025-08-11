import pandas as pd
import time
import re
import random
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from tqdm import tqdm
import os

def setup_driver():
    """Setup ChromeDriver for detail scraping."""
    options = uc.ChromeOptions()
    options.add_argument('--no-first-run')
    options.add_argument('--no-default-browser-check')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-blink-features=AutomationControlled')
    
    driver = uc.Chrome(options=options, version_main=137)
    return driver

def wait_for_load(driver, max_wait=5):
    """Wait for page to load."""
    start_time = time.time()
    while time.time() - start_time < max_wait:
        try:
            page_source = driver.page_source.lower()
            if any(indicator in page_source for indicator in [
                'cloudflare', 'checking your browser', 'security check'
            ]):
                time.sleep(0.5)
                continue
            
            if 'building' in page_source or 'office' in page_source:
                return True
            time.sleep(0.2)
        except:
            return True
    return True

def extract_details(html, url):
    """Extract all possible building details."""
    soup = BeautifulSoup(html, 'html.parser')
    details = {'url': url}
    text = soup.get_text().lower()
    
    # Building name and alternative name from property head
    property_head = soup.select_one('.property-head')
    if property_head:
        name_elem = property_head.select_one('.name')
        alt_name_elem = property_head.select_one('.alt-name')
        if name_elem:
            details['building_name'] = name_elem.get_text(strip=True)
        if alt_name_elem:
            details['building_name_alt'] = alt_name_elem.get_text(strip=True)
    
    # Address and location info from property head
    if property_head:
        location_list = property_head.select('.list-separator li')
        if len(location_list) >= 2:
            details['address'] = location_list[1].get_text(strip=True)
        if len(location_list) >= 3:
            details['district'] = location_list[2].get_text(strip=True)
    
    # Extract meta information from the bg-light div
    meta_div = soup.select_one('.bg-light.py-3.px-2')
    if meta_div:
        meta_items = meta_div.select('.list-meta li')
        for item in meta_items:
            value_elem = item.select_one('.value')
            label_elem = item.select_one('.label')
            if value_elem and label_elem:
                value = value_elem.get_text(strip=True)
                label = label_elem.get_text(strip=True).lower()
                
                if label == 'type':
                    details['building_type'] = value
                elif label == 'age':
                    details['building_age'] = value
                elif label == 'grade':
                    details['building_grade'] = value
    
    # Extract detailed specifications from the table
    spec_table = soup.select_one('.table.table-data.table-spec')
    if spec_table:
        rows = spec_table.select('tbody tr')
        for row in rows:
            th_elem = row.select_one('th')
            td_elem = row.select_one('td')
            if th_elem and td_elem:
                header = th_elem.get_text(strip=True).lower()
                value = td_elem.get_text(strip=True)
                
                if 'usage' in header:
                    details['usage'] = value
                elif 'storeys' in header or 'no. of storeys' in header:
                    details['total_storeys'] = value
                elif 'year completed' in header:
                    details['year_completed'] = value
                elif 'ownership' in header:
                    details['ownership'] = value
                elif 'total gfa' in header:
                    details['total_gfa'] = value
                elif 'typical floor area' in header:
                    details['typical_floor_area'] = value
                elif 'floor system' in header:
                    details['floor_system'] = value
                elif 'air conditioning' in header:
                    details['air_conditioning'] = value
                elif 'management fee' in header:
                    details['management_fee'] = value
                elif 'elevator' in header:
                    details['elevator'] = value
                elif 'management company' in header:
                    details['management_company'] = value
                elif 'mtr' in header:
                    details['mtr_info'] = value
                elif 'carpark (inhouse)' in header:
                    details['carpark_inhouse'] = value
                elif 'carpark nearby' in header:
                    details['carpark_nearby'] = value
                elif 'hotel nearby' in header:
                    details['hotel_nearby'] = value
                elif 'last updated' in header:
                    details['last_updated'] = value
                elif 'ref.' in header:
                    details['reference_number'] = value
    
    # Fallback extraction for building name if not found in property head
    if 'building_name' not in details or not details['building_name']:
        title = soup.select_one('h1, .title, .building-title')
        if title:
            details['building_name'] = title.get_text(strip=True)
    
    # Fallback extraction for address if not found in property head
    if 'address' not in details or not details['address']:
        for addr_selector in ['.address', '.location', '[class*="address"]']:
            addr = soup.select_one(addr_selector)
            if addr and len(addr.get_text(strip=True)) > 5:
                details['address'] = addr.get_text(strip=True)
                break
    
    # Fallback extraction for district if not found in property head
    if 'district' not in details or not details['district']:
        for dist_selector in ['.district', '.area', '[class*="district"]']:
            dist = soup.select_one(dist_selector)
            if dist:
                details['district'] = dist.get_text(strip=True)
                break
    
    # Extract year from year_completed if available
    if 'year_completed' in details and details['year_completed']:
        year_match = re.search(r'(\d{4})', details['year_completed'])
        if year_match:
            details['year_built'] = year_match.group(1)
    
    # Extract floor count from total_storeys if available
    if 'total_storeys' in details and details['total_storeys']:
        floor_match = re.search(r'(\d+)', details['total_storeys'])
        if floor_match:
            details['total_floors'] = floor_match.group(1)
    
    # Extract GFA numbers
    if 'total_gfa' in details and details['total_gfa']:
        gfa_match = re.search(r'([\d,]+)', details['total_gfa'])
        if gfa_match:
            details['gfa_sqft'] = gfa_match.group(1).replace(',', '')
    
    if 'typical_floor_area' in details and details['typical_floor_area']:
        floor_area_match = re.search(r'([\d,]+)', details['typical_floor_area'])
        if floor_area_match:
            details['typical_floor_sqft'] = floor_area_match.group(1).replace(',', '')
    
    # Extract management fee amount
    if 'management_fee' in details and details['management_fee']:
        fee_match = re.search(r'HK\$([\d.]+)', details['management_fee'])
        if fee_match:
            details['management_fee_amount'] = fee_match.group(1)
    
    # Extract elevator count
    if 'elevator' in details and details['elevator']:
        elevator_match = re.search(r'(\d+)', details['elevator'])
        if elevator_match:
            details['elevator_count'] = elevator_match.group(1)
    
    # Extract age in years
    if 'building_age' in details and details['building_age']:
        age_match = re.search(r'(\d+)', details['building_age'])
        if age_match:
            details['age_years'] = age_match.group(1)
    
    # Property type classification
    if 'building_type' in details and details['building_type']:
        building_type = details['building_type'].lower()
        if 'office' in building_type:
            details['property_type'] = 'office'
        elif 'retail' in building_type or 'shop' in building_type:
            details['property_type'] = 'retail'
        elif 'industrial' in building_type:
            details['property_type'] = 'industrial'
    
    # Images
    images = soup.select('img')
    details['image_count'] = len(images)
    
    # All text content for analysis
    details['full_text_length'] = len(soup.get_text())
    
    return details

def scrape_details(buildings_df, batch_size=None, start_idx=0):
    """Scrape building details."""
    print(f"🏢 Building Detail Scraper - Starting from index {start_idx}")
    
    if batch_size:
        buildings_df = buildings_df.iloc[start_idx:start_idx + batch_size]
    else:
        buildings_df = buildings_df.iloc[start_idx:]
    
    print(f"📊 Scraping {len(buildings_df)} buildings")
    
    driver = setup_driver()
    results = []
    base_url = "https://www.leasinghub.com"
    
    try:
        with tqdm(total=len(buildings_df), desc="Scraping details", unit="building") as pbar:
            for idx, row in buildings_df.iterrows():
                try:
                    url = base_url + row['url']
                    name = row['name'][:30]
                    
                    pbar.set_postfix_str(f"{name}...")
                    
                    driver.get(url)
                    wait_for_load(driver)
                    
                    # Quick scroll
                    try:
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(0.3)
                    except:
                        pass
                    
                    # Extract details
                    details = extract_details(driver.page_source, row['url'])
                    details['original_name'] = row['name']
                    details['scrape_index'] = idx
                    
                    results.append(details)
                    pbar.set_postfix_str(f"✅ {name} ({len(results)})")
                    pbar.update(1)
                    
                    # Save progress every 25 buildings
                    if len(results) % 25 == 0:
                        temp_df = pd.DataFrame(results)
                        temp_df.to_csv(f'temp_details_{start_idx}_{len(results)}.csv', index=False)
                    
                    time.sleep(random.uniform(0.8, 1.5))
                    
                except Exception as e:
                    error_record = {
                        'url': row['url'],
                        'original_name': row['name'],
                        'scrape_index': idx,
                        'error': str(e)[:100]
                    }
                    results.append(error_record)
                    pbar.set_postfix_str(f"❌ Error: {str(e)[:20]}")
                    pbar.update(1)
                    
    finally:
        driver.quit()
        print("🔒 Browser closed")
    
    return results

def main():
    # Load buildings
    buildings_file = 'leasinghub_buildings_clean.csv'
    if not os.path.exists(buildings_file):
        print("❌ Buildings file not found. Run the main scraper first!")
        return
    
    df = pd.read_csv(buildings_file)
    print(f"📋 Found {len(df)} buildings to scrape")
    
    # Get user input
    batch_input = input(f"Batch size (default: all {len(df)}): ").strip()
    start_input = input("Start index (default: 0): ").strip()
    
    batch_size = int(batch_input) if batch_input.isdigit() else None
    start_idx = int(start_input) if start_input.isdigit() else 0
    
    print("\n🚀 Starting detailed scraping...")
    
    # Scrape
    results = scrape_details(df, batch_size, start_idx)
    
    if not results:
        print("❌ No results")
        return
    
    # Process results
    results_df = pd.DataFrame(results)
    
    # Count success/failure
    has_error = 'error' in results_df.columns and results_df['error'].notna()
    if isinstance(has_error, bool):
        has_error = pd.Series([has_error] * len(results_df))
    
    successful = results_df[~has_error]
    failed = results_df[has_error] if has_error.any() else pd.DataFrame()
    
    print(f"\n🎉 SCRAPING COMPLETE!")
    print(f"✅ Successful: {len(successful)}")
    print(f"❌ Failed: {len(failed)}")
    print(f"📈 Success rate: {len(successful)/len(results_df)*100:.1f}%")
    
    # Show sample
    if len(successful) > 0:
        print(f"\n📋 Sample data:")
        sample_cols = ['original_name', 'building_name', 'building_type', 'building_grade', 'address', 'district', 'year_completed', 'total_storeys', 'total_gfa', 'management_fee']
        available_cols = [col for col in sample_cols if col in successful.columns]
        print(successful[available_cols].head(3))
    
    # Save
    output_file = f'building_details_{start_idx}_{len(results_df)}.csv'
    results_df.to_csv(output_file, index=False)
    print(f"\n💾 Saved to: {output_file}")
    
    if len(successful) > 0:
        clean_file = f'building_details_clean_{start_idx}_{len(successful)}.csv'
        successful.to_csv(clean_file, index=False)
        print(f"💾 Clean data: {clean_file}")
    
    # Stats
    if len(successful) > 0:
        print(f"\n📊 Statistics:")
        # Core building info
        core_cols = ['building_name', 'address', 'district', 'building_type', 'building_grade']
        # Detailed specs
        spec_cols = ['year_completed', 'total_storeys', 'ownership', 'total_gfa', 'typical_floor_area', 'floor_system', 'air_conditioning', 'management_fee', 'elevator', 'management_company']
        # Location and facilities
        location_cols = ['mtr_info', 'carpark_inhouse', 'carpark_nearby', 'hotel_nearby']
        # Extracted numbers
        number_cols = ['year_built', 'total_floors', 'gfa_sqft', 'typical_floor_sqft', 'management_fee_amount', 'elevator_count', 'age_years']
        
        all_cols = core_cols + spec_cols + location_cols + number_cols
        
        for col in all_cols:
            if col in successful.columns:
                count = successful[col].notna().sum()
                if count > 0:
                    print(f"   • {col}: {count} buildings ({count/len(successful)*100:.1f}%)")

if __name__ == "__main__":
    main() 