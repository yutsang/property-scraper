"""
Midland ICI - New Transaction Page Scraper
URL: https://www.midlandici.com.hk/zh-hk/listing/transaction/ics
This page HAS Jan 30, 2026 data - need to extract it!
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time
import json
import pandas as pd
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import re

def setup_driver():
    """Setup Chrome with network logging"""
    options = webdriver.ChromeOptions()
    # Don't use headless first - let's see what's happening
    # options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    # Enable performance logging to capture network requests
    options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
    
    driver = webdriver.Chrome(options=options)
    return driver


def method_1_selenium_with_network_capture():
    """Load page with Selenium and capture API calls"""
    print("="*80)
    print("METHOD 1: Selenium with Network Capture")
    print("="*80)
    
    driver = None
    try:
        driver = setup_driver()
        url = "https://www.midlandici.com.hk/zh-hk/listing/transaction/ics"
        
        print(f"\n📱 Loading: {url}")
        driver.get(url)
        
        # Wait for page to load
        print("⏳ Waiting for page to load (10 seconds)...")
        time.sleep(10)
        
        print(f"✓ Page loaded: {driver.title}")
        
        # Capture network logs to find API calls
        print("\n🔍 Analyzing network requests...")
        logs = driver.get_log('performance')
        
        api_calls = []
        for entry in logs:
            try:
                log_data = json.loads(entry['message'])
                message = log_data['message']
                
                if message['method'] == 'Network.responseReceived':
                    response = message['params']['response']
                    url_called = response['url']
                    
                    # Look for API calls
                    if any(keyword in url_called for keyword in ['api', 'json', 'transaction', 'graphql']):
                        if 'midland' in url_called:
                            api_calls.append({
                                'url': url_called,
                                'status': response.get('status'),
                                'type': response.get('mimeType')
                            })
                            print(f"  ✓ Found API call: {url_called}")
            except:
                pass
        
        if api_calls:
            print(f"\n📡 Found {len(api_calls)} API calls!")
            
            # Try to fetch from these APIs
            for call in api_calls:
                print(f"\n  Testing: {call['url']}")
                try:
                    response = requests.get(call['url'], timeout=5)
                    if response.status_code == 200:
                        data = response.json()
                        if data:
                            print(f"    ✓ Got data!")
                            if isinstance(data, dict):
                                if 'transactions' in data:
                                    print(f"    ✓ Has {len(data['transactions'])} transactions")
                                    return data
                            elif isinstance(data, list):
                                print(f"    ✓ Has {len(data)} items")
                                return data
                except:
                    pass
        
        # Try to extract data directly from page JavaScript
        print("\n🔍 Extracting data from page JavaScript...")
        
        js_extract_script = """
        // Try to find transaction data in various places
        if (typeof __NUXT__ !== 'undefined') {
            return JSON.stringify(__NUXT__);
        } else if (typeof window.app !== 'undefined' && window.app.data) {
            return JSON.stringify(window.app.data);
        } else if (typeof initialState !== 'undefined') {
            return JSON.stringify(initialState);
        } else if (typeof transactions !== 'undefined') {
            return JSON.stringify({transactions: transactions});
        }
        
        // Check Vuex store
        if (typeof window.$nuxt !== 'undefined' && window.$nuxt.$store) {
            return JSON.stringify(window.$nuxt.$store.state);
        }
        
        return null;
        """
        
        data_from_js = driver.execute_script(js_extract_script)
        
        if data_from_js:
            print("  ✓ Found data in JavaScript!")
            try:
                data = json.loads(data_from_js)
                print(f"  Type: {type(data)}")
                if isinstance(data, dict):
                    print(f"  Keys: {list(data.keys())[:10]}")
                
                # Save for inspection
                with open('midland_ici_page_data.json', 'w') as f:
                    json.dump(data, f, indent=2)
                print(f"  ✓ Saved to: midland_ici_page_data.json")
                
                return data
            except Exception as e:
                print(f"  Error parsing: {e}")
        
        # Look for transaction elements in the rendered HTML
        print("\n🔍 Looking for transaction elements in rendered page...")
        
        # Try various selectors
        selectors = [
            (By.CLASS_NAME, "transaction-item"),
            (By.CLASS_NAME, "transaction-card"),
            (By.CLASS_NAME, "list-item"),
            (By.CSS_SELECTOR, "[data-transaction]"),
            (By.CSS_SELECTOR, ".transaction"),
            (By.XPATH, "//div[contains(@class, 'transaction')]"),
            (By.XPATH, "//tr[contains(@class, 'record')]"),
        ]
        
        for by, selector in selectors:
            try:
                elements = driver.find_elements(by, selector)
                if elements:
                    print(f"  ✓ Found {len(elements)} elements with: {selector}")
                    
                    # Extract text from first few
                    sample_data = []
                    for elem in elements[:10]:
                        text = elem.text.strip()
                        if text:
                            sample_data.append(text)
                            # Check for dates
                            if '2026' in text or '2025-1' in text:
                                print(f"    ✓ Has recent date: {text[:100]}")
                    
                    if sample_data:
                        return sample_data
            except:
                pass
        
        # Last resort - check page source for data
        print("\n🔍 Checking page source for embedded data...")
        page_source = driver.page_source
        
        # Look for JSON data in page
        json_pattern = r'(?:transactions|data)\s*[:=]\s*(\[{.*?}\])'
        matches = re.findall(json_pattern, page_source, re.DOTALL)
        
        if matches:
            print(f"  ✓ Found {len(matches)} potential JSON data blocks")
            for i, match in enumerate(matches[:3]):
                try:
                    data = json.loads(match)
                    if data:
                        print(f"  ✓ Match {i}: Valid JSON with {len(data)} items")
                        return data
                except:
                    pass
        
        # Save page source for manual inspection
        with open('midland_ici_page_source.html', 'w', encoding='utf-8') as f:
            f.write(page_source)
        print(f"\n✓ Page source saved to: midland_ici_page_source.html")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass
    
    return None


def method_2_find_new_api_endpoint():
    """Try to find the correct API endpoint for the new page"""
    print("\n" + "="*80)
    print("METHOD 2: Find New API Endpoint")
    print("="*80)
    
    # The new page might use different endpoints
    base_urls = [
        "https://www.midlandici.com.hk/zh-hk/listing/transaction/ics/json",
        "https://www.midlandici.com.hk/api/listing/transaction",
        "https://www.midlandici.com.hk/listing/api/transaction",
        "https://api.midlandici.com.hk/listing/transaction",
        "https://service.midlandici.com/transaction/list",
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Referer': 'https://www.midlandici.com.hk/zh-hk/listing/transaction/ics'
    }
    
    for url in base_urls:
        print(f"\n📡 Testing: {url}")
        
        # Try different parameter combinations
        param_sets = [
            {},  # No params
            {'page': 1, 'limit': 100},
            {'pageSize': 100, 'pageIndex': 1},
            {'cursor': 1, 'page_size': 100},
        ]
        
        for params in param_sets:
            try:
                response = requests.get(url, headers=headers, params=params, timeout=5)
                if response.status_code == 200:
                    print(f"  ✓ 200 OK with params: {params}")
                    try:
                        data = response.json()
                        if data:
                            print(f"  ✓ Got JSON data!")
                            
                            # Check if it has transactions
                            if isinstance(data, dict):
                                if 'transactions' in data or 'data' in data or 'items' in data:
                                    print(f"  ✅ SUCCESS! This endpoint works!")
                                    print(f"  Data keys: {data.keys()}")
                                    return url, params, data
                            elif isinstance(data, list) and len(data) > 0:
                                print(f"  ✅ SUCCESS! Got {len(data)} items!")
                                return url, params, data
                    except:
                        pass
            except:
                pass
    
    return None, None, None


def method_3_intercept_ajax_calls():
    """Use requests to simulate what the page JavaScript does"""
    print("\n" + "="*80)
    print("METHOD 3: Simulate Page JavaScript AJAX Calls")
    print("="*80)
    
    # First, load the page normally to get cookies/session
    session = requests.Session()
    
    main_url = "https://www.midlandici.com.hk/zh-hk/listing/transaction/ics"
    
    print(f"📱 Loading main page to establish session...")
    response = session.get(main_url, timeout=10)
    print(f"  Status: {response.status_code}")
    print(f"  Cookies: {len(session.cookies)} cookies set")
    
    # Now try to call the API with the session cookies
    api_url = "https://www.midlandici.com.hk/ics/property/transaction/json"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'zh-HK,zh-TW;q=0.9,zh;q=0.8,en;q=0.7',
        'Referer': main_url,
        'X-Requested-With': 'XMLHttpRequest',
        'Origin': 'https://www.midlandici.com.hk'
    }
    
    # Try different parameter combinations
    param_sets = [
        {'lang': 'zh-hk', 'page_size': 100, 'cursor': 1, 'order': 'tx_date-desc'},
        {'lang': 'chinese', 'page_size': 100, 'cursor': 1},
        {'pageSize': 100, 'pageNo': 1},
        {},  # No params - get defaults
    ]
    
    for params in param_sets:
        print(f"\n  Testing params: {params}")
        try:
            response = session.get(api_url, headers=headers, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data and data is not None:
                    if isinstance(data, dict):
                        trans = data.get('transactions', [])
                        count = data.get('count', 0)
                        print(f"    ✓ Got data: {len(trans)} transactions (total: {count})")
                        
                        if trans:
                            # Check dates
                            dates = [t.get('tx_date') for t in trans[:10] if t.get('tx_date')]
                            print(f"    Sample dates: {dates[:5]}")
                            
                            # Check for recent data
                            recent = [d for d in dates if '2025-1' in str(d) or '2026' in str(d)]
                            if recent:
                                print(f"    ✅ HAS RECENT DATA!")
                                return session, api_url, params, data
                    elif isinstance(data, list):
                        print(f"    ✓ Got list: {len(data)} items")
                        return session, api_url, params, data
                else:
                    print(f"    ✗ Null response")
        except Exception as e:
            print(f"    Error: {e}")
    
    return None, None, None, None


def method_4_selenium_extract_all():
    """Use Selenium to fully render page and extract everything"""
    print("\n" + "="*80)
    print("METHOD 4: Full Selenium Extraction")
    print("="*80)
    
    driver = None
    try:
        driver = setup_driver()
        url = "https://www.midlandici.com.hk/zh-hk/listing/transaction/ics"
        
        print(f"\n📱 Loading: {url}")
        driver.get(url)
        
        # Wait for content to load
        print("⏳ Waiting for dynamic content to load...")
        time.sleep(5)
        
        # Scroll to trigger lazy loading
        print("📜 Scrolling to trigger data loading...")
        for i in range(3):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
        
        # Try to extract window.__NUXT__ or similar
        print("\n🔍 Extracting data from window object...")
        
        extraction_scripts = [
            "return window.__NUXT__ || null;",
            "return window.app || null;",
            "return window.initialState || null;",
            "return window.pageData || null;",
            """
            // Try to find Vue/Nuxt state
            if (window.$nuxt && window.$nuxt.$store) {
                return window.$nuxt.$store.state;
            }
            return null;
            """,
            """
            // Look for any transaction data
            for (let key in window) {
                if (typeof window[key] === 'object' && window[key] !== null) {
                    if (JSON.stringify(window[key]).includes('tx_date') || 
                        JSON.stringify(window[key]).includes('transaction')) {
                        return {found_in: key, data: window[key]};
                    }
                }
            }
            return null;
            """
        ]
        
        for i, script in enumerate(extraction_scripts):
            try:
                print(f"\n  Try {i+1}: {script[:50]}...")
                result = driver.execute_script(script)
                if result:
                    print(f"    ✅ SUCCESS! Got data")
                    print(f"    Type: {type(result)}")
                    
                    # Save and analyze
                    filename = f'midland_extracted_data_{i}.json'
                    with open(filename, 'w') as f:
                        json.dump(result, f, indent=2, default=str)
                    print(f"    ✓ Saved to: {filename}")
                    
                    # Try to find transactions in the data
                    result_str = json.dumps(result, default=str)
                    if 'tx_date' in result_str or '2026' in result_str:
                        print(f"    ✅ Data contains transaction dates!")
                        return result
            except Exception as e:
                print(f"    ✗ Failed: {str(e)[:100]}")
        
        # Get all text visible on page
        print("\n📄 Extracting visible text from page...")
        try:
            body_text = driver.find_element(By.TAG_NAME, 'body').text
            
            # Look for dates in visible text
            dates_found = re.findall(r'202[0-9]-[01][0-9]-[0-3][0-9]', body_text)
            if dates_found:
                print(f"  ✓ Found {len(dates_found)} dates in visible text")
                recent = [d for d in dates_found if d >= '2025-11']
                if recent:
                    print(f"  ✅ Found {len(recent)} recent dates!")
                    print(f"  Sample: {list(set(recent))[:10]}")
                    
                    # Save visible text
                    with open('midland_ici_visible_text.txt', 'w') as f:
                        f.write(body_text)
                    print(f"  ✓ Saved to: midland_ici_visible_text.txt")
                    
                    return {'visible_dates': recent, 'text': body_text}
        except Exception as e:
            print(f"  Error: {e}")
        
        # Screenshot for manual review
        print("\n📸 Taking screenshot...")
        try:
            driver.save_screenshot('midland_ici_page.png')
            print("  ✓ Screenshot saved: midland_ici_page.png")
        except:
            pass
            
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if driver:
            try:
                driver.quit()
                print("\n✓ Browser closed")
            except:
                pass
    
    return None


# ==================== MAIN ====================

if __name__ == "__main__":
    print("\n" + "="*80)
    print("MIDLAND ICI NEW PAGE SCRAPER")
    print("URL: https://www.midlandici.com.hk/zh-hk/listing/transaction/ics")
    print("GOAL: Extract Jan 30, 2026 transactions")
    print("="*80)
    
    # Method 1: Selenium with network capture
    print("\n🚀 Starting comprehensive extraction...\n")
    
    data_1 = method_1_selenium_with_network_capture()
    
    if data_1:
        print("\n✅ METHOD 1 SUCCESS!")
        print("="*80)
    else:
        print("\n⚠️ METHOD 1: No data extracted")
        print("Trying alternative methods...")
        
        # Method 2: Find new API
        time.sleep(2)
        new_url, params, data_2 = method_2_find_new_api_endpoint()
        
        if data_2:
            print(f"\n✅ METHOD 2 SUCCESS!")
            print(f"Working endpoint: {new_url}")
            print(f"Working params: {params}")
            print("="*80)
        else:
            # Method 3: Session with cookies
            time.sleep(2)
            session, api_url, params, data_3 = method_3_intercept_ajax_calls()
            
            if data_3:
                print(f"\n✅ METHOD 3 SUCCESS!")
                print("="*80)
            else:
                # Method 4: Full extraction
                time.sleep(2)
                data_4 = method_4_selenium_extract_all()
                
                if data_4:
                    print(f"\n✅ METHOD 4 SUCCESS!")
                    print("="*80)
    
    print("\n" + "="*80)
    print("INVESTIGATION COMPLETE")
    print("="*80)
    print("\nCheck generated files:")
    print("  - midland_ici_page_data.json (if data found in JS)")
    print("  - midland_ici_page_source.html (full HTML)")
    print("  - midland_ici_visible_text.txt (visible text)")
    print("  - midland_ici_page.png (screenshot)")
