#!/usr/bin/env python3
"""
Test script to verify LeasingHub scraper setup

This script performs basic tests to ensure everything is working
before running the full scraping process.
"""

import os
import sys
import time
from pathlib import Path

def test_imports():
    """Test if all required packages can be imported."""
    print("🧪 Testing package imports...")
    
    required_packages = {
        'pandas': 'pandas',
        'selenium': 'selenium', 
        'beautifulsoup4': 'bs4',
        'tqdm': 'tqdm',
        'undetected-chromedriver': 'undetected_chromedriver'
    }
    
    missing = []
    for package_name, import_name in required_packages.items():
        try:
            __import__(import_name)
            print(f"  ✅ {package_name}")
        except ImportError:
            print(f"  ❌ {package_name}")
            missing.append(package_name)
    
    if missing:
        print(f"\n❌ Missing packages: {', '.join(missing)}")
        print("Install with: pip install " + " ".join(missing))
        return False
    
    print("✅ All packages available!")
    return True

def test_chrome_setup():
    """Test if Chrome and undetected_chromedriver work."""
    print("\n🌐 Testing Chrome setup...")
    
    try:
        import undetected_chromedriver as uc
        
        # Try to create driver with minimal options
        options = uc.ChromeOptions()
        options.add_argument('--headless')  # Use headless for testing
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        
        print("  🔧 Creating Chrome driver...")
        driver = uc.Chrome(options=options, version_main=137)
        
        print("  🌍 Testing basic navigation...")
        driver.get("https://www.google.com")
        
        if "google" in driver.page_source.lower():
            print("  ✅ Chrome navigation working!")
            success = True
        else:
            print("  ❌ Chrome navigation failed!")
            success = False
            
        driver.quit()
        return success
        
    except Exception as e:
        print(f"  ❌ Chrome setup failed: {str(e)[:100]}")
        return False

def test_leasinghub_access():
    """Test if we can access LeasingHub main page."""
    print("\n🏢 Testing LeasingHub access...")
    
    try:
        import undetected_chromedriver as uc
        import time
        
        options = uc.ChromeOptions()
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        
        print("  🔧 Creating driver for LeasingHub test...")
        driver = uc.Chrome(options=options, version_main=137)
        
        print("  🌍 Accessing LeasingHub...")
        driver.get("https://www.leasinghub.com")
        
        # Wait a bit for page load
        time.sleep(3)
        
        page_source = driver.page_source.lower()
        
        if 'leasinghub' in page_source:
            print("  ✅ LeasingHub accessible!")
            success = True
        elif 'cloudflare' in page_source:
            print("  ⚠️  Cloudflare challenge detected (normal)")
            success = True
        else:
            print("  ❌ Could not access LeasingHub")
            success = False
        
        driver.quit()
        return success
        
    except Exception as e:
        print(f"  ❌ LeasingHub access failed: {str(e)[:100]}")
        return False

def test_file_permissions():
    """Test if we can create output files."""
    print("\n📁 Testing file permissions...")
    
    test_files = [
        'test_output.csv',
        'test_temp.csv'
    ]
    
    try:
        for test_file in test_files:
            # Test write
            with open(test_file, 'w') as f:
                f.write("test,data\n1,2\n")
            
            # Test read
            with open(test_file, 'r') as f:
                content = f.read()
            
            if 'test,data' in content:
                print(f"  ✅ {test_file} - write/read OK")
            else:
                print(f"  ❌ {test_file} - content mismatch")
                return False
                
            # Clean up
            os.remove(test_file)
        
        print("  ✅ File permissions OK!")
        return True
        
    except Exception as e:
        print(f"  ❌ File permission test failed: {e}")
        return False

def test_scraper_files():
    """Test if scraper files exist and are readable."""
    print("\n📄 Testing scraper files...")
    
    required_files = [
        'leasinghub_comprehensive_scraper.py',
        'run_leasinghub_scraper.py'
    ]
    
    for file_path in required_files:
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r') as f:
                    content = f.read()
                if len(content) > 1000:  # Should be substantial files
                    print(f"  ✅ {file_path}")
                else:
                    print(f"  ⚠️  {file_path} - file too small")
            except Exception as e:
                print(f"  ❌ {file_path} - cannot read: {e}")
                return False
        else:
            print(f"  ❌ {file_path} - missing")
            return False
    
    print("  ✅ All scraper files present!")
    return True

def run_mini_scrape_test():
    """Run a minimal scraping test."""
    print("\n🕷️ Running mini scrape test...")
    
    try:
        # Import the scraper
        sys.path.append('.')
        from leasinghub_comprehensive_scraper import setup_undetected_driver, wait_for_cloudflare_check
        
        print("  🔧 Setting up driver...")
        driver = setup_undetected_driver()
        
        print("  🌍 Testing office buildings page...")
        driver.get("https://www.leasinghub.com/office/buildings")
        
        print("  ⏳ Waiting for page load...")
        success = wait_for_cloudflare_check(driver, max_wait=10)
        
        if success:
            page_source = driver.page_source.lower()
            if 'building' in page_source or 'office' in page_source:
                print("  ✅ Mini scrape test passed!")
                result = True
            else:
                print("  ⚠️  Page loaded but no building content found")
                result = True  # Still count as success
        else:
            print("  ❌ Page load timeout")
            result = False
        
        driver.quit()
        return result
        
    except Exception as e:
        print(f"  ❌ Mini scrape test failed: {str(e)[:100]}")
        return False

def main():
    """Run all tests."""
    print("🧪 LeasingHub Scraper Setup Test")
    print("=" * 40)
    
    tests = [
        ("Package Imports", test_imports),
        ("Chrome Setup", test_chrome_setup), 
        ("LeasingHub Access", test_leasinghub_access),
        ("File Permissions", test_file_permissions),
        ("Scraper Files", test_scraper_files),
        ("Mini Scrape", run_mini_scrape_test)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        print(f"\n{'='*50}")
        print(f"🧪 {test_name}")
        print("=" * 50)
        
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
            results[test_name] = False
    
    # Summary
    print(f"\n{'='*50}")
    print("📊 TEST SUMMARY")
    print("=" * 50)
    
    passed = sum(results.values())
    total = len(results)
    
    for test_name, passed_test in results.items():
        status = "✅ PASS" if passed_test else "❌ FAIL"
        print(f"{status} {test_name}")
    
    print(f"\n📈 Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 ALL TESTS PASSED!")
        print("✅ Your setup is ready for scraping!")
        print("\nNext steps:")
        print("1. Run: python run_leasinghub_scraper.py --check")
        print("2. Run: python run_leasinghub_scraper.py")
    else:
        print(f"\n⚠️  {total - passed} tests failed!")
        print("❌ Please fix the issues before running the scraper.")
        
        if not results.get("Package Imports", False):
            print("\n💡 Try: pip install pandas selenium beautifulsoup4 tqdm undetected-chromedriver")
        
        if not results.get("Chrome Setup", False):
            print("\n💡 Make sure Chrome browser is installed and updated")

if __name__ == "__main__":
    main() 