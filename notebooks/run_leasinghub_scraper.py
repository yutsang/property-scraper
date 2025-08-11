#!/usr/bin/env python3
"""
Simple script to run the LeasingHub Comprehensive Scraper

This script provides easy-to-use functions for scraping LeasingHub data:
1. Scrape building lists from all three property types
2. Scrape detailed information from each building
3. Combined workflow for complete data collection

Usage examples:
    python run_leasinghub_scraper.py                    # Run complete scraping
    python run_leasinghub_scraper.py --buildings-only   # Just scrape building lists
    python run_leasinghub_scraper.py --details-only     # Just scrape details (requires existing building list)
"""

import subprocess
import sys
import os
import argparse
from pathlib import Path

def run_command(cmd):
    """Run a command and return the result."""
    print(f"🚀 Running: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(f"⚠️  {result.stderr}")
    
    return result.returncode == 0

def scrape_buildings_only():
    """Scrape building lists from all property types."""
    print("🏢 SCRAPING BUILDING LISTS ONLY")
    print("🎯 Scraping ALL property types: office, shop, industrial")
    print("="*50)
    
    cmd = "python leasinghub_comprehensive_scraper.py --mode buildings --property-types office shop industrial"
    return run_command(cmd)

def scrape_details_only(start_idx=0, batch_size=None):
    """Scrape building details from existing building list."""
    print("🔍 SCRAPING BUILDING DETAILS ONLY")
    print("="*50)
    
    # Check if building list exists
    if not os.path.exists('leasinghub_all_buildings.csv'):
        print("❌ Building list not found! Run buildings scraping first.")
        return False
    
    cmd = f"python leasinghub_comprehensive_scraper.py --mode details --start-idx {start_idx}"
    if batch_size:
        cmd += f" --batch-size {batch_size}"
    
    return run_command(cmd)

def scrape_complete():
    """Run building list scraping for all property types."""
    print("🚀 LEASINGHUB BUILDING LIST SCRAPER")
    print("🎯 Scraping ALL property types: office, shop, industrial")
    print("="*60)
    
    cmd = "python leasinghub_comprehensive_scraper.py --property-types office shop industrial"
    return run_command(cmd)

def scrape_specific_property_types(property_types):
    """Scrape specific property types only."""
    print(f"🎯 SCRAPING SPECIFIC PROPERTY TYPES: {', '.join(property_types)}")
    print("="*60)
    
    valid_types = ['office', 'shop', 'industrial']
    for ptype in property_types:
        if ptype not in valid_types:
            print(f"❌ Invalid property type: {ptype}")
            print(f"   Valid types: {', '.join(valid_types)}")
            return False
    
    property_args = ' '.join(property_types)
    cmd = f"python leasinghub_comprehensive_scraper.py --mode both --property-types {property_args}"
    return run_command(cmd)

def check_requirements():
    """Check if required packages are installed."""
    required_packages = {
        'pandas': 'pandas',
        'selenium': 'selenium', 
        'beautifulsoup4': 'bs4',
        'tqdm': 'tqdm',
        'undetected-chromedriver': 'undetected_chromedriver'
    }
    
    missing_packages = []
    for package_name, import_name in required_packages.items():
        try:
            __import__(import_name)
        except ImportError:
            missing_packages.append(package_name)
    
    if missing_packages:
        print("❌ Missing required packages:")
        for package in missing_packages:
            print(f"   • {package}")
        print("\n💡 Install missing packages with:")
        print(f"   pip install {' '.join(missing_packages)}")
        return False
    
    return True

def show_status():
    """Show current status of scraped data."""
    print("📊 LEASINGHUB SCRAPING STATUS")
    print("="*40)
    
    files_info = [
        ('leasinghub_all_buildings.csv', 'Combined building list'),
        ('leasinghub_office_buildings.csv', 'Office buildings'),
        ('leasinghub_shop_buildings.csv', 'Shop buildings'),
        ('leasinghub_industrial_buildings.csv', 'Industrial buildings'),
    ]
    
    for filename, description in files_info:
        if os.path.exists(filename):
            try:
                import pandas as pd
                df = pd.read_csv(filename)
                print(f"✅ {description}: {len(df)} buildings")
            except ImportError:
                print(f"⚠️  {description}: File exists but pandas not available")
            except Exception:
                print(f"⚠️  {description}: File exists but couldn't read")
        else:
            print(f"❌ {description}: Not found")
    
    # Check for detail files
    detail_files = [f for f in os.listdir('.') if f.startswith('leasinghub_details_') and f.endswith('.csv')]
    if detail_files:
        print(f"\n🔍 Building detail files found:")
        for detail_file in sorted(detail_files)[-3:]:  # Show last 3
            try:
                import pandas as pd
                df = pd.read_csv(detail_file)
                print(f"   • {detail_file}: {len(df)} buildings")
            except ImportError:
                print(f"   • {detail_file}: Pandas not available")
            except Exception:
                print(f"   • {detail_file}: Couldn't read")
    else:
        print("\n❌ No building detail files found")

def main():
    parser = argparse.ArgumentParser(description='Easy LeasingHub Scraper Runner')
    parser.add_argument('--buildings-only', action='store_true',
                        help='Only scrape building lists')
    parser.add_argument('--details-only', action='store_true',
                        help='Only scrape building details')
    parser.add_argument('--property-types', nargs='+', 
                        choices=['office', 'shop', 'industrial'],
                        help='Specific property types to scrape')
    parser.add_argument('--start-idx', type=int, default=0,
                        help='Start index for detail scraping')
    parser.add_argument('--batch-size', type=int, default=None,
                        help='Batch size for detail scraping')
    parser.add_argument('--status', action='store_true',
                        help='Show current scraping status')
    parser.add_argument('--check', action='store_true',
                        help='Check requirements')
    
    args = parser.parse_args()
    
    # Change to the script's directory
    script_dir = Path(__file__).parent
    os.chdir(script_dir)
    
    if args.check:
        success = check_requirements()
        sys.exit(0 if success else 1)
    
    if args.status:
        show_status()
        return
    
    # Check requirements before proceeding
    if not check_requirements():
        print("\n❌ Please install missing requirements first!")
        sys.exit(1)
    
    # Determine what to do based on arguments
    success = False
    
    if args.buildings_only:
        success = scrape_buildings_only()
    elif args.details_only:
        success = scrape_details_only(args.start_idx, args.batch_size)
    elif args.property_types:
        success = scrape_specific_property_types(args.property_types)
    else:
        # Default: run complete scraping
        success = scrape_complete()
    
    if success:
        print("\n🎉 Scraping completed successfully!")
        print("\n📊 Final status:")
        show_status()
    else:
        print("\n❌ Scraping failed!")
        sys.exit(1)

if __name__ == "__main__":
    main() 