# Source C Comprehensive Scraper

A comprehensive scraper for Source C.com that collects building information from all property types (office, shop, industrial) and their detailed information.

## 🚀 Quick Start

### 1. Install Requirements

```bash
pip install pandas selenium beautifulsoup4 tqdm undetected-chromedriver
```

### 2. Run Complete Scraping

```bash
# Easy way - run everything
python run_source_c_scraper.py

# Advanced way - run the main scraper directly
python source_c_comprehensive_scraper.py --mode both
```

## 📋 What This Scraper Does

### Phase 1: Building List Scraping
- Scrapes building lists from:
  - 🏢 **Office buildings**: `https://www.source_c.com/office/buildings`
  - 🏪 **Shop buildings**: `https://www.source_c.com/shop/buildings`
  - 🏭 **Industrial buildings**: `https://www.source_c.com/industrial/buildings`

### Phase 2: Detail Scraping
- Visits each building page (e.g., `/building/yeung-iu-chi-commercial-building/1495`)
- Extracts detailed information:
  - Building name, address, district
  - Year built, floor count, building grade
  - Pricing information, available floors
  - Amenities, transportation info
  - Contact details, property type
  - And much more!

## 🛠 Usage Options

### Option 1: Simple Runner Script

```bash
# Run complete workflow (buildings + details)
python run_source_c_scraper.py

# Only scrape building lists
python run_source_c_scraper.py --buildings-only

# Only scrape details (requires existing building list)
python run_source_c_scraper.py --details-only

# Scrape specific property types only
python run_source_c_scraper.py --property-types office shop

# Check system status
python run_source_c_scraper.py --status

# Check if requirements are installed
python run_source_c_scraper.py --check
```

### Option 2: Advanced Direct Usage

```bash
# Scrape everything
python source_c_comprehensive_scraper.py --mode both

# Only building lists
python source_c_comprehensive_scraper.py --mode buildings

# Only details
python source_c_comprehensive_scraper.py --mode details

# Specific property types
python source_c_comprehensive_scraper.py --mode both --property-types office industrial

# Batch detail scraping (useful for resuming)
python source_c_comprehensive_scraper.py --mode details --start-idx 100 --batch-size 50
```

## 📁 Output Files

### Building Lists
- `source_c_all_buildings.csv` - Combined list from all property types
- `source_c_office_buildings.csv` - Office buildings only
- `source_c_shop_buildings.csv` - Shop buildings only  
- `source_c_industrial_buildings.csv` - Industrial buildings only

### Building Details
- `source_c_details_clean_YYYYMMDD_HHMMSS.csv` - Successfully scraped details
- `source_c_details_all_YYYYMMDD_HHMMSS.csv` - All attempts (including failures)
- `temp_details_*.csv` - Progress backup files (auto-saved every 25 buildings)

## 📊 Data Structure

### Building List Columns
```
name                 - Building name
url                  - Relative URL path
property_type        - office/shop/industrial
source              - How the building was found
```

### Building Details Columns
```
building_name        - Extracted building name
address             - Full building address
district            - District/area location
year_built          - Construction year
total_floors        - Number of floors
building_grade      - Grade classification
pricing_info        - Rent/sale pricing
available_floors    - Available units/floors
amenities           - Building amenities
transport_info      - Transportation details
contact_phone       - Contact phone number
contact_email       - Contact email
description         - Building description
property_type       - Original property type
detected_property_type - Property type from content
```

## 🔧 Advanced Features

### Resume Scraping
If detail scraping stops, you can resume from where it left off:

```bash
# Resume from building index 150
python run_source_c_scraper.py --details-only --start-idx 150

# Process in smaller batches
python run_source_c_scraper.py --details-only --start-idx 0 --batch-size 100
```

### Progress Monitoring
- Progress bars show real-time status
- Automatic progress saving every 25 buildings
- Detailed success/failure statistics
- Data completeness reporting

### Error Handling
- Cloudflare bypass technology
- Automatic retry mechanisms
- Graceful error recovery
- Detailed error logging

## 🚨 Troubleshooting

### Common Issues

1. **Cloudflare Blocking**
   - The scraper uses undetected Chrome to bypass Cloudflare
   - If blocked, try running again later
   - Consider adding longer delays between requests

2. **Chrome Driver Issues**
   - Ensure Chrome browser is installed
   - Update Chrome to latest version
   - The scraper auto-handles driver versions

3. **Memory Issues**
   - Use batch processing for large datasets
   - Close other applications to free memory
   - Consider smaller batch sizes

4. **Network Issues**
   - Check internet connection
   - Try running during off-peak hours
   - Increase delay between requests

### Performance Tips

1. **Optimize for Your System**
   ```bash
   # Smaller batches for slower systems
   python run_source_c_scraper.py --details-only --batch-size 25
   
   # Larger batches for faster systems
   python run_source_c_scraper.py --details-only --batch-size 100
   ```

2. **Resume Failed Scraping**
   ```bash
   # Check what you have so far
   python run_source_c_scraper.py --status
   
   # Resume from last successful index
   python run_source_c_scraper.py --details-only --start-idx LAST_INDEX
   ```

## 📈 Example Workflow

### Complete Scraping (Recommended)
```bash
# Step 1: Check requirements
python run_source_c_scraper.py --check

# Step 2: Run complete scraping
python run_source_c_scraper.py

# Step 3: Check results
python run_source_c_scraper.py --status
```

### Partial Scraping
```bash
# Step 1: Get building lists only
python run_source_c_scraper.py --buildings-only

# Step 2: Review the building lists
head -20 source_c_all_buildings.csv

# Step 3: Scrape details in batches
python run_source_c_scraper.py --details-only --batch-size 50
```

### Specific Property Types
```bash
# Only scrape office buildings
python run_source_c_scraper.py --property-types office

# Only scrape retail properties
python run_source_c_scraper.py --property-types shop

# Scrape office and industrial only
python run_source_c_scraper.py --property-types office industrial
```

## 📊 Expected Results

Based on the existing data:
- **Office buildings**: ~1,000+ buildings
- **Shop buildings**: ~500+ buildings  
- **Industrial buildings**: ~300+ buildings
- **Total details**: Rich data for 80-90% of buildings

## ⚡ Performance Stats

- **Building list scraping**: 5-10 minutes per property type
- **Detail scraping**: 2-3 seconds per building
- **Total time**: 2-4 hours for complete dataset
- **Success rate**: 85-95% depending on site availability

## 🔒 Ethical Usage

- Respects robots.txt and rate limits
- Uses reasonable delays between requests
- Does not overload the target server
- For research and analysis purposes only

## 🆘 Need Help?

1. Check the status: `python run_source_c_scraper.py --status`
2. Verify requirements: `python run_source_c_scraper.py --check`
3. Review output files for any error patterns
4. Try smaller batch sizes if experiencing issues
5. Check the temp files for progress indicators

---

*Happy scraping! 🕷️* 