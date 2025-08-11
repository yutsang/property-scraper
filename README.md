# Property Scraper

A comprehensive property data scraping and processing pipeline built with Kedro framework. This project scrapes property transaction data from multiple sources including Centaline and Midland, processes and enriches the data, and outputs standardized datasets for analysis.

## Features

- **Multi-source Data Collection**: Scrapes from Centaline (Residential & OIR) and Midland (Residential & ICI)
- **Intelligent Node Tracking**: Prevents redundant scraping with configurable execution intervals
- **Data Enrichment**: Estate name matching using fuzzy string matching
- **Data Cleansing**: Comprehensive cleaning and standardization of property data
- **Incremental Processing**: Efficient handling of large datasets with incremental updates
- **Excel Export**: Automated generation of Excel reports with date-range splitting

## Data Sources

- **Centaline Residential**: Property listings and transaction data
- **Centaline OIR**: Office, Industrial, and Retail property data
- **Midland Residential**: Residential property transactions and estate details
- **Midland ICI**: Industrial, Commercial, and Investment property data

## Pipeline Structure

The project follows a modular pipeline structure:

```
src/property_scraper/pipelines/
├── centaline_res/     # Centaline residential data
├── centaline_oir/     # Centaline OIR data
├── midland_res/       # Midland residential data
├── midland_ici/       # Midland ICI data
└── data_process/      # Data cleaning and processing
```

## Setup

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Parameters**:
   Edit `conf/base/parameters.yml` to customize scraping behavior, node tracking intervals, and data processing options.

3. **Run the Pipeline**:
   ```bash
   kedro run
   ```

## Usage

### Full Pipeline Run
```bash
kedro run
```

### Run Specific Pipeline
```bash
kedro run --pipeline=centaline_res
kedro run --pipeline=midland_ici
```

### Reset Node Tracking
To force re-scraping of specific data sources:
```bash
python reset_midland_ici_nodes.py
python reset_all_midland_nodes.py
```

## Data Flow

1. **Raw Data Collection**: Scrapes property listings and transaction data
2. **Intermediate Processing**: Joins transaction data with building/estate details
3. **Data Enrichment**: Matches estate names using fuzzy string matching
4. **Data Cleansing**: Standardizes formats, cleans data, and removes unwanted columns
5. **Final Output**: Generates Excel reports split by date ranges (2020-2022, 2023-current)

## Output Files

- **Parquet Files**: Processed data in `data/03_primary/`
- **Excel Reports**: 
  - `data/03_primary/Combined_Dateset_2020_2022.xlsx`
  - `data/03_primary/Combined_Dateset_2023_2025.xlsx`

## Configuration

Key configuration options in `conf/base/parameters.yml`:

- **Node Tracking**: Configure execution intervals for different node types
- **Scraping Parameters**: Request delays, retry limits, and pagination settings
- **Data Processing**: Column mappings, cleaning rules, and output formats

## Troubleshooting

1. **Empty Transaction Data**: Use reset scripts to force re-scraping
2. **Rate Limiting**: Adjust request delays in configuration
3. **Memory Issues**: Enable incremental processing for large datasets

### Debug Scripts

- `debug_estate_columns.py`: Check estate DataFrame structure
- `test_estate_scraping.py`: Test estate data fetching
- `test_node_tracker.py`: Verify node tracking functionality

## License

This project is proprietary and all rights are reserved by the copyright holder (TSANG Yu).

No part of this software or its associated files may be used, copied, modified, merged, published, distributed, sublicensed, or sold in any form or by any means, in whole or in part, without prior written authorization from the copyright holder.

For licensing inquiries, please contact me.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Support

For issues and questions, please check the existing issues or create a new one in the repository.

