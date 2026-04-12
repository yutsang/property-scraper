# Property Scraper

A comprehensive property data scraping and processing pipeline built with Kedro framework. This project scrapes property transaction data from multiple sources including Source A and Source B, processes and enriches the data, and outputs standardized datasets for analysis.

## Features

- **Multi-source Data Collection**: Scrapes from Source A and Source B across residential and commercial domains
- **Intelligent Node Tracking**: Prevents redundant scraping with configurable execution intervals
- **Data Enrichment**: Estate name matching using fuzzy string matching
- **Data Cleansing**: Comprehensive cleaning and standardization of property data
- **Incremental Processing**: Efficient handling of large datasets with incremental updates
- **Live Freshness Checks**: Source A RES can compare lightweight website signals before deciding whether to scrape
- **Excel Export**: Automated generation of Excel reports with date-range splitting

## Data Sources

- **Source A Residential**: Property listings and transaction data
- **Source A Commercial**: Office, industrial, and retail property data
- **Source B Residential**: Residential property transactions and estate details
- **Source B Commercial**: Industrial, commercial, and investment property data

## Pipeline Structure

The project follows a modular pipeline structure:

```
src/property_scraper/pipelines/
├── source_a_res/     # Source A residential data
├── source_a_commercial/     # Source A commercial data
├── source_b_res/       # Source B residential data
├── source_b_commercial/       # Source B commercial data
└── data_process/      # Data cleaning and processing
```

## Current Status

- **Source A residential** now uses Playwright for the JS-heavy estate and transaction paths.
- **Live website checks** are used to avoid unnecessary reruns when upstream state has not changed.
- **Backup-aware recovery** is available for corrupt Source A RES transaction parquet files so a broken incremental file does not silently trigger a huge historical rescrape.

## Documentation

- [Source A rescraping guide](docs/source_a_res_rescraping.md)
- [Node tracking and live freshness checks](docs/node_tracking.md)
- [Development log / current progress](docs/DEVLOG.md)

## Setup

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Install Playwright browsers** (required for Source A residential scraping; the PyPI package alone does not ship Chromium):
   ```bash
   python -m playwright install chromium
   ```

3. **Configure Parameters**:
   Edit `conf/base/parameters.yml` to customize scraping behavior, node tracking intervals, and data processing options.

4. **Run the Pipeline**:
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
kedro run --pipeline=source_a_res
kedro run --pipeline=source_b_commercial
```

### Reset Node Tracking
To reset a tracked node or all tracked nodes, use the node tracker utility instead of the old reset script references:
```bash
python -c "from property_scraper.utils.node_tracker import get_node_tracker; get_node_tracker().reset_node('transaction_data_scraper')"
python -c "from property_scraper.utils.node_tracker import get_node_tracker; get_node_tracker().reset_all_nodes()"
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

1. **Corrupt Source A RES transaction parquet**: restore the latest backup or use the built-in recovery path instead of forcing a full historical rerun.
2. **Rate Limiting / Slow Runs**: adjust Playwright worker count, delays, and page limits in `conf/base/parameters.yml`.
3. **Live probe failures**: if sitemap or page probes fail, check connectivity/TLS first before forcing a full rescrape.
4. **`BrowserType.launch: Executable doesn't exist` / missing `headless_shell`**: install Playwright’s bundled browser in the same environment you use for `kedro run` (`python -m playwright install chromium`).

### Debug Scripts

- `scripts/backup_source_a_res_once.py`: one-time backup of Source A residential outputs before rerun
- `scripts/backup_and_rescrape_source_a_transactions.py`: backup helper for Source A residential transaction rescrapes
- `notebooks/source_a_playwright_test.py`: exploratory Playwright checks for Source A pages

## License

This project is proprietary and all rights are reserved by the copyright holder (TSANG Yu).

No part of this software or its associated files may be used, copied, modified, merged, published, distributed, sublicensed, or sold in any form or by any means, in whole or in part, without prior written authorization from the copyright holder.

For licensing inquiries, please contact me.

## Contributing

This repository is currently maintained as a proprietary project. Internal contributors should work through normal branch / PR review, but public fork-and-PR workflow is not the intended default.

## Support

For issues and questions, please check the existing issues or create a new one in the repository.

