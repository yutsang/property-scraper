# Node Execution Tracking

This document describes the node execution tracking system implemented to handle Source A's anti-scraping measures while still reacting to real website changes.

## Overview

The node execution tracker stores:

1. Execution metadata such as last run time and record counts.
2. Local dataset state such as file validity, row count, and max transaction date.
3. Lightweight live website state such as sitemap lastmod or estate page-1 change signals.

The goal is to answer:

- "Has this node run before?"
- "Is the local dataset still valid?"
- "Does the website appear to have changed since the last successful run?"

## Node Types and Rules

### 1. Transaction Nodes (`transaction`)
- **Primary rule**: Compare live website freshness signals plus local dataset freshness.
- **Fallback rule**: If live probing fails, use local dataset freshness instead of forcing a huge full rerun.
- **Rationale**: Transaction data changes frequently, but we should avoid both redundant runs and accidental historical rescans when local files are broken.
- **Examples**: 
  - `transaction_data_scraper` (Source A Residential)
  - `scrape_transaction` (Source A OIR)
  - `fetch_source_b_transactions` (Source B Residential)
  - `source_b_commercial_scrape_trans` (Source B ICI)

### 2. Estate Nodes (`estate`)
- **Primary rule**: Compare page-1 website counts / totals or equivalent live probes.
- **Fallback rule**: If probes fail, keep existing data and only rescrape conservatively.
- **Rationale**: Estate data should be driven by upstream changes, not a fixed day count alone.
- **Examples**:
  - `estate_listing_scraper` (Source A Residential)

### 3. Default Nodes (`default`)
- **Rule**: Always run unless a caller provides a custom live-state decision.
- **Rationale**: Processing and enrichment nodes do not hit external sites directly.
- **Examples**:
  - Data processing nodes
  - Data enrichment nodes
  - Merge operations

## Implementation

### Core Components

1. **NodeExecutionTracker Class** (`src/property_scraper/utils/node_tracker.py`)
   - Manages execution history in JSON format
   - Stores live-state hashes and dataset summaries
   - Records both decisions and successful executions

2. **Integration Points**
  - Nodes can record a run/skip decision before scraping
  - Nodes record execution metadata upon completion
  - Existing data can be returned when live state indicates no change

### Usage in Nodes

```python
from ...utils.node_tracker import evaluate_node_run, record_node_decision, record_node_execution

def scrape_transaction_data(area_df, params):
    node_name = "transaction_data_scraper"
    live_state = probe_live_site_state()
    decision = evaluate_node_run(
        node_name=node_name,
        node_type="transaction",
        data_file_path="data/01_raw/centaline_res_trans_lv_0.parquet",
        live_state=live_state,
    )
    record_node_decision(
        node_name=node_name,
        node_type="transaction",
        should_run=decision["should_run"],
        reason=decision["reason"],
        live_state=live_state,
        dataset_state=decision.get("dataset_state"),
    )
    if not decision["should_run"]:
        return load_existing_data()

    record_node_execution(
        node_name="transaction_data_scraper",
        node_type="transaction",
        metadata={
            "records_processed": len(combined_df),
            "areas_processed": len(area_df),
            "execution_time": datetime.now().isoformat()
        }
    )
    
    return combined_df
```

## Configuration

### Tracking File Location
- **Default**: `data/node_execution_tracker.json`
- **Format**: JSON with execution history
- **Structure**:
```json
{
  "node_name": {
    "last_run": "2024-01-15T10:30:00",
    "node_type": "transaction",
    "execution_count": 5,
    "metadata": {
      "records_processed": 1500,
      "areas_processed": 18
    }
  }
}
```

### Customization
You can modify the tracking file location by passing a custom path to the NodeExecutionTracker constructor:

```python
tracker = NodeExecutionTracker(tracking_file="custom/path/tracker.json")
```

## Testing

Suggested test coverage:

1. Corrupt / empty parquet detection for transaction datasets.
2. Live-state hashing and unchanged-state skip behavior.
3. Probe failure fallback behavior.
4. Reset-node and reset-all behavior.

## Monitoring

### Check Node Status
```python
from property_scraper.utils.node_tracker import get_node_status

status = get_node_status("transaction_data_scraper")
print(f"Last run: {status['last_run']}")
print(f"Should run: {status['should_run']}")
print(f"Execution count: {status['execution_count']}")
```

### View All Node Statuses
```python
tracker = get_node_tracker()
all_statuses = tracker.get_all_node_statuses()
for node_name, status in all_statuses.items():
    print(f"{node_name}: {status['should_run']}")
```

## Benefits

1. **Anti-Scraping Protection**: Prevents excessive requests that could trigger blocking
2. **Efficiency**: Avoids unnecessary work and processing time
3. **Data Freshness**: Ensures appropriate update frequency for different data types
4. **Monitoring**: Provides visibility into pipeline execution patterns
5. **Flexibility**: Easy to adjust rules for different node types

## Troubleshooting

### Reset Node History
```python
tracker = get_node_tracker()
tracker.reset_node("node_name")  # Reset specific node
tracker.reset_all_nodes()        # Reset all nodes
```

### Force Execution
To force a node to run despite tracking rules, reset its tracker entry or set the relevant rerun parameter in `conf/base/parameters.yml`.

### Debug Issues
- Check the tracking file for corruption
- Verify node names match between pipeline definition and tracking calls
- Ensure proper node type classification

## Future Enhancements

1. **Deeper site probes**: Use richer transaction totals or district-level signals where lightweight probes are reliable.
2. **Integration**: Web UI for monitoring and management.
3. **Notifications**: Alerts when probes repeatedly fail or local datasets become corrupt.