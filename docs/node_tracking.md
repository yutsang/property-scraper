# Node Execution Tracking

This document describes the node execution tracking system implemented to handle Centaline's strict anti-scraping measures.

## Overview

The node execution tracker records when each pipeline node was last executed and prevents unnecessary re-runs based on configurable rules. This helps avoid triggering anti-scraping mechanisms while ensuring data freshness.

## Node Types and Rules

### 1. Transaction Nodes (`transaction`)
- **Rule**: Skip if already run on the same day
- **Rationale**: Transaction data changes frequently, but running multiple times per day is unnecessary and risky
- **Examples**: 
  - `transaction_data_scraper` (Centaline Residential)
  - `scrape_transaction` (Centaline OIR)
  - `fetch_midland_transactions` (Midland Residential)
  - `ml_ici_scrape_trans` (Midland ICI)

### 2. Estate Nodes (`estate`)
- **Rule**: Skip if run within the last 7 days
- **Rationale**: Estate information changes less frequently, but should be updated weekly
- **Examples**:
  - `estate_listing_scraper` (Centaline Residential)

### 3. Default Nodes (`default`)
- **Rule**: Always run (no restrictions)
- **Rationale**: Processing and enrichment nodes don't involve scraping
- **Examples**:
  - Data processing nodes
  - Data enrichment nodes
  - Merge operations

## Implementation

### Core Components

1. **NodeExecutionTracker Class** (`src/property_scraper/utils/node_tracker.py`)
   - Manages execution history in JSON format
   - Provides methods to check if nodes should run
   - Records execution metadata

2. **Integration Points**
   - Each scraping node checks execution status before running
   - Nodes record their execution upon completion
   - Existing data is returned if node should be skipped

### Usage in Nodes

```python
from ...utils.node_tracker import should_run_node, record_node_execution

def scrape_transaction_data(area_df, params):
    # Check if node should run
    node_name = "transaction_data_scraper"
    if not should_run_node(node_name, "transaction"):
        logger.info(f"Node '{node_name}' already run today - returning existing data")
        # Return existing data if available
        return load_existing_data()
    
    # ... scraping logic ...
    
    # Record execution
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

Run the test script to verify the functionality:

```bash
python test_node_tracker.py
```

This will:
1. Test node execution checks
2. Record sample executions
3. Verify skip logic works correctly
4. Show node status information

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
To force a node to run despite tracking rules, you can temporarily reset its history or modify the tracking file directly.

### Debug Issues
- Check the tracking file for corruption
- Verify node names match between pipeline definition and tracking calls
- Ensure proper node type classification

## Future Enhancements

1. **Dynamic Rules**: Configurable skip periods via parameters
2. **Conditional Logic**: Skip based on data freshness rather than time
3. **Integration**: Web UI for monitoring and management
4. **Notifications**: Alerts when nodes are skipped for extended periods 