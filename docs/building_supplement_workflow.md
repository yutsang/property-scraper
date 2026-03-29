# Building Supplement Workflow

This workflow helps you manually supplement missing building masters through one
Excel workbook in `data/02_intermediate/` and writes approved rows directly back
into the source building parquet datasets.

## What It Produces

- `data/02_intermediate/buildings.xlsx`
- `data/03_primary/consolidated_commercial_building_master.parquet`

The workbook is the editing surface. All four source systems get their own
workbook tab, and approved rows are written back into the matching source
building parquet file with a manual provenance marker.

## Recommended Loop

1. Build or refresh the workbook.

```bash
PYTHONPATH=src python scripts/build_manual_building_review_queue.py
```

2. Open `data/02_intermediate/buildings.xlsx`.

3. Review the source tabs:
- `centaline_res`
- `centaline_oir`
- `midland_res`
- `midland_ici`

Each tab now mixes:
- `native` rows already known from the source building or estate lists
- `unmatched_candidate` rows ranked by `occurrence_count`
- `manual_approved` rows that you have marked for inclusion

4. Fill the manual columns directly in the source tab row:
- `manual_canonical_name`
- `manual_address`
- `manual_completion_year`
- `manual_management_company`
- `manual_url`
- `manual_notes`
- `manual_include`

Approval rule:
- a row is treated as approved when `manual_include` is truthy and
  `manual_canonical_name` is not empty.

5. Re-run the workbook builder after manual edits.

```bash
PYTHONPATH=src python scripts/build_manual_building_review_queue.py
```

This will:
- rebuild each source sheet while preserving your manual edits,
- keep old unmarked rows in place and append newly discovered unmatched rows,
- rewrite approved manual rows into the source building parquet files,
- refresh `data/03_primary/consolidated_commercial_building_master.parquet`.

The source parquet writeback updates:
- `data/02_intermediate/centanet_oir_details.parquet`
- `data/02_intermediate/midland_ici_building_details.parquet`
- `data/01_raw/centaline_estate_lv_2.parquet`
- `data/01_raw/midland_res_estates.parquet`

Manual writeback rows are marked with `record_source=manual_workbook` and
`is_manual_record=True`, so native scraped rows can still be counted
independently for freshness and node-skip checks.

6. Re-run the commercial pipelines and downstream processing.

```bash
kedro run --pipeline commercial
kedro run --pipeline data_process
```

7. Open the workbook again and continue working down the highest `occurrence_count`
   rows in each source tab.

## Match Order

For commercial data, the pipelines now follow this order:

1. Native source join
2. Manual exact key match using `source_join_key`
3. Manual reviewed name match using normalized name plus district, and zone for Centaline OIR
4. Remaining rows stay unmatched

## Output Fields

After the supplement is applied:

- Centaline OIR keeps `_match_method`, `match_origin`, and `record_source`
- Midland ICI now keeps `has_building_match`
- Midland ICI now keeps `building_match_method`
- Midland ICI now keeps `matched_building_name`
- Midland ICI now keeps `match_origin` and `record_source`

This makes the improvement visible in final outputs instead of hiding the mapping
status in intermediate tables only.
