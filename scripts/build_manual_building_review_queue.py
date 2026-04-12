#!/usr/bin/env python
"""
Build the authoritative building workbook and write approved rows back to source parquet.

This script reads current native building datasets and unmatched transaction gaps for all
four sources, writes one combined Excel sheet per source into a single workbook under
`data/02_intermediate/`, syncs approved manual rows into the source building parquet
datasets, and refreshes the consolidated commercial building master table.

Usage:
  PYTHONPATH=src python scripts/build_manual_building_review_queue.py
  PYTHONPATH=src python scripts/build_manual_building_review_queue.py --limit 500
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from property_scraper.utils.building_supplement import (
    build_mega_building_workbook,
    merge_manual_rows_into_source_buildings,
    write_mega_building_workbook,
)


ROOT = Path(__file__).resolve().parents[1]
PARAMS_PATH = ROOT / "conf/base/parameters.yml"


def load_webscraper_params() -> dict[str, Any]:
    with PARAMS_PATH.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)["webscraper"]


def load_optional_frame(path: Path, *, parquet: bool = True) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    if parquet:
        return pd.read_parquet(path)
    return pd.read_csv(path)


def load_source_c_frames() -> list[pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    for path in sorted((ROOT / "notebooks").glob("source_c*.csv")):
        try:
            frames.append(pd.read_csv(path))
        except Exception:
            continue
    return frames


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum rows per unmatched source tab. Defaults to all rows.",
    )
    args = parser.parse_args()

    params = load_webscraper_params()
    supplement_params = params["building_supplement"]
    include_limit = (
        args.limit
        if args.limit is not None
        else int(supplement_params.get("default_limit", 0))
    )

    source_a_commercial = load_optional_frame(ROOT / "data/03_primary/centaline_oir.parquet")
    source_b_commercial_base = load_optional_frame(ROOT / "data/02_intermediate/midland_ici_base.parquet")
    source_b_commercial_primary = load_optional_frame(ROOT / "data/03_primary/midland_ici.parquet")
    source_a_res = load_optional_frame(ROOT / "data/03_primary/centaline_res.parquet")
    source_b_res = load_optional_frame(ROOT / "data/03_primary/midland_res.parquet")
    source_c_frames = load_source_c_frames()
    centaline_buildings = load_optional_frame(
        ROOT / "data/02_intermediate/centanet_oir_details.parquet"
    )
    midland_buildings = load_optional_frame(
        ROOT / "data/02_intermediate/midland_ici_building_details.parquet"
    )
    source_a_res_buildings = load_optional_frame(
        ROOT / "data/01_raw/centaline_estate_lv_2.parquet"
    )
    source_b_res_buildings = load_optional_frame(
        ROOT / "data/01_raw/midland_res_estates.parquet"
    )

    workbook_path = ROOT / supplement_params["workbook_file"]
    consolidated_master_path = ROOT / supplement_params["consolidated_master_file"]

    workbook_sheets, approved_master, consolidated_master = build_mega_building_workbook(
        source_a_commercial=source_a_commercial,
        source_b_commercial_base=source_b_commercial_base,
        source_b_commercial_primary=source_b_commercial_primary,
        source_a_res=source_a_res,
        source_b_res=source_b_res,
        source_a_commercial_buildings=centaline_buildings,
        source_b_commercial_buildings=midland_buildings,
        source_a_res_buildings=source_a_res_buildings,
        source_b_res_buildings=source_b_res_buildings,
        source_c_frames=source_c_frames,
        workbook_path=workbook_path,
        include_limit=include_limit,
    )
    updated_source_frames = merge_manual_rows_into_source_buildings(
        approved_master=approved_master,
        source_a_commercial_buildings=centaline_buildings,
        source_b_commercial_buildings=midland_buildings,
        source_a_res_buildings=source_a_res_buildings,
        source_b_res_buildings=source_b_res_buildings,
    )

    consolidated_master_path.parent.mkdir(parents=True, exist_ok=True)

    write_mega_building_workbook(workbook_path, workbook_sheets)
    updated_source_frames["source_a_commercial"].to_parquet(
        ROOT / "data/02_intermediate/centanet_oir_details.parquet",
        index=False,
    )
    updated_source_frames["source_b_commercial"].to_parquet(
        ROOT / "data/02_intermediate/midland_ici_building_details.parquet",
        index=False,
    )
    updated_source_frames["source_a_res"].to_parquet(
        ROOT / "data/01_raw/centaline_estate_lv_2.parquet",
        index=False,
    )
    updated_source_frames["source_b_res"].to_parquet(
        ROOT / "data/01_raw/midland_res_estates.parquet",
        index=False,
    )
    consolidated_master.to_parquet(consolidated_master_path, index=False)

    print(f"Saved workbook: {workbook_path}")
    print("Updated source building parquet files:")
    print(f"  - {ROOT / 'data/02_intermediate/centanet_oir_details.parquet'}")
    print(f"  - {ROOT / 'data/02_intermediate/midland_ici_building_details.parquet'}")
    print(f"  - {ROOT / 'data/01_raw/centaline_estate_lv_2.parquet'}")
    print(f"  - {ROOT / 'data/01_raw/midland_res_estates.parquet'}")
    print(f"Saved consolidated commercial master: {consolidated_master_path}")
    print(
        "Rows by sheet: "
        + ", ".join(
            f"{sheet_name}={len(frame):,}"
            for sheet_name, frame in workbook_sheets.items()
        )
    )


if __name__ == "__main__":
    main()
