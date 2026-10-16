#!/usr/bin/env python3
"""Standalone CLI for the Midland residential building-per-estate pull.

The puller itself lives in
`src/property_scraper/utils/midland_res_estate_buildings_client.py`
(MidlandEstateBuildingsPuller), shared with the Kedro node in
`src/property_scraper/pipelines/building_master/buildings.py::pull_midland_estate_buildings`.
This script is a thin wrapper for manual/ad-hoc runs outside Kedro.

Usage
-----
  python scripts/midland_res_estate_buildings_pull.py                # full pull, resumable
  python scripts/midland_res_estate_buildings_pull.py --estate-ids E12168 E00118
  python scripts/midland_res_estate_buildings_pull.py --delay 0.5
Re-run the same command to resume; completed estate ids are skipped.
"""
from __future__ import annotations

import argparse
import logging
import sys

import pandas as pd

sys.path.insert(0, "src")
from property_scraper.utils.midland_res_estate_buildings_client import (  # noqa: E402
    DEFAULT_OUT,
    RateLimited,
    MidlandEstateBuildingsPuller,
)

ESTATE_PARQUET = "data/01_raw/midland_res_estates.parquet"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("midland_res_estate_buildings_pull")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--delay", type=float, default=0.3, help="base delay between calls (s)")
    ap.add_argument("--estate-ids", nargs="*", help="specific estate ids (default: all from estate parquet)")
    args = ap.parse_args()

    if args.estate_ids:
        estate_ids = args.estate_ids
    else:
        df = pd.read_parquet(ESTATE_PARQUET, columns=["id"])
        estate_ids = sorted(df["id"].dropna().astype(str).unique())
        logger.info("loaded %d unique estate ids from %s", len(estate_ids), ESTATE_PARQUET)

    try:
        MidlandEstateBuildingsPuller(args.out, args.delay).run(estate_ids)
    except RateLimited as exc:
        logger.error("STOPPING (resumable): %s", exc)
        logger.error("re-run the same command to resume from here.")
        raise SystemExit(2)


if __name__ == "__main__":
    main()
