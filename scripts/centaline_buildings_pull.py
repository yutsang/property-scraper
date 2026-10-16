#!/usr/bin/env python3
"""Standalone CLI for the Centaline residential block/building pull.

The puller itself lives in
`src/property_scraper/utils/centaline_buildings_client.py` (CentalineBuildingsPuller),
shared with the Kedro node in
`src/property_scraper/pipelines/building_master/buildings.py::pull_centaline_building_blocks`.
This script is a thin wrapper for manual/ad-hoc runs outside Kedro.

Usage
-----
  python scripts/centaline_buildings_pull.py                # full pull, resumable
  python scripts/centaline_buildings_pull.py --codes 2-SDPPWPPHKS
  python scripts/centaline_buildings_pull.py --delay 0.5
Re-run the same command to resume; completed codes are skipped.
"""
from __future__ import annotations

import argparse
import logging
import sys

import pandas as pd

sys.path.insert(0, "src")
from property_scraper.utils.centaline_buildings_client import (  # noqa: E402
    DEFAULT_OUT,
    RateLimited,
    CentalineBuildingsPuller,
)

ESTATE_PARQUET = "data/01_raw/centaline_estate_lv_1.parquet"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("centaline_buildings_pull")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--delay", type=float, default=0.3, help="base delay between calls (s)")
    ap.add_argument("--codes", nargs="*", help="specific phase typeCodes (default: all from estate parquet)")
    args = ap.parse_args()

    if args.codes:
        codes = args.codes
    else:
        df = pd.read_parquet(ESTATE_PARQUET, columns=["nuxt_typeCode"])
        codes = sorted(
            c for c in df["nuxt_typeCode"].dropna().astype(str).unique() if c.startswith("2-")
        )
        logger.info("loaded %d unique phase codes from %s", len(codes), ESTATE_PARQUET)

    try:
        CentalineBuildingsPuller(args.out, args.delay).run(codes)
    except RateLimited as exc:
        logger.error("STOPPING (resumable): %s", exc)
        logger.error("re-run the same command to resume from here.")
        raise SystemExit(2)


if __name__ == "__main__":
    main()
