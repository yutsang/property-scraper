"""Kedro nodes for the buildings-enumeration pulls (Centaline blocks, Midland
per-estate buildings). Both wrap a resumable, checkpointed puller (see
`utils/centaline_buildings_client.py` / `utils/midland_res_estate_buildings_client.py`,
also used by the standalone CLIs in scripts/) so a first full run takes hours
but every subsequent run is a near-instant no-op unless the upstream estate
catalog has grown — the checkpoint is what makes that cheap, not a time-based
skip, so no separate gating is needed here.
"""
from __future__ import annotations

import logging
from typing import Any, Dict

import pandas as pd

from ...utils.centaline_buildings_client import CentalineBuildingsPuller, RateLimited as CentalineRateLimited
from ...utils.midland_res_estate_buildings_client import (
    MidlandEstateBuildingsPuller,
    RateLimited as MidlandRateLimited,
)

logger = logging.getLogger(__name__)


def pull_centaline_building_blocks(
    estate_listings: pd.DataFrame,
    params: Dict[str, Any],
) -> pd.DataFrame:
    """Phase -> block-level names via ConsumptionTableEstateMenu (see
    utils/centaline_buildings_client.py for endpoint details)."""
    bm_params = params.get("building_master", {})
    if not bm_params.get("enable_centaline_buildings_pull", True):
        return pd.DataFrame(columns=["phase_code", "block_name", "block_type_code"])

    if estate_listings.empty or "nuxt_typeCode" not in estate_listings.columns:
        logger.warning("No estate listings with nuxt_typeCode — skipping Centaline buildings pull")
        return pd.DataFrame(columns=["phase_code", "block_name", "block_type_code"])

    phase_codes = sorted(
        c for c in estate_listings["nuxt_typeCode"].dropna().astype(str).unique() if c.startswith("2-")
    )
    out_dir = bm_params.get("centaline_buildings_out", "data/centaline_buildings_full")
    delay = float(bm_params.get("centaline_buildings_delay", 0.3))
    puller = CentalineBuildingsPuller(out_dir, delay)
    try:
        puller.run(phase_codes)
    except CentalineRateLimited as exc:
        logger.warning("Centaline buildings pull stopped early (%s) — returning partial results", exc)

    rows = puller.load_all_rows()
    logger.info("Centaline buildings: %d blocks across %d phase codes on disk", len(rows), len(puller.done))
    return pd.DataFrame(rows, columns=["phase_code", "block_name", "block_type_code"])


def pull_midland_estate_buildings(
    estates: pd.DataFrame,
    params: Dict[str, Any],
) -> pd.DataFrame:
    """Per-estate building enumeration via search/v2/filters (see
    utils/midland_res_estate_buildings_client.py for the known partial-coverage
    caveat — this is NOT a guaranteed-complete building list)."""
    bm_params = params.get("building_master", {})
    if not bm_params.get("enable_midland_buildings_pull", True):
        return pd.DataFrame()

    if estates.empty or "id" not in estates.columns:
        logger.warning("No estates with an 'id' column — skipping Midland buildings pull")
        return pd.DataFrame()

    estate_ids = sorted(estates["id"].dropna().astype(str).unique())
    out_dir = bm_params.get("midland_buildings_out", "data/midland_res_estate_buildings_full")
    delay = float(bm_params.get("midland_buildings_delay", 0.3))
    puller = MidlandEstateBuildingsPuller(out_dir, delay)
    try:
        puller.run(estate_ids)
    except MidlandRateLimited as exc:
        logger.warning("Midland buildings pull stopped early (%s) — returning partial results", exc)

    rows = puller.load_all_rows()
    logger.info("Midland buildings: %d buildings across %d estates on disk", len(rows), len(puller.done))
    return pd.json_normalize(rows) if rows else pd.DataFrame()
