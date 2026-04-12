from typing import Any, Dict

import pandas as pd

from ...utils.source_a_health_check import check_source_a_health
from ...utils.source_a_sitemap import update_area_codes_from_sitemap as _update_area_codes_impl


def check_source_a_health_node(params: Dict[str, Any]) -> None:
    """Kedro node: Validate API freshness before pipeline run."""
    check_source_a_health(
        params,
        max_stale_days=params.get("source_a_res", {}).get("health_check_max_stale_days", 10),
    )


def update_area_codes_from_sitemap(params: Dict[str, Any]) -> pd.DataFrame:
    """Kedro node: Update area codes from Source A sitemap, then return the DataFrame."""
    source_settings = params.get("source_a_res", {})
    area_path = source_settings.get(
        "area_code_path",
        "data/01_raw/Centanet_Res_Area_Code.csv",
    )
    return _update_area_codes_impl(
        area_code_path=area_path,
        source_config=source_settings.get("site", {}),
    )
