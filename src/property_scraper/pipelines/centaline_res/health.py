from typing import Any, Dict

import pandas as pd

from ...utils.centaline_health_check import check_centaline_api_health
from ...utils.centaline_sitemap import update_area_codes_from_sitemap as _update_area_codes_impl


def check_centaline_api_health_node(params: Dict[str, Any]) -> None:
    """Kedro node: Validate API freshness before pipeline run."""
    check_centaline_api_health(
        params,
        max_stale_days=params.get("centaline_res", {}).get("health_check_max_stale_days", 10),
    )


def update_area_codes_from_sitemap(params: Dict[str, Any]) -> pd.DataFrame:
    """Kedro node: Update area codes from Centanet sitemap, then return the DataFrame."""
    area_path = params.get("centaline_res", {}).get(
        "area_code_path",
        "data/01_raw/Centanet_Res_Area_Code.csv",
    )
    return _update_area_codes_impl(area_code_path=area_path)
