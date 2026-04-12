#!/usr/bin/env python
"""
Audit live commercial website counts against local Source B ICI / Source A OIR datasets.

This script is intentionally rate-limited because Source A's APIs time out when
they are probed too aggressively. It compares:
- Source B ICI transaction counts by `distCode`
- Source B ICI building counts by district + property type
- Source A OIR transaction counts by district code
- Source A OIR building counts by district code

Outputs:
- CSV files for each audit table
- A JSON summary with totals, deltas, timeout counts, and Source B duplicate clues

Usage:
  PYTHONPATH=src python scripts/audit_commercial_live_counts.py
  PYTHONPATH=src python scripts/audit_commercial_live_counts.py --output-dir data/08_reporting/commercial_live_audit
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional

import pandas as pd
import requests
import yaml


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "data/08_reporting/commercial_live_audit"


def load_webscraper_params() -> Dict[str, Any]:
    params_path = ROOT / "conf/base/parameters.yml"
    with params_path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)["webscraper"]


def request_with_backoff(
    session: requests.Session,
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    json_payload: Optional[Dict[str, Any]] = None,
    timeout: float = 20.0,
    max_retries: int = 4,
    sleep_seconds: float = 1.0,
) -> requests.Response:
    last_error: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            response = session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_payload,
                timeout=timeout,
            )
            response.raise_for_status()
            return response
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt == max_retries:
                break
            time.sleep(sleep_seconds * attempt)
    assert last_error is not None
    raise last_error


def write_outputs(name: str, frame: pd.DataFrame, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_dir / f"{name}.csv", index=False)


def audit_midland_transactions(params: Dict[str, Any]) -> tuple[pd.DataFrame, Dict[str, Any]]:
    raw_path = ROOT / "data/01_raw/midland_ici_trans.parquet"
    db = pd.read_parquet(raw_path)
    db_counts = db.groupby("dist_code").size().to_dict()
    district_names = (
        db.groupby("dist_code")["dist_name_zh"]
        .agg(lambda series: series.dropna().astype(str).mode().iloc[0] if not series.dropna().empty else None)
        .to_dict()
    )

    dedup_cols = ["tx_date", "building_id", "floor", "flat", "tx_type", "sell", "rent", "area"]
    duplicate_rows = int(db.duplicated(subset=dedup_cols, keep="last").sum())

    session = requests.Session()
    midland_params = params["source_b_commercial"]
    request_with_backoff(
        session,
        "GET",
        midland_params["transaction_landing_url"],
        headers=midland_params["transaction_headers_main"],
        timeout=20.0,
        max_retries=3,
        sleep_seconds=2.0,
    )

    base_params = dict(midland_params["transaction_api_params"])
    base_params.update({"page": 1, "limit": 1, "lang": midland_params["transaction_lang_zh"]})

    global_response = request_with_backoff(
        session,
        "GET",
        midland_params["transaction_url"],
        headers=midland_params["transaction_headers_api"],
        params=base_params,
        timeout=20.0,
        max_retries=3,
        sleep_seconds=2.0,
    ).json()
    global_total = int((global_response[0] or {}).get("count", 0)) if global_response else 0

    rows: list[Dict[str, Any]] = []
    for dist_code in sorted(code for code in db_counts if pd.notna(code)):
        params_copy = dict(base_params)
        params_copy["distCode"] = dist_code
        response = request_with_backoff(
            session,
            "GET",
            midland_params["transaction_url"],
            headers=midland_params["transaction_headers_api"],
            params=params_copy,
            timeout=20.0,
            max_retries=3,
            sleep_seconds=2.0,
        ).json()
        live_count = int((response[0] or {}).get("count", 0)) if response else 0
        db_count = int(db_counts.get(dist_code, 0))
        rows.append(
            {
                "dist_code": dist_code,
                "dist_name_zh": district_names.get(dist_code),
                "live_count": live_count,
                "db_count": db_count,
                "delta_db_minus_live": db_count - live_count,
            }
        )
        time.sleep(0.1)

    result = pd.DataFrame(rows).sort_values(
        by=["delta_db_minus_live", "dist_code"],
        ascending=[False, True],
    )
    summary = {
        "global_live_total": global_total,
        "district_live_total": int(result["live_count"].sum()),
        "db_total": int(len(db)),
        "delta_db_minus_live": int(len(db) - global_total),
        "districts_with_delta": int((result["delta_db_minus_live"] != 0).sum()),
        "duplicate_rows_on_scraper_key": duplicate_rows,
        "largest_positive_delta": result.head(5).to_dict(orient="records"),
        "largest_negative_delta": result.sort_values("delta_db_minus_live").head(5).to_dict(orient="records"),
        "likely_causes": [
            "Current raw parquet still contains duplicate rows on the scraper dedupe key.",
            "The scraper skips a full refetch when API totals are close to the existing row count.",
            "Live site churn can move the current total slightly between audit time and the last scrape.",
        ],
    }
    return result, summary


def audit_midland_buildings(params: Dict[str, Any]) -> tuple[pd.DataFrame, Dict[str, Any]]:
    db = pd.read_parquet(ROOT / "data/01_raw/midland_ici_buildings.parquet")
    area_codes = pd.read_csv(ROOT / "data/01_raw/midland_ici_area_code.csv")
    area_codes = area_codes[area_codes["ID"] != 0].copy()
    db_counts = db.groupby(["district_id", "property_type_code"]).size().to_dict()

    property_types = {
        "mr_comm": "Office",
        "mr_ind": "Industrial",
        "mr_shop": "Shop",
    }
    graphql_query = """
        query ($districtId: ID, $query: String, $sbu: String) {
          buildings(districtId: $districtId, nameSearch: $query, sbu: $sbu) {
            id
          }
        }
    """

    session = requests.Session()
    headers = params["source_b_commercial"]["headers"]
    rows: list[Dict[str, Any]] = []

    for district in area_codes.itertuples(index=False):
        for sbu, property_type in property_types.items():
            payload = {
                "query": graphql_query,
                "variables": {"districtId": int(district.ID), "query": "", "sbu": sbu},
            }
            response = request_with_backoff(
                session,
                "POST",
                params["source_b_commercial"]["buildings_url"],
                headers=headers,
                json_payload=payload,
                timeout=20.0,
                max_retries=3,
                sleep_seconds=2.0,
            ).json()
            live_count = len(response["data"]["buildings"])
            db_count = int(db_counts.get((int(district.ID), sbu), 0))
            rows.append(
                {
                    "district_id": int(district.ID),
                    "district_name_en": district.Name_EN,
                    "property_type_code": sbu,
                    "property_type": property_type,
                    "live_count": live_count,
                    "db_count": db_count,
                    "delta_db_minus_live": db_count - live_count,
                }
            )
            time.sleep(0.1)

    result = pd.DataFrame(rows).sort_values(
        by=["delta_db_minus_live", "district_id", "property_type_code"],
        ascending=[False, True, True],
    )
    summary = {
        "live_total": int(result["live_count"].sum()),
        "db_total": int(result["db_count"].sum()),
        "rows_with_delta": int((result["delta_db_minus_live"] != 0).sum()),
    }
    return result, summary


def audit_centaline_transactions(params: Dict[str, Any]) -> tuple[pd.DataFrame, Dict[str, Any]]:
    area_codes = pd.read_csv(ROOT / "data/01_raw/centanet_oir_area_code.csv")
    db = pd.read_parquet(ROOT / "data/01_raw/centaline_oir_trans_lv_0.parquet")
    db_counts = db.groupby("AreaCode").size().to_dict()
    session = requests.Session()

    rows: list[Dict[str, Any]] = []
    errors = 0
    for area in area_codes.itertuples(index=False):
        error_message = None
        live_count = None
        try:
            response = request_with_backoff(
                session,
                "GET",
                params["source_a_commercial"]["transaction_api_url"],
                params={
                    "pageindex": 1,
                    "pagesize": 1,
                    "districtids": area.Code,
                    "lang": "EN",
                    "sellpricetype": "TOTAL",
                    "rentpricetype": "TOTAL",
                    "daterang": "01/01/2000-31/12/2099",
                },
                timeout=20.0,
                max_retries=4,
                sleep_seconds=3.0,
            ).json()
            live_count = int(response["data"]["recordList"]["totalCount"])
        except Exception as exc:  # pragma: no cover - operational script
            errors += 1
            error_message = str(exc)

        db_count = int(db_counts.get(area.Code, 0))
        rows.append(
            {
                "region": area.Region,
                "district": area.District,
                "area_code": area.Code,
                "live_count": live_count,
                "db_count": db_count,
                "delta_db_minus_live": None if live_count is None else db_count - live_count,
                "error": error_message,
            }
        )
        time.sleep(0.25)

    result = pd.DataFrame(rows).sort_values(
        by=["error", "delta_db_minus_live", "area_code"],
        ascending=[True, False, True],
        na_position="last",
    )
    comparable = result[result["live_count"].notna()].copy()
    summary = {
        "live_total_comparable": int(comparable["live_count"].sum()) if not comparable.empty else 0,
        "db_total_comparable": int(comparable["db_count"].sum()) if not comparable.empty else 0,
        "districts_with_delta": int((comparable["delta_db_minus_live"] != 0).sum()) if not comparable.empty else 0,
        "error_count": errors,
        "likely_causes": [
            "District-level gap thresholds can leave medium-sized historical gaps unfilled.",
            "The Source A APIs frequently time out unless requests are heavily rate-limited.",
            "Repeated deduplication and district skip logic can hide missing historical rows.",
        ],
    }
    return result, summary


def audit_centaline_buildings(params: Dict[str, Any]) -> tuple[pd.DataFrame, Dict[str, Any]]:
    area_codes = pd.read_csv(ROOT / "data/01_raw/centanet_oir_area_code.csv")
    db = pd.read_parquet(ROOT / "data/01_raw/centanet_oir_buildings.parquet")
    db_counts = db.groupby("queriedCode").size().to_dict()
    session = requests.Session()

    rows: list[Dict[str, Any]] = []
    errors = 0
    for area in area_codes.itertuples(index=False):
        error_message = None
        live_count = None
        try:
            response = request_with_backoff(
                session,
                "GET",
                params["source_a_commercial"]["property_api_url"],
                params={"PageSize": 1, "districtids": area.Code, "lang": "EN"},
                timeout=20.0,
                max_retries=4,
                sleep_seconds=3.0,
            ).json()
            live_count = int(response["data"]["totalCount"])
        except Exception as exc:  # pragma: no cover - operational script
            errors += 1
            error_message = str(exc)

        db_count = int(db_counts.get(area.Code, 0))
        rows.append(
            {
                "region": area.Region,
                "district": area.District,
                "area_code": area.Code,
                "live_count": live_count,
                "db_count": db_count,
                "delta_db_minus_live": None if live_count is None else db_count - live_count,
                "error": error_message,
            }
        )
        time.sleep(0.25)

    result = pd.DataFrame(rows).sort_values(
        by=["error", "delta_db_minus_live", "area_code"],
        ascending=[True, False, True],
        na_position="last",
    )
    comparable = result[result["live_count"].notna()].copy()
    summary = {
        "live_total_comparable": int(comparable["live_count"].sum()) if not comparable.empty else 0,
        "db_total_comparable": int(comparable["db_count"].sum()) if not comparable.empty else 0,
        "districts_with_delta": int((comparable["delta_db_minus_live"] != 0).sum()) if not comparable.empty else 0,
        "error_count": errors,
    }
    return result, summary


def print_summary_block(title: str, summary: Dict[str, Any]) -> None:
    print(f"\n## {title}")
    for key, value in summary.items():
        if key == "likely_causes":
            print("- likely_causes:")
            for item in value:
                print(f"  - {item}")
        else:
            print(f"- {key}: {value}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit live commercial website counts against local datasets.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where CSV and JSON audit outputs will be written.",
    )
    args = parser.parse_args()

    params = load_webscraper_params()

    midland_tx_df, midland_tx_summary = audit_midland_transactions(params)
    midland_bldg_df, midland_bldg_summary = audit_midland_buildings(params)
    centaline_tx_df, centaline_tx_summary = audit_centaline_transactions(params)
    centaline_bldg_df, centaline_bldg_summary = audit_centaline_buildings(params)

    write_outputs("midland_transactions", midland_tx_df, args.output_dir)
    write_outputs("midland_buildings", midland_bldg_df, args.output_dir)
    write_outputs("centaline_transactions", centaline_tx_df, args.output_dir)
    write_outputs("centaline_buildings", centaline_bldg_df, args.output_dir)

    summary = {
        "midland_transactions": midland_tx_summary,
        "midland_buildings": midland_bldg_summary,
        "centaline_transactions": centaline_tx_summary,
        "centaline_buildings": centaline_bldg_summary,
    }
    with (args.output_dir / "summary.json").open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)

    print_summary_block("Source B Transactions", midland_tx_summary)
    print_summary_block("Source B Buildings", midland_bldg_summary)
    print_summary_block("Source A Transactions", centaline_tx_summary)
    print_summary_block("Source A Buildings", centaline_bldg_summary)
    print(f"\nSaved audit outputs to {args.output_dir}")


if __name__ == "__main__":
    main()
