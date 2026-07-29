#!/usr/bin/env python
"""Compare live Source A residential transactions with the local raw dataset."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from playwright.async_api import async_playwright

from property_scraper.utils.source_a_utils import parse_date_from_string

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "data/08_reporting/residential_live_audit"
DEFAULT_SAMPLE_CODES = ["HMA031", "HMA050", "HMA058"]
INITIAL_LOAD_RETRIES = 3
logger = logging.getLogger(__name__)


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_webscraper_params() -> dict[str, Any]:
    with (ROOT / "conf/base/parameters.yml").open(encoding="utf-8") as handle:
        params = yaml.safe_load(handle)["webscraper"]
    local_path = ROOT / "conf/local/parameters.yml"
    if local_path.exists():
        with local_path.open(encoding="utf-8") as handle:
            local = (yaml.safe_load(handle) or {}).get("webscraper", {})
        params = _deep_merge(params, local)
    return params


def select_areas(
    area_frame: pd.DataFrame,
    *,
    all_areas: bool,
    area_codes: list[str] | None,
) -> pd.DataFrame:
    if all_areas:
        return area_frame.copy()
    selected_codes = area_codes or DEFAULT_SAMPLE_CODES
    selected = area_frame[area_frame["Code"].isin(selected_codes)].copy()
    missing = sorted(set(selected_codes) - set(selected["Code"]))
    if missing:
        raise ValueError(f"Unknown area codes: {missing}")
    return selected


async def audit_area(  # noqa: PLR0912, PLR0913
    context,
    area: dict[str, Any],
    *,
    base_url: str,
    target_date: date,
    max_pages: int,
    semaphore: asyncio.Semaphore,
) -> dict[str, Any]:
    async with semaphore:
        page = await context.new_page()
        live_records: dict[str, dict[str, Any]] = {}
        pages_scanned = 0
        website_total = None
        try:
            slug = str(area["Subdistrict"]).replace(" ", "-").lower()
            url = (
                f"{base_url}/{slug}_19-{area['Code']}"
                f"?q=audit_{int(datetime.now().timestamp())}"
            )
            initial_load_error: Exception | None = None
            for attempt in range(1, INITIAL_LOAD_RETRIES + 1):
                try:
                    await page.goto(
                        url,
                        wait_until="domcontentloaded",
                        timeout=45_000,
                    )
                    await page.wait_for_function(
                        "() => "
                        "(window.__NUXT__?.state?.transaction?.transactionList"
                        "?.data?.length || 0) > 0",
                        timeout=20_000,
                    )
                    initial_load_error = None
                    break
                except Exception as exc:
                    initial_load_error = exc
                    if attempt < INITIAL_LOAD_RETRIES:
                        await page.wait_for_timeout(attempt * 1_500)
            if initial_load_error is not None:
                raise initial_load_error

            for page_number in range(1, max_pages + 1):
                transaction_list = await page.evaluate(
                    "() => window.__NUXT__?.state?.transaction?.transactionList || {}"
                )
                website_total = website_total or transaction_list.get("count")
                page_rows = transaction_list.get("data") or []
                pages_scanned = page_number

                for record in page_rows:
                    transaction_date = parse_date_from_string(record.get("insDate"))
                    if transaction_date is None:
                        continue
                    if transaction_date != target_date:
                        continue
                    transaction_id = str(record.get("id") or "").strip()
                    if transaction_id:
                        live_records[transaction_id] = record

                next_button = page.locator(
                    "button.btn-next:not(.disabled):not([disabled]), "
                    "a.pagination-next:not(.disabled)"
                )
                if await next_button.count() == 0:
                    break
                previous_id = str(page_rows[0].get("id") or "") if page_rows else ""
                await next_button.first.click()
                try:
                    await page.wait_for_function(
                        "previousId => "
                        "String(window.__NUXT__?.state?.transaction?.transactionList"
                        "?.data?.[0]?.id || '') !== previousId",
                        arg=previous_id,
                        timeout=12_000,
                    )
                except Exception:
                    await page.wait_for_timeout(1_500)

            return {
                "success": True,
                "area": area,
                "website_total": website_total,
                "pages_scanned": pages_scanned,
                "records": live_records,
            }
        except Exception as exc:
            return {
                "success": False,
                "area": area,
                "pages_scanned": pages_scanned,
                "records": {},
                "error": f"{type(exc).__name__}: {exc}",
            }
        finally:
            await page.close()


async def collect_live_records(
    areas: pd.DataFrame,
    params: dict[str, Any],
    target_date: date,
    max_pages: int,
    workers: int,
) -> list[dict[str, Any]]:
    base_url = params["source_a_res"]["site"]["transaction_list_url"]
    semaphore = asyncio.Semaphore(max(1, workers))

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=params["global"].get("headless", True)
        )
        context = await browser.new_context(
            user_agent=params["global"].get("user_agent")
        )
        try:
            tasks = [
                audit_area(
                    context,
                    row._asdict(),
                    base_url=base_url,
                    target_date=target_date,
                    max_pages=max_pages,
                    semaphore=semaphore,
                )
                for row in areas.itertuples(index=False)
            ]
            return await asyncio.gather(*tasks)
        finally:
            await context.close()
            await browser.close()


def compare_with_local(
    results: list[dict[str, Any]],
    raw: pd.DataFrame,
    target_date: date,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    raw_dates = raw["date"].map(parse_date_from_string)
    target_raw = raw.loc[raw_dates.eq(target_date)].copy()
    area_rows: list[dict[str, Any]] = []
    missing_rows: list[dict[str, Any]] = []

    for result in results:
        area = result["area"]
        code = area["Code"]
        local_ids = set(
            target_raw.loc[
                target_raw["area_code"].eq(code), "transaction_id"
            ].dropna().astype(str)
        )
        live_records = result["records"]
        live_ids = set(live_records)
        missing_ids = sorted(live_ids - local_ids)
        extra_ids = sorted(local_ids - live_ids)
        area_rows.append(
            {
                "target_date": target_date.isoformat(),
                "area_code": code,
                "subdistrict": area["Subdistrict"],
                "success": result["success"],
                "pages_scanned": result["pages_scanned"],
                "website_total": result.get("website_total"),
                "live_target_count": len(live_ids),
                "local_target_count": len(local_ids),
                "missing_local_count": len(missing_ids),
                "local_not_live_count": len(extra_ids),
                "error": result.get("error"),
            }
        )
        for transaction_id in missing_ids:
            record = live_records[transaction_id]
            missing_rows.append(
                {
                    "target_date": target_date.isoformat(),
                    "area_code": code,
                    "subdistrict": area["Subdistrict"],
                    "transaction_id": transaction_id,
                    "address": record.get("address"),
                    "price": record.get("transactionPrice"),
                    "net_area": record.get("nArea"),
                    "gross_area": record.get("gArea"),
                }
            )

    return pd.DataFrame(area_rows), pd.DataFrame(missing_rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-date", required=True, type=date.fromisoformat)
    parser.add_argument("--all-areas", action="store_true")
    parser.add_argument(
        "--area-codes",
        help="Comma-separated area codes; defaults to the three regression samples",
    )
    parser.add_argument("--max-pages", type=int, default=50)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = parse_args()
    params = load_webscraper_params()
    area_path = ROOT / params["source_a_res"]["area_code_path"]
    raw_path = ROOT / params["source_a_res"]["res_trans_path"]
    areas = pd.read_csv(area_path)
    selected_areas = select_areas(
        areas,
        all_areas=args.all_areas,
        area_codes=(
            [code.strip() for code in args.area_codes.split(",") if code.strip()]
            if args.area_codes
            else None
        ),
    )
    raw = pd.read_parquet(raw_path)
    results = asyncio.run(
        collect_live_records(
            selected_areas,
            params,
            args.target_date,
            args.max_pages,
            args.workers,
        )
    )
    area_report, missing_report = compare_with_local(
        results, raw, args.target_date
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    successful_areas = int(area_report["success"].sum())
    if successful_areas:
        area_report.to_csv(args.output_dir / "area_comparison.csv", index=False)
        missing_report.to_csv(
            args.output_dir / "missing_live_transactions.csv", index=False
        )
        summary_path = args.output_dir / "summary.json"
    else:
        attempt_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        area_report.to_csv(
            args.output_dir / f"failed_attempt_{attempt_stamp}.csv",
            index=False,
        )
        summary_path = args.output_dir / f"failed_attempt_{attempt_stamp}.json"
    summary = {
        "target_date": args.target_date.isoformat(),
        "areas_checked": int(len(area_report)),
        "areas_failed": int((~area_report["success"]).sum()),
        "live_target_count": int(area_report["live_target_count"].sum()),
        "local_target_count": int(area_report["local_target_count"].sum()),
        "missing_local_count": int(area_report["missing_local_count"].sum()),
        "local_not_live_count": int(area_report["local_not_live_count"].sum()),
    }
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)
    logger.info(json.dumps(summary, indent=2))
    if not successful_areas:
        raise SystemExit("All live area probes failed; prior valid audit preserved")


if __name__ == "__main__":
    main()
