# src/kedro_source_a/pipelines/data_processing/nodes.py
import time
import random
import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, List, Tuple
from tqdm import tqdm
import configparser
import re
import os
import json
from pathlib import Path
from typing import Optional, Set
import pickle
import hashlib
from datetime import datetime, timedelta
from difflib import SequenceMatcher
import sys

import asyncio
import requests
from playwright.sync_api import sync_playwright, Page, TimeoutError as PlaywrightTimeout
from playwright.async_api import async_playwright

# Import node tracking utilities
from ...utils.node_tracker import (
    evaluate_node_run,
    get_node_status,
    record_node_decision,
    record_node_execution,
)
from ...utils.playwright_browser import launch_browser, launch_browser_async
from ...utils.source_a_health_check import check_source_a_health
from ...utils.source_a_res_backup import restore_latest_backup
from ...utils.source_runtime import get_required_setting
from ...utils.source_a_utils import (
    parse_date_from_string,
    extract_completion_year,
    parse_html_area,
    parse_html_price,
    parse_html_ft_price,
    block_slow_resources,
    block_slow_resources_async,
    estate_changed,
)
from ...utils.web_scraping import random_sleep
from ...utils.data_processing import drop_non_parquet_serializable_columns, fix_transaction_df_parquet_types


logger = logging.getLogger(__name__)


def _incremental_boundary(max_date, lookback_days: int):
    """Return an inclusive boundary that revisits recently published records."""
    safe_lookback = max(1, int(lookback_days))
    return max_date - timedelta(days=safe_lookback - 1)


def _partition_transaction_page(
    page_data: List[Dict[str, Any]],
    boundary_date,
) -> Tuple[List[Dict[str, Any]], bool]:
    """Keep in-window rows and report whether the complete page is older.

    Source A pages are not strictly ordered by transaction date. The caller
    must inspect the entire page rather than stopping at the first old row.
    Unparseable dates are retained so a parser change cannot silently discard
    source records.
    """
    accepted: List[Dict[str, Any]] = []
    parsed_dates = []
    has_unparseable_date = False

    for record in page_data:
        parsed_date = parse_date_from_string(record.get("date"))
        if parsed_date is None:
            has_unparseable_date = True
            accepted.append(record)
            continue
        parsed_dates.append(parsed_date)
        if parsed_date >= boundary_date:
            accepted.append(record)

    complete_page_is_old = bool(parsed_dates) and not has_unparseable_date and all(
        parsed_date < boundary_date for parsed_date in parsed_dates
    )
    return accepted, complete_page_is_old


def _deduplicate_transaction_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate IDs without collapsing every row whose ID is blank."""
    if frame.empty:
        return frame

    if "transaction_id" not in frame.columns:
        fallback = [
            column
            for column in ["date", "address", "price", "area"]
            if column in frame.columns
        ]
        return (
            frame.drop_duplicates(subset=fallback, keep="last")
            if fallback
            else frame
        )

    ids = frame["transaction_id"].astype("string").str.strip()
    valid_id = ids.notna() & ids.ne("") & ids.ne("None")
    with_id = frame.loc[valid_id].drop_duplicates(
        subset=["transaction_id"], keep="last"
    )
    without_id = frame.loc[~valid_id]
    fallback = [
        column
        for column in ["date", "address", "price", "area"]
        if column in without_id.columns
    ]
    if fallback:
        without_id = without_id.drop_duplicates(subset=fallback, keep="last")
    return pd.concat([with_id, without_id], ignore_index=True)


def _parse_nuxt_transaction_list(transactions: list) -> list:
    """Parse raw transaction list from __NUXT__ into record dicts. Shared by sync/async scrapers and the raw-API jsonl pulls (same NUXT-shaped objects)."""
    parsed_transactions = []
    for txn in transactions:
        try:
            scope = txn.get('scope', {})
            display_text = txn.get('displayText', {}).get('addr', {})
            big_estate = txn.get('bigEstateName', '').strip()
            estate = txn.get('estateName', '').strip()
            building = txn.get('buildingName', '').strip()
            formatted_address = display_text.get('line1', '').strip()
            if big_estate and estate and building:
                full_name = f"{big_estate} {estate} {building}"
            elif big_estate and estate:
                full_name = f"{big_estate} {estate}"
            elif big_estate and building:
                full_name = f"{big_estate} {building}"
            elif big_estate:
                full_name = big_estate
            elif estate and building:
                full_name = f"{estate} {building}"
            elif building:
                full_name = building
            elif estate:
                full_name = estate
            elif formatted_address:
                parts = formatted_address.split()
                name_parts = []
                for part in parts:
                    if part in ['Upper', 'Middle', 'Lower', 'High', 'Mid', 'Low'] or 'Floor' in part or '/F' in part:
                        break
                    name_parts.append(part)
                full_name = ' '.join(name_parts) if name_parts else formatted_address
            else:
                full_name = ''
            completion_year_str = txn.get('opYear', '')
            completion_year = None
            if completion_year_str:
                year_match = re.search(r'(\d{4})', completion_year_str)
                if year_match:
                    try:
                        completion_year = int(year_match.group(1))
                    except Exception:
                        pass
            age = None
            if completion_year:
                age = datetime.now().year - completion_year
                if age < 0:
                    age = None
            n_area = txn.get('nArea')
            g_area = txn.get('gArea')
            salable_area = txn.get('salableArea')
            floor_area = txn.get('floorArea')
            build_area = txn.get('buildUpArea')
            usable_area = txn.get('usableArea')
            area_value = (n_area or g_area or salable_area or floor_area or build_area or usable_area)
            n_unit_price = txn.get('nUnitPrice')
            g_unit_price = txn.get('gUnitPrice')
            ft_price_value = n_unit_price if n_unit_price is not None else g_unit_price
            is_carpark = bool((full_name and 'carpark' in full_name.lower()) or (building and 'carpark' in building.lower()))
            property_type = 'Carpark' if is_carpark else 'residential'
            record = {
                "date": txn.get("insDate", ""),
                "date_original": txn.get("insDate", ""),
                "region": scope.get("terr", ""),
                "district": scope.get("db", ""),
                "subdistrict": scope.get("hma", ""),
                "Name": full_name if full_name else None,
                "Tower": building if building else (estate if estate and not building else None),
                "Floor": txn.get("yAxis", ""),
                "Flat": txn.get("xAxis", ""),
                "transaction_type": "SALE" if txn.get("postType") == "S" else "RENT" if txn.get("postType") == "R" else "",
                "area": area_value,
                "price": txn.get("transactionPrice"),
                "ft_price": ft_price_value,
                "source": "source_a_res",
                "property_type": property_type,
                "address": formatted_address,
                "street_address": txn.get("address", ""),
                "building_code": txn.get("typeCode", ""),
                "estate_code": txn.get("estateId") or txn.get("bigEstateId") or txn.get("estateCode"),
                "g_area": g_area,
                "g_unit_price": g_unit_price,
                "completion_year": completion_year,
                "age": age,
                "estate_type": txn.get("estateType", ""),
                "transaction_url": txn.get("detailUrl", ""),
                "transaction_id": txn.get("id", ""),
                "title_lg": display_text.get("line5", ""),
                "rooms": txn.get("bedroomCount"),
                "direction": txn.get("direction", ""),
                "estate_name": estate if estate else None,
                "building_name": building if building else None,
            }
            known_keys = {
                "scope", "displayText", "bigEstateName", "estateName", "buildingName",
                "opYear", "nArea", "gArea", "salableArea", "floorArea", "buildUpArea", "usableArea",
                "nUnitPrice", "gUnitPrice", "postType", "insDate", "yAxis", "xAxis",
                "transactionPrice", "address", "typeCode", "estateType", "detailUrl", "id",
                "bedroomCount", "direction", "estateId", "bigEstateId", "estateCode",
                "media",
            }
            for key, val in txn.items():
                if key not in known_keys and val is not None and val != "":
                    if isinstance(val, (dict, list)):
                        continue
                    safe_key = f"nuxt_{key}" if not key.startswith("nuxt_") else key
                    record[safe_key] = val
            parsed_transactions.append(record)
        except Exception as e:
            logger.debug(f"Error parsing transaction: {e}")
            continue
    return parsed_transactions


def scroll_down(page: Page) -> None:
    """Scroll to bottom of page to trigger lazy loading"""
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    time.sleep(random.uniform(0.5, 1.5))


def _build_tracker_params(params: Dict[str, Any]) -> Dict[str, Any]:
    return params.get("node_tracking", {})


def _probe_source_a_live_state(
    params: Dict[str, Any],
    node_name: str,
) -> Dict[str, Any]:
    """Use a lightweight website probe to decide if a node likely needs work."""
    centa_params = params.get("source_a_res", {})
    if node_name == "transaction_data_scraper":
        health = check_source_a_health(
            params,
            max_stale_days=centa_params.get("health_check_max_stale_days", 10),
        )
        return {
            "node_name": node_name,
            "source": "sitemap_lastmod",
            "probe_failed": not health.get("ok", False),
            "latest_date": health.get("latest_date"),
            "message": health.get("message"),
        }

    return {
        "node_name": node_name,
        "source": "none",
        "probe_failed": True,
        "message": "No live probe configured",
    }


def _load_existing_transactions_with_recovery(
    transaction_file: str,
    params: Dict[str, Any],
) -> tuple[pd.DataFrame, dict]:
    """
    Load transactions, optionally restoring the newest backup when the file is corrupt.

    Returns (dataframe, recovery_metadata); the metadata is empty when nothing had
    to be recovered. It must never be None — Kedro refuses to save None to any
    dataset, which would fail the whole pipeline on the healthy path.
    """
    centa_params = params.get("source_a_res", {})
    restore_on_corruption = centa_params.get("restore_from_backup_on_corruption", True)
    abort_on_corruption = centa_params.get("abort_on_corrupt_transaction_file", True)

    try:
        existing = pd.read_parquet(transaction_file)
        return existing, {}
    except Exception as exc:
        logger.error("Error loading existing data from %s: %s", transaction_file, exc)
        recovery_metadata = {
            "file": transaction_file,
            "load_error": str(exc),
            "restored_from_backup": False,
            "backup_path": None,
        }

        if restore_on_corruption:
            restore_result = restore_latest_backup(transaction_file, base_dir=".")
            recovery_metadata["backup_path"] = restore_result.get("backup_path")
            recovery_metadata["restore_message"] = restore_result.get("message")
            if restore_result.get("restored"):
                try:
                    restored_df = pd.read_parquet(transaction_file)
                    recovery_metadata["restored_from_backup"] = True
                    logger.warning(
                        "Recovered %s by restoring latest backup %s",
                        transaction_file,
                        restore_result.get("backup_path"),
                    )
                    return restored_df, recovery_metadata
                except Exception as restore_exc:
                    recovery_metadata["restore_error"] = str(restore_exc)
                    logger.error(
                        "Backup restore succeeded but restored file still failed to load: %s",
                        restore_exc,
                    )

        if abort_on_corruption:
            raise RuntimeError(
                "Existing transaction file is unreadable and safe recovery failed. "
                "Restore a valid backup or disable abort_on_corrupt_transaction_file "
                "if you explicitly want to ignore the broken file."
            ) from exc

        logger.warning(
            "Proceeding without existing transaction data after corruption because "
            "abort_on_corrupt_transaction_file is disabled."
        )
        return pd.DataFrame(), recovery_metadata


_OUTAGE_FAILURE_SHARE = 0.3


class SourceServerError(RuntimeError):
    """The source returned a 5xx, so this area failed upstream, not locally."""


def _raise_for_server_error(response: Any, area: str) -> None:
    """Surface a 5xx as itself instead of letting it read as a local timeout.

    A site-wide outage otherwise shows up only as `wait_for_function` timeouts,
    which sends the whole run chasing a scraper bug that isn't there.
    """
    status = getattr(response, "status", None)
    if status is not None and status >= 500:
        raise SourceServerError(f"HTTP {status} for {area}")


class _SourceAResApiClient:
    """Direct-POST transport for the transaction search API, replacing per-page
    Playwright navigation. The anonymous bearer token is minted once via a
    single headless-browser visit (see scripts/centaline_res_api_pull.py,
    which validated this endpoint against the full history) and reused across
    all areas for the run; only 401/403 triggers a re-mint.
    """

    API_URL = "https://hk.centanet.com/findproperty/api/Transaction/Search"
    TOKEN_PAGE = "https://hk.centanet.com/findproperty/en/list/transaction/anderson_19-HMA051"
    PAGE_SIZE = 100

    def __init__(self, user_agent: str, delay: float):
        self.user_agent = user_agent
        self.delay = delay
        self.token: Optional[str] = None
        self.session = requests.Session()

    def _mint_token(self) -> None:
        from playwright.sync_api import sync_playwright

        holder: Dict[str, Any] = {"t": None}
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx = browser.new_context(user_agent=self.user_agent)
            page = ctx.new_page()

            def grab(req):
                if "/findproperty/api/" in req.url and "authorization" in req.headers:
                    holder["t"] = req.headers["authorization"]

            page.on("request", grab)
            page.goto(
                f"{self.TOKEN_PAGE}?q=incr_{int(time.time())}",
                wait_until="domcontentloaded",
                timeout=45000,
            )
            page.wait_for_timeout(3000)
            browser.close()
        if not holder["t"]:
            raise RuntimeError("could not obtain source_a_res API token")
        self.token = holder["t"]
        logger.info("source_a_res API token acquired")

    def _headers(self) -> Dict[str, str]:
        return {
            "authorization": self.token,
            "content-type": "application/json",
            "referer": self.TOKEN_PAGE,
            "user-agent": self.user_agent,
        }

    def fetch_page(self, area_code: str, offset: int) -> list:
        if self.token is None:
            self._mint_token()
        body = {
            "postType": "Both", "sort": "InsOrRegDate", "order": "Descending",
            "size": self.PAGE_SIZE, "offset": offset, "pageSource": "search",
            "bigPhotoMode": False, "hmas": [area_code], "mtrs": [],
            "primarySchoolNets": [], "markets": [], "universities": [],
            "typeCodes": [f"19-{area_code}"], "unitPlanGroupFilter": {},
        }
        backoff = 20.0
        for attempt in range(5):
            try:
                r = self.session.post(self.API_URL, headers=self._headers(), json=body, timeout=40)
            except requests.exceptions.RequestException as exc:
                logger.warning("source_a_res API network error (%s); backing off %.0fs", exc, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 300)
                continue
            if r.status_code == 200:
                time.sleep(self.delay)
                return r.json().get("data", [])
            if r.status_code in (401, 403):
                logger.info("source_a_res API token expired — refreshing")
                self._mint_token()
                continue
            if r.status_code == 429 or r.status_code >= 500:
                logger.warning("source_a_res API HTTP %d — backing off %.0fs", r.status_code, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 300)
                continue
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
        raise RuntimeError("source_a_res API: too many consecutive failures")


def load_existing_transaction_snapshot(
    params: Dict[str, Any],
) -> tuple[pd.DataFrame, Dict[str, Any]]:
    """Load and recover the current transaction snapshot as an explicit node."""
    source_params = params.get("source_a_res", {})
    if source_params.get("full_rerun", False) or source_params.get(
        "transaction_full_rerun",
        False,
    ):
        return pd.DataFrame(), {}
    transaction_file = source_params.get(
        "res_trans_path",
        "data/01_raw/centaline_res_trans_lv_0.parquet",
    )
    if not os.path.exists(transaction_file):
        return pd.DataFrame(), {}
    return _load_existing_transactions_with_recovery(transaction_file, params)


########################## scrape transaction data ##########################
    
# Below is the updated version of the `scrape_transaction_data` function that includes robust error handling, pagination, and control date checks.

def scrape_transaction_data(
    area_df: pd.DataFrame,
    existing_data: pd.DataFrame,
    recovery_metadata: Optional[Dict[str, Any]],
    params: Dict[str, Any],
) -> pd.DataFrame:
    """
    Enhanced Kedro-compatible transaction data scraper with incremental updating.
    Includes node execution tracking to avoid re-running on the same day.
    """
    
    # Date-based decision: incremental date logic below handles when to skip.
    transaction_file = params['source_a_res'].get('res_trans_path', 'data/01_raw/centaline_res_trans_lv_0.parquet')
    
    # ============ DEFINE NESTED FUNCTIONS FIRST ============
    def _smart_sleep():
        random_sleep(params["global"]["min_delay"], params["global"]["max_delay"])

    def enhanced_scroll_down(page: Page):
        """Enhanced scrolling strategy for dynamic content loading"""
        last_height = page.evaluate("document.body.scrollHeight")
        scroll_attempts = 0
        while scroll_attempts < 3:
            scroll_distance = random.randint(300, 800)
            page.evaluate(f"window.scrollBy(0, {scroll_distance})")
            _smart_sleep()
            new_height = page.evaluate("document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
            scroll_attempts += 1

    def extract_nuxt_transactions(page: Page):
        """
        Extract transaction data from window.__NUXT__ JavaScript object.
        Gets ALL data: gArea, nArea, gUnitPrice, nUnitPrice, region, district, building codes.
        """
        try:
            nuxt_data = page.evaluate("() => window.__NUXT__")
            if not nuxt_data:
                logger.warning("window.__NUXT__ is empty")
                return []
            transactions = nuxt_data.get("state", {}).get("transaction", {}).get("transactionList", {}).get("data", [])
            if not transactions:
                logger.debug("No transactions found in __NUXT__ object")
                return []
            return _parse_nuxt_transaction_list(transactions)
        except Exception as e:
            logger.error(f"Error extracting __NUXT__ data: {e}")
            return []

    def get_nuxt_transaction_total(page: Page) -> Optional[int]:
        """Extract website total count from __NUXT__ for validation. Returns None if not found."""
        try:
            txl = page.evaluate(
                "() => window.__NUXT__?.state?.transaction?.transactionList"
            )
            if not txl:
                return None
            total = txl.get("count") or txl.get("total") or txl.get("totalCount")
            if total is not None:
                return int(total)
            pag = txl.get("pagination") or txl.get("pageInfo")
            if pag and isinstance(pag, dict):
                t = pag.get("total") or pag.get("totalCount")
                return int(t) if t is not None else None
            return None
        except Exception:
            return None
    
    def extract_combined_data(page: Page):
        """
        Extract transaction data from BOTH JavaScript __NUXT__ and HTML table.
        - JavaScript provides: metadata (building codes, dates, etc.)
        - HTML table provides: VISIBLE area, price, ft_price (always shown on list page)
        - Uses HTML as FALLBACK for missing JavaScript data
        Returns merged data with all available fields.
        """
        try:
            js_data = extract_nuxt_transactions(page)
            try:
                html_data = extract_table_data(page)
            except Exception as e:
                logger.warning(f"HTML table extraction failed: {e}")
                html_data = []
            
            if not js_data and not html_data:
                logger.debug("No data from either source")
                return []
            
            # If we have both sources, merge them
            if js_data and html_data:
                # Merge by position/index
                merged_data = []
                area_fallback_count = 0
                
                for i, js_rec in enumerate(js_data):
                    # Use HTML data at same index if available
                    if i < len(html_data):
                        html_rec = html_data[i]
                        
                        # Parse HTML values and use as FALLBACK when JS is missing
                        if js_rec.get('area') is None and html_rec.get('area'):
                            parsed_area = parse_html_area(html_rec.get('area'))
                            if parsed_area:
                                js_rec['area'] = parsed_area
                                area_fallback_count += 1
                                logger.info(f"   ✓ Used HTML area fallback for record #{i+1}: {html_rec.get('area')} → {parsed_area} ({js_rec.get('Name', 'unknown')})")
                        
                        if js_rec.get('price') is None and html_rec.get('price'):
                            parsed_price = parse_html_price(html_rec.get('price'))
                            if parsed_price:
                                js_rec['price'] = parsed_price
                                logger.debug(f"Used HTML price fallback: {parsed_price}")
                        
                        if js_rec.get('ft_price') is None and html_rec.get('ft_price'):
                            parsed_ft_price = parse_html_ft_price(html_rec.get('ft_price'))
                            if parsed_ft_price:
                                js_rec['ft_price'] = parsed_ft_price
                                logger.debug(f"Used HTML ft_price fallback: {parsed_ft_price}")
                    
                    merged_data.append(js_rec)
                
                #logger.info(f"   ✅ Merged {len(merged_data)} records from JS and HTML table")
                if area_fallback_count > 0:
                    logger.info(f"   📊 Used HTML fallback for {area_fallback_count} area values")
                return merged_data
            
            # If only one source has data, use it
            elif js_data:
                logger.debug("Using JavaScript data only (no HTML data)")
                return js_data
            else:
                logger.debug("Using HTML data only (no JavaScript data)")
                return html_data
            
        except Exception as e:
            logger.error(f"Error in extract_combined_data: {e}")
            return []
    
    def extract_table_data(page: Page):
        """Extract visible data from HTML table on the transaction list page."""
        table_data = []
        try:
            enhanced_scroll_down(page)
            rows = page.locator("tr.cv-structured-list-item").all()
            logger.debug(f"   Found {len(rows)} table rows for HTML extraction")
            
            for row in rows:
                try:
                    cells = row.locator("td.cv-structured-list-data").all()
                    if len(cells) < 6:
                        continue
                    cell_text = lambda i: (cells[i].text_content() or "").strip()

                    transaction_url = row.locator("a[href*='/transaction/'], a.transaction-link").first.get_attribute("href") or row.get_attribute("data-href") or ""

                    date_span = cells[0].locator(".info-date span").first.text_content()
                    date_text = (date_span or "").strip() or cell_text(0)
                    addr_el = cells[1].locator(".addr").first.text_content()
                    address_text = (addr_el or "").strip() or cell_text(1)

                    title_lg_text = cell_text(2)
                    rooms_text = cell_text(3)
                    transaction_type = "SALE"
                    price_text = cell_text(4)
                    if "租" in price_text:
                        transaction_type = "RENT"
                    elif price_text.startswith("$") and len(price_text) < 10 and any(c.isdigit() for c in price_text):
                        transaction_type = "RENT"

                    area_text = cell_text(5)
                    if area_text and area_text != "--":
                        logger.debug(f"   HTML area extracted: {area_text}")

                    ft_price_text = cell_text(6) if len(cells) >= 7 else ""
                    changes_text = ""
                    if len(cells) >= 8:
                        ch_el = cells[7].locator(".riseBox span").first.text_content()
                        changes_text = (ch_el or cell_text(7))
                        
                        record = {
                            'date': date_text,
                            'address': address_text,
                            'title_lg': title_lg_text,  # Address with native separators
                            'rooms': rooms_text,
                            'price': price_text,
                            'area': area_text,
                            'ft_price': ft_price_text,
                            'changes': changes_text,
                            'transaction_type': transaction_type,
                            'transaction_url': transaction_url,  # NEW: URL for duplicate matching
                        }
                        table_data.append(record)
                        
                except Exception as e:
                    logger.debug(f"Error processing row: {str(e)}")
                    continue
                    
            mobile_cards = page.locator(".transactions-content").all()
            title_lg_values = []
            price_values = []
            for card in mobile_cards:
                try:
                    title_lg_text = None
                    text01 = card.locator(".text01 .title-lg").first.text_content()
                    if text01:
                        title_lg_text = text01.strip()
                    if not title_lg_text:
                        title_lg_text = (card.locator(".title-lg").first.text_content() or "").strip()
                    if not title_lg_text:
                        card_txt = (card.text_content() or "").strip()
                        lines = card_txt.split("\n")
                        if lines and len(lines[0].strip()) > 5:
                            title_lg_text = lines[0].strip()
                    if title_lg_text:
                        title_lg_values.append(title_lg_text)
                    price_text = None
                    for sel in [".content-price .saleprice span", ".content-price .saleprice", ".content-price span", ".saleprice span", ".saleprice"]:
                        el = card.locator(sel).first.text_content()
                        if el and "$" in el and any(c.isdigit() for c in el):
                            if "M" in el or ("," in el and len(el) > 6):
                                price_text = el.strip()
                                break
                            price_text = price_text or el.strip()
                    if not price_text:
                        for pat in [r"\$\d+\.?\d*M", r"\$\d{1,3}(?:,\d{3})*", r"\$\d+"]:
                            m = re.findall(pat, card.text_content() or "")
                            if m:
                                price_text = m[0]
                                break
                    price_values.append(price_text or "")
                except Exception as e:
                    logger.debug(f"Error processing mobile card: {e}")
                    title_lg_values.append("")
                    price_values.append("")
            

            
            # Enrich table data with title-lg and price information
            # Map title-lg and price values to table records based on matching addresses
            for i, record in enumerate(table_data):
                record_address = record['address'].lower()
                matched_title_lg = None
                matched_price = None
                
                # Try to find matching title-lg and price based on address similarity
                for j, title_lg in enumerate(title_lg_values):
                    title_lg_lower = title_lg.lower()
                    record_address_lower = record_address.lower()
                    
                    # Method 1: Check if the title-lg contains key parts of the address
                    if any(part in title_lg_lower for part in record_address_lower.split() if len(part) > 2):
                        matched_title_lg = title_lg
                        if j < len(price_values):
                            matched_price = price_values[j]
                        break
                    
                    # Method 2: Check if address contains key parts of title-lg
                    elif any(part in record_address_lower for part in title_lg_lower.split() if len(part) > 2):
                        matched_title_lg = title_lg
                        if j < len(price_values):
                            matched_price = price_values[j]
                        break
                
                # If no match found, use the first available title-lg and price or fallback
                if matched_title_lg:
                    record['title_lg'] = matched_title_lg
                    if matched_price and (not record['price'] or record['price'].strip() == ''):
                        record['price'] = matched_price
                elif title_lg_values:
                    record['title_lg'] = title_lg_values[0]  # Use first available
                    if price_values and (not record['price'] or record['price'].strip() == ''):
                        record['price'] = price_values[0]
                    title_lg_values = title_lg_values[1:]  # Remove used value
                    price_values = price_values[1:]  # Remove used value
                else:
                    record['title_lg'] = record['address'] if record['address'] else ''
            
            return table_data
        except Exception as e:
            logger.error(f"Error extracting table data: {str(e)}")
            return []

    def go_to_next_page(page: Page):
        """Navigate to next page with enhanced verification"""
        try:
            next_loc = page.locator("button.btn-next:not(.disabled):not([disabled]), a.pagination-next:not(.disabled)")
            if next_loc.count() > 0:
                next_btn = next_loc.first
                first_id = page.evaluate(
                    "() => String(window.__NUXT__?.state?.transaction?.transactionList?.data?.[0]?.id || '')"
                )
                next_btn.scroll_into_view_if_needed()
                _smart_sleep()
                next_btn.click()
                try:
                    page.wait_for_function(
                        "previousId => String(window.__NUXT__?.state?.transaction?.transactionList?.data?.[0]?.id || '') !== previousId",
                        arg=first_id,
                        timeout=4000,
                    )
                except Exception:
                    page.wait_for_timeout(1500)
                return True
            return False
        except Exception:
            return False

    # ============ INCREMENTAL UPDATE LOGIC ============
    transaction_file = params.get("source_a_res", {}).get(
        "res_trans_path", "data/01_raw/centaline_res_trans_lv_0.parquet"
    )
    lookback_days = params.get("source_a_res", {}).get(
        "transaction_lookback_days", 60
    )
    required_stale_pages = max(
        1,
        int(
            params.get("source_a_res", {}).get(
                "transaction_consecutive_stale_pages", 3
            )
        ),
    )
    stop_on_stale_pages = params.get("source_a_res", {}).get(
        "transaction_stop_on_stale_pages", False
    )
    tracker_params = _build_tracker_params(params)
    full_rerun = params.get("source_a_res", {}).get("full_rerun", False)
    transaction_full_rerun = params.get("source_a_res", {}).get("transaction_full_rerun", False)
    live_state = {
        "node_name": "transaction_data_scraper",
        "source": "manual_full_rerun" if (full_rerun or transaction_full_rerun) else "pending",
        "probe_failed": False,
    }

    if full_rerun or transaction_full_rerun:
        logger.info("Full rerun mode: starting fresh (no existing data loaded)")

    if not (full_rerun or transaction_full_rerun) and not existing_data.empty:
        logger.info(f"Loaded {len(existing_data)} existing transactions")

        if "date" in existing_data.columns and not existing_data.empty:
            existing_data_temp = existing_data.copy()
            existing_data_temp["parsed_date"] = existing_data_temp["date"].apply(
                lambda x: parse_date_from_string(x) if pd.notna(x) else None
            )

            valid_dates = existing_data_temp["parsed_date"].dropna()
            if not valid_dates.empty:
                max_date = valid_dates.max()
                control_date = _incremental_boundary(max_date, lookback_days)
                logger.info(
                    "✅ Using incremental lookback boundary: %s "
                    "(max existing: %s, lookback: %s days)",
                    control_date,
                    max_date,
                    lookback_days,
                )
            else:
                control_date = pd.to_datetime(
                    params.get("source_a_res", {}).get(
                        "control_date",
                        params["global"]["start_date"],
                    )
                ).date()
                logger.info(
                    f"⚠️ No valid dates found in existing data, using parameter control date: {control_date}"
                )
        else:
            control_date = pd.to_datetime(
                params.get("source_a_res", {}).get(
                    "control_date",
                    params["global"]["start_date"],
                )
            ).date()
            logger.info(f"No date column found, using parameter control date: {control_date}")
    else:
        if not (full_rerun or transaction_full_rerun):
            logger.info("No existing transaction file found, starting fresh")
        control_date = pd.to_datetime(
            params.get("source_a_res", {}).get("control_date", params["global"]["start_date"])
        ).date()
        if full_rerun or transaction_full_rerun:
            control_date = datetime(1900, 1, 1).date()

    # Set end date to today
    end_date = datetime.now().date()
    logger.info(f"Scraping transactions from {control_date} to {end_date}")

    allow_live_skip = params.get("source_a_res", {}).get(
        "allow_transaction_live_skip", False
    )
    if not (full_rerun or transaction_full_rerun) and allow_live_skip:
        live_state = _probe_source_a_live_state(params, "transaction_data_scraper")
        tracker_decision = evaluate_node_run(
            node_name="transaction_data_scraper",
            node_type="transaction",
            tracking_params=tracker_params,
            data_file_path=transaction_file,
            live_state=live_state,
        )
        if recovery_metadata:
            tracker_decision.setdefault("dataset_state", {})
            tracker_decision["dataset_state"] = tracker_decision.get("dataset_state") or {}
            tracker_decision["dataset_state"]["recovery"] = recovery_metadata

        record_node_decision(
            node_name="transaction_data_scraper",
            node_type="transaction",
            should_run=tracker_decision["should_run"],
            reason=tracker_decision["reason"],
            metadata={
                "records_seen_locally": len(existing_data),
                "control_date": control_date.isoformat(),
            },
            live_state=live_state,
            dataset_state=tracker_decision.get("dataset_state"),
        )

        if not tracker_decision["should_run"]:
            logger.info(
                "Skipping transaction scrape because live state indicates no upstream change (%s)",
                tracker_decision["reason"],
            )
            return existing_data if not existing_data.empty else pd.DataFrame()
    elif not (full_rerun or transaction_full_rerun):
        logger.info(
            "Transaction live skip disabled; running the configured lookback "
            "to capture delayed records"
        )

    # If control date is already today or later, return existing data
    if control_date >= end_date:
        logger.info("No new transactions to scrape - control date is current")
        return existing_data if not existing_data.empty else pd.DataFrame()

    # ============ MAIN SCRAPING LOGIC (Playwright) ============
    from tqdm.auto import tqdm

    headless = params["global"].get("headless", True)
    user_agent = params["global"].get("user_agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    max_concurrent = min(
        params.get("source_a_res", {}).get(
            "max_transaction_workers",
            params.get("global", {}).get("max_threads", 5),
        ),
        8,
    )
    area_rows = list(area_df.iterrows())
    # The source starts refusing requests part way through a run, so whichever
    # areas sit at the front are the only ones that ever get scraped. Put last
    # run's failures first so successive runs cover the whole list.
    previously_failed = set(
        get_node_status("transaction_data_scraper")
        .get("metadata", {})
        .get("failed_area_codes", [])
    )
    if previously_failed:
        area_rows.sort(key=lambda pair: pair[1].get("Code") not in previously_failed)
        logger.info(
            "↻ Prioritising %s areas that failed on the previous run",
            len(previously_failed),
        )

    async def _run_async_scrape(rows=None):
        """Async concurrent scraping using Playwright async API (safe for parallelism)."""
        rows = area_rows if rows is None else rows
        semaphore = asyncio.Semaphore(max_concurrent)

        async def extract_nuxt_async(page):
            nuxt_data = await page.evaluate("() => window.__NUXT__")
            if not nuxt_data:
                return []
            transactions = nuxt_data.get('state', {}).get('transaction', {}).get('transactionList', {}).get('data', [])
            return _parse_nuxt_transaction_list(transactions) if transactions else []

        async def go_to_next_page_async(page):
            try:
                next_loc = page.locator("button.btn-next:not(.disabled):not([disabled]), a.pagination-next:not(.disabled)")
                if await next_loc.count() > 0:
                    first_id = await page.evaluate(
                        "() => String(window.__NUXT__?.state?.transaction?.transactionList?.data?.[0]?.id || '')"
                    )
                    await next_loc.first.scroll_into_view_if_needed()
                    await asyncio.sleep(random.uniform(params['global']['min_delay'], params['global']['max_delay']))
                    await next_loc.first.click()
                    try:
                        await page.wait_for_function(
                            "previousId => String(window.__NUXT__?.state?.transaction?.transactionList?.data?.[0]?.id || '') !== previousId",
                            arg=first_id,
                            timeout=4000,
                        )
                    except Exception:
                        await asyncio.sleep(
                            max(
                                0.5,
                                random.uniform(
                                    params['global']['min_delay'],
                                    params['global']['max_delay'],
                                )
                                * 2,
                            )
                        )
                    return True
            except Exception:
                pass
            return False

        async def extract_table_data_async(page):
            """Async HTML table extraction (fallback when __NUXT__ missing)."""
            table_data = []
            try:
                rows = await page.locator("tr.cv-structured-list-item").all()
                for row in rows:
                    try:
                        cells = await row.locator("td.cv-structured-list-data").all()
                        if len(cells) < 6:
                            continue

                        async def _ct(i):
                            t = await cells[i].text_content()
                            return (t or "").strip()

                        href = await row.locator("a[href*='/transaction/'], a.transaction-link").first.get_attribute("href") or ""
                        date_el = cells[0].locator(".info-date span").first
                        date_text = (await date_el.text_content() or "").strip() or await _ct(0)
                        addr_el = cells[1].locator(".addr").first
                        address_text = (await addr_el.text_content() or "").strip() or await _ct(1)
                        title_lg_text = await _ct(2)
                        rooms_text = await _ct(3)
                        price_text = await _ct(4)
                        transaction_type = "RENT" if "租" in price_text else "SALE"
                        area_text = await _ct(5)
                        ft_price_text = (await _ct(6)) if len(cells) >= 7 else ""
                        record = {
                            "date": date_text, "address": address_text, "title_lg": title_lg_text,
                            "rooms": rooms_text, "price": price_text, "area": area_text,
                            "ft_price": ft_price_text, "transaction_type": transaction_type,
                            "transaction_url": href,
                        }
                        table_data.append(record)
                    except Exception:
                        continue
            except Exception:
                pass
            return table_data

        async def extract_combined_async(page):
            """Prefer complete __NUXT__ records; use HTML only as a fallback."""
            js_data = await extract_nuxt_async(page)
            if js_data:
                return js_data
            return await extract_table_data_async(page)

        async def scrape_one_area(context, area_row):
            async with semaphore:
                page = await context.new_page()
                base_url = get_required_setting(
                    params, "source_a_res", "site", "transaction_list_url"
                )
                area_data = []
                try:
                    subdistrict = area_row["Subdistrict"].replace(" ", "-").lower()
                    url = f"{base_url}/{subdistrict}_19-{area_row['Code']}?q=session_{int(datetime.now().timestamp())}"
                    response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    _raise_for_server_error(response, area_row["Subdistrict"])
                    await asyncio.sleep(random.uniform(params['global']['min_delay'], params['global']['max_delay']))
                    try:
                        await page.wait_for_function(
                            "() => (window.__NUXT__?.state?.transaction?.transactionList?.data?.length || 0) > 0",
                            timeout=12000,
                        )
                    except Exception:
                        await page.wait_for_selector("tr.cv-structured-list-item, .transactions-content", timeout=8000)
                    website_total = None
                    try:
                        website_total = await page.evaluate(
                            "() => { const t = window.__NUXT__?.state?.transaction?.transactionList; "
                            "return t?.count ?? t?.total ?? t?.totalCount ?? t?.pagination?.total ?? t?.pageInfo?.total ?? null; }"
                        )
                        if website_total is not None:
                            website_total = int(website_total)
                    except Exception:
                        pass
                    page_num = 1
                    consecutive_stale_pages = 0
                    max_pages = params.get("source_a_res", {}).get(
                        "max_pages_per_area",
                        params["global"].get("max_pages_per_area", 50),
                    )
                    if not (full_rerun or transaction_full_rerun):
                        max_pages = min(
                            max_pages,
                            int(
                                params.get("source_a_res", {}).get(
                                    "transaction_incremental_max_pages", 20
                                )
                            ),
                        )
                    while page_num <= max_pages:
                        page_data = await extract_combined_async(page)
                        accepted_records, page_is_old = _partition_transaction_page(
                            page_data, control_date
                        )
                        for record in accepted_records:
                            record['area_code'] = area_row['Code']
                            record['scrape_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            area_data.append(record)
                        consecutive_stale_pages = (
                            consecutive_stale_pages + 1 if page_is_old else 0
                        )
                        if (
                            stop_on_stale_pages
                            and consecutive_stale_pages >= required_stale_pages
                        ):
                            break
                        if not await go_to_next_page_async(page):
                            break
                        page_num += 1
                    if website_total is not None:
                        if len(area_data) > website_total:
                            logger.warning(
                                f"⚠️ Transaction count exceeds website {area_row['Subdistrict']} ({area_row['Code']}): "
                                f"scraped={len(area_data)} website_total={website_total}"
                            )
                        elif consecutive_stale_pages < required_stale_pages and len(area_data) < website_total:
                            logger.info(
                                f"   {area_row['Subdistrict']}: scraped={len(area_data)} website_total={website_total} (pagination complete)"
                            )
                    return {"success": True, "data": area_data, "area": area_row["Subdistrict"]}
                except Exception as e:
                    # Keep what this area already yielded. Failing on page 150 used
                    # to discard the previous 149 pages, and the lookback window
                    # never comes back for them.
                    logger.warning(
                        "Error scraping %s after %s records: %s",
                        area_row["Subdistrict"],
                        len(area_data),
                        e,
                    )
                    return {"success": False, "data": area_data, "area": area_row["Subdistrict"], "error": str(e)}
                finally:
                    await page.close()

        async with async_playwright() as p:
            browser = await launch_browser_async(p, headless=headless)
            try:
                context = await browser.new_context(user_agent=user_agent)
                results = [None] * len(rows)
                total_records = [0]  # mutable for closure

                def make_done_cb(i):
                    def cb(fut):
                        try:
                            if fut.cancelled():
                                res = None
                            elif fut.exception():
                                res = fut.exception()
                            else:
                                res = fut.result()
                        except Exception as e:
                            res = e
                        results[i] = res
                        if isinstance(res, dict) and res.get("success"):
                            total_records[0] += len(res.get("data", []))
                        area = (res.get("area", "?") if isinstance(res, dict) else "err")[:15]
                        try:
                            pbar.set_postfix(area=area, total=total_records[0])
                            pbar.update(1)
                        except Exception:
                            pass
                    return cb

                pbar = tqdm(
                    total=len(rows),
                    desc="Scraping areas",
                    file=sys.stderr,
                    dynamic_ncols=True,
                    mininterval=0.5,
                    initial=0,
                )
                pbar.refresh()  # Force display before first task completes
                tasks = []
                for i, (_, row) in enumerate(rows):
                    t = asyncio.create_task(scrape_one_area(context, row))
                    t.add_done_callback(make_done_cb(i))
                    tasks.append(t)

                await asyncio.gather(*tasks)
                pbar.close()
                return results
            finally:
                try:
                    await context.close()
                except Exception:
                    pass
                await browser.close()

    def scrape_area_transactions(page, area_row):
        """Scrape transactions for a single area. Must run in the same thread as sync_playwright."""
        base_url = get_required_setting(
            params, "source_a_res", "site", "transaction_list_url"
        )
        area_data = []
        try:
            subdistrict = area_row["Subdistrict"].replace(" ", "-").lower()
            session_id = f"session_{int(datetime.now().timestamp())}"
            url = f"{base_url}/{subdistrict}_19-{area_row['Code']}?q={session_id}"

            _raise_for_server_error(
                page.goto(url, wait_until="domcontentloaded", timeout=30000),
                area_row["Subdistrict"],
            )
            _smart_sleep()
            try:
                page.wait_for_selector("tr.cv-structured-list-item, .transactions-content", timeout=12000)
            except Exception:
                pass
            website_total = get_nuxt_transaction_total(page)

            page_num = 1
            consecutive_stale_pages = 0
            max_pages = params.get("source_a_res", {}).get(
                "max_pages_per_area",
                params["global"].get("max_pages_per_area", 50),
            )
            if not (full_rerun or transaction_full_rerun):
                max_pages = min(
                    max_pages,
                    int(
                        params.get("source_a_res", {}).get(
                            "transaction_incremental_max_pages", 20
                        )
                    ),
                )

            while page_num <= max_pages:
                page_data = extract_combined_data(page)
                accepted_records, page_is_old = _partition_transaction_page(
                    page_data, control_date
                )
                for record in accepted_records:
                    record['area_code'] = area_row['Code']
                    record['scrape_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    area_data.append(record)

                consecutive_stale_pages = (
                    consecutive_stale_pages + 1 if page_is_old else 0
                )
                if (
                    stop_on_stale_pages
                    and consecutive_stale_pages >= required_stale_pages
                ):
                    break
                if not go_to_next_page(page):
                    break
                page_num += 1

            if website_total is not None:
                if len(area_data) > website_total:
                    logger.warning(
                        f"⚠️ Transaction count exceeds website {area_row['Subdistrict']} ({area_row['Code']}): "
                        f"scraped={len(area_data)} website_total={website_total}"
                    )
                elif consecutive_stale_pages < required_stale_pages and len(area_data) < website_total:
                    logger.info(
                        f"   {area_row['Subdistrict']}: scraped={len(area_data)} website_total={website_total} (pagination complete)"
                    )

            return {"success": True, "data": area_data, "area": area_row["Subdistrict"]}

        except Exception as e:
            logger.warning(
                "Error scraping %s after %s records: %s",
                area_row["Subdistrict"],
                len(area_data),
                e,
            )
            return {"success": False, "data": area_data, "area": area_row["Subdistrict"], "error": str(e)}

    def scrape_area_transactions_via_api(api_client: "_SourceAResApiClient", area_row):
        """Direct-POST equivalent of scrape_area_transactions for the incremental
        lookback window. No offset-ceiling segmentation is needed here — a
        lookback of days-to-weeks never approaches the ~10k-row ceiling that
        the full-history puller (scripts/centaline_res_api_pull.py) has to
        work around."""
        area_data = []
        area_code = area_row["Code"]
        subdistrict = area_row["Subdistrict"]
        max_pages = min(
            params.get("source_a_res", {}).get(
                "max_pages_per_area", params["global"].get("max_pages_per_area", 50)
            ),
            int(params.get("source_a_res", {}).get("transaction_incremental_max_pages", 20)),
        )
        try:
            offset = 0
            page_num = 1
            consecutive_stale_pages = 0
            while page_num <= max_pages:
                raw_page = api_client.fetch_page(area_code, offset)
                if not raw_page:
                    break
                parsed_page = _parse_nuxt_transaction_list(raw_page)
                accepted_records, page_is_old = _partition_transaction_page(parsed_page, control_date)
                for record in accepted_records:
                    record['area_code'] = area_code
                    record['scrape_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    area_data.append(record)
                consecutive_stale_pages = consecutive_stale_pages + 1 if page_is_old else 0
                if stop_on_stale_pages and consecutive_stale_pages >= required_stale_pages:
                    break
                if len(raw_page) < api_client.PAGE_SIZE:
                    break
                offset += api_client.PAGE_SIZE
                page_num += 1
            return {"success": True, "data": area_data, "area": subdistrict}
        except Exception as e:
            logger.warning("Error scraping %s via API after %s records: %s", subdistrict, len(area_data), e)
            return {"success": False, "data": area_data, "area": subdistrict, "error": str(e)}

    all_data = []
    failed_areas = []

    def _collect(raw_results, rows):
        """Accumulate one pass's data and return the areas still needing a retry."""
        still_failed = []
        for i, res in enumerate(raw_results):
            row = rows[i][1]
            area = row.get("Subdistrict", "unknown")
            if isinstance(res, Exception) or res is None:
                logger.warning(f"Unexpected exception for {area}: {res}")
                still_failed.append({"success": False, "data": [], "area": area, "error": str(res)})
            elif res.get("success"):
                all_data.extend(res["data"])
            else:
                # Partial output counts; the retry pass picks up the remainder and
                # the downstream dedup drops whatever both passes saw.
                all_data.extend(res.get("data") or [])
                still_failed.append(res)
        return still_failed

    use_api_transport = params.get("source_a_res", {}).get("use_api_transport", True)
    if use_api_transport and not (full_rerun or transaction_full_rerun):
        logger.info("Scraping transaction areas via direct API (incremental)")
        api_client = _SourceAResApiClient(
            user_agent=user_agent, delay=params["global"].get("min_delay", 0.3)
        )
        with tqdm(total=len(area_rows), desc="Scraping areas (API)") as pbar:
            for _, row in area_rows:
                try:
                    result = scrape_area_transactions_via_api(api_client, row)
                except Exception as exc:
                    logger.warning(f"Unexpected exception for {row.get('Subdistrict', 'unknown')}: {exc}")
                    result = {"success": False, "data": [], "area": row.get("Subdistrict", "unknown"), "error": str(exc)}
                all_data.extend(result.get("data") or [])
                if result["success"]:
                    pbar.set_postfix({"area": result["area"][:15], "total": len(all_data)})
                else:
                    failed_areas.append(result)
                pbar.update(1)
    elif max_concurrent > 1:
        logger.info(f"Scraping transaction areas with {max_concurrent} concurrent workers (Playwright async API)")
        failed_areas = _collect(asyncio.run(_run_async_scrape()), area_rows)
        # A timeout here is usually transient, but an area missed on this pass is
        # missed for good: the lookback window moves on and never revisits it.
        if failed_areas:
            server_errors = sum(
                1
                for result in failed_areas
                if "HTTP 5" in str(result.get("error", ""))
            )
            if server_errors > len(area_rows) * _OUTAGE_FAILURE_SHARE:
                logger.error(
                    "⛔ %s of %s areas returned a server error — the source looks "
                    "down. Skipping the retry pass and keeping what was collected.",
                    server_errors,
                    len(area_rows),
                )
            else:
                retry_names = {result.get("area") for result in failed_areas}
                retry_rows = [
                    pair
                    for pair in area_rows
                    if pair[1].get("Subdistrict") in retry_names
                ]
                logger.warning(f"🔁 Retrying {len(retry_rows)} failed areas")
                failed_areas = _collect(
                    asyncio.run(_run_async_scrape(retry_rows)), retry_rows
                )
    else:
        logger.info("Scraping transaction areas sequentially")
        with sync_playwright() as playwright:
            browser = launch_browser(playwright, headless=headless)
            context = browser.new_context(user_agent=user_agent)
            page = context.new_page()
            page.route("**/*", block_slow_resources)
            try:
                with tqdm(total=len(area_rows), desc="Scraping areas") as pbar:
                    for _, row in area_rows:
                        try:
                            result = scrape_area_transactions(page, row)
                        except Exception as exc:
                            logger.warning(f"Unexpected exception for {row.get('Subdistrict', 'unknown')}: {exc}")
                            result = {"success": False, "data": [], "area": row.get("Subdistrict", "unknown"), "error": str(exc)}
                        all_data.extend(result.get("data") or [])
                        if result["success"]:
                            pbar.set_postfix({"area": result["area"][:15], "total": len(all_data)})
                        else:
                            failed_areas.append(result)
                        pbar.update(1)
            finally:
                try:
                    context.close()
                except Exception as _cleanup_err:
                    logger.debug(f"Context close error: {_cleanup_err}")
                try:
                    browser.close()
                except Exception as _cleanup_err:
                    logger.debug(f"Browser close error: {_cleanup_err}")
    
    logger.info(f"✅ Successfully scraped {len(all_data)} transactions from {len(area_rows) - len(failed_areas)} areas")
    if failed_areas:
        logger.warning(f"⚠️ Failed to scrape {len(failed_areas)} areas")
        logger.warning(
            "Failed transaction areas: %s",
            ", ".join(
                str(result.get("area", "unknown"))
                for result in failed_areas
            ),
        )

    # ============ COMBINE AND CLEAN DATA ============
    new_data_df = pd.DataFrame(all_data)
    
    # Log data quality
    if not new_data_df.empty:
        logger.info(f"📊 New data quality:")
        logger.info(f"  Total records: {len(new_data_df)}")
        logger.info(f"  With region: {new_data_df['region'].notna().sum()} ({new_data_df['region'].notna().sum()/len(new_data_df)*100:.1f}%)")
        logger.info(f"  With district: {new_data_df['district'].notna().sum()} ({new_data_df['district'].notna().sum()/len(new_data_df)*100:.1f}%)")
        logger.info(f"  With building_code: {new_data_df['building_code'].notna().sum()} ({new_data_df['building_code'].notna().sum()/len(new_data_df)*100:.1f}%)")
        
        # Transaction type breakdown
        if 'transaction_type' in new_data_df.columns:
            type_counts = new_data_df['transaction_type'].value_counts()
            logger.info(f"  Transaction types:")
            for ttype, count in type_counts.items():
                logger.info(f"    {ttype}: {count} ({count/len(new_data_df)*100:.1f}%)")
    
    if not existing_data.empty and not new_data_df.empty:
        logger.info(f"Combining {len(existing_data)} existing and {len(new_data_df)} new transactions")
        combined_df = pd.concat([existing_data, new_data_df], ignore_index=True)
    elif not new_data_df.empty:
        logger.info(f"Using {len(new_data_df)} new transactions (no existing data)")
        combined_df = new_data_df
    elif not existing_data.empty:
        logger.info(f"No new transactions found, returning {len(existing_data)} existing transactions")
        combined_df = existing_data
    else:
        logger.info("No transactions found")
        combined_df = pd.DataFrame()

    # Deduplicate and clean data types
    if not combined_df.empty:
        combined_df = drop_non_parquet_serializable_columns(combined_df)
        combined_df = fix_transaction_df_parquet_types(combined_df)

        # Normalize timestamp
        if 'scrape_timestamp' in combined_df.columns:
            combined_df['scrape_timestamp'] = pd.to_datetime(
                combined_df['scrape_timestamp'], errors='coerce'
            ).dt.strftime('%Y-%m-%d %H:%M:%S')

        before_dedup = len(combined_df)
        combined_df = _deduplicate_transaction_rows(combined_df)
        after_dedup = len(combined_df)
        if before_dedup != after_dedup:
            logger.info(
                "Removed %s duplicate transactions using IDs and fallback keys",
                before_dedup - after_dedup,
            )

    logger.info(f"Final dataset contains {len(combined_df)} transactions")
    
    # Record node execution
    record_node_execution(
        node_name="transaction_data_scraper",
        node_type="transaction",
        metadata={
            "records_processed": len(combined_df),
            "areas_processed": len(area_df),
            "failed_areas": len(failed_areas),
            "failed_area_codes": sorted(
                str(row["Code"])
                for _, row in area_rows
                if row.get("Subdistrict")
                in {result.get("area") for result in failed_areas}
            ),
            "execution_time": datetime.now().isoformat(),
        },
        live_state=live_state,
        dataset_state={
            "path": transaction_file,
            "row_count": len(combined_df),
            "recovery": recovery_metadata,
        },
    )
    
    return combined_df

    


########################## scrape transaction data ##########################

'''def process_transaction_data(
    trans_df: pd.DataFrame,
    params: Dict[str, Any]
) -> pd.DataFrame:
    """Process and structure raw transaction data"""
    logger.info("Processing transaction data")
    
    """Safe numeric conversion with error handling"""
    logger.info("Processing transaction prices")
    
    # Clean price columns with NaN handling
    trans_df['price'] = trans_df['price'].replace('', np.nan)
    trans_df['ft_price'] = trans_df['ft_price'].replace('', np.nan)
    
    # Convert to numeric types
    trans_df['price'] = pd.to_numeric(
        trans_df['price'].str.replace('[^\d]', '', regex=True),
        errors='coerce'
    )
    
    trans_df['ft_price'] = pd.to_numeric(
        trans_df['ft_price'].str.replace('[^\d]', '', regex=True),
        errors='coerce'
    )
    
    # Address parsing
    keywords = {
        'Phase': params['phase_keywords'],
        'Tower/Block': params['block_keywords'],
        'Floor': params['floor_keywords'],
        'Flat': params['flat_keywords']
    }
    
    def parse_address(address: str) -> dict:
        parts = address.split('・')
        result = {'Building': parts[0], 'Phase': None, 'Tower/Block': None, 
                 'Floor': None, 'Flat': None, 'Floor_Type': None}
        
        for part in parts[1:]:
            for key, terms in keywords.items():
                if any(term in part for term in terms):
                    result[key] = part
                    break
            if 'Upper Floor' in part or 'Middle Floor' in part or 'Lower Floor' in part:
                result['Floor_Type'] = part
                
        return result
    
    address_components = trans_df['address'].apply(parse_address).apply(pd.Series)
    return pd.concat([trans_df, address_components], axis=1)'''
    
def process_transaction_data(
    trans_df: pd.DataFrame,
    params: Dict[str, Any]
) -> pd.DataFrame:
    """
    Process transaction data - UPDATED for JavaScript extraction.
    Data from JavaScript is already clean, just do minimal validation.
    """
    logger.info(f"🔄 Processing transaction data: {len(trans_df)} records")
    
    # Create a copy to avoid modifying original
    processed_df = trans_df.copy()
    
    # Filter out rows with empty or invalid dates
    initial_count = len(processed_df)
    
    # Remove rows where date is empty, None, or invalid
    if 'date' in processed_df.columns:
        processed_df = processed_df.dropna(subset=['date'])
        processed_df = processed_df[processed_df['date'] != '']
        if processed_df['date'].dtype == 'object':
            processed_df = processed_df[processed_df['date'].astype(str).str.strip() != '']
    
    # Check Name column instead of address (JavaScript extraction uses 'Name' not 'address')
    if 'Name' in processed_df.columns:
        processed_df = processed_df.dropna(subset=['Name'])
        processed_df = processed_df[processed_df['Name'] != '']
        if processed_df['Name'].dtype == 'object':
            processed_df = processed_df[processed_df['Name'].astype(str).str.strip() != '']
    
    # Remove rows where price is empty (these are likely incomplete records)
    if 'price' in processed_df.columns:
        processed_df = processed_df.dropna(subset=['price'])
        processed_df = processed_df[processed_df['price'] != 0]
    
    final_count = len(processed_df)
    removed_count = initial_count - final_count
    
    logger.info(f"📊 Data cleaning results:")
    logger.info(f"  - Initial records: {initial_count:,}")
    logger.info(f"  - Valid records: {final_count:,}")
    logger.info(f"  - Removed invalid records: {removed_count:,}")
    if initial_count > 0:
        logger.info(f"  - Success rate: {(final_count/initial_count)*100:.1f}%")
    else:
        logger.info(f"  - Success rate: N/A (no initial records)")
    
    # Additional data cleaning (minimal - JavaScript data is already clean)
    if len(processed_df) > 0:
        logger.info(f"✅ Data processing completed successfully")
    
    return processed_df


def load_existing_enriched_snapshot(params: Dict[str, Any]) -> pd.DataFrame:
    """Load the current enriched snapshot as an explicit Kedro node."""
    path = params.get("source_a_res", {}).get(
        "base_file",
        "data/02_intermediate/centaline_res_base.parquet",
    )
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_parquet(path)


def enrich_estate_data(
    estate_details_df: pd.DataFrame,
    transactions_df: pd.DataFrame,
    params: Dict[str, Any] = None,
    estate_listings: Optional[pd.DataFrame] = None,
    existing_enriched: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Enhanced estate data enrichment with 3-layer region matching and duplicate estate handling.
    Matches transactions to estates using Name + Region + District + Subdistrict.
    For duplicate estates, checks transaction HTML to find correct match.
    """
    logger.info("🏗️ Starting enhanced estate data enrichment with 3-layer region matching")
    
    # Set default params if not provided
    if params is None:
        params = {}
    
    estate_listings = (
        estate_listings.copy()
        if estate_listings is not None
        else pd.DataFrame()
    )
    existing_enriched = (
        existing_enriched.copy()
        if existing_enriched is not None
        else pd.DataFrame()
    )
    logger.info("Existing enriched records: %s", len(existing_enriched))

    # Simple estate name extraction (without complex matching)
    def extract_estate_name_from_address(address: str) -> str:
        """Extract basic estate name from transaction address"""
        try:
            if pd.isna(address) or address is None:
                return ""
            
            address = str(address).strip()
            if not address:
                return ""
            
            # Simple extraction: take first part before common separators
            stop_indicators = [
                'Tower', 'Block', 'Upper', 'Lower', 'Middle', 'Floor',
                '/F', 'LG', 'UG', 'Flat', 'No.', 'G/F', 'B/M', 'U/L',
                'Carpark', 'Site'
            ]
            
            words = address.split()
            estate_words = []
            
            for i, word in enumerate(words):
                if any(indicator in word for indicator in stop_indicators):
                    break
                if re.match(r'\d+/F', word):
                    break
                estate_words.append(word)
            
            result = ' '.join(estate_words)
            result = re.sub(r'[,\(\)\[\]]+$', '', result).strip()
            return result
            
        except Exception as e:
            logger.debug(f"Error extracting estate name from '{address}': {str(e)}")
            return ""

    # Process transactions - NEW APPROACH: Use building_code for matching
    logger.info("🔍 Starting two-step estate enrichment (building_code -> name)...")
    
    try:
        transactions_copy = transactions_df.copy()
        
        # Transactions from JavaScript already have:
        # - region, district, subdistrict (100% complete)
        # - building_code (for matching)
        # - estate_name, building_name
        # We just need to enrich with estate details
        
        logger.info(f"📊 Transaction data quality:")
        logger.info(f"  Total transactions: {len(transactions_copy)}")
        if 'region' in transactions_copy.columns:
            logger.info(f"  With region: {transactions_copy['region'].notna().sum()} ({transactions_copy['region'].notna().sum()/len(transactions_copy)*100:.1f}%)")
        if 'building_code' in transactions_copy.columns:
            logger.info(f"  With building_code: {transactions_copy['building_code'].notna().sum()} ({transactions_copy['building_code'].notna().sum()/len(transactions_copy)*100:.1f}%)")
        
        # Initialize enrichment columns
        transactions_copy['matched_estate_name'] = ''
        transactions_copy['estate_region'] = ''
        transactions_copy['estate_district'] = ''
        transactions_copy['estate_subdistrict'] = ''
        transactions_copy['estate_blocks'] = ''
        transactions_copy['estate_units'] = ''
        transactions_copy['match_method'] = 'no_match'
        
        # Add processing metadata (if not already present)
        if 'processing_timestamp' not in transactions_copy.columns:
            transactions_copy['processing_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if 'source' not in transactions_copy.columns:
            transactions_copy['source'] = 'source_a_res'
        if 'property_type' not in transactions_copy.columns:
            transactions_copy['property_type'] = 'residential'
        
        # Fix date format (remove the incorrect year column creation)
        if 'date' in transactions_copy.columns:
            # Convert date to dd/mm/yyyy format
            try:
                parsed_dates = transactions_copy['date'].map(parse_date_from_string)
                date_dt = pd.to_datetime(parsed_dates, errors='coerce')
                transactions_copy['date'] = date_dt.dt.strftime('%d/%m/%Y')
                logger.info("✅ Fixed date format to dd/mm/yyyy")
            except Exception as e:
                logger.warning(f"⚠️ Could not fix date format: {e}")
        
        # Add building completion year and age from estate details
        if not estate_details_df.empty:
            try:
                # Extract completion year from Occupation Permit column
                estate_details_copy = estate_details_df.copy()
                
                # Extract completion year from Occupation Permit
                estate_details_copy['completion_year'] = estate_details_copy['Occupation Permit'].apply(extract_completion_year)

                # Deduplicate estate details before joining to prevent duplicates
                estate_completion_unique = estate_details_copy.drop_duplicates(subset='Scraped Estate Name', keep='first')

                # Join with estate details to get completion year
                transactions_copy = transactions_copy.merge(
                    estate_completion_unique[['Scraped Estate Name', 'completion_year']],
                    left_on='estate_name',
                    right_on='Scraped Estate Name',
                    how='left'
                )
                
                # Drop the duplicate column
                if 'Scraped Estate Name' in transactions_copy.columns:
                    transactions_copy = transactions_copy.drop('Scraped Estate Name', axis=1)
                
                # Calculate building age and update year column
                if 'completion_year' in transactions_copy.columns:
                    current_year = datetime.now().year
                    transactions_copy['age'] = transactions_copy['completion_year'].apply(
                        lambda x: max(0, current_year - x) if pd.notna(x) else None
                    )
                    
                    # Replace the year column with completion_year (this is what we want)
                    transactions_copy['year'] = transactions_copy['completion_year']
                    
                    # Drop the completion_year column since we're using year
                    transactions_copy = transactions_copy.drop('completion_year', axis=1)
                    
                    logger.info(f"✅ Added building completion year and age from {estate_details_copy['completion_year'].notna().sum()} estates")
                    logger.info(f"✅ Updated year column to contain completion years")
                else:
                    logger.warning("⚠️ No completion year data found in estate details")
                
                # MULTI-STEP MAPPING: estate_code -> building_code -> exact name -> fuzzy name
                try:
                    centa_params = params.get("source_a_res", {})
                    logger.info(f"Enriching {len(transactions_copy)} transactions with estate details")
                    logger.info("   Matching: estate_code -> building_code -> name -> fuzzy")
                    if (
                        "EstateCode" not in estate_listings.columns
                        and "Link" in estate_listings.columns
                    ):
                        estate_listings["EstateCode"] = estate_listings["Link"].apply(
                            lambda value: value.rstrip("/").split("/")[-1]
                            if pd.notna(value)
                            else ""
                        )
                    logger.info(
                        "   Received %s estates for matching",
                        len(estate_listings),
                    )

                    estate_code_map = {}
                    building_code_map = {}
                    name_map = {}
                    estate_names_list = []
                    if not estate_listings.empty:
                        for _, estate in estate_listings.iterrows():
                            e = estate.to_dict()
                            ec = str(estate.get("EstateCode", "")).strip() if pd.notna(estate.get("EstateCode")) else ""
                            if ec:
                                estate_code_map[ec] = e
                                building_code_map[ec] = e  # typeCode / building_code may match EstateCode
                            name = estate.get("Name", "")
                            if name:
                                if name not in name_map:
                                    name_map[name] = []
                                name_map[name].append(e)
                                estate_names_list.append(name)
                    fuzzy_threshold = params.get("source_a_res", {}).get("fuzzy_match_threshold", 85)
                    try:
                        from rapidfuzz import fuzz, process as rf_process
                        has_fuzz = True
                    except ImportError:
                        has_fuzz = False
                    logger.info(f"   estate_code map: {len(estate_code_map)}, building_code: {len(building_code_map)}, name: {len(name_map)}, fuzzy: {has_fuzz}")
                    matched_by_estate_code = 0
                    matched_by_building_code = 0
                    matched_by_name = 0
                    matched_by_fuzzy = 0
                    no_match = 0
                    from tqdm.auto import tqdm
                    for idx, row in tqdm(transactions_copy.iterrows(), total=len(transactions_copy), desc="Enriching"):
                        estate_info = None
                        txn_name = row.get("Name", "") or ""
                        estate_code = row.get("estate_code")
                        if estate_code is not None:
                            estate_code = str(estate_code).strip()
                        else:
                            estate_code = ""
                        building_code = (row.get("building_code") or "").strip() if pd.notna(row.get("building_code")) else ""
                        if estate_code and estate_code in estate_code_map:
                            estate_info = estate_code_map[estate_code]
                            matched_by_estate_code += 1
                            transactions_copy.at[idx, "match_method"] = "estate_code"
                        elif building_code and building_code in building_code_map:
                            estate_info = building_code_map[building_code]
                            matched_by_building_code += 1
                            transactions_copy.at[idx, "match_method"] = "building_code"
                        elif txn_name and txn_name in name_map:
                            estate_info = name_map[txn_name][0]
                            matched_by_name += 1
                            transactions_copy.at[idx, "match_method"] = "name"
                        elif has_fuzz and txn_name and estate_names_list:
                            match, score, _ = rf_process.extractOne(
                                txn_name, estate_names_list, scorer=fuzz.token_set_ratio
                            )
                            if score >= fuzzy_threshold:
                                estate_info = name_map[match][0]
                                matched_by_fuzzy += 1
                                transactions_copy.at[idx, "match_method"] = f"fuzzy_{score}"
                            else:
                                no_match += 1
                        else:
                            no_match += 1
                        
                        # Enrich with estate details if matched
                        if estate_info:
                            transactions_copy.at[idx, 'matched_estate_name'] = estate_info.get('Name', '')
                            transactions_copy.at[idx, 'estate_region'] = estate_info.get('Region', '')
                            transactions_copy.at[idx, 'estate_district'] = estate_info.get('District', '')
                            transactions_copy.at[idx, 'estate_subdistrict'] = estate_info.get('Subdistrict', '')
                            transactions_copy.at[idx, 'estate_blocks'] = str(estate_info.get('Blocks', ''))
                            transactions_copy.at[idx, 'estate_units'] = str(estate_info.get('Units', ''))
                            
                            # Add additional building info from estate details
                            transactions_copy.at[idx, 'estate_full_address'] = estate_info.get('estate_detailed_address', '')
                            transactions_copy.at[idx, 'developer'] = estate_info.get('developer', '')
                            transactions_copy.at[idx, 'estate_chinese_name'] = estate_info.get('chinese_name', '')
                            
                            # Add additional building info from estate details
                            if 'estate_detailed_address' in estate_info:
                                transactions_copy.at[idx, 'estate_full_address'] = estate_info.get('estate_detailed_address', '')
                            if 'developer' in estate_info:
                                transactions_copy.at[idx, 'developer'] = estate_info.get('developer', '')
                            if 'chinese_name' in estate_info:
                                transactions_copy.at[idx, 'estate_chinese_name'] = estate_info.get('chinese_name', '')
                    
                    logger.info(f"   Enrichment: estate_code={matched_by_estate_code}, building_code={matched_by_building_code}, name={matched_by_name}, fuzzy={matched_by_fuzzy}, no_match={no_match}")
                    logger.info("✅ Estate enrichment complete using building_code and name matching")
                    
                except Exception as e:
                    logger.warning(f"⚠️ Could not add location and developer data: {e}")
                    import traceback
                    logger.warning(traceback.format_exc())
            except Exception as e:
                logger.warning(f"⚠️ Could not add building completion year: {e}")
        
        logger.info(f"📊 Processed {len(transactions_copy)} transactions")
        
    except Exception as e:
        logger.error(f"⚠️ Error in estate name extraction: {str(e)}")
        transactions_copy = transactions_df.copy()
        transactions_copy['estate_name'] = ""
        transactions_copy['building_name'] = ""
        transactions_copy['district'] = ""
        transactions_copy['region'] = ""
        transactions_copy['subdistrict'] = ""
        transactions_copy['processing_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        transactions_copy['source'] = 'source_a_res'
        transactions_copy['property_type'] = 'residential'

    # Combine with existing data but remove duplicates
    # This allows incremental updates without duplication, and for each set
    # of duplicate keys we MERGE cells to keep the best non-null/non-empty
    # value from any source (JS or HTML), instead of just keeping "latest".
    try:
        def merge_group(group: pd.DataFrame) -> pd.Series:
            """Merge a group of duplicate rows into a single best-effort record.

            Rules:
            - For numeric-like fields (price/area etc.), prefer non-null and non-zero.
            - For strings, prefer the first non-empty, non-placeholder value.
            - If all values are null/empty, fall back to the last value.
            """
            base = group.iloc[0].copy()

            # Columns where 0 is effectively "missing" and we prefer a non-zero value
            zero_is_missing = {'price', 'ft_price', 'area', 'g_area', 'g_unit_price'}

            def is_invalid_string(val: Any) -> bool:
                if val is None:
                    return True
                if not isinstance(val, str):
                    return False
                v = val.strip()
                return v == '' or v.lower() in {'none', 'nan', '<na>'}

            for col in group.columns:
                vals = group[col]
                # Drop real NaN values
                non_na = vals.dropna()
                if non_na.empty:
                    # All NaN: keep last value (will stay NaN)
                    base[col] = vals.iloc[-1]
                    continue

                # Numeric-like merging for specific columns
                if col in zero_is_missing:
                    # Try to treat as numeric
                    try:
                        numeric_vals = pd.to_numeric(non_na, errors='coerce')
                        non_zero = numeric_vals[numeric_vals != 0]
                        if not non_zero.empty:
                            # Use the first non-zero numeric value
                            base[col] = non_zero.iloc[0]
                        else:
                            # Fall back to the first numeric value (even if zero)
                            base[col] = numeric_vals.iloc[0]
                        continue
                    except Exception:
                        # Fall through to generic handling if conversion fails
                        pass

                # String-like merging: pick first meaningful string
                picked = None
                for v in non_na:
                    if isinstance(v, str):
                        if not is_invalid_string(v):
                            picked = v
                            break
                    else:
                        # Non-string, non-NaN value
                        picked = v
                        break

                if picked is not None:
                    base[col] = picked
                else:
                    # All values are technically "invalid" strings; keep last
                    base[col] = non_na.iloc[-1]

            return base

        def merge_duplicates(df: pd.DataFrame, key_cols: list[str]) -> pd.DataFrame:
            if not key_cols:
                return df
            if key_cols == ['transaction_id'] and 'transaction_id' in df.columns:
                work = df.copy()
                ids = work['transaction_id'].astype('string').str.strip()
                valid_id = ids.notna() & ids.ne('') & ids.ne('None')
                identified = work.loc[valid_id].copy()
                object_columns = identified.select_dtypes(
                    include=['object', 'string']
                ).columns
                identified[object_columns] = identified[object_columns].replace(
                    ['', 'None', 'nan', '<NA>'], pd.NA
                )
                merged_identified = (
                    identified.groupby(
                        'transaction_id',
                        as_index=False,
                        sort=False,
                        dropna=False,
                    )
                    .last()
                )
                unidentified = work.loc[~valid_id].drop_duplicates(
                    subset=[
                        column
                        for column in ['date', 'Name', 'Tower', 'Floor', 'Flat', 'price']
                        if column in work.columns
                    ],
                    keep='last',
                )
                return pd.concat(
                    [merged_identified, unidentified],
                    ignore_index=True,
                    sort=False,
                )
            # Group by key columns and merge each group
            merged = (
                df.groupby(key_cols, as_index=False, dropna=False)
                  .apply(merge_group)
            )
            # groupby.apply puts key columns in the index; reset to plain df
            merged = merged.reset_index(drop=True)
            return merged

        # First, deduplicate/merge within the current batch itself
        current = transactions_copy.copy()
        # Preferred keys: transaction_id; fallback composite key
        if 'transaction_id' in current.columns:
            batch_keys = ['transaction_id']
        else:
            fallback_keys = ['date', 'Name', 'Tower', 'Floor', 'Flat', 'price']
            batch_keys = [k for k in fallback_keys if k in current.columns]

            if batch_keys:
                before_batch = len(current)
                current = merge_duplicates(current, batch_keys)
                after_batch = len(current)
                if after_batch != before_batch:
                    logger.info(
                        f"🧬 Merged {before_batch - after_batch:,} duplicate rows inside current batch using keys {batch_keys}"
                    )
            else:
                logger.warning("⚠️  No suitable keys found for within-batch merge; leaving batch as-is")

        # Then merge with any existing enriched data (for incremental runs)
        if not existing_enriched.empty:
            logger.info(f"📊 Found {len(existing_enriched)} existing records")

            existing_to_merge = existing_enriched
            if (
                'transaction_id' in existing_enriched.columns
                and 'transaction_id' in current.columns
            ):
                current_ids = set(
                    current['transaction_id'].dropna().astype(str)
                )
                existing_to_merge = existing_enriched[
                    existing_enriched['transaction_id'].astype(str).isin(
                        current_ids
                    )
                ]
                stale_count = len(existing_enriched) - len(existing_to_merge)
                if stale_count:
                    logger.warning(
                        "Dropping %s stale enriched rows absent from the "
                        "authoritative raw transaction dataset",
                        stale_count,
                    )

            combined = pd.concat(
                [existing_to_merge, current],
                ignore_index=True,
                sort=False,
            )
            logger.info(f"📊 Combined to {len(combined)} total records (before cross-run merge)")

            if 'transaction_id' in combined.columns:
                combined_keys = ['transaction_id']
            else:
                fallback_keys = ['date', 'Name', 'Tower', 'Floor', 'Flat', 'price']
                combined_keys = [k for k in fallback_keys if k in combined.columns]

            if combined_keys:
                before_dedup = len(combined)
                final_df = merge_duplicates(combined, combined_keys)
                after_dedup = len(final_df)
                if before_dedup != after_dedup:
                    logger.info(
                        f"🧬 Merged {before_dedup - after_dedup:,} duplicate transactions across runs using keys {combined_keys}"
                    )
                logger.info(f"✅ Final clean dataset: {len(final_df):,} unique, merged transactions")
            else:
                logger.warning("⚠️  No suitable keys found for cross-run merge; keeping all combined rows")
                final_df = combined
        else:
            final_df = current
            logger.info(f"📊 Using new transaction data: {len(final_df)} records (no existing data)")

    except Exception as e:
        logger.error(f"⚠️ Error in combining/merging duplicate data: {str(e)}")
        final_df = transactions_copy
        logger.info(f"📊 Falling back to new data only: {len(final_df)} records")

    # Fix all data types for parquet compatibility
    try:
        logger.info("\n🔧 Fixing data types for parquet compatibility...")
        
        # Integer columns (nullable)
        int_cols = ['rooms', 'completion_year', 'age']
        for col in int_cols:
            if col in final_df.columns:
                final_df[col] = pd.to_numeric(final_df[col], errors='coerce').astype('Int64')
        
        # Float columns
        float_cols = ['area', 'price', 'ft_price', 'g_area', 'g_unit_price']
        for col in float_cols:
            if col in final_df.columns:
                final_df[col] = pd.to_numeric(final_df[col], errors='coerce').astype('float64')
        
        # String columns - ensure they're strings
        str_cols = ['date', 'region', 'district', 'subdistrict', 'Name', 'Tower', 'Floor', 'Flat',
                    'transaction_type', 'source', 'property_type', 'street_address', 'building_code',
                    'estate_type', 'transaction_url', 'transaction_id', 'title_lg', 'direction',
                    'estate_name', 'building_name', 'matched_estate_name', 'estate_region',
                    'estate_district', 'estate_subdistrict', 'estate_blocks', 'estate_units', 'match_method']
        for col in str_cols:
            if col in final_df.columns:
                final_df[col] = final_df[col].astype(str).replace('nan', '').replace('<NA>', '')
        
        logger.info(f"✅ Data types fixed")
        
    except Exception as e:
        logger.error(f"⚠️ Error fixing data types: {e}")
    
    # Set final column order as requested
    try:
        logger.info("\n📋 Setting final column order...")
        
        # Exact column order as requested
        requested_columns = [
            'date', 'region', 'district', 'subdistrict', 'Name', 'Tower', 'Floor', 'Flat',
            'transaction_type', 'area', 'price', 'ft_price', 'source', 'property_type',
            'street_address', 'building_code', 'g_area', 'g_unit_price', 'completion_year',
            'age', 'estate_type', 'transaction_url', 'transaction_id', 'title_lg',
            'matched_estate_name', 'estate_region', 'estate_district', 'estate_subdistrict',
            'estate_blocks', 'estate_units', 'match_method'
        ]
        
        # Add any remaining columns
        remaining = [col for col in final_df.columns if col not in requested_columns]
        all_columns = requested_columns + remaining
        
        # Reorder (only existing columns)
        existing_columns = [col for col in all_columns if col in final_df.columns]
        final_df = final_df[existing_columns]
        
        logger.info(f"✅ Column order set: {len(existing_columns)} columns")
        
    except Exception as e:
        logger.error(f"⚠️ Error setting column order: {e}")
    
    # Generate statistics
    try:
        logger.info("\n📈 Enrichment Statistics:")
        
        if 'match_method' in final_df.columns:
            match_counts = final_df['match_method'].value_counts()
            logger.info(f"   Matching results:")
            for method, count in match_counts.items():
                logger.info(f"     {method}: {count:,} ({count/len(final_df)*100:.1f}%)")
        
        if 'transaction_type' in final_df.columns:
            type_counts = final_df['transaction_type'].value_counts()
            logger.info(f"   Transaction types:")
            for ttype, count in type_counts.items():
                logger.info(f"     {ttype}: {count:,} ({count/len(final_df)*100:.1f}%)")
        
        logger.info(f"   Total records: {len(final_df):,}")
            
    except Exception as e:
        logger.error(f"⚠️ Error generating statistics: {e}")

    # Ensure data types are consistent before saving
    try:
        # Convert age column to float64 if it exists
        if 'age' in final_df.columns:
            final_df['age'] = pd.to_numeric(final_df['age'], errors='coerce').astype('float64')
            logger.info("✅ Fixed age column data type to float64")
        
        # Convert year column to Int64 if it exists
        if 'year' in final_df.columns:
            final_df['year'] = pd.to_numeric(final_df['year'], errors='coerce').astype('Int64')
            logger.info("✅ Fixed year column data type to Int64")
            
    except Exception as e:
        logger.warning(f"⚠️ Error fixing data types: {e}")
    
    logger.info("✅ Simplified estate data enrichment completed successfully!")
    logger.info("📝 Note: Complex building matching has been moved to the centralized buildings pipeline")
    
    return final_df



