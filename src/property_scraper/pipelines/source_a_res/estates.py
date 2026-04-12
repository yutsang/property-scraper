import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd
import yaml
from bs4 import BeautifulSoup
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeout, sync_playwright

from ...utils.source_a_utils import (
    block_slow_resources,
    clean_subdistrict,
    estate_changed,
    parse_html_area,
    parse_html_ft_price,
    parse_html_price,
    safe_extract_bs4,
)
from ...utils.data_processing import drop_non_parquet_serializable_columns
from ...utils.node_tracker import record_node_execution
from ...utils.playwright_browser import launch_browser
from ...utils.source_runtime import get_required_setting
from ...utils.web_scraping import generate_session_id, random_sleep

logger = logging.getLogger(__name__)


def scrape_estate_listings(area_df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """Estate scraper with per-district change detection. Uses Playwright probes and pagination."""
    centa_params = params.get("source_a_res", {})
    listings_file = centa_params.get("estate_listings_file", "data/01_raw/centaline_estate_lv_1.parquet")
    meta_file = listings_file.replace(".parquet", "_meta.json")
    full_rerun = centa_params.get("full_rerun", False)
    estate_full_rerun = centa_params.get("estate_full_rerun", False)
    estate_url_template = get_required_setting(
        params, "source_a_res", "site", "estate_list_url_template"
    )
    estate_base_url = get_required_setting(params, "source_a_res", "site", "estate_base_url")
    required_columns = [
        "Name", "Address", "Blocks", "Units", "UnitRate",
        "MoM", "ForSale", "ForRent", "Link", "EstateCode", "Region",
        "District", "Subdistrict", "Code", "LastScraped",
    ]

    existing_listings = pd.DataFrame(columns=required_columns)
    district_meta: dict = {}
    if not (full_rerun or estate_full_rerun):
        try:
            if os.path.exists(listings_file):
                existing_listings = pd.read_parquet(listings_file)
                existing_listings["Subdistrict"] = existing_listings["Subdistrict"].str.strip()
                existing_listings["Code"] = existing_listings["Code"].astype(str).str.strip()
                logger.info(f"Loaded {len(existing_listings)} existing estate listings")
        except Exception as e:
            logger.error(f"Failed to load existing listings: {e}")
        try:
            if os.path.exists(meta_file):
                with open(meta_file, "r") as f:
                    district_meta = json.load(f)
        except Exception as e:
            logger.warning(f"Could not load district metadata: {e}")
    else:
        logger.info("Full rerun mode: rescraping all districts (no skip)")

    logger.info(f"🏘️  Will check {len(area_df)} districts for updates...")

    def _run_estate_listings():
        with sync_playwright() as p:
            browser = launch_browser(p, headless=params["global"].get("headless", True))
            page = browser.new_page()
            page.route("**/*", block_slow_resources)
            try:
                return _scrape_estate_listings_impl(
                    page,
                    area_df,
                    params,
                    listings_file,
                    meta_file,
                    required_columns,
                    district_meta,
                    existing_listings,
                )
            finally:
                try:
                    browser.close()
                except Exception:
                    pass

    try:
        return _run_estate_listings()
    except Exception as e:
        logger.error(f"Estate listings failed: {e}")
        return existing_listings[existing_listings["Name"].notnull()] if not existing_listings.empty else pd.DataFrame()


def _scrape_estate_listings_impl(page, area_df, params, listings_file, meta_file, required_columns, district_meta, existing_listings):
    """Estate listings: Playwright probes page 1, then full-scrapes only changed districts."""
    from tqdm.auto import tqdm

    new_or_updated_estates = []
    skipped_districts = []
    zero_count_districts = []
    district_changes = []
    probe_timeout_ms = int(params["global"].get("page_load_timeout", 35) * 1000)

    def _probe_page1_with_playwright(url):
        """Load page 1 once and return HTML-derived + NUXT-derived counts."""
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            random_sleep(params["global"]["min_delay"], params["global"]["max_delay"])
            try:
                page.wait_for_selector(
                    "a.property-text.flex.def-property-box, .transactions-content, body",
                    timeout=probe_timeout_ms,
                )
            except Exception:
                pass

            html = page.content()
            soup = BeautifulSoup(html, "html.parser")
            estate_items_page1 = soup.select("a.property-text.flex.def-property-box")
            nuxt_estates = _extract_nuxt_estate_list(page)
            page1_count = max(len(estate_items_page1), len(nuxt_estates))

            website_total = None
            total_selectors = [
                ".result-count",
                ".total-count",
                "[data-total]",
                ".search-result-count",
                ".count-label",
                "[class*='count']",
                "[class*='total']",
            ]
            for sel in total_selectors:
                el = soup.select_one(sel)
                if el:
                    digits = re.sub(r"[^\d]", "", el.get_text())
                    if digits:
                        website_total = int(digits)
                        break

            if website_total is None:
                try:
                    website_total = page.evaluate(
                        """
                        () => {
                          const state = window.__NUXT__?.state || {};
                          const buckets = [state.estate, state.property, state.search];
                          for (const bucket of buckets) {
                            if (!bucket || typeof bucket !== "object") continue;
                            for (const value of Object.values(bucket)) {
                              if (!value || typeof value !== "object") continue;
                              const total = value.total ?? value.totalCount ?? value.pagination?.total ?? value.pageInfo?.total;
                              if (typeof total === "number") return total;
                            }
                          }
                          return null;
                        }
                        """
                    )
                    if website_total is not None:
                        website_total = int(website_total)
                except Exception:
                    website_total = None

            if website_total is None:
                for el in soup.find_all(["span", "div", "p", "h2"]):
                    txt = el.get_text(strip=True)
                    match = re.search(r"(\d+)\s*Estate", txt, re.I)
                    if match:
                        website_total = int(match.group(1))
                        break
                    if re.match(r"^\d+$", txt):
                        n = int(txt)
                        if n >= page1_count:
                            website_total = n
                            break

            return {
                "estate_items_page1": estate_items_page1,
                "page1_count": page1_count,
                "website_total": website_total,
            }
        except Exception as e:
            logger.debug(f"Playwright page-1 probe failed for {url}: {e}")
            return {
                "estate_items_page1": [],
                "page1_count": 0,
                "website_total": None,
            }

    try:
        logger.info("Phase 1: Checking district page 1 with Playwright probes")
        with tqdm(area_df.iterrows(), total=len(area_df), desc="Checking districts") as district_iter:
            for _, row in district_iter:
                subdistrict = str(row["Subdistrict"]).strip()
                code = str(row["Code"]).strip()
                try:
                    subdistrict_clean = clean_subdistrict(subdistrict)
                    session_id = generate_session_id()
                    url = estate_url_template.format(
                        subdistrict_slug=subdistrict_clean,
                        code=code,
                        session_id=session_id,
                    )

                    probe = _probe_page1_with_playwright(url)
                    estate_items_page1 = probe["estate_items_page1"]
                    page1_count = probe["page1_count"]
                    website_total = probe["website_total"]

                    stored = district_meta.get(code, {})
                    stored_page1 = stored.get("page1_count")
                    stored_total = stored.get("total")

                    if page1_count == 0:
                        zero_count_districts.append(code)
                        logger.debug(
                            "Page-1 probe returned 0 items for %s (%s) — forcing full scrape",
                            subdistrict,
                            code,
                        )

                    page1_unchanged = stored_page1 is not None and page1_count == stored_page1
                    total_unchanged = (
                        website_total is not None
                        and stored_total is not None
                        and website_total == stored_total
                    )

                    if page1_unchanged and total_unchanged:
                        skipped_districts.append(code)
                        logger.debug(f"Skipping unchanged {subdistrict} ({code})")
                        district_iter.set_postfix({"district": subdistrict[:12], "skipped": len(skipped_districts)})
                        continue

                    reason = []
                    if not page1_unchanged:
                        reason.append(f"page1 {stored_page1}->{page1_count}")
                    if not total_unchanged and website_total is not None:
                        reason.append(f"total {stored_total}->{website_total}")
                    if stored_total is None:
                        reason.append("no previous data")

                    district_changes.append({
                        "district": subdistrict,
                        "code": code,
                        "row": row,
                        "stored_total": stored_total,
                        "website_total": website_total,
                        "page1_count": page1_count,
                        "estate_items_page1": estate_items_page1,
                        "reason": ", ".join(reason),
                    })
                    district_iter.set_postfix({"district": subdistrict[:12], "skipped": len(skipped_districts), "to_scrape": len(district_changes)})
                    continue

                except Exception as e:
                    logger.error(f"District check failed: {subdistrict} ({code}) - {e}")
                    continue

            if district_changes:
                logger.info(f"Phase 2: Scraping {len(district_changes)} changed district(s) with Playwright")
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                for ch in district_changes:
                    subdistrict, code, row = ch["district"], ch["code"], ch["row"]
                    estate_items_page1, page1_count = ch["estate_items_page1"], ch["page1_count"]
                    try:
                        subdistrict_clean = clean_subdistrict(subdistrict)
                        url = estate_url_template.format(
                            subdistrict_slug=subdistrict_clean,
                            code=code,
                            session_id=generate_session_id(),
                        )
                        page.goto(url, wait_until="domcontentloaded", timeout=60000)
                        random_sleep(params["global"]["min_delay"], params["global"]["max_delay"])
                        page.wait_for_selector("a.property-text.flex.def-property-box", timeout=15000)

                        nuxt_estates = _extract_nuxt_estate_list(page)
                        nuxt_by_code = {}
                        if nuxt_estates:
                            for e in nuxt_estates:
                                estate_key = (e.get("estateCode") or e.get("id") or e.get("typeCode") or "")
                                if isinstance(estate_key, str) and estate_key:
                                    nuxt_by_code[estate_key] = e
                            logger.debug(f"   __NUXT__ estate list: {len(nuxt_estates)} items")

                        district_estates = []
                        current_page = 1
                        if estate_items_page1:
                            for item in estate_items_page1:
                                try:
                                    rec = process_estate_item(item, row, estate_base_url)
                                    if rec.get("EstateCode") and rec["EstateCode"] in nuxt_by_code:
                                        for k, v in nuxt_by_code[rec["EstateCode"]].items():
                                            if k not in rec and v is not None and v != "" and not isinstance(v, (dict, list)):
                                                rec[f"nuxt_{k}"] = v
                                    district_estates.append(rec)
                                except Exception as e:
                                    logger.error(f"Error processing estate on page 1: {e}")
                        else:
                            soup_p1 = BeautifulSoup(page.content(), "html.parser")
                            for item in soup_p1.select("a.property-text.flex.def-property-box"):
                                try:
                                    rec = process_estate_item(item, row, estate_base_url)
                                    if rec.get("EstateCode") and rec["EstateCode"] in nuxt_by_code:
                                        for k, v in nuxt_by_code[rec["EstateCode"]].items():
                                            if k not in rec and v is not None and v != "" and not isinstance(v, (dict, list)):
                                                rec[f"nuxt_{k}"] = v
                                    district_estates.append(rec)
                                except Exception as e:
                                    logger.error(f"Error processing estate on page 1: {e}")

                        while True:
                            try:
                                next_loc = page.locator("button.btn-next:not([disabled])")
                                if next_loc.count() == 0:
                                    break
                                next_loc.first.click()
                                random_sleep(params["global"]["min_delay"], params["global"]["max_delay"])
                                current_page += 1
                                page.wait_for_selector("a.property-text.flex.def-property-box", timeout=20000)
                                for e in (_extract_nuxt_estate_list(page) or []):
                                    estate_key = (e.get("estateCode") or e.get("id") or e.get("typeCode") or "")
                                    if isinstance(estate_key, str) and estate_key:
                                        nuxt_by_code[estate_key] = e
                                soup_page = BeautifulSoup(page.content(), "html.parser")
                                for item in soup_page.select("a.property-text.flex.def-property-box"):
                                    try:
                                        rec = process_estate_item(item, row, estate_base_url)
                                        if rec.get("EstateCode") and rec["EstateCode"] in nuxt_by_code:
                                            for k, v in nuxt_by_code[rec["EstateCode"]].items():
                                                if k not in rec and v is not None and v != "" and not isinstance(v, (dict, list)):
                                                    rec[f"nuxt_{k}"] = v
                                        district_estates.append(rec)
                                    except Exception as e:
                                        logger.error(f"Error processing estate on page {current_page}: {e}")
                            except PlaywrightTimeout:
                                logger.warning(f"Timeout in {subdistrict} page {current_page}")
                                break
                            except Exception:
                                break

                        changed_existing = existing_listings[
                            (existing_listings["Subdistrict"] == subdistrict) &
                            (existing_listings["Code"] == code)
                        ].copy()
                        if not changed_existing.empty and set(changed_existing.columns) >= {"Name", "Address"}:
                            existing_idx = {
                                (str(r["Name"]).strip(), str(r["Address"]).strip()): r
                                for _, r in changed_existing.iterrows()
                            }
                            final_batch = []
                            for rec in district_estates:
                                key = (str(rec.get("Name", "")).strip(), str(rec.get("Address", "")).strip())
                                old = existing_idx.get(key)
                                if old is not None:
                                    rec["Changed"] = estate_changed(old, rec)
                                final_batch.append(rec)
                            district_estates = final_batch

                        for rec in district_estates:
                            rec["LastScraped"] = current_time

                        new_or_updated_estates.extend(district_estates)

                        scraped_total = len(district_estates)
                        district_meta[code] = {
                            "page1_count": page1_count,
                            "total": scraped_total,
                            "checked_at": current_time,
                        }
                        logger.info(f"{subdistrict} ({code}): {scraped_total} estates from {current_page} page(s)")
                    except Exception as e:
                        logger.error(f"District scrape failed: {subdistrict} ({code}) - {e}")

        if new_or_updated_estates:
            new_df = pd.DataFrame(new_or_updated_estates)
            updated_districts = set(
                (e["Subdistrict"], e["Code"]) for e in new_or_updated_estates
            )
            existing_to_keep = existing_listings[
                ~existing_listings[["Subdistrict", "Code"]]
                .apply(tuple, axis=1).isin(updated_districts)
            ]
            logger.info(f"Keeping {len(existing_to_keep)} estates from {len(area_df) - len(district_changes)} unchanged districts")
            final_df = pd.concat([existing_to_keep, new_df], ignore_index=True)
        else:
            logger.info("No changed districts — preserving all existing data")
            final_df = existing_listings.copy()

        logger.info(f"\n{'='*60}")
        logger.info("ESTATE SCRAPING COMPLETION REPORT")
        logger.info(f"{'='*60}")
        logger.info(f"  Districts checked:  {len(area_df)}")
        logger.info(f"  Skipped (no change): {len(skipped_districts)}")
        logger.info(f"  Re-scraped:         {len(district_changes)}")
        logger.info(f"  Phase-1 zero-count probes: {len(zero_count_districts)}")
        logger.info(f"  Estates from changed districts: {len(new_or_updated_estates)}")

        if district_changes:
            logger.info("\n  Changed districts:")
            for ch in district_changes:
                prev = ch["stored_total"] if ch["stored_total"] is not None else "new"
                web = ch["website_total"] if ch["website_total"] is not None else "?"
                logger.info(
                    f"    {ch['district']} ({ch['code']}): "
                    f"DB={prev} → Web≈{web}, scraped={district_meta.get(ch['code'], {}).get('total', '?')} "
                    f"[{ch['reason']}]"
                )

        final_df = drop_non_parquet_serializable_columns(final_df)

        before_dedup = len(final_df)
        final_df = final_df.drop_duplicates(
            subset=["Name", "Address", "Region", "District", "Subdistrict", "Code"],
            keep="last",
        )
        if before_dedup != len(final_df):
            logger.info(f"Removed {before_dedup - len(final_df)} duplicate estates")

        final_df = final_df[final_df["Name"].notnull() & (final_df["Name"].str.strip() != "")]
        final_df.to_parquet(listings_file, index=False)
        logger.info(f"📦 Saved {len(final_df)} total estates to {listings_file}")

        try:
            with open(meta_file, "w") as f:
                json.dump(district_meta, f, indent=2)
            logger.info(f"📝 Saved district metadata to {meta_file}")
        except Exception as e:
            logger.warning(f"Could not save district metadata: {e}")

        live_state = {
            "source": "estate_page1_probe",
            "probe_failed": False,
            "districts_checked": len(area_df),
            "districts_skipped": len(skipped_districts),
            "districts_changed": len(district_changes),
            "zero_probe_districts": zero_count_districts,
            "changed_codes": [ch["code"] for ch in district_changes],
        }
        record_node_execution(
            node_name="estate_listing_scraper",
            node_type="estate",
            metadata={
                "estates_total": len(final_df),
                "districts_checked": len(area_df),
                "districts_scraped": len(district_changes),
                "districts_skipped": len(skipped_districts),
            },
            live_state=live_state,
            dataset_state={
                "path": listings_file,
                "row_count": len(final_df),
                "meta_path": meta_file,
            },
        )

        return final_df
    except Exception as e:
        logger.error(f"Data consolidation failed: {e}")
        return existing_listings[existing_listings["Name"].notnull()]


def log_district_completion(subdistrict, code, current_count, final_df):
    """Deprecated — kept for backward compatibility only."""
    actual_scraped_count = len(final_df[
        (final_df["Subdistrict"] == subdistrict) &
        (final_df["Code"] == code)
    ])
    logger.info(f"  {subdistrict} ({code}): scraped={actual_scraped_count} website_page1={current_count}")


def _extract_nuxt_estate_list(page: Page) -> list:
    """Attempt to extract estate list from common __NUXT__ locations."""
    try:
        nuxt = page.evaluate("() => window.__NUXT__")
        if not nuxt:
            return []
        state = (nuxt.get("state") or {})
        candidates = []
        for bucket in ["estate", "property", "search"]:
            b = state.get(bucket)
            if isinstance(b, dict):
                candidates.append(b)
                for v in b.values():
                    if isinstance(v, dict):
                        candidates.append(v)
        for obj in candidates:
            for key in ["estateList", "list", "data", "items", "result"]:
                val = obj.get(key) if isinstance(obj, dict) else None
                if isinstance(val, list) and val:
                    if any(isinstance(x, dict) and (x.get("estateCode") or x.get("estateName") or x.get("name")) for x in val):
                        return val
        return []
    except Exception:
        return []


def process_estate_item(item, district_row, estate_base_url: str) -> dict:
    """Extract estate listing card into a flat record."""
    href = item.get("href", "")
    link = href if href.startswith("http") else f"{estate_base_url}{href}"
    estate_code = href.rstrip("/").split("/")[-1] if href else ""
    name = safe_extract_bs4(item, ".property-text-title")
    address = safe_extract_bs4(item, ".property-text-detail")

    blocks = None
    units = None
    unit_rate = None
    mom = None
    for_sale = None
    for_rent = None

    lines = [el.get_text(" ", strip=True) for el in item.select(".property-text-info, .property-text-data, .property-text-sub")]
    all_text = " | ".join(lines)

    m_blocks = re.search(r"Block\(s\)\s*[:：]?\s*(\d+)", all_text, re.I)
    if m_blocks:
        blocks = m_blocks.group(1)
    m_units = re.search(r"Unit\(s\)\s*[:：]?\s*(\d+)", all_text, re.I)
    if m_units:
        units = m_units.group(1)
    m_rate = re.search(r"Unit Rate\s*[:：]?\s*([^\|\n]+)", all_text, re.I)
    if m_rate:
        unit_rate = parse_html_ft_price(m_rate.group(1))
    m_mom = re.search(r"MoM\s*[:：]?\s*([^\|\n]+)", all_text, re.I)
    if m_mom:
        mom = m_mom.group(1)
    m_sale = re.search(r"For Sale\s*[:：]?\s*([^\|\n]+)", all_text, re.I)
    if m_sale:
        for_sale = parse_html_price(m_sale.group(1))
    m_rent = re.search(r"For Rent\s*[:：]?\s*([^\|\n]+)", all_text, re.I)
    if m_rent:
        for_rent = parse_html_price(m_rent.group(1))

    return {
        "Name": name,
        "Address": address,
        "Blocks": blocks,
        "Units": units,
        "UnitRate": unit_rate,
        "MoM": mom,
        "ForSale": for_sale,
        "ForRent": for_rent,
        "Link": link,
        "EstateCode": estate_code,
        "Region": district_row.get("Region", ""),
        "District": district_row.get("District", ""),
        "Subdistrict": district_row.get("Subdistrict", ""),
        "Code": str(district_row.get("Code", "")).strip(),
    }


def _extract_element_text_playwright(page: Page, xpath: str) -> Optional[str]:
    """Extract text from element by XPath (Playwright)."""
    try:
        el = page.locator(f"xpath={xpath}").first
        el.wait_for(state="attached", timeout=10000)
        return (el.text_content() or "").strip()
    except Exception:
        return None


def scrape_estate_details(listings_df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """
    Scrape detailed estate information with incremental updates and gap-filling.
    """
    details_file = params.get("source_a_res", {}).get(
        "estate_details_file", "data/01_raw/centaline_estate_lv_2.parquet"
    )
    full_rerun = params.get("source_a_res", {}).get("full_rerun", False)
    logger.info(f"Details File: {details_file}")

    existing_details = pd.DataFrame()
    if not full_rerun and os.path.exists(details_file):
        existing_details = pd.read_parquet(details_file)
        logger.info(f"Loaded {len(existing_details)} existing estate details")
    elif full_rerun:
        logger.info("Full rerun mode: scraping all estates (no skip)")

    done_links: set = set()
    if not existing_details.empty and "Link" in existing_details.columns:
        ec_col = "estate_code" if "estate_code" in existing_details.columns else None
        sn_col = "scraped_estate_name" if "scraped_estate_name" in existing_details.columns else None

        if ec_col and sn_col:
            done_mask = (
                existing_details[ec_col].notna() &
                (existing_details[ec_col].astype(str).str.strip() != "") &
                existing_details[sn_col].notna() &
                (existing_details[sn_col].astype(str).str.strip() != "")
            )
            incomplete_count = (~done_mask).sum()
            done_links = set(existing_details.loc[done_mask, "Link"])
            logger.info(f"✓ {len(done_links):,} estates complete, {incomplete_count:,} incomplete/missing")
        elif ec_col or sn_col:
            col = ec_col or sn_col
            done_mask = existing_details[col].notna() & (existing_details[col].astype(str).str.strip() != "")
            done_links = set(existing_details.loc[done_mask, "Link"])
            logger.info(f"✓ {len(done_links):,} estates complete (schema: {col} only)")
        else:
            done_links = set(existing_details["Link"])
            logger.info(
                f"ℹ️  Old schema, no scraped_estate_name/estate_code columns — treating all {len(done_links):,} existing records as complete"
            )
    else:
        logger.info("📂 No existing details — starting fresh")

    new_mask = ~listings_df["Link"].isin(done_links)
    estates_to_scrape = listings_df[new_mask].copy()
    already_done = len(listings_df) - len(estates_to_scrape)

    if estates_to_scrape.empty:
        logger.info("✅ All estates already scraped — nothing to do")
        return existing_details

    logger.info(f"🎯 Estates: {len(listings_df):,} total | {already_done:,} already done | {len(estates_to_scrape):,} to scrape now")

    from tqdm.auto import tqdm

    headless = params["global"].get("headless", True)

    def scrape_single_estate(page, row):
        try:
            page.goto(row["Link"], wait_until="domcontentloaded", timeout=30000)
            random_sleep(params["global"]["min_delay"], params["global"]["max_delay"])

            estate_code = row["Link"].rstrip("/").split("/")[-1] if row.get("Link") else ""

            title_el = page.locator(".estate-detail-banner-title").first
            title_el.wait_for(state="visible", timeout=10000)
            scraped_name = (title_el.text_content() or "").strip()
            detail_data = {
                "Name": row["Name"],
                "scraped_estate_name": scraped_name,
                "occupation_permit": _extract_element_text_playwright(
                    page, "//div[contains(text(), 'Date of Occupation Permit')]/following-sibling::div"
                ),
                "scraped_blocks": _extract_element_text_playwright(
                    page, "//div[contains(text(), 'No. of Block(s)')]/following-sibling::div"
                ),
                "chinese_name": None,
                "school_net_info": None,
                "estate_detailed_address": None,
                "developer": None,
                "estate_code": estate_code,
                "Link": row["Link"],
                "Region": row["Region"],
                "District": row["District"],
            }

            try:
                cn_el = page.locator(".estate-detail-banner-title-cn, .chinese-name, h1.cn, .title-cn").first.text_content()
                detail_data["chinese_name"] = (cn_el or "").strip()
            except Exception:
                try:
                    banner = page.locator(".estate-detail-banner").first.text_content()
                    chinese_matches = re.findall(r"[\u4e00-\u9fff]+", banner or "")
                    if chinese_matches:
                        detail_data["chinese_name"] = "".join(chinese_matches[:10])
                except Exception:
                    pass

            try:
                items = page.locator(".item").all()
                for div in items:
                    label_el = div.locator(".label-item-left").first.text_content()
                    if label_el and "School Net" in label_el:
                        links = div.locator("a").all()
                        if len(links) >= 2:
                            detail_data["school_net_info"] = f"{(links[0].text_content() or '').strip()} | {(links[1].text_content() or '').strip()}"
                        break
            except Exception:
                pass

            try:
                addr_el = page.locator(".estate-detail-banner-position").first.text_content()
                detail_data["estate_detailed_address"] = (addr_el or "").strip()
            except Exception:
                pass

            try:
                for item in page.locator(".item").all():
                    label_el = item.locator(".label-item-left").first.text_content()
                    if label_el and "Developer" in label_el:
                        detail_data["developer"] = (item.locator(".label-item-right").first.text_content() or "").strip()
                        break
            except Exception:
                pass

            return {"success": True, "data": detail_data}
        except Exception as e:
            logger.debug(f"Failed to scrape {row.get('Name', 'Unknown')}: {e}")
            return {"success": False, "name": row.get("Name", "Unknown"), "link": row.get("Link", ""), "error": str(e)}

    new_details = []
    failed_estates = []

    logger.info("Scraping estates sequentially (Playwright sync API is single-threaded)")

    with sync_playwright() as playwright:
        browser = launch_browser(playwright, headless=headless)
        context = browser.new_context(user_agent=params["global"].get("user_agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"))
        page = context.new_page()
        try:
            estate_rows = list(estates_to_scrape.iterrows())
            with tqdm(total=len(estate_rows), desc="Scraping estates") as pbar:
                for _, row in estate_rows:
                    try:
                        result = scrape_single_estate(page, row)
                    except Exception as exc:
                        result = {"success": False, "name": row.get("Name", "?"), "link": row.get("Link", ""), "error": str(exc)}
                    if result["success"]:
                        new_details.append(result["data"])
                    else:
                        failed_estates.append(result)
                    pbar.update(1)
        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass

    logger.info(f"✅ Successfully scraped: {len(new_details)} estates")
    if failed_estates:
        logger.warning(f"⚠️  Failed to scrape: {len(failed_estates)} estates")
        logger.info("   These will be retried on next run (gap-filling mechanism)")

    try:
        if new_details:
            new_df = pd.DataFrame(new_details)
            scraped_links = set(new_df["Link"]) if "Link" in new_df.columns else set()
            if not existing_details.empty and "Link" in existing_details.columns and scraped_links:
                existing_to_keep = existing_details[~existing_details["Link"].isin(scraped_links)]
            else:
                existing_to_keep = existing_details
            updated_df = pd.concat([existing_to_keep, new_df], ignore_index=True)
            updated_df.to_parquet(details_file, index=False)
            logger.info(f"💾 Saved {len(updated_df):,} estate details ({len(new_df):,} new/updated)")

            record_node_execution(
                node_name="estate_detail_scraper",
                node_type="estate",
                metadata={
                    "estates_processed": len(updated_df),
                    "new_details_scraped": len(new_df),
                    "execution_time": datetime.now().isoformat(),
                },
                dataset_state={
                    "path": details_file,
                    "row_count": len(updated_df),
                    "failed_estates": len(failed_estates),
                },
            )
            return updated_df[updated_df["Name"].notnull()]

        record_node_execution(
            node_name="estate_detail_scraper",
            node_type="estate",
            metadata={
                "estates_processed": len(existing_details),
                "new_details_scraped": 0,
                "execution_time": datetime.now().isoformat(),
            },
            dataset_state={
                "path": details_file,
                "row_count": len(existing_details),
                "failed_estates": len(failed_estates),
            },
        )
        return existing_details
    except Exception as e:
        logger.error(f"Fatal error in scrape_estate_details: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return existing_details if not existing_details.empty else pd.DataFrame()


def update_control_date(params: Dict[str, Any]) -> None:
    try:
        params_path = "conf/base/parameters.yml"
        with open(params_path, "r") as file:
            parameters = yaml.safe_load(file)

        parameters["source_a_estates"] = datetime.now().strftime("%Y-%m-%d")

        with open(params_path, "w") as file:
            yaml.dump(parameters, file, default_flow_style=False)

        logger.info("Successfully updated control date for estate details")
    except Exception as e:
        logger.error(f"Failed to update control date: {str(e)}")
