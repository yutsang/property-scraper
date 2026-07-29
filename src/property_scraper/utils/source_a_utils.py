"""
Source A-specific utilities for property scraper pipelines.

Shared parsers, helpers, and extraction logic used across source_a_res nodes.
"""

import re
import logging
from datetime import datetime, date
from typing import Optional, Any, Callable

import pandas as pd

logger = logging.getLogger(__name__)


# ============ DATE PARSING ============

def parse_date_from_string(date_str: Any) -> Optional[date]:
    """
    Parse date from various string formats (ISO, dd/mm/yyyy, etc.).
    Used for transaction control dates and incremental scraping.
    """
    if not date_str or pd.isna(date_str):
        return None
    date_str = str(date_str).strip()
    # Parse explicit formats before pandas inference. Inference treats ambiguous
    # values such as 07/12/2026 as month-first on some systems, which can move
    # the incremental watermark by months and skip valid transactions.
    normalized = date_str.replace("Z", "+00:00")
    try:
        parsed_iso = datetime.fromisoformat(normalized)
        return parsed_iso.date()
    except (ValueError, TypeError):
        pass

    for fmt in (
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%Y%m%d",
        "%Y/%m/%d",
    ):
        try:
            return datetime.strptime(date_str, fmt).date()
        except (ValueError, TypeError):
            continue

    try:
        parsed = pd.to_datetime(date_str, errors="coerce", dayfirst=True)
        if pd.notna(parsed):
            return parsed.date()
    except Exception:
        pass
    return None


def extract_completion_year(permit_str: Any) -> Optional[int]:
    """Extract year from occupation permit string (e.g. '2004/11', '1986/2')."""
    if pd.isna(permit_str) or permit_str == "None":
        return None
    try:
        if "/" in str(permit_str):
            year_part = str(permit_str).split("/")[0]
            if year_part.isdigit() and len(year_part) == 4:
                return int(year_part)
    except Exception:
        pass
    return None


# ============ HTML VALUE PARSING (Source A list pages) ============

def parse_html_area(area_text: Any) -> Optional[float]:
    """Parse area from HTML like '401呎' to 401.0."""
    if not area_text or area_text == "--":
        return None
    area_clean = re.sub(r"[^\d,.]", "", str(area_text)).replace(",", "")
    return float(area_clean) if area_clean else None


def parse_html_price(price_text: Any) -> Optional[float]:
    """Parse price from HTML like '$545.5萬' to 5455000.0."""
    if not price_text or price_text == "--":
        return None
    price_str = str(price_text).replace("$", "").replace(",", "")
    # Match decimal numbers properly (e.g. "545.5" not "545" and ".5" separately)
    if "萬" in price_str:
        m = re.findall(r"\d+(?:\.\d+)?", price_str)
        return float(m[0]) * 10000 if m else None
    if "億" in price_str:
        m = re.findall(r"\d+(?:\.\d+)?", price_str)
        return float(m[0]) * 100_000_000 if m else None
    m = re.findall(r"\d+(?:\.\d+)?", price_str)
    return float(m[0]) if m else None


def parse_html_ft_price(ft_price_text: Any) -> Optional[float]:
    """Parse ft_price from HTML like '@$13,603' to 13603.0."""
    if not ft_price_text or ft_price_text == "--":
        return None
    clean = str(ft_price_text).replace("@", "").replace("$", "").replace(",", "")
    m = re.findall(r"[\d.]+", clean)
    return float(m[0]) if m else None


# ============ URL / STRING HELPERS ============

def clean_subdistrict(subdistrict: str) -> str:
    """Clean subdistrict for URL (e.g. 'Residence Bel-air' -> 'residence-bel-air')."""
    cleaned = re.sub(r"[^A-Za-z0-9]+", "-", subdistrict)
    return cleaned.strip("-").lower()


def block_slow_resources(route: Any):
    """Playwright route handler: abort images, fonts, analytics, maps."""
    req = route.request
    if req.resource_type in ("image", "media", "font"):
        return route.abort()
    url = (req.url or "").lower()
    if any(x in url for x in ("google-analytics", "googletagmanager", "maps.googleapis", "assets.giocdn.com")):
        return route.abort()
    return route.continue_()


async def block_slow_resources_async(route: Any):
    """Async Playwright route handler: abort images, fonts, analytics, maps."""
    req = route.request
    if req.resource_type in ("image", "media", "font"):
        await route.abort()
        return
    url = (req.url or "").lower()
    if any(x in url for x in ("google-analytics", "googletagmanager", "maps.googleapis", "assets.giocdn.com")):
        await route.abort()
        return
    await route.continue_()


# ============ ESTATE HELPERS ============

def estate_changed(existing: Any, new: dict) -> bool:
    """Compare key fields for estate change detection."""
    fields = ["Address", "Blocks", "Units", "UnitRate", "ForSale", "ForRent"]
    return any(str(existing.get(f, "")) != str(new.get(f, "")) for f in fields)


def safe_extract_bs4(item, selector: str) -> str:
    """Safe text extraction from BeautifulSoup element."""
    try:
        el = item.select_one(selector)
        return el.get_text(strip=True) if el else ""
    except Exception:
        return ""
