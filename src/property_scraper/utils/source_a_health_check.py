"""
API health check for Source A before pipeline run.
Validates that sitemap/data is up to date (lastmod within max_stale_days).
If stale, requires user to set confirm_stale_data=True in extra_params to proceed.
"""
import logging
from datetime import datetime, timezone

import xml.etree.ElementTree as ET
from .source_a_http import fetch_bytes
from .source_runtime import get_optional_setting, get_required_setting

logger = logging.getLogger(__name__)

SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


class StaleDataError(Exception):
    """Raised when API data is stale and user has not confirmed to proceed."""

    def __init__(self, message: str, latest_date: str | None):
        super().__init__(message)
        self.latest_date = latest_date


def parse_lastmod(xml_content: bytes) -> list[str]:
    """Extract all lastmod values from sitemap index."""
    dates = []
    try:
        root = ET.fromstring(xml_content)
        for lastmod in root.findall(".//sm:lastmod", SITEMAP_NS):
            if lastmod.text:
                dates.append(lastmod.text.strip())
        for elem in root.iter():
            if elem.tag.endswith("lastmod") and elem.text:
                dates.append(elem.text.strip())
    except ET.ParseError:
        pass
    return dates


def parse_lastmod_to_datetime(date_str: str) -> datetime | None:
    """Parse ISO format lastmod to datetime."""
    try:
        # Handle 2026-03-13T18:45:00.5307257+08:00
        parts = date_str.split("T")
        if len(parts) >= 2:
            date_part = parts[0]
            time_part = parts[1].split("+")[0].split("-")[0].split(".")[0]
            clean = f"{date_part}T{time_part}"
            return datetime.fromisoformat(clean.replace("Z", "+00:00"))
    except Exception:
        pass
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except Exception:
        return None


def check_source_a_health(
    params: dict,
    max_stale_days: int = 10,
    timeout: int = 15,
) -> dict:
    """
    Check if Source A APIs/sitemap provide up-to-date data.
    Returns dict with: ok (bool), latest_date (str|None), message (str).
    If data is stale and confirm_stale_data is not True, raises StaleDataError.
    """
    confirm_stale = params.get("source_a_res", {}).get("confirm_stale_data", False)
    global_params = params.get("global", {})
    if isinstance(global_params, dict):
        confirm_stale = confirm_stale or global_params.get("confirm_stale_data", False)

    sitemap_url = get_required_setting(params, "source_a_res", "site", "sitemap_index_url")
    request_headers = get_optional_setting(
        params,
        "source_a_res",
        "site",
        "request_headers",
        default={"User-Agent": USER_AGENT},
    )
    try:
        xml_content = fetch_bytes(
            sitemap_url,
            timeout=timeout,
            headers=request_headers,
        )
        if not xml_content:
            raise OSError("empty sitemap response")
    except OSError as e:
        logger.warning(f"Health check: could not fetch sitemap: {e}")
        return {
            "ok": False,
            "latest_date": None,
            "message": f"Could not fetch sitemap: {e}",
        }

    lastmods = parse_lastmod(xml_content)
    if not lastmods:
        return {"ok": True, "latest_date": None, "message": "No lastmod in sitemap (proceeding)"}

    now = datetime.now(timezone.utc)
    best_date: datetime | None = None

    for lm in lastmods:
        dt = parse_lastmod_to_datetime(lm)
        if dt:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if best_date is None or dt > best_date:
                best_date = dt

    if best_date is None:
        return {"ok": True, "latest_date": None, "message": "Could not parse lastmod (proceeding)"}

    age_days = (now - best_date).total_seconds() / 86400
    latest_str = best_date.strftime("%Y-%m-%d")

    if age_days <= max_stale_days:
        return {
            "ok": True,
            "latest_date": latest_str,
            "message": f"Data is up to date (lastmod: {latest_str}, {age_days:.1f} days ago)",
        }

    # Stale
    msg = (
        f"Source A sitemap data is {age_days:.1f} days old (lastmod: {latest_str}). "
        f"Maximum allowed: {max_stale_days} days. "
        "Set confirm_stale_data=True in extra_params to proceed anyway."
    )
    if not confirm_stale:
        raise StaleDataError(msg, latest_str)

    logger.warning(msg)
    return {
        "ok": True,
        "latest_date": latest_str,
        "message": msg + " (User confirmed to proceed)",
    }
