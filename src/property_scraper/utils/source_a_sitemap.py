"""
Update Source A residential area codes from the configured sitemap.
Fetches sitemap XML, parses list URLs to extract HMA/region codes,
and merges any new codes into the local area-code CSV.
"""
import logging
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from .source_a_http import fetch_bytes

logger = logging.getLogger(__name__)

SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
# URL pattern: .../list/transaction/pok-fu-lam_19-HMA155 or .../list/estate/pok-fu-lam_19-HMA155
AREA_URL_PATTERN = re.compile(
    r"/([a-z0-9-]+)_19-(HMA\d+)(?:\?|$|/)",
    re.I,
)


def fetch_url(url: str, timeout: int = 15) -> Optional[bytes]:
    """Fetch URL content with standard headers."""
    return fetch_bytes(url, timeout=timeout, headers={"User-Agent": USER_AGENT})


def parse_sitemap_index(xml_content: bytes) -> list[str]:
    """Extract child sitemap <loc> URLs from sitemap index."""
    urls = []
    try:
        root = ET.fromstring(xml_content)
        for loc in root.findall(".//sm:loc", SITEMAP_NS):
            if loc.text:
                urls.append(loc.text.strip())
    except ET.ParseError as e:
        logger.warning(f"Sitemap parse error: {e}")
    return urls


def extract_area_codes_from_sitemap(xml_content: bytes) -> list[tuple[str, str]]:
    """
    Parse sitemap URLs and extract (subdistrict_slug, code) pairs.
    Returns e.g. [("pok-fu-lam", "HMA155"), ...].
    """
    pairs: list[tuple[str, str]] = []
    try:
        root = ET.fromstring(xml_content)
        # Handle both sitemap index and urlset
        for elem in root.iter():
            if elem.tag.endswith("loc") and elem.text:
                url = elem.text.strip()
                m = AREA_URL_PATTERN.search(url)
                if m:
                    subdistrict_slug, code = m.groups()
                    pairs.append((subdistrict_slug, code))
    except ET.ParseError as e:
        logger.warning(f"Sitemap parse error: {e}")
    return pairs


def slug_to_subdistrict(slug: str) -> str:
    """Convert URL slug to display name (e.g. pok-fu-lam -> Pok Fu Lam)."""
    return slug.replace("-", " ").title()


def update_area_codes_from_sitemap(
    area_code_path: str = "data/01_raw/Centanet_Res_Area_Code.csv",
    source_config: Optional[dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Fetch Source A sitemap, extract area codes from findproperty URLs,
    merge any new codes into the existing CSV, and return the full area codes DataFrame.

    If sitemap fetch fails, returns existing CSV contents unchanged.
    """
    path = Path(area_code_path)
    if not path.exists():
        logger.warning(f"Area code file not found: {path}")
        return pd.DataFrame()

    existing = pd.read_csv(path, encoding="utf-8-sig")
    required = ["Region", "District", "Subdistrict", "Code"]
    for col in required:
        if col not in existing.columns:
            logger.warning(f"Existing CSV missing column: {col}")
            return existing

    site_settings = source_config or {}
    sitemap_url = site_settings.get("sitemap_index_url")
    if not sitemap_url:
        raise ValueError(
            "Missing required private config 'source_a_res.site.sitemap_index_url'. "
            "Define it in conf/local/parameters.yml."
        )
    sitemap_patterns = site_settings.get(
        "sitemap_patterns",
        ["sitemap_TC_HMA_buy.xml", "sitemap_TC_HMA_rent.xml", "sitemap_TC_HMA_transaction.xml"],
    )
    request_headers = site_settings.get("request_headers", {"User-Agent": USER_AGENT})

    # Fetch sitemap index
    index_xml = fetch_bytes(sitemap_url, timeout=15, headers=request_headers)
    if not index_xml:
        logger.info("Sitemap index unavailable — keeping existing area codes")
        return existing

    child_urls = parse_sitemap_index(index_xml)
    hma_urls = [u for u in child_urls if any(p in u for p in sitemap_patterns)]

    all_pairs: set[tuple[str, str]] = set()
    for url in hma_urls[:6]:  # Limit to avoid too many requests
        xml = fetch_bytes(url, timeout=15, headers=request_headers)
        if xml:
            pairs = extract_area_codes_from_sitemap(xml)
            all_pairs.update(pairs)

    if not all_pairs:
        logger.info("No area codes extracted from sitemap — keeping existing")
        return existing

    existing_codes = set(existing["Code"].astype(str).str.strip())
    existing_key = set(zip(existing["Subdistrict"].astype(str).str.strip(), existing["Code"].astype(str).str.strip()))

    new_rows = []
    for slug, code in all_pairs:
        if (slug, code) in existing_key:
            continue
        subdistrict = slug_to_subdistrict(slug)
        # Infer region from code prefix or default
        region = "Hong Kong Island"  # Default; could be refined from sitemap structure
        district = subdistrict
        new_rows.append({
            "Region": region,
            "District": district,
            "Subdistrict": subdistrict,
            "Code": code,
        })

    if not new_rows:
        logger.info("Area codes are up to date — no new codes from sitemap")
        return existing

    new_df = pd.DataFrame(new_rows)
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["Subdistrict", "Code"], keep="last")
    combined.to_csv(path, index=False, encoding="utf-8-sig")
    logger.info(f"Updated area codes: added {len(new_rows)} new codes from sitemap")
    return combined
