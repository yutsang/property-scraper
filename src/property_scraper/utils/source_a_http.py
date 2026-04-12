import logging
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/133.0.0.0 Safari/537.36"
)


def build_retry_session(
    total_retries: int = 3,
    backoff_factor: float = 0.8,
    status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504),
) -> requests.Session:
    """Create a requests Session with retry-aware adapters."""
    retry = Retry(
        total=total_retries,
        read=total_retries,
        connect=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset({"GET", "HEAD"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def fetch_response(
    url: str,
    timeout: int = 15,
    headers: Optional[dict[str, str]] = None,
    session: Optional[requests.Session] = None,
) -> Optional[requests.Response]:
    """Fetch a URL with retry-aware requests and return the raw response."""
    merged_headers = {"User-Agent": DEFAULT_USER_AGENT}
    if headers:
        merged_headers.update(headers)

    owns_session = session is None
    session = session or build_retry_session()
    try:
        response = session.get(url, headers=merged_headers, timeout=timeout)
        response.raise_for_status()
        return response
    except requests.RequestException as exc:
        logger.warning("Failed to fetch %s: %s", url, exc)
        return None
    finally:
        if owns_session:
            session.close()


def fetch_bytes(
    url: str,
    timeout: int = 15,
    headers: Optional[dict[str, str]] = None,
    session: Optional[requests.Session] = None,
) -> Optional[bytes]:
    """Fetch response body as bytes."""
    response = fetch_response(url, timeout=timeout, headers=headers, session=session)
    return response.content if response is not None else None


def fetch_text(
    url: str,
    timeout: int = 15,
    headers: Optional[dict[str, str]] = None,
    session: Optional[requests.Session] = None,
) -> Optional[str]:
    """Fetch response body as text."""
    response = fetch_response(url, timeout=timeout, headers=headers, session=session)
    return response.text if response is not None else None


def fetch_json(
    url: str,
    timeout: int = 15,
    headers: Optional[dict[str, str]] = None,
    session: Optional[requests.Session] = None,
) -> Optional[Any]:
    """Fetch a JSON response."""
    response = fetch_response(url, timeout=timeout, headers=headers, session=session)
    if response is None:
        return None

    try:
        return response.json()
    except ValueError as exc:
        logger.warning("Failed to decode JSON from %s: %s", url, exc)
        return None
