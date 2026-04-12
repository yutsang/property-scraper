"""
API client utilities for property scraper pipelines.

This module provides common functions for API interactions including:
- GraphQL query execution
- REST API request handling
- Response parsing and validation
- Rate limiting and throttling
- Authentication and header management
"""

import requests
import json
import time
import random
import logging
from typing import Dict, Any, Optional, List, Union, Tuple
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Optional imports for tenacity
TENACITY_AVAILABLE = True
try:
    from tenacity import retry, wait_exponential, stop_after_attempt  # type: ignore
except ImportError:
    TENACITY_AVAILABLE = False
    retry = None
    wait_exponential = None
    stop_after_attempt = None

logger = logging.getLogger(__name__)

# ============ AUTHENTICATION AND HEADERS ============

def create_source_b_headers(
    authorization: Optional[str] = None, extra_headers: Optional[Dict[str, str]] = None
) -> Dict[str, str]:
    """Create public-safe default headers for Source B requests."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
    }
    if authorization:
        headers["Authorization"] = authorization
    if extra_headers:
        headers.update(extra_headers)
    return headers


def create_source_a_headers(extra_headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """Create public-safe default headers for Source A requests."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }
    if extra_headers:
        headers.update(extra_headers)
    return headers


def create_source_a_cookies(extra_cookies: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """Create default cookies for Source A requests."""
    cookies = {"lang": "en", "currency": "HKD", "unit": "feet"}
    if extra_cookies:
        cookies.update(extra_cookies)
    return cookies

# ============ RATE LIMITING AND THROTTLING ============

def smart_delay(min_delay: float = 0.3, max_delay: float = 1.2) -> None:
    """
    Intelligent delay with random timing.
    
    Args:
        min_delay: Minimum delay in seconds
        max_delay: Maximum delay in seconds
    """
    delay = random.uniform(min_delay, max_delay)
    time.sleep(delay)

def throttled_request(url: str, method: str = 'GET', 
                     delay_range: Tuple[float, float] = (0.2, 0.8),
                     **kwargs) -> requests.Response:
    """
    Make a throttled HTTP request with automatic delay.
    
    Args:
        url: Target URL
        method: HTTP method
        delay_range: Tuple of (min_delay, max_delay) in seconds
        **kwargs: Additional arguments for requests
        
    Returns:
        requests.Response object
    """
    smart_delay(delay_range[0], delay_range[1])
    
    if method.upper() == 'GET':
        return requests.get(url, **kwargs)
    elif method.upper() == 'POST':
        return requests.post(url, **kwargs)
    else:
        raise ValueError(f"Unsupported HTTP method: {method}")

# ============ GRAPHQL UTILITIES ============

def execute_graphql_query(url: str, query: str, variables: Optional[Dict[str, Any]] = None,
                         headers: Optional[Dict[str, str]] = None, 
                         max_retries: int = 3) -> Dict[str, Any]:
    """
    Execute a GraphQL query with retry logic.
    
    Args:
        url: GraphQL endpoint URL
        query: GraphQL query string
        variables: Query variables
        headers: HTTP headers
        max_retries: Maximum number of retries
        
    Returns:
        Dictionary containing the GraphQL response
    """
    payload = {
        "query": query,
        "variables": variables or {}
    }
    
    if headers is None:
        headers = {'Content-Type': 'application/json'}
    
    for attempt in range(max_retries):
        try:
            response = throttled_request(
                url=url,
                method='POST',
                json=payload,
                headers=headers,
                timeout=30
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise
            logger.warning(f"GraphQL request failed (attempt {attempt + 1}/{max_retries}): {e}")
            time.sleep(2 ** attempt)  # Exponential backoff
    
    # This should never be reached due to the raise in the except block
    raise Exception("GraphQL query failed after all retries")

def create_source_b_buildings_query(district_id: str, sbu: str) -> Dict[str, Any]:
    """
    Create GraphQL query for Source B buildings.
    
    Args:
        district_id: District ID
        sbu: Property type code (mr_ind, mr_comm, mr_shop)
        
    Returns:
        GraphQL query payload
    """
    return {
        "query": """
            query ($districtId: ID, $query: String, $sbu: String) {
              buildings(districtId: $districtId, nameSearch: $query, sbu: $sbu) {
                sbu
                id
                nameEn
                nameZh
                addressEn
                addressZh
                __typename
              }
            }
        """,
        "variables": {
            "sbu": sbu,
            "districtId": district_id,
            "query": ""
        }
    }

# ============ REST API UTILITIES ============

def make_paginated_request(base_url: str, params: Dict[str, Any], 
                          headers: Optional[Dict[str, str]] = None,
                          page_size: int = 24, 
                          max_pages: int = 100) -> List[Dict[str, Any]]:
    """
    Make paginated REST API requests.
    
    Args:
        base_url: Base URL for the API
        params: Base parameters for the request
        headers: HTTP headers
        page_size: Number of items per page
        max_pages: Maximum number of pages to fetch
        
    Returns:
        List of all items from all pages
    """
    all_items = []
    page_index = 1
    
    while page_index <= max_pages:
        current_params = params.copy()
        current_params.update({
            "PageSize": page_size,
            "pageindex": page_index
        })
        
        try:
            response = throttled_request(
                url=base_url,
                method='GET',
                params=current_params,
                headers=headers,
                timeout=30
            )
            
            response.raise_for_status()
            result = response.json()
            
            items = result.get("data", {}).get("items", [])
            if not items:
                break
                
            all_items.extend(items)
            page_index += 1
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching page {page_index}: {e}")
            break
    
    return all_items

def fetch_district_data(url: str, headers: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
    """
    Fetch district data from API.
    
    Args:
        url: API endpoint URL
        headers: HTTP headers
        
    Returns:
        List of district data
    """
    try:
        response = throttled_request(url=url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching district data: {e}")
        return []

def fetch_estate_data(district_code: str, base_url: str, headers: Optional[Dict[str, str]] = None,
                     limit: int = 500) -> List[Dict[str, Any]]:
    """
    Fetch estate data for a specific district.
    
    Args:
        district_code: District code
        base_url: Base URL for the API
        headers: HTTP headers
        limit: Items per page limit
        
    Returns:
        List of estate data
    """
    results = []
    page = 1
    
    while True:
        params = {
            "ad": "true",
            "lang": "en",
            "currency": "HKD",
            "unit": "feet",
            "search_behavior": "normal",
            "intsmdist_ids": district_code,
            "page": page,
            "limit": limit
        }
        
        try:
            response = throttled_request(
                url=base_url,
                method='GET',
                params=params,
                headers=headers,
                timeout=30
            )
            
            response.raise_for_status()
            data = response.json()
            
            if 'result' in data and data['result']:
                results.extend(data['result'])
                page += 1
            else:
                break
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching estate data for district {district_code}, page {page}: {e}")
            break
    
    return results

# ============ TRANSACTION DATA UTILITIES ============

def generate_month_ranges(start_date: Union[str, datetime], end_date: Optional[datetime] = None) -> List[Tuple[str, str]]:
    """
    Generate month ranges for transaction data fetching.
    
    Args:
        start_date: Start date (string or datetime)
        end_date: End date (defaults to current date)
        
    Returns:
        List of (start_date, end_date) tuples for each month
    """
    if end_date is None:
        end_date = datetime.now()
    
    if isinstance(start_date, str):
        start_date = datetime.fromisoformat(start_date)
    
    month_ranges = []
    current = end_date
    
    while current >= start_date:
        month_end = current
        month_start = current.replace(day=1)
        month_ranges.append((
            month_start.strftime("%Y-%m-%d"),
            month_end.strftime("%Y-%m-%d")
        ))
        current = month_start - relativedelta(days=1)
    
    return month_ranges

def fetch_transaction_data(base_url: str, headers: Dict[str, str], 
                         month_ranges: List[Tuple[str, str]],
                         additional_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Fetch transaction data for given month ranges.
    
    Args:
        base_url: Base URL for the API
        headers: HTTP headers
        month_ranges: List of (start_date, end_date) tuples
        additional_params: Additional parameters for the request
        
    Returns:
        List of transaction data
    """
    all_transactions = []
    
    for start_date, end_date in month_ranges:
        params = {
            "start_date": start_date,
            "end_date": end_date
        }
        
        if additional_params:
            params.update(additional_params)
        
        try:
            response = throttled_request(
                url=base_url,
                method='GET',
                params=params,
                headers=headers,
                timeout=30
            )
            
            response.raise_for_status()
            data = response.json()
            
            if isinstance(data, list):
                all_transactions.extend(data)
            elif isinstance(data, dict) and 'data' in data:
                all_transactions.extend(data['data'])
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching transactions for {start_date} to {end_date}: {e}")
            continue
    
    return all_transactions

# ============ RESPONSE VALIDATION ============

def validate_api_response(response: Dict[str, Any], required_fields: List[str] = None) -> bool:
    """
    Validate API response structure.
    
    Args:
        response: API response dictionary
        required_fields: List of required field names
        
    Returns:
        True if response is valid, False otherwise
    """
    if not isinstance(response, dict):
        return False
    
    if required_fields:
        for field in required_fields:
            if field not in response:
                logger.warning(f"Missing required field: {field}")
                return False
    
    return True

def extract_api_data(response: Dict[str, Any], data_path: List[str] = None) -> Any:
    """
    Extract data from nested API response.
    
    Args:
        response: API response dictionary
        data_path: Path to the data (e.g., ['data', 'items'])
        
    Returns:
        Extracted data or None if path doesn't exist
    """
    if data_path is None:
        return response
    
    current = response
    for key in data_path:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None
    
    return current

# ============ ERROR HANDLING ============

def handle_api_error(response: requests.Response, context: str = "") -> None:
    """
    Handle API errors with appropriate logging.
    
    Args:
        response: requests.Response object
        context: Additional context for error logging
    """
    try:
        error_data = response.json()
        error_msg = error_data.get('message', 'Unknown error')
    except:
        error_msg = response.text
    
    logger.error(f"API Error {context}: {response.status_code} - {error_msg}")
    
    if response.status_code == 429:
        logger.warning("Rate limit exceeded. Consider increasing delays.")
    elif response.status_code >= 500:
        logger.warning("Server error. May retry later.")

# ============ RETRY DECORATORS ============

def retry_api_call(max_attempts: int = 3, delay: float = 1.0):
    """
    Decorator for retrying API calls with exponential backoff.
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        
    Returns:
        Decorator function
    """
    def decorator(func):
        if TENACITY_AVAILABLE:
            return retry(
                wait=wait_exponential(multiplier=delay, min=delay, max=10),
                stop=stop_after_attempt(max_attempts)
            )(func)
        else:
            # Fallback manual retry implementation
            def wrapper(*args, **kwargs):
                for attempt in range(max_attempts):
                    try:
                        return func(*args, **kwargs)
                    except Exception as e:
                        if attempt == max_attempts - 1:
                            raise
                        logger.warning(f"Attempt {attempt + 1} failed: {e}")
                        time.sleep(delay * (2 ** attempt))
            return wrapper
    return decorator

# ============ UTILITY FUNCTIONS ============

def clean_url_text(text: str) -> str:
    """
    Clean text for URL usage.
    
    Args:
        text: Text to clean
        
    Returns:
        URL-safe text
    """
    import urllib.parse
    return urllib.parse.quote(text.strip().lower())

def build_query_string(params: Dict[str, Any]) -> str:
    """
    Build query string from parameters.
    
    Args:
        params: Dictionary of parameters
        
    Returns:
        Query string
    """
    import urllib.parse
    return urllib.parse.urlencode(params)

def parse_api_date(date_str: str) -> Optional[datetime]:
    """
    Parse date string from API response.
    
    Args:
        date_str: Date string from API
        
    Returns:
        Parsed datetime object or None
    """
    if not date_str:
        return None
    
    # Common API date formats
    formats = [
        '%Y-%m-%dT%H:%M:%S.%fZ',
        '%Y-%m-%dT%H:%M:%SZ',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d'
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    return None

def create_session_with_defaults(headers: Dict[str, str] = None, 
                               timeout: int = 30) -> requests.Session:
    """
    Create a requests session with default settings.
    
    Args:
        headers: Default headers
        timeout: Default timeout
        
    Returns:
        Configured requests.Session
    """
    session = requests.Session()
    
    if headers:
        session.headers.update(headers)
    
    # Set default timeout via HTTPAdapter instead of lambda (avoids infinite recursion)
    original_request = session.request
    session.request = lambda *args, **kwargs: original_request(*args, **{**{'timeout': timeout}, **kwargs})

    return session 