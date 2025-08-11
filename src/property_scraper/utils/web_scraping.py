"""
Web scraping utilities for property scraper pipelines.

This module provides common functions for web scraping operations including:
- Driver initialization and management
- Session management for requests
- Delay and retry mechanisms
- Request headers and user agents
- Cookie handling
"""

import time
import random
import requests
import logging
from typing import Dict, Any, Optional, List, Union

# Optional imports for selenium
SELENIUM_AVAILABLE = True
try:
    from selenium import webdriver  # type: ignore
    from selenium.webdriver.common.by import By  # type: ignore
    from selenium.webdriver.support.ui import WebDriverWait  # type: ignore
    from selenium.webdriver.support import expected_conditions as EC  # type: ignore
    from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, NoSuchElementException  # type: ignore
    import chromedriver_autoinstaller  # type: ignore
except ImportError:
    SELENIUM_AVAILABLE = False
    webdriver = None
    By = None
    WebDriverWait = None
    EC = None
    TimeoutException = None
    StaleElementReferenceException = None
    NoSuchElementException = None
    chromedriver_autoinstaller = None

# Optional imports for undetected_chromedriver
UNDETECTED_CHROME_AVAILABLE = True
try:
    import undetected_chromedriver as uc  # type: ignore
except ImportError:
    UNDETECTED_CHROME_AVAILABLE = False
    uc = None

# Optional imports for tenacity
TENACITY_AVAILABLE = True
try:
    from tenacity import retry, wait_exponential, stop_after_attempt  # type: ignore
except ImportError:
    TENACITY_AVAILABLE = False
    retry = None
    wait_exponential = None
    stop_after_attempt = None

# Optional imports for urllib3
URLLIB3_AVAILABLE = True
try:
    from requests.adapters import HTTPAdapter  # type: ignore
    from urllib3.util.retry import Retry  # type: ignore
except ImportError:
    URLLIB3_AVAILABLE = False
    HTTPAdapter = None
    Retry = None

logger = logging.getLogger(__name__)

# ============ DRIVER INITIALIZATION ============

def setup_undetected_driver():
    """Setup undetected ChromeDriver to bypass Cloudflare."""
    if not UNDETECTED_CHROME_AVAILABLE:
        raise ImportError("undetected_chromedriver is not available. Install it with: pip install undetected-chromedriver")
    
    options = uc.ChromeOptions()
    
    # Basic options for better performance
    options.add_argument('--no-first-run')
    options.add_argument('--no-default-browser-check')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-software-rasterizer')
    
    # Non-headless mode for better Cloudflare compatibility
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-blink-features=AutomationControlled')
    
    # Create undetected driver with correct Chrome version
    driver = uc.Chrome(options=options, version_main=137)
    
    return driver

def wait_for_cloudflare_check(driver, max_wait=5):
    """Wait for Cloudflare security check to complete."""
    start_time = time.time()
    cloudflare_detected = False
    
    while time.time() - start_time < max_wait:
        try:
            page_source = driver.page_source.lower()
            current_url = driver.current_url.lower()
            
            # Check for Cloudflare indicators
            if any(indicator in page_source for indicator in [
                'checking your browser',
                'cloudflare',
                'security check',
                'ray id',
                'please wait',
                'just a moment',
                'challenge'
            ]) or 'cloudflare' in current_url:
                cloudflare_detected = True
                time.sleep(0.5)
                continue
            
            # If we had Cloudflare but now don't, wait a bit more to be sure
            if cloudflare_detected:
                time.sleep(1)
                cloudflare_detected = False
                continue
            
            # Check if we have building content and correct URL
            if (('building' in page_source or 'office' in page_source or 'leasinghub' in page_source) 
                and 'leasinghub.com' in current_url):
                return True
            
            time.sleep(0.2)
        except Exception as e:
            # If we can't check, assume it's working
            return True
    
    return False

def initialize_chrome_driver(params: Dict[str, Any]) -> Any:
    """
    Initialize Chrome driver with anti-detection configuration.
    
    Args:
        params: Configuration parameters containing global settings
        
    Returns:
        webdriver.Chrome: Configured Chrome driver instance
    """
    if not SELENIUM_AVAILABLE:
        raise ImportError("Selenium is not available. Install it with: pip install selenium")
    
    chromedriver_autoinstaller.install()
    options = webdriver.ChromeOptions()
    
    # Basic configuration
    if params.get('global', {}).get('headless', True):
        options.add_argument("--headless=new")
    
    # Anti-detection options
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    
    # User agent
    user_agent = params.get('global', {}).get('user_agent', 
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')
    options.add_argument(f"--user-agent={user_agent}")
    options.add_argument("--window-size=1920,1080")
    
    # Additional performance options
    options.add_argument("--disable-web-security")
    options.add_argument("--disable-features=VizDisplayCompositor")
    options.add_argument("--disable-extensions")
    
    driver = webdriver.Chrome(options=options)
    
    # Execute script to hide webdriver property
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

def initialize_edge_driver(params: Dict[str, Any]) -> Any:
    """
    Initialize Edge driver with anti-detection configuration.
    
    Args:
        params: Configuration parameters containing global settings
        
    Returns:
        webdriver.Edge: Configured Edge driver instance
    """
    if not SELENIUM_AVAILABLE:
        raise ImportError("Selenium is not available. Install it with: pip install selenium")
    
    options = webdriver.EdgeOptions()
    
    # Basic configuration
    if params.get('global', {}).get('headless', True):
        options.add_argument("--headless=new")
    
    # Anti-detection options
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    
    # User agent
    user_agent = params.get('global', {}).get('user_agent', 
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0')
    options.add_argument(f"--user-agent={user_agent}")
    options.add_argument("--window-size=1920,1080")
    
    driver = webdriver.Edge(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

def initialize_driver(params: Dict[str, Any]) -> Any:
    """
    Initialize web driver based on configuration.
    
    Args:
        params: Configuration parameters containing driver preferences
        
    Returns:
        webdriver.Remote: Configured driver instance
    """
    driver_type = params.get('global', {}).get('driver_type', 'chrome').lower()
    
    if driver_type == 'chrome':
        return initialize_chrome_driver(params)
    elif driver_type == 'edge':
        return initialize_edge_driver(params)
    else:
        logger.warning(f"Unknown driver type: {driver_type}, defaulting to Chrome")
        return initialize_chrome_driver(params)

# ============ DELAY AND RETRY MECHANISMS ============

def smart_sleep(params: Dict[str, Any]) -> None:
    """
    Intelligent sleep function with random timing based on configuration.
    
    Args:
        params: Configuration parameters containing delay settings
    """
    min_delay = params.get('global', {}).get('min_delay', 1.0)
    max_delay = params.get('global', {}).get('max_delay', 3.0)
    sleep_time = random.uniform(min_delay, max_delay)
    time.sleep(sleep_time)

def random_sleep(min_delay: float = 1.0, max_delay: float = 3.0) -> None:
    """
    Random sleep with specified range.
    
    Args:
        min_delay: Minimum delay in seconds
        max_delay: Maximum delay in seconds
    """
    sleep_time = random.uniform(min_delay, max_delay)
    time.sleep(sleep_time)

# ============ SCROLLING UTILITIES ============

def scroll_down(driver: webdriver.Chrome) -> None:
    """
    Simple scroll down function.
    
    Args:
        driver: WebDriver instance
    """
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

def enhanced_scroll_down(driver: Any, params: Dict[str, Any]) -> None:
    """
    Enhanced scrolling strategy for dynamic content loading.
    
    Args:
        driver: WebDriver instance
        params: Configuration parameters for delays
    """
    if not SELENIUM_AVAILABLE:
        raise ImportError("Selenium is not available. Install it with: pip install selenium")
    
    last_height = driver.execute_script("return document.body.scrollHeight")
    scroll_attempts = 0
    
    while scroll_attempts < 3:
        scroll_distance = random.randint(300, 800)
        driver.execute_script(f"window.scrollBy(0, {scroll_distance})")
        smart_sleep(params)
        
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        
        last_height = new_height
        scroll_attempts += 1

# ============ ELEMENT INTERACTION UTILITIES ============

def adaptive_wait(driver: Any, selector: str, timeout: int = 30, poll: int = 3) -> bool:
    """
    Adaptive wait for element to be present and visible.
    
    Args:
        driver: WebDriver instance
        selector: CSS selector for the element
        timeout: Maximum wait time in seconds
        poll: Polling interval in seconds
        
    Returns:
        bool: True if element is found, False otherwise
    """
    if not SELENIUM_AVAILABLE:
        raise ImportError("Selenium is not available. Install it with: pip install selenium")
    
    try:
        element = WebDriverWait(driver, timeout, poll_frequency=poll).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
        )
        return element is not None
    except TimeoutException:
        return False

def safe_find_element(driver: Any, selector: str, method: str = "css") -> Optional[Any]:
    """
    Safely find element without throwing exceptions.
    
    Args:
        driver: WebDriver instance
        selector: Element selector
        method: Selection method ('css', 'xpath', 'id', 'class')
        
    Returns:
        Element if found, None otherwise
    """
    if not SELENIUM_AVAILABLE:
        raise ImportError("Selenium is not available. Install it with: pip install selenium")
    
    try:
        if method == "css":
            return driver.find_element(By.CSS_SELECTOR, selector)
        elif method == "xpath":
            return driver.find_element(By.XPATH, selector)
        elif method == "id":
            return driver.find_element(By.ID, selector)
        elif method == "class":
            return driver.find_element(By.CLASS_NAME, selector)
        else:
            logger.warning(f"Unknown method: {method}")
            return None
    except NoSuchElementException:
        return None

def safe_get_text(element: Any, selector: str) -> str:
    """
    Safely extract text from element.
    
    Args:
        element: Parent element
        selector: CSS selector for target element
        
    Returns:
        str: Element text or empty string if not found
    """
    if not SELENIUM_AVAILABLE:
        raise ImportError("Selenium is not available. Install it with: pip install selenium")
    
    try:
        target = element.find_element(By.CSS_SELECTOR, selector)
        return target.text.strip() if target else ""
    except (NoSuchElementException, StaleElementReferenceException):
        return ""

def safe_extract(item, selector: str) -> str:
    """
    Safely extract text from BeautifulSoup element.
    
    Args:
        item: BeautifulSoup element
        selector: CSS selector
        
    Returns:
        str: Extracted text or empty string if not found
    """
    try:
        element = item.select_one(selector)
        return element.get_text(strip=True) if element else ""
    except Exception:
        return ""

# ============ SESSION MANAGEMENT ============

def create_session_with_retries(max_retries: int = 3, backoff_factor: float = 0.3) -> requests.Session:
    """
    Create requests session with retry strategy.
    
    Args:
        max_retries: Maximum number of retries
        backoff_factor: Backoff factor for retries
        
    Returns:
        requests.Session: Configured session with retry strategy
    """
    session = requests.Session()
    
    retry_strategy = Retry(
        total=max_retries,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=["HEAD", "GET", "OPTIONS"],
        backoff_factor=backoff_factor
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def get_random_user_agent() -> str:
    """
    Get a random user agent string.
    
    Returns:
        str: Random user agent string
    """
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0'
    ]
    return random.choice(user_agents)

def create_realistic_headers() -> Dict[str, str]:
    """
    Create realistic HTTP headers for requests.
    
    Returns:
        Dict[str, str]: Dictionary of HTTP headers
    """
    return {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "User-Agent": get_random_user_agent()
    }

# ============ RETRY DECORATORS ============

def retry_on_exception(max_attempts: int = 3, delay: float = 1.0):
    """
    Decorator for retrying functions on exceptions.
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay: Delay between retries in seconds
        
    Returns:
        Decorator function
    """
    return retry(
        wait=wait_exponential(multiplier=delay, min=delay, max=10),
        stop=stop_after_attempt(max_attempts)
    )

@retry_on_exception(max_attempts=5, delay=1.0)
def robust_request(url: str, method: str = 'GET', **kwargs) -> requests.Response:
    """
    Make a robust HTTP request with retry logic.
    
    Args:
        url: Target URL
        method: HTTP method
        **kwargs: Additional arguments for requests
        
    Returns:
        requests.Response: Response object
    """
    session = create_session_with_retries()
    
    if method.upper() == 'GET':
        response = session.get(url, **kwargs)
    elif method.upper() == 'POST':
        response = session.post(url, **kwargs)
    else:
        raise ValueError(f"Unsupported HTTP method: {method}")
    
    response.raise_for_status()
    return response

# ============ NAVIGATION UTILITIES ============

def check_for_next_page(driver: webdriver.Chrome, selectors: List[str] = None) -> Optional[any]:
    """
    Check if next page button exists and is clickable.
    
    Args:
        driver: WebDriver instance
        selectors: List of CSS selectors for next page buttons
        
    Returns:
        Element if found and clickable, None otherwise
    """
    if selectors is None:
        selectors = [
            "button.btn-next:not(.disabled):not([disabled])",
            "a.pagination-next:not(.disabled)",
            ".pagination-next:not(.disabled)",
            "button[aria-label*='Next']",
            "a[aria-label*='Next']"
        ]
    
    try:
        for selector in selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    if element.is_displayed() and element.is_enabled():
                        return element
            except Exception:
                continue
        return None
    except Exception:
        return None

def go_to_next_page(driver: webdriver.Chrome, params: Dict[str, Any], selectors: List[str] = None) -> bool:
    """
    Navigate to next page with enhanced verification.
    
    Args:
        driver: WebDriver instance
        params: Configuration parameters for delays
        selectors: List of CSS selectors for next page buttons
        
    Returns:
        bool: True if navigation was successful, False otherwise
    """
    try:
        next_button = check_for_next_page(driver, selectors)
        if next_button:
            # Scroll to button
            driver.execute_script(
                "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", 
                next_button
            )
            smart_sleep(params)
            
            # Click the button
            driver.execute_script("arguments[0].click();", next_button)
            smart_sleep(params)
            smart_sleep(params)  # Extra wait for page load
            
            return True
        return False
    except Exception as e:
        logger.debug(f"Error navigating to next page: {str(e)}")
        return False

# ============ UTILITY FUNCTIONS ============

def generate_session_id(length: int = 10) -> str:
    """
    Generate a random session ID.
    
    Args:
        length: Length of the session ID
        
    Returns:
        str: Random session ID
    """
    import string
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

def clean_subdistrict(subdistrict: str) -> str:
    """
    Clean subdistrict name for URL usage.
    
    Args:
        subdistrict: Raw subdistrict name
        
    Returns:
        str: Cleaned subdistrict name
    """
    return subdistrict.replace(' ', '-').lower() 