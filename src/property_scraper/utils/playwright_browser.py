"""
Platform-aware Playwright browser launcher.
- macOS: Chromium or system Chrome (no chromedriver needed)
- Windows: MS Edge (company PCs often block chromedriver/Chromium; Edge works)
"""
import logging
import sys
from typing import Any

logger = logging.getLogger(__name__)


def get_browser_launch_args(
    headless: bool = True,
    user_agent: str | None = None,
) -> dict[str, Any]:
    """
    Return Playwright launch kwargs for the current platform.
    - Darwin (macOS): Chromium (or Chrome via channel)
    - Windows: MS Edge (channel='msedge') — avoids chromedriver/Chrome restrictions
    """
    is_windows = sys.platform == "win32"
    args: dict[str, Any] = {
        "headless": headless,
    }

    if is_windows:
        args["channel"] = "msedge"
        logger.info("Windows detected — using MS Edge for scraping")
    else:
        # macOS / Linux: use Chromium (bundled, no chromedriver)
        args["channel"] = None  # default Chromium
        logger.info("macOS/Linux detected — using Chromium for scraping")

    if user_agent:
        # Applied via context, not launch — so we don't set it here
        pass

    return args


def _reraise_if_missing_browser_binary(exc: Exception) -> None:
    """Re-raise with a setup hint when Playwright's bundled browser was never installed."""
    msg = str(exc)
    if "Executable doesn't exist" in msg:
        raise RuntimeError(
            "Playwright browser binaries are missing. In this environment run: "
            "python -m playwright install chromium"
        ) from exc
    raise exc


def launch_browser(playwright, headless: bool = True):
    """
    Launch the appropriate browser for the current platform.
    Returns a browser instance from sync_playwright().
    """
    args = get_browser_launch_args(headless=headless)
    channel = args.pop("channel", None)
    try:
        if channel:
            return playwright.chromium.launch(channel=channel, **args)
        return playwright.chromium.launch(**args)
    except Exception as e:
        _reraise_if_missing_browser_binary(e)


async def launch_browser_async(playwright, headless: bool = True):
    """
    Async launch for Playwright async_api.
    Returns a browser instance from async_playwright().
    """
    args = get_browser_launch_args(headless=headless)
    channel = args.pop("channel", None)
    try:
        if channel:
            return await playwright.chromium.launch(channel=channel, **args)
        return await playwright.chromium.launch(**args)
    except Exception as e:
        _reraise_if_missing_browser_binary(e)
