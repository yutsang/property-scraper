import time
import random
import re
import os
import string
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import chromedriver_autoinstaller

def generate_session_id(length=10):
    """Generate a random session ID consisting of lowercase letters and digits."""
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

def clean_subdistrict(subdistrict):
    """
    Clean the subdistrict string to generate a URL-friendly slug.
    Any sequence of non-alphanumeric characters is replaced by a hyphen.
    The result is lowercased and stripped of extra hyphens.
    """
    cleaned = re.sub(r'[^A-Za-z0-9]+', '-', subdistrict)
    return cleaned.strip('-').lower()

def initialize_driver():
    """
    Initializes ChromeDriver with custom options including headless mode.
    chromedriver_autoinstaller installs the correct version if needed.
    """
    chromedriver_autoinstaller.install()
    options = webdriver.ChromeOptions()
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                         "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.6943.127 Safari/537.36")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--disable-extensions")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--headless")
    return webdriver.Chrome(options=options)

def random_sleep(min_delay=1, max_delay=3):
    """Pause execution for a random duration between min_delay and max_delay seconds."""
    time.sleep(random.uniform(min_delay, max_delay))

def scroll_down(driver):
    """Scrolls down to the bottom of the page to trigger lazy-loaded content."""
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    random_sleep()

def extract_estate_data(driver):
    """
    Extracts estate information from the current page.
    Expected DOM structure is used to extract fields.
    """
    data = []
    try:
        estate_items = WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.property-text.flex.def-property-box"))
        )
        for item in estate_items:
            try:
                estate_link = item.get_attribute("href")
                name = item.find_element(By.CSS_SELECTOR, "div.main-text").text.strip()
                address = item.find_element(By.CSS_SELECTOR, "div.address.f-middle").text.strip()
                blocks = item.find_element(By.XPATH, ".//div[contains(text(), 'No. of Block(s)')]/following-sibling::div").text.strip()
                units = item.find_element(By.XPATH, ".//div[contains(text(), 'No. of Units')]/following-sibling::div").text.strip()
                unit_rate = item.find_element(By.XPATH, ".//div[contains(text(), 'Unit Rate of Saleable Area')]/following-sibling::div").text.strip()
                mom = item.find_element(By.XPATH, ".//div[contains(text(), 'MoM')]/following-sibling::div").text.strip()
                trans_record = item.find_element(By.XPATH, ".//div[contains(text(), 'Trans. Record')]/following-sibling::div").text.strip()
                for_sale = item.find_element(By.XPATH, ".//div[contains(text(), 'For Sale')]/following-sibling::div").text.strip()
                for_rent = item.find_element(By.XPATH, ".//div[contains(text(), 'For Rent')]/following-sibling::div").text.strip()
                data.append([name, address, blocks, units, unit_rate, mom, trans_record, for_sale, for_rent, estate_link])
            except Exception:
                continue
    except Exception:
        pass
    return data

def scrape_estates(area_code_df: pd.DataFrame, base_url: str) -> pd.DataFrame:
    """
    Scrapes estate listings for each area code provided in the DataFrame.
    The output DataFrame includes estate details along with region meta-data.
    """
    driver = initialize_driver()
    all_rows = []
    try:
        for idx, row in area_code_df.iterrows():
            region = row["Region"]
            district = row["District"]
            subdistrict = row["Subdistrict"]
            code = row["Code"]
            subdistrict_part = clean_subdistrict(subdistrict)
            session_id = generate_session_id()
            area_url = f"{base_url}/{subdistrict_part}_19-{code}?q={session_id}"
            driver.get(area_url)
            while True:
                scroll_down(driver)
                page_data = extract_estate_data(driver)
                if page_data:
                    for row_data in page_data:
                        all_rows.append(row_data + [region, district, subdistrict, code])
                else:
                    break
                try:
                    next_button = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn-next:not([disabled])"))
                    )
                    driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                    driver.execute_script("arguments[0].click();", next_button)
                    random_sleep()
                except Exception:
                    break
            driver.delete_all_cookies()
            random_sleep()
    finally:
        driver.quit()
    df = pd.DataFrame(
        all_rows,
        columns=[
            "Name", "Address", "Blocks", "Units", "Unit Rate", "MoM",
            "Trans Record", "For Sale", "For Rent", "Estate Link",
            "Region", "District", "Subdistrict", "Code"
        ]
    )
    return df
