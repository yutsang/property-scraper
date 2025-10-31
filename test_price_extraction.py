#!/usr/bin/env python3

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import re

def test_price_extraction():
    """Test price extraction from mobile cards"""
    
    # Setup Chrome driver
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    driver = webdriver.Chrome(options=options)
    
    try:
        # Navigate to a sample page
        url = "https://hk.centanet.com/findproperty/en/list/transaction/kennedy-town_19-HMA111"
        driver.get(url)
        time.sleep(3)
        
        # Find mobile cards
        mobile_cards = driver.find_elements(By.CSS_SELECTOR, ".transactions-content")
        print(f"Found {len(mobile_cards)} mobile cards")
        
        if mobile_cards:
            # Test first few cards
            for i, card in enumerate(mobile_cards[:3]):
                print(f"\n--- Card {i+1} ---")
                card_text = card.text
                print(f"Full card text:\n{card_text}")
                
                # Try different price selectors
                price_selectors = [
                    ".content-price .saleprice span",
                    ".content-price .saleprice",
                    ".content-price span",
                    ".content-price",
                    ".saleprice span",
                    ".saleprice"
                ]
                
                for selector in price_selectors:
                    price_elements = card.find_elements(By.CSS_SELECTOR, selector)
                    if price_elements:
                        price_text = price_elements[0].text.strip()
                        print(f"Selector '{selector}': {price_text}")
                
                # Try regex extraction
                price_patterns = [
                    r'\$\d+\.?\d*M',  # $1.2M, $2M
                    r'\$\d{1,3}(?:,\d{3})*',  # $1,234, $12,345
                    r'\$\d+',  # $1234
                ]
                
                for pattern in price_patterns:
                    matches = re.findall(pattern, card_text)
                    if matches:
                        print(f"Regex pattern '{pattern}': {matches}")
                
                # Look for any text containing $ and numbers
                lines = card_text.split('\n')
                for line in lines:
                    if '$' in line and any(char.isdigit() for char in line):
                        print(f"Line with $ and numbers: {line}")
        
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        driver.quit()

if __name__ == "__main__":
    test_price_extraction()
