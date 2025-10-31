#!/usr/bin/env python3

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time

def test_updated_extraction():
    """Test the updated extraction logic"""
    
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
        
        title_lg_values = []
        price_values = []
        
        if mobile_cards:
            # Test first few cards
            for i, card in enumerate(mobile_cards[:3]):
                print(f"\n--- Card {i+1} ---")
                
                # Extract title-lg with multiple approaches
                title_lg_text = None
                
                # Method 1: Try .text01 .title-lg
                text01_elements = card.find_elements(By.CSS_SELECTOR, ".text01")
                if text01_elements:
                    title_lg_elements = text01_elements[0].find_elements(By.CSS_SELECTOR, ".title-lg")
                    if title_lg_elements:
                        title_lg_text = title_lg_elements[0].text.strip()
                        print(f"Method 1 (text01.title-lg): {title_lg_text}")
                
                # Method 2: Try direct .title-lg
                if not title_lg_text:
                    title_lg_elements = card.find_elements(By.CSS_SELECTOR, ".title-lg")
                    if title_lg_elements:
                        title_lg_text = title_lg_elements[0].text.strip()
                        print(f"Method 2 (direct .title-lg): {title_lg_text}")
                
                # Method 3: Try to extract from the first line of card text
                if not title_lg_text:
                    card_text = card.text.strip()
                    lines = card_text.split('\n')
                    if lines:
                        first_line = lines[0].strip()
                        if first_line and len(first_line) > 5:
                            title_lg_text = first_line
                            print(f"Method 3 (first line): {title_lg_text}")
                
                if title_lg_text:
                    title_lg_values.append(title_lg_text)
                    print(f"✅ Final title-lg: {title_lg_text}")
                else:
                    print("❌ No title-lg found")
                
                # Extract price
                price_text = None
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
                        temp_price = price_elements[0].text.strip()
                        if temp_price and '$' in temp_price and any(char.isdigit() for char in temp_price):
                            if 'M' in temp_price or (',' in temp_price and len(temp_price) > 6):
                                price_text = temp_price
                                break
                            elif not price_text:
                                price_text = temp_price
                
                if price_text:
                    price_values.append(price_text)
                    print(f"✅ Final price: {price_text}")
                else:
                    print("❌ No price found")
                    price_values.append("")
        
        print(f"\n📊 Summary:")
        print(f"Title-lg values extracted: {len(title_lg_values)}")
        print(f"Price values extracted: {len(price_values)}")
        print(f"Title-lg samples: {title_lg_values[:3]}")
        print(f"Price samples: {price_values[:3]}")
        
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        driver.quit()

if __name__ == "__main__":
    test_updated_extraction()
