#!/usr/bin/env python3

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time

def test_mobile_card_extraction():
    """Test mobile card extraction on a sample page"""
    
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
            # Test first card
            card = mobile_cards[0]
            print(f"First card text: {card.text[:200]}...")
            
            # Try to find title-lg
            text01_elements = card.find_elements(By.CSS_SELECTOR, ".text01")
            print(f"Found {len(text01_elements)} .text01 elements")
            
            if text01_elements:
                title_lg_elements = text01_elements[0].find_elements(By.CSS_SELECTOR, ".title-lg")
                print(f"Found {len(title_lg_elements)} .title-lg elements in first .text01")
                
                if title_lg_elements:
                    title_lg_text = title_lg_elements[0].text.strip()
                    print(f"Title-lg text: {title_lg_text}")
                else:
                    print("No .title-lg found in .text01")
            else:
                # Try direct .title-lg
                title_lg_elements = card.find_elements(By.CSS_SELECTOR, ".title-lg")
                print(f"Found {len(title_lg_elements)} direct .title-lg elements")
                
                if title_lg_elements:
                    title_lg_text = title_lg_elements[0].text.strip()
                    print(f"Direct title-lg text: {title_lg_text}")
                else:
                    print("No direct .title-lg found")
            
            # Try to find price
            price_elements = card.find_elements(By.CSS_SELECTOR, ".content-price .saleprice span")
            print(f"Found {len(price_elements)} price elements")
            
            if price_elements:
                price_text = price_elements[0].text.strip()
                print(f"Price text: {price_text}")
            else:
                print("No price found")
        
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        driver.quit()

if __name__ == "__main__":
    test_mobile_card_extraction()
