# src/midland_property/pipelines/midland_area_codes/nodes.py
import requests
import pandas as pd

def get_district_data(url, headers):
    """
    Fetch district data from Midland API
    
    Args:
        url: API endpoint URL
        headers: HTTP headers including authorization
        
    Returns:
        DataFrame: Processed district data
    """
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Validate successful response
    return response.json()

def process_district_data(api_response):
    """
    Transform nested JSON structure into flat table format
    
    Args:
        api_response: JSON response from API
        
    Returns:
        DataFrame: Processed district data in tabular format
    """
    processed_records = []
    for region in api_response:
        for district in region.get('int_district', []):
            for subdistrict in district.get('int_sm_district', []):
                processed_records.append({
                    'subdistrict_code': subdistrict['id'],
                    'parent_district_id': district['id'],
                    'parent_district_name': district['name'],
                    'subdistrict_name': subdistrict['name'],
                    'sort_order': subdistrict['seq']
                })
    return pd.DataFrame(processed_records)

def fetch_district_codes():
    """
    Main function to fetch and process district codes
    
    Returns:
        DataFrame: Processed district data
    """
    # API endpoint configuration
    url = "https://data.midland.com.hk/info/v1/region/int_district/int_sm_district?lang=en"
    
    # Configure request headers with provided authentication
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Authorization": "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJndWlkIjoibXItMjAyNC0xMi0wNy0tLVlWN0hKU2QxelRzOHpwVDhJNEdjdGxLcjQ1Z0l4cWhsdVp3SEdvZXVSX1o3RkU2cmh1Q1NjVVpqM1E3SXIzZWVQSmZpMy1JSSIsImF1ZCI6Im15cGFnZWFwcC1tbm5rYiIsInN1YiI6Im1yLTIwMjQtMTItMDctLS1ZVjdISlNkMXpUczh6cFQ4STRHY3RsS3I0NWdJeHFobHVad0hHb2V1Ul9aN0ZFNnJodUNTY1VaajNRN0lyM2VlUEpmaTMtSUkiLCJpYXQiOjE3MzM1NDk0MjUsImV4cCI6MTc2ODEwOTQyNSwiaXNzIjoiZGF0YS5taWRsYW5kLmNvbS5oayJ9.LOOVgc_Nw7OPNnAlB8iC1kRHL0W8UVNVa0GaJYaxTxVZtO33ZbkR64rxMHSifvZOzYr38aJENj-SDIbkq4Y75CxqMPegyBUgHtaub-Fez5qaH2W0Dz71pUdYijDG3rB4Dkbdf8k21QsHerJmOFnpryzTVnZDxv-3g8Lmjz2WUhmrqMamKox3w-T9wRJ4p_wzcJwvXWgtvxkapr3Ep0YSJy3fJsV-Nwm_QiJf2JR0V4rOAu7f-YLMSy7IYje3W-HvVqAZV2cDphg_cYnf6CpirJPu_ix2z6BtIMpYMXeSiZyZtKCHiWFNtUm6QTD2adArWtLl_NvbgcH9mhVYuWi8NcrZBdBh4c72bSNRm104oEbRb9-vb1AylH2oFkEz33xXXEAJRtbQxoQ3qZj_yoDIexrinOSlkJB50fSu98Xizv9eZstnbtzkgVjfKpOAWQFdHKennjN9Azq6yTlejDVspL7A0JsY4ZlO4HQNdkNhiOQDYypHgx8jQMm0B0rbaa0cEz1S0s43Lh01eNVBN9Is35jAWFsJIP-iLvHqXJ9d0pGoHe0N7PQk2dmLo9E5szP0U04MZxt4m9TEpJkn-0uS_ZDSVABlBU2KGIkTmuzm1VltsDhPhoNrbJBJVdxJJdublpDnVFk8aO1gFWKNzptw48ipmLfpRosynC_x3Ud6QMU",
        "Origin": "https://www.midland.com.hk",
        "Referer": "https://www.midland.com.hk/",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
    }
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    def flatten_district_data(api_response):
        records = []
        for region in api_response:
            for district in region.get('int_district', []):
                for subdistrict in district.get('int_sm_district', []):
                    records.append({
                        'subdistrict_code': subdistrict['id'],
                        'parent_district_id': district['id'],
                        'parent_district_name': district['name'],
                        'subdistrict_name': subdistrict['name'],
                        'sort_order': subdistrict['seq']
                    })
        return records
    
    district_data = flatten_district_data(response.json())
    df = pd.DataFrame(district_data)
    return df.astype('string')
