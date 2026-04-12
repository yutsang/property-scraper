"""
Data cleaning utilities for property scraper pipelines.

This module provides common functions for data cleaning operations including:
- Date processing and standardization
- Price and numeric data cleaning
- Area and size data extraction
- Address parsing and estate name extraction
- Text cleaning and standardization
"""

import pandas as pd  # type: ignore
import numpy as np  # type: ignore
import re
import logging
from datetime import datetime, date, timedelta
from typing import Dict, Any, Optional, List, Union, Tuple
from difflib import SequenceMatcher

# Optional imports for fuzzy matching
RAPIDFUZZ_AVAILABLE = True
try:
    from rapidfuzz import fuzz, process  # type: ignore
except ImportError:
    RAPIDFUZZ_AVAILABLE = False
    fuzz = None
    process = None

logger = logging.getLogger(__name__)

# ============ DATE PROCESSING UTILITIES ============

def parse_date_from_string(date_str: str) -> Optional[date]:
    """
    Parse date from various string formats.
    
    Args:
        date_str: Date string in various formats
        
    Returns:
        date: Parsed date object or None if parsing fails
    """
    if not date_str or pd.isna(date_str):
        return None
    
    # Common date formats
    date_formats = [
        '%Y-%m-%d',
        '%d/%m/%Y',
        '%m/%d/%Y',
        '%Y%m%d',
        '%d-%m-%Y',
        '%Y-%m-%d %H:%M:%S',
        '%d/%m/%Y %H:%M:%S'
    ]
    
    date_str = str(date_str).strip()
    
    for fmt in date_formats:
        try:
            return pd.to_datetime(date_str, format=fmt).date()
        except ValueError:
            continue
    
    # Fallback to pandas automatic parsing
    try:
        return pd.to_datetime(date_str, errors='coerce').date()
    except:
        return None

def clean_date_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Clean and standardize date column in DataFrame.
    
    Args:
        df: Input DataFrame
        column: Name of the date column
        
    Returns:
        DataFrame with cleaned date column
    """
    def clean_date(date_value):
        """Convert various date formats to standard datetime"""
        try:
            if pd.isna(date_value):
                return None
            
            # If already datetime, extract date
            if isinstance(date_value, pd.Timestamp):
                return date_value.date()
            
            # Convert string dates
            date_str = str(date_value).strip()
            if not date_str:
                return None
                
            # Try multiple date formats
            date_formats = ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y%m%d', '%d-%m-%Y']
            for fmt in date_formats:
                try:
                    return pd.to_datetime(date_str, format=fmt).date()
                except ValueError:
                    continue
            
            # Fallback to pandas automatic parsing
            return pd.to_datetime(date_str, errors='coerce').date()
            
        except Exception as e:
            logger.debug(f"Error converting date '{date_value}': {str(e)}")
            return None
    
    if column in df.columns:
        df = df.copy()
        df[column] = df[column].apply(clean_date)
        logger.info(f"Cleaned {df[column].notna().sum()} dates in column '{column}'")
    
    return df

def standardize_date_formats(df: pd.DataFrame, date_columns: List[str], 
                           output_format: str = '%d/%m/%Y') -> pd.DataFrame:
    """
    Standardize multiple date columns to a consistent format.
    
    Args:
        df: Input DataFrame
        date_columns: List of date column names
        output_format: Desired output format
        
    Returns:
        DataFrame with standardized date columns
    """
    df = df.copy()
    
    for col in date_columns:
        if col in df.columns:
            # Convert to datetime and format
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].dt.strftime(output_format)
            logger.info(f"Standardized date format for column: {col}")
    
    return df

# ============ PRICE AND NUMERIC CLEANING ============

def clean_price_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Clean and convert price column to numeric values.
    
    Args:
        df: Input DataFrame
        column: Name of the price column
        
    Returns:
        DataFrame with cleaned price column
    """
    def clean_price(price_value):
        """Extract numeric value from price strings"""
        try:
            if pd.isna(price_value) or price_value == '' or price_value == '--':
                return None
                
            price_str = str(price_value).strip()
            if not price_str:
                return None
            
            # Remove currency symbols and common separators
            price_str = re.sub(r'[HK$,\s]', '', price_str)
            
            # Handle millions (M) and thousands (K)
            if 'M' in price_str.upper():
                price_str = price_str.upper().replace('M', '')
                multiplier = 1000000
            elif 'K' in price_str.upper():
                price_str = price_str.upper().replace('K', '')
                multiplier = 1000
            else:
                multiplier = 1
            
            # Extract numbers using regex
            numbers = re.findall(r'\d+\.?\d*', price_str)
            if numbers:
                return float(numbers[0]) * multiplier
            
            return None
            
        except Exception as e:
            logger.debug(f"Error converting price '{price_value}': {str(e)}")
            return None
    
    if column in df.columns:
        df = df.copy()
        df[column] = df[column].apply(clean_price)
        logger.info(f"Cleaned {df[column].notna().sum()} prices in column '{column}'")
    
    return df

def clean_area_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Clean and convert area column to numeric values.
    
    Args:
        df: Input DataFrame
        column: Name of the area column
        
    Returns:
        DataFrame with cleaned area column
    """
    def clean_area(area_value):
        """Extract numeric value from area strings like '385ft²'"""
        try:
            if pd.isna(area_value) or area_value == '' or area_value == '--':
                return None
                
            area_str = str(area_value).strip()
            if not area_str:
                return None
            
            # Extract numbers using regex
            numbers = re.findall(r'\d+\.?\d*', area_str)
            if numbers:
                return float(numbers[0])
            
            return None
            
        except Exception as e:
            logger.debug(f"Error converting area '{area_value}': {str(e)}")
            return None
    
    if column in df.columns:
        df = df.copy()
        df[column] = df[column].apply(clean_area)
        logger.info(f"Cleaned {df[column].notna().sum()} areas in column '{column}'")
    
    return df

def clean_ft_price_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Clean and convert price per square foot column to numeric values.
    
    Args:
        df: Input DataFrame
        column: Name of the ft_price column
        
    Returns:
        DataFrame with cleaned ft_price column
    """
    def clean_ft_price(ft_price_value):
        """Extract numeric value from ft_price strings"""
        try:
            if pd.isna(ft_price_value) or ft_price_value == '' or ft_price_value == '--':
                return None
                
            ft_price_str = str(ft_price_value).strip()
            if not ft_price_str:
                return None
            
            # Remove currency symbols and common text
            ft_price_str = re.sub(r'[HK$,\s]', '', ft_price_str)
            ft_price_str = re.sub(r'(per|/)?f(ee)?t|sq|²', '', ft_price_str, flags=re.IGNORECASE)
            
            # Extract numbers using regex
            numbers = re.findall(r'\d+\.?\d*', ft_price_str)
            if numbers:
                return float(numbers[0])
            
            return None
            
        except Exception as e:
            logger.debug(f"Error converting ft_price '{ft_price_value}': {str(e)}")
            return None
    
    if column in df.columns:
        df = df.copy()
        df[column] = df[column].apply(clean_ft_price)
        logger.info(f"Cleaned {df[column].notna().sum()} ft_prices in column '{column}'")
    
    return df

# ============ TEXT CLEANING UTILITIES ============

def clean_text_column(df: pd.DataFrame, column: str, remove_special: bool = True) -> pd.DataFrame:
    """
    Clean text column by removing special characters and standardizing format.
    
    Args:
        df: Input DataFrame
        column: Name of the text column
        remove_special: Whether to remove special characters
        
    Returns:
        DataFrame with cleaned text column
    """
    def clean_text(text_value):
        """Clean text value"""
        try:
            if pd.isna(text_value) or text_value == '':
                return None
                
            text_str = str(text_value).strip()
            if not text_str:
                return None
            
            if remove_special:
                # Remove special characters but keep spaces, hyphens, and basic punctuation
                text_str = re.sub(r'[^\w\s\-\.\,\(\)]', '', text_str)
            
            # Normalize whitespace
            text_str = re.sub(r'\s+', ' ', text_str).strip()
            
            return text_str if text_str else None
            
        except Exception as e:
            logger.debug(f"Error cleaning text '{text_value}': {str(e)}")
            return None
    
    if column in df.columns:
        df = df.copy()
        df[column] = df[column].apply(clean_text)
        logger.info(f"Cleaned text in column '{column}'")
    
    return df

def extract_year_and_age(completion_year_str: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Extract year and age from completion year string.
    
    Args:
        completion_year_str: String like "1980 (45 Year(s))"
        
    Returns:
        Tuple of (year, age) or (None, None) if parsing fails
    """
    try:
        if pd.isna(completion_year_str) or completion_year_str == '':
            return None, None
        
        # Pattern: "1980 (45 Year(s))"
        match = re.search(r'(\d{4})\s*\((\d+)\s*Year', str(completion_year_str))
        if match:
            year = int(match.group(1))
            age = int(match.group(2))
            return year, age
        else:
            # Try to extract just the year if pattern doesn't match
            year_match = re.search(r'(\d{4})', str(completion_year_str))
            if year_match:
                year = int(year_match.group(1))
                # Calculate age based on current year
                age = datetime.now().year - year
                return year, age
            
        return None, None
    except (ValueError, TypeError):
        return None, None

# ============ ADDRESS AND ESTATE NAME EXTRACTION ============

def extract_estate_name_from_address(address: str) -> str:
    """
    Extract estate name from full address using pattern matching.
    
    Args:
        address: Full address string
        
    Returns:
        Extracted estate name or empty string if extraction fails
    """
    try:
        if not address or pd.isna(address):
            return ""
        
        address = str(address).strip()
        
        # Stop indicators that typically mark the end of estate names
        stop_indicators = ['Unit', 'Flat', 'Floor', 'Room', 'Shop', 'Office', 'Block', 'Tower', 'Wing']
        
        # Common estate name patterns
        compound_patterns = [
            r'^([A-Za-z\s\-\']+(?:Court|Building|House|Garden|Heights|Mansion|Terrace|Rise|Hill|Plaza|Centre|Center|Estate|Village|Towers?|Blocks?))\s*[,\s]',
            r'^([A-Za-z\s\-\']+)\s+(?:Court|Building|House|Garden|Heights|Mansion|Terrace|Rise|Hill|Plaza|Centre|Center|Estate|Village|Towers?|Blocks?)[,\s]',
            r'^([A-Za-z\s\-\']+(?:Court|Building|House|Garden|Heights|Mansion|Terrace|Rise|Hill|Plaza|Centre|Center|Estate|Village|Towers?|Blocks?))[,\s]'
        ]
        
        # Try compound patterns first
        for pattern in compound_patterns:
            match = re.match(pattern, address)
            if match:
                base_name = match.group(1)
                if len(match.groups()) > 1:
                    building_type = match.group(2)
                    if building_type in ['Court', 'Building', 'House', 'Garden', 'Heights', 'Mansion', 'Terrace', 'Rise', 'Hill']:
                        return f"{base_name} {building_type}"
                return base_name
        
        # Fallback: word-by-word analysis
        words = address.split()
        estate_words = []
        
        for i, word in enumerate(words):
            if any(indicator in word for indicator in stop_indicators):
                break
            if re.match(r'\d+/F', word):
                break
            if word.isdigit() and i > 0:
                if i + 1 < len(words) and any(x in words[i+1] for x in stop_indicators):
                    break
            estate_words.append(word)
        
        result = ' '.join(estate_words)
        result = re.sub(r'[,\(\)\[\]]+$', '', result).strip()
        return result
        
    except Exception as e:
        logger.debug(f"Error extracting estate name from '{address}': {str(e)}")
        return ""

def normalize_estate_names(df: pd.DataFrame, address_column: str, 
                         estate_column: str = 'estate_name') -> pd.DataFrame:
    """
    Extract and normalize estate names from address column.
    
    Args:
        df: Input DataFrame
        address_column: Column containing full addresses
        estate_column: New column name for extracted estate names
        
    Returns:
        DataFrame with new estate name column
    """
    df = df.copy()
    
    if address_column in df.columns:
        df[estate_column] = df[address_column].apply(extract_estate_name_from_address)
        logger.info(f"Extracted estate names into column '{estate_column}'")
    
    return df

# ============ FUZZY MATCHING UTILITIES ============

def fuzzy_match_properties(df1: pd.DataFrame, df2: pd.DataFrame, 
                         name_col1: str, name_col2: str, 
                         threshold: float = 80.0) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Perform fuzzy matching between two DataFrames based on property names.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        name_col1: Name column in first DataFrame
        name_col2: Name column in second DataFrame
        threshold: Matching threshold (0-100)
        
    Returns:
        Tuple of (matched_df1, matched_df2) with match information
    """
    if not RAPIDFUZZ_AVAILABLE:
        logger.warning("RapidFuzz not available. Falling back to simple string matching.")
        # Simple fallback matching
        df1 = df1.copy()
        df2 = df2.copy()
        df1['match_score'] = 0
        df2['match_score'] = 0
        return df1, df2
    
    df1 = df1.copy()
    df2 = df2.copy()
    
    # Create clean name columns for matching
    df1['clean_name'] = df1[name_col1].fillna('').str.strip().str.lower()
    df2['clean_name'] = df2[name_col2].fillna('').str.strip().str.lower()
    
    # Get unique names for matching
    unique_names1 = df1['clean_name'].unique()
    unique_names2 = df2['clean_name'].unique()
    
    # Perform fuzzy matching
    matches = {}
    for name1 in unique_names1:
        if name1 and process:  # Skip empty names and check if process is available
            match_result = process.extractOne(name1, unique_names2, score_cutoff=threshold)
            if match_result:
                matches[name1] = {
                    'match': match_result[0],
                    'score': match_result[1]
                }
    
    # Add match information to DataFrames
    df1['match_name'] = df1['clean_name'].map(lambda x: matches.get(x, {}).get('match', None))
    df1['match_score'] = df1['clean_name'].map(lambda x: matches.get(x, {}).get('score', 0))
    
    # Add reverse matching info to df2
    reverse_matches = {v['match']: k for k, v in matches.items()}
    df2['match_name'] = df2['clean_name'].map(lambda x: reverse_matches.get(x, None))
    df2['match_score'] = df2['clean_name'].map(lambda x: matches.get(reverse_matches.get(x, ''), {}).get('score', 0))
    
    # Clean up temporary columns
    df1 = df1.drop('clean_name', axis=1)
    df2 = df2.drop('clean_name', axis=1)
    
    logger.info(f"Fuzzy matching completed. Found {len(matches)} matches above threshold {threshold}")
    
    return df1, df2

# ============ COLUMN MAPPING AND FIELD EXTRACTION ============

def extract_fields_from_dict(data: Dict[str, Any], field_map: Dict[str, List[str]]) -> Dict[str, Any]:
    """
    Extract fields from a nested dictionary using a field map.
    
    Args:
        data: Nested dictionary containing data
        field_map: Mapping of output fields to paths in the nested dictionary
        
    Returns:
        Dictionary with extracted fields
    """
    result = {}
    
    for key, path in field_map.items():
        temp = data
        for p in path:
            temp = temp.get(p, None) if isinstance(temp, dict) else None
            if temp is None:
                break
        result[key] = temp
    
    return result

def standardize_column_names(df: pd.DataFrame, column_mapping: Dict[str, str]) -> pd.DataFrame:
    """
    Standardize column names using a mapping dictionary.
    
    Args:
        df: Input DataFrame
        column_mapping: Dictionary mapping old names to new names
        
    Returns:
        DataFrame with standardized column names
    """
    df = df.copy()
    
    # Only rename columns that exist in the DataFrame
    existing_mapping = {k: v for k, v in column_mapping.items() if k in df.columns}
    
    if existing_mapping:
        df = df.rename(columns=existing_mapping)
        logger.info(f"Renamed {len(existing_mapping)} columns: {existing_mapping}")
    
    return df

def remove_illegal_characters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove illegal characters from DataFrame for Excel compatibility.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with illegal characters removed
    """
    # Pattern for illegal characters in Excel
    illegal_chars = re.compile(r'[\x00-\x1F\x7F-\x9F]')
    
    def clean_cell(cell_value):
        if isinstance(cell_value, str):
            return illegal_chars.sub('', cell_value)
        return cell_value
    
    df = df.copy()
    return df.map(clean_cell)

def sanitize_worksheet_name(name: str, max_length: int = 31) -> str:
    """
    Sanitize worksheet name for Excel compatibility.
    
    Args:
        name: Original worksheet name
        max_length: Maximum allowed length
        
    Returns:
        Sanitized worksheet name
    """
    # Remove invalid characters
    name = re.sub(r'[\\/*?:\[\]]', '_', name)
    
    # Truncate if too long
    if len(name) > max_length:
        name = name[:max_length]
    
    return name

# ============ DATA TYPE CONVERSION UTILITIES ============

def convert_to_string_dtype(df: pd.DataFrame, columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Convert specified columns to string dtype.
    
    Args:
        df: Input DataFrame
        columns: List of columns to convert (None for all object columns)
        
    Returns:
        DataFrame with converted columns
    """
    df = df.copy()
    
    if columns is None:
        columns = df.select_dtypes(include=['object']).columns.tolist()
    
    # Type guard to ensure columns is not None
    if columns is not None:
        for col in columns:
            if col in df.columns:
                df[col] = df[col].astype('string')
        
        logger.info(f"Converted {len(columns)} columns to string dtype")
    
    return df

def handle_missing_values(df: pd.DataFrame, strategy: str = 'drop_empty', 
                        threshold: float = 0.5) -> pd.DataFrame:
    """
    Handle missing values in DataFrame.
    
    Args:
        df: Input DataFrame
        strategy: Strategy for handling missing values ('drop_empty', 'fill_empty', 'drop_threshold')
        threshold: Threshold for dropping columns/rows (0.0 to 1.0)
        
    Returns:
        DataFrame with missing values handled
    """
    df = df.copy()
    
    if strategy == 'drop_empty':
        # Drop rows and columns that are completely empty
        df = df.dropna(how='all', axis=0)  # Drop empty rows
        df = df.dropna(how='all', axis=1)  # Drop empty columns
        
    elif strategy == 'fill_empty':
        # Fill empty values with appropriate defaults
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].fillna('')
            else:
                df[col] = df[col].fillna(0)
                
    elif strategy == 'drop_threshold':
        # Drop columns with too many missing values
        missing_ratio = df.isnull().sum() / len(df)
        cols_to_drop = missing_ratio[missing_ratio > threshold].index.tolist()
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)
            logger.info(f"Dropped {len(cols_to_drop)} columns with >{threshold*100}% missing values")
    
    return df


# ============ COMMON DATA PROCESSING UTILITIES ============

def fill_none_values(df: pd.DataFrame, preserve_columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Fill empty cells and standardize None values across DataFrame.
    Preserve data types for important columns.
    
    Args:
        df: Input DataFrame
        preserve_columns: List of columns to preserve data types (dates, numbers, etc.)
        
    Returns:
        DataFrame with standardized None values
    """
    df = df.copy()
    
    # Default columns to preserve if not specified
    if preserve_columns is None:
        preserve_columns = [
            'estate_name', 'Name', 'title_lg', 'building_name',
            'date', 'price', 'area', 'ft_price', 'age', 'year',
            'Tower', 'Floor', 'Flat', 'g_area', 'g_unit_price', 'building_code',
        ]
    
    for col in df.columns:
        try:
            if col in preserve_columns:
                # For important columns, just clean up obvious null indicators
                if pd.api.types.is_object_dtype(df[col]):
                    df[col] = df[col].replace(['', ' ', '--', 'NULL', 'null', 'N/A'], None)
            else:
                # For other columns, convert to string
                df[col] = df[col].astype(str)
                df[col] = df[col].replace(['', ' ', 'none', 'None', 'nan', '--', 'NULL', 'null', 'N/A'], 'None')
                df[col] = df[col].fillna('None')
        except Exception as e:
            logger.warning(f"Error processing column '{col}': {e}")
            continue
    
    return df


def convert_transaction_type(trans_type: str, mapping: Optional[Dict[str, str]] = None) -> str:
    """
    Convert transaction type codes to readable labels.
    
    Args:
        trans_type: Transaction type code
        mapping: Custom mapping dictionary (default: {'R': 'RENT', 'S': 'SALE', 'L': 'RENT'})
        
    Returns:
        Readable transaction type label
    """
    if pd.isna(trans_type) or trans_type == '':
        return ''
    
    if mapping is None:
        mapping = {'R': 'RENT', 'S': 'SALE', 'L': 'RENT'}
    
    trans_type_str = str(trans_type).upper().strip()
    return mapping.get(trans_type_str, trans_type_str)


def calculate_building_age(completion_date: Union[str, datetime, date], 
                           current_year: Optional[int] = None) -> Optional[float]:
    """
    Calculate building age from completion date.
    
    Args:
        completion_date: Completion date as string, datetime, or date
        current_year: Current year (default: current year from system)
        
    Returns:
        Building age in years or None if calculation fails
    """
    try:
        if pd.isna(completion_date) or completion_date == 'None':
            return None
        
        if current_year is None:
            current_year = datetime.now().year
        
        # Convert to datetime if string
        if isinstance(completion_date, str):
            completion_dt = pd.to_datetime(completion_date, errors='coerce')
        elif isinstance(completion_date, (datetime, date)):
            completion_dt = pd.Timestamp(completion_date)
        else:
            return None
        
        if pd.isna(completion_dt):
            return None
        
        # Calculate age
        age = (pd.Timestamp.now() - completion_dt).days / 365.25
        return round(age, 1)
        
    except Exception as e:
        logger.debug(f"Error calculating age for '{completion_date}': {e}")
        return None


def standardize_date_to_format(date_value: Union[str, datetime, date], 
                               output_format: str = '%Y-%m-%d') -> Optional[str]:
    """
    Standardize date to specified format.
    
    Args:
        date_value: Date as string, datetime, or date object
        output_format: Desired output format (default: '%Y-%m-%d')
        
    Returns:
        Formatted date string or None if conversion fails
    """
    try:
        if pd.isna(date_value) or date_value == '' or date_value == 'None':
            return None
        
        # Convert to datetime
        if isinstance(date_value, str):
            dt = pd.to_datetime(date_value, errors='coerce')
        elif isinstance(date_value, (datetime, date)):
            dt = pd.Timestamp(date_value)
        else:
            return None
        
        if pd.isna(dt):
            return None
        
        # Remove timezone if present
        if dt.tz is not None:
            dt = dt.tz_localize(None)
        
        return dt.strftime(output_format)
        
    except Exception as e:
        logger.debug(f"Error formatting date '{date_value}': {e}")
        return None


def merge_price_columns(df: pd.DataFrame, 
                       price_col: str, 
                       rental_col: str, 
                       transaction_type_col: Optional[str] = None) -> pd.DataFrame:
    """
    Merge price and rental columns into a single price column.
    
    Args:
        df: Input DataFrame
        price_col: Name of the price column
        rental_col: Name of the rental column
        transaction_type_col: Optional transaction type column for better merging
        
    Returns:
        DataFrame with merged price column
    """
    df = df.copy()
    
    def merge_price_rental(row):
        """Merge price and rental, taking whichever is non-zero"""
        try:
            price_val = row[price_col] if pd.notna(row[price_col]) and row[price_col] != 0 else 0
            rental_val = row[rental_col] if pd.notna(row[rental_col]) and row[rental_col] != 0 else 0
            
            # If transaction type is available, use it to determine which value to use
            if transaction_type_col and transaction_type_col in row:
                trans_type = str(row[transaction_type_col]).upper()
                if 'SALE' in trans_type and price_val != 0:
                    return price_val
                elif 'RENT' in trans_type and rental_val != 0:
                    return rental_val
            
            # Otherwise, return non-zero value (prioritizing price)
            if price_val != 0:
                return price_val
            elif rental_val != 0:
                return rental_val
            else:
                return 0
        except Exception as e:
            logger.warning(f"Error processing price/rental: {e}")
            return 0
    
    if price_col in df.columns and rental_col in df.columns:
        df[price_col] = df.apply(merge_price_rental, axis=1)
        logger.info(f"Merged {price_col} and {rental_col} columns")
    
    return df


def clean_grade_column(grade_value: str) -> str:
    """
    Clean grade column by removing 'Grade' suffix.
    
    Args:
        grade_value: Grade value string
        
    Returns:
        Cleaned grade value
    """
    if pd.isna(grade_value) or not grade_value:
        return 'None'
    
    grade_str = str(grade_value).strip()
    if grade_str.lower().endswith(' grade'):
        return grade_str[:-6].strip()
    elif grade_str.lower().endswith('grade'):
        return grade_str[:-5].strip()
    else:
        return grade_str


def extract_address_from_url(url: str) -> Optional[str]:
    """
    Extract address information from Source B transaction URL.
    
    Args:
        url: Transaction URL
        
    Returns:
        Extracted address or None
    """
    if pd.isna(url) or not url:
        return None
    
    try:
        # Example transaction URL format from the local Source B config.
        url_parts = str(url).split('/transaction/')
        if len(url_parts) > 1:
            location_part = url_parts[1]
            # Remove the identifier at the end (e.g., I20240802455)
            address_parts = location_part.split('-')
            if len(address_parts) > 3:
                # Reconstruct address from parts
                address = ' '.join(address_parts[:-1])  # Exclude the last identifier
                return address.replace('-', ' ').strip()
        return None
    except Exception as e:
        logger.debug(f"Error extracting address from URL '{url}': {e}")
        return None


def drop_unwanted_columns(df: pd.DataFrame, columns_to_drop: List[str]) -> pd.DataFrame:
    """
    Drop unwanted columns from DataFrame.
    
    Args:
        df: Input DataFrame
        columns_to_drop: List of column names to drop
        
    Returns:
        DataFrame with columns dropped
    """
    df = df.copy()
    existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
    
    if existing_columns_to_drop:
        df = df.drop(columns=existing_columns_to_drop)
        logger.info(f"Dropped {len(existing_columns_to_drop)} unwanted columns")
    
    return df 