import pandas as pd
import logging
from datetime import datetime
import numpy as np
import re
from typing import Dict, Any
import datetime

# Configure logging
logger = logging.getLogger(__name__)


############################## 1. Centaline Res Start ##############################
def cleanse_centaline_res(centaline_res_base_df: pd.DataFrame) -> pd.DataFrame:
    """
    Enhanced cleanse and standardize Centaline residential data with proper data type conversions,
    precise age calculation with month consideration, and column management
    
    Args:
        centaline_res_base_df: Raw enriched data from enrich_estate_data function
    
    Returns:
        pd.DataFrame: Cleaned and standardized data ready for analysis
    """
    logger.info("🧹 Starting enhanced Centaline residential data cleansing...")
    
    # Create a copy to avoid modifying the original dataframe
    df = centaline_res_base_df.copy()
    
    # ============ DATE CLEANING ============
    logger.info("📅 Step 1/6: Converting date column to proper date format...")
    
    def clean_date(date_value):
        """Convert various date formats to standard datetime with validation.
        - Prefer day-first when ambiguous
        - If the parsed date is in the future, try the alternate interpretation
        - Clamp to None if still invalid
        """
        try:
            if pd.isna(date_value):
                return None
            today = pd.Timestamp.now().date()

            # Already timestamp
            if isinstance(date_value, pd.Timestamp):
                parsed = date_value.date()
            else:
                date_str = str(date_value).strip()
                if not date_str:
                    return None

                # First pass: strict known formats (prefer Y-m-d, then day-first)
                primary_formats = ['%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%Y%m%d']
                parsed = None
                for fmt in primary_formats:
                    try:
                        parsed = pd.to_datetime(date_str, format=fmt).date()
                        break
                    except ValueError:
                        continue

                # Fallback: pandas with dayfirst=True
                if parsed is None:
                    parsed_ts = pd.to_datetime(date_str, errors='coerce', dayfirst=True)
                    parsed = parsed_ts.date() if not pd.isna(parsed_ts) else None

                # If still None, give up
                if parsed is None:
                    return None

            # If parsed date is in future, try alternate interpretation (month-first)
            if parsed > today:
                alt_ts = pd.to_datetime(str(date_value).strip(), errors='coerce', dayfirst=False)
                alt_date = alt_ts.date() if not pd.isna(alt_ts) else None
                if alt_date and alt_date <= today:
                    return alt_date
                return None

            return parsed
        except Exception as e:
            logger.debug(f"Error converting date '{date_value}': {str(e)}")
            return None
    
    # No date backfilling – keep transaction dates strictly as scraped; missing dates remain null

    if 'date' in df.columns:
        df['date'] = df['date'].apply(clean_date)
        # Convert to yyyy-mm-dd format
        df['date'] = df['date'].apply(
            lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) and hasattr(x, 'strftime') else None
        )
        logger.info(f"   ✅ Converted {df['date'].notna().sum()} dates to yyyy-mm-dd format")
    
    # ============ PRICE CLEANING ============
    logger.info("💰 Step 2/6: Converting price columns to numeric values...")
    
    def clean_price(price_value):
        """Convert price strings like $5.05M, $25,500 to numeric values"""
        try:
            if pd.isna(price_value) or price_value == '' or price_value == '--':
                return None
                
            price_str = str(price_value).strip()
            if not price_str:
                return None
            
            # Remove currency symbols and spaces
            price_clean = price_str.replace('$', '').replace(',', '').strip()
            
            # Handle empty or dash cases
            if not price_clean or price_clean == '--':
                return None
            
            # Check for M (millions) suffix
            if price_clean.upper().endswith('M'):
                number_part = price_clean[:-1].strip()
                return float(number_part) * 1000000
            
            # Regular number (thousands)
            return float(price_clean)
            
        except Exception as e:
            logger.debug(f"Error converting price '{price_value}': {str(e)}")
            return None
    
    # Apply price cleaning to price column
    if 'price' in df.columns:
        df['price'] = df['price'].apply(clean_price)
        logger.info(f"   ✅ Converted {df['price'].notna().sum()} price values successfully")
    
    # ============ AREA CLEANING ============
    logger.info("📏 Step 3/6: Extracting numeric values from area column...")
    
    def clean_area(area_value):
        """Extract numeric value from area strings like '385ft²' or '10,000ft²'"""
        try:
            if pd.isna(area_value) or area_value == '' or area_value == '--':
                return None
                
            area_str = str(area_value).strip()
            if not area_str:
                return None
            
            # Extract numbers using regex - handle comma separators properly
            import re
            # First remove all non-digit, non-decimal, non-comma characters
            clean_str = re.sub(r'[^\d,.]', '', area_str)
            
            # Remove commas (thousand separators) and convert to float
            if clean_str:
                # Handle cases like "10,000" or "1,234.56"
                numeric_str = clean_str.replace(',', '')
                if numeric_str:
                    return float(numeric_str)
            
            return None
            
        except Exception as e:
            logger.debug(f"Error converting area '{area_value}': {str(e)}")
            return None
    
    if 'area' in df.columns:
        df['area'] = df['area'].apply(clean_area)
        logger.info(f"   ✅ Converted {df['area'].notna().sum()} area values successfully")
    
    # ============ FT_PRICE CLEANING ============
    logger.info("💱 Step 4/6: Extracting numeric values from ft_price column...")
    
    def clean_ft_price(ft_price_value):
        """Extract numeric value from ft_price strings like '@$13,117'"""
        try:
            if pd.isna(ft_price_value) or ft_price_value == '' or ft_price_value == '--':
                return None
                
            ft_price_str = str(ft_price_value).strip()
            if not ft_price_str:
                return None
            
            # Remove @ symbol, $ symbol, and commas
            price_clean = ft_price_str.replace('@', '').replace('$', '').replace(',', '').strip()
            
            if not price_clean or price_clean == '--':
                return None
            
            # Extract numbers using regex
            import re
            numbers = re.findall(r'\d+\.?\d*', price_clean)
            if numbers:
                return float(numbers[0])
            
            return None
            
        except Exception as e:
            logger.debug(f"Error converting ft_price '{ft_price_value}': {str(e)}")
            return None
    
    if 'ft_price' in df.columns:
        df['ft_price'] = df['ft_price'].apply(clean_ft_price)
        logger.info(f"   ✅ Converted {df['ft_price'].notna().sum()} ft_price values successfully")
    
    # ============ COMPLETION YEAR AND AGE CALCULATION ============
    logger.info("🏗️ Step 5/6: Calculating completion year and age from year column...")
    
    # The year column should already contain completion years from the enrich_estate_data function
    if 'year' in df.columns:
        try:
            # Calculate age based on completion year
            current_year = pd.Timestamp.now().year
            df['age'] = df['year'].apply(
                lambda x: max(0, current_year - x) if pd.notna(x) and x != 'None' else None
            )
            
            # Ensure correct data types
            df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
            df['age'] = pd.to_numeric(df['age'], errors='coerce').astype('float64')
            
            logger.info(f"   ✅ Calculated age for {df['age'].notna().sum()} properties from completion year")
        except Exception as e:
            logger.warning(f"   ⚠️ Error calculating age: {e}")
            df['age'] = None
    else:
        logger.warning("   ⚠️ No year column found - age column not created")
    
    # ============ TITLE-LG ADOPTION FOR PROPERTY NAMES ============
    logger.info("🏠 Step 6/9: Setting property names...")
    
    # Support both old and new data formats
    if 'Name' in df.columns:
        # New JavaScript extraction format
        df['property_name'] = df['Name']
        logger.info(f"   ✅ Used Name column for {df['Name'].notna().sum()} property names")
    elif 'title_lg' in df.columns and 'address' in df.columns:
        # Old format with both
        df['property_name'] = df['title_lg'].fillna(df['address'])
        logger.info(f"   ✅ Used title_lg/address for property names")
    elif 'title_lg' in df.columns:
        # Has title_lg only
        df['property_name'] = df['title_lg']
        logger.info(f"   ✅ Used title_lg for property names")
    elif 'address' in df.columns:
        # Old format
        df['property_name'] = df['address']
        logger.info(f"   ✅ Used address for property names")
    else:
        # No suitable column
        df['property_name'] = ''
        logger.warning(f"   ⚠️ No name column found - using empty")
    
    # ============ ADDRESS PARSING ============
    logger.info("🏢 Step 7/9: Parsing Tower/Block, Floor, and Flat from address...")
    
    def parse_address_components(address):
        """Parse Tower/Block, Floor, and Flat from address string"""
        if pd.isna(address) or not address:
            return {'Tower': None, 'Floor': None, 'Flat': None}
        
        address = str(address).strip()
        
        # Initialize components
        tower = None
        floor = None
        flat = None
        
        try:
            # Parse Tower/Block (Tower xx or Block xx)
            tower_patterns = [
                r'Tower\s+(\w+)',
                r'Block\s+(\w+)',
                r'座\s*(\w+)',  # Chinese for Block/Tower
                r'(\w+)\s*座'
            ]
            
            for pattern in tower_patterns:
                match = re.search(pattern, address, re.IGNORECASE)
                if match:
                    tower = match.group(1)
                    break
            
            # Parse Floor (10/F, Lower Floor, Middle Floor, High Floor, etc.)
            floor_patterns = [
                r'(\d+)/F',
                r'(\d+)F',
                r'(\d+)\s*樓',  # Chinese for Floor
                r'(Lower|Middle|High|Upper)\s*Floor',
                r'(Lower|Middle|High|Upper)\s*樓',
                r'G/F',  # Ground Floor
                r'LG/F',  # Lower Ground
                r'UG/F',  # Upper Ground
                r'B/M'   # Basement
            ]
            
            for pattern in floor_patterns:
                match = re.search(pattern, address, re.IGNORECASE)
                if match:
                    if pattern in ['G/F', 'LG/F', 'UG/F', 'B/M']:
                        floor = match.group(0)
                    else:
                        floor = match.group(1)
                    break
            
            # Parse Flat/Room
            flat_patterns = [
                r'Flat\s+(\w+)',
                r'Room\s+(\w+)',
                r'Unit\s+(\w+)',
                r'室\s*(\w+)',  # Chinese for Room
                r'(\w+)\s*室',
                r'(\w+)\s*號'   # Chinese for Number
            ]
            
            for pattern in flat_patterns:
                match = re.search(pattern, address, re.IGNORECASE)
                if match:
                    flat = match.group(1)
                    break
            
        except Exception as e:
            logger.debug(f"Error parsing address '{address}': {str(e)}")
        
        return {'Tower': tower, 'Floor': floor, 'Flat': flat}
    
    if 'address' in df.columns:
        # Only parse address if Tower/Floor/Flat columns don't already exist
        if not all(col in df.columns for col in ['Tower', 'Floor', 'Flat']):
            address_components = df['address'].apply(parse_address_components).apply(pd.Series)
            df = pd.concat([df, address_components], axis=1)
            logger.info(f"   ✅ Parsed address components for {len(df)} records")
        else:
            logger.info(f"   ℹ️ Tower/Floor/Flat columns already exist, skipping address parsing")
    
    # ============ TYPE CLASSIFICATION ============
    logger.info("🚗 Step 8/9: Creating Type column and parsing carpark details...")
    
    def classify_property_type(address):
        """Classify property as Residential or Carpark"""
        if pd.isna(address) or not address:
            return 'Residential'
        
        address_str = str(address).lower()
        if 'carpark' in address_str or 'car park' in address_str or '停車場' in address_str:
            return 'Carpark'
        else:
            return 'Residential'
    
    def extract_carpark_details(address):
        """Extract carpark floor and number details"""
        if pd.isna(address) or not address:
            return {'Carpark_Floor': None, 'Carpark_Number': None}
        
        address_str = str(address)
        carpark_floor = None
        carpark_number = None
        
        if 'carpark' in address_str.lower() or 'car park' in address_str.lower():
            # Extract floor information for carparks with improved mapping
            floor_patterns = [
                r'(BM\d+)',  # BM1, BM2, BM3, BM4, BM5
                r'(LG\d+)',  # LG1, LG2, LG3, LG4
                r'(M/F)',    # M/F
                r'(PODIUM)', # PODIUM
                r'(R/F)',    # R/F
                r'(U/G|L/G|B\d+|G/F)',  # Underground, Lower Ground, Basement, Ground
                r'(\d+)/F',  # Numbered floors
            ]
            
            for pattern in floor_patterns:
                match = re.search(pattern, address_str, re.IGNORECASE)
                if match:
                    carpark_floor = match.group(1)
                    break
            
            # Extract carpark number/space
            number_patterns = [
                r'No\.\s*(\w+)',
                r'Space\s+(\w+)',
                r'(\w+)\s*號'  # Chinese for Number
            ]
            
            for pattern in number_patterns:
                match = re.search(pattern, address_str, re.IGNORECASE)
                if match:
                    carpark_number = match.group(1)
                    break
        
        return {'Carpark_Floor': carpark_floor, 'Carpark_Number': carpark_number}
    
    if 'estate_type' in df.columns:
        # JavaScript provides estate_type already
        df['Type'] = df['estate_type']
        logger.info(f"   ✅ Used estate_type from JavaScript")
    elif 'Name' in df.columns:
        # Classify from Name
        df['Type'] = df['Name'].apply(classify_property_type)
        carpark_details = df['Name'].apply(extract_carpark_details).apply(pd.Series)
        df = pd.concat([df, carpark_details], axis=1)
        logger.info(f"   ✅ Classified {(df['Type'] == 'Carpark').sum()} carpark records from Name")
    elif 'address' in df.columns:
        df['Type'] = df['address'].apply(classify_property_type)
        carpark_details = df['address'].apply(extract_carpark_details).apply(pd.Series)
        df = pd.concat([df, carpark_details], axis=1)
        logger.info(f"   ✅ Classified {(df['Type'] == 'Carpark').sum()} carpark records")
    else:
        df['Type'] = 'Residential'
        logger.info(f"   ℹ No classification column - defaulted to Residential")
    
    # ============ DATASOURCE COLUMN ============
    logger.info("📊 Step 9/9: Adding Datasource column...")
    df['Datasource'] = 'Centaline'
    
    # ============ COLUMN REMOVAL ============
    logger.info("🗑️ Step 10/10: Removing unwanted columns...")
    
    # Remove unwanted columns as specified
    columns_to_remove = [
        'standard_date',  # Remove as requested
        'scrape_timestamp',
        'extracted_estate_name',
        'matched_estate_name',
        'MoM',
        'Scraped Estate Name',
        'Scraped Blocks',
        'Scraped Units',
        'Estate Detailed Address',
        'For Sale',
        'For Rent',
        'Region',
        'District',
        'Subdistrict',
        'Code',
        'scraped_estate_name',
        'occupation_permit',
        'scraped_blocks',
        'school_net_info',
        'estate_detailed_address',
        'Link',
        'estate_match_found',
        'high_confidence_match',
        'exact_match',
        'processing_timestamp'
    ]
    
    # Remove columns if they exist
    existing_columns_to_remove = [col for col in columns_to_remove if col in df.columns]
    if existing_columns_to_remove:
        df = df.drop(columns=existing_columns_to_remove)
        logger.info(f"   🗑️ Removed {len(existing_columns_to_remove)} unwanted columns")
    else:
        logger.info("   ℹ️ No matching columns found to remove")
    
    # ============ COLUMN REORDERING PLACEHOLDER ============
    # PLACEHOLDER: Reorder columns
    # Uncomment and modify the list below to set your preferred column order
    """
    preferred_column_order = [
        # Add your preferred column order here
        # Example:
        'date',
        'address',
        'price',
        'area',
        'ft_price',
        'rooms',
        'age',  # New precise age column
        'transaction_type',
        'agency',
        'Name',  # Estate name
        'Address',  # Estate address
        'Blocks',
        'Units',
        'Unit Rate',
        'Trans Record',
        'Estate Link',
        'School Net Info',
        'Developer'
        # Add any other columns you want in specific order
    ]
    
    # Reorder columns if they exist
    existing_preferred_columns = [col for col in preferred_column_order if col in df.columns]
    remaining_columns = [col for col in df.columns if col not in existing_preferred_columns]
    
    if existing_preferred_columns:
        # Reorder: preferred columns first, then remaining columns
        final_column_order = existing_preferred_columns + remaining_columns
        df = df[final_column_order]
        print(f"   📝 Reordered columns according to preference")
        print(f"   📝 Column order: {final_column_order}")
    """
    
    # ============ FINAL STATISTICS ============
    logger.info("\n📊 Enhanced Data Cleansing Summary:")
    logger.info(f"   - Total records processed: {len(df):,}")
    logger.info(f"   - Date values: {df['date'].notna().sum():,} valid dates")
    if 'price' in df.columns:
        logger.info(f"   - Price values: {df['price'].notna().sum():,} valid prices")
    if 'area' in df.columns:
        logger.info(f"   - Area values: {df['area'].notna().sum():,} valid areas")
    if 'ft_price' in df.columns:
        logger.info(f"   - Ft_price values: {df['ft_price'].notna().sum():,} valid ft_prices")
    if 'age' in df.columns:
        logger.info(f"   - Age values: {df['age'].notna().sum():,} calculated ages")
        # Show age distribution
        if df['age'].notna().sum() > 0:
            min_age = df['age'].min()
            max_age = df['age'].max()
            avg_age = df['age'].mean()
            print(f"   - Age range: {min_age}-{max_age} years (average: {avg_age:.1f} years)")
    logger.info(f"   - Final columns: {len(df.columns)}")
    #logger.info(f"   - Remaining columns: {list(df.columns)}")
    
    # ============ FILL NONE VALUES ============
    logger.info("🔄 Final step: Cleaning empty cells...")
    
    def fill_none_values(df):
        """Fill empty cells and standardize None values - preserve data types for important columns"""
        
        # Critical columns that should preserve their data types and NOT be converted to string
        preserve_columns = [
            'estate_name', 'Name', 'title_lg', 'building_name',  # Name-related columns
            'date', 'price', 'area', 'ft_price', 'age', 'year',  # Numeric/date columns
            'Tower', 'Floor', 'Flat',  # Address components
            'g_area', 'g_unit_price', 'building_code',  # Additional JavaScript fields
        ]
        
        for col in df.columns:
            try:
                if col in preserve_columns:
                    # For important columns, just clean up obvious null indicators but preserve data type
                    # Replace string representations of null with actual NaN
                    if pd.api.types.is_object_dtype(df[col]):
                        df[col] = df[col].replace(['', ' ', '--', 'NULL', 'null', 'N/A'], None)
                    # Don't convert to string, keep as-is (dates stay dates, numbers stay numbers, strings stay strings)
                else:
                    # For other columns, apply the full string conversion (backward compatibility)
                    df[col] = df[col].astype(str)
                    df[col] = df[col].replace(['', ' ', 'none', 'None', 'nan', '--', 'NULL', 'null', 'N/A'], 'None')
                    df[col] = df[col].fillna('None')
            except Exception as e:
                logger.warning(f"Error processing column '{col}': {e}")
                continue
        return df
    
    df = fill_none_values(df)
    logger.info("   ✅ Cleaned empty values while preserving important data types")
    
    # ============ REMOVE DUPLICATES ============
    initial_count = len(df)
    df = df.drop_duplicates()
    duplicates_removed = initial_count - len(df)
    if duplicates_removed > 0:
        logger.info(f"🗑️  Removed {duplicates_removed:,} duplicate rows")
    else:
        logger.info("✅ No duplicate rows found")
    
    logger.info(f"Enhanced data cleansing completed: {len(df)} records processed")
    logger.info("✅ Enhanced Centaline residential data cleansing completed successfully!")
    
    return df

############################## 1. Centaline Res End ##############################

############################## 2. Centaline Oir Start ##############################

def cleanse_centaline_oir(merged_data: pd.DataFrame) -> pd.DataFrame:

    """
    Kedro node to process merged property transaction data.
    
    This function performs the following transformations:
    1. Merges price and rental columns into one price column
    2. Converts transactionDate to dd/mm/yyyy format
    3. Handles unit column to prevent Excel date conversion
    4. Extracts year and age from completion_year
    5. Includes comprehensive error handling
    6. Provides placeholders for column dropping and reordering
    
    Args:
        merged_data (pd.DataFrame): Input dataframe with merged transaction and building data
        params (Dict[str, Any], optional): Parameters for processing configuration
    
    Returns:
        pd.DataFrame: Processed dataframe with transformed columns
    """
    
    # Initialize logging
    logger = logging.getLogger(__name__)
    logger.info("Starting transaction data processing")
    
    # Create a copy to avoid modifying the original dataframe
    processed_df = merged_data.copy()
    
    try:
        # 1. Merge price and rental columns
        def merge_price_rental(row):
            """Merge price and rental, taking whichever is non-zero"""
            try:
                price_val = row['price'] if pd.notna(row['price']) and row['price'] != 0 else 0
                rental_val = row['rental'] if pd.notna(row['rental']) and row['rental'] != 0 else 0
                
                # Return the non-zero value, prioritizing price if both are non-zero
                if price_val != 0:
                    return price_val
                elif rental_val != 0:
                    return rental_val
                else:
                    return 0
            except (ValueError, TypeError):
                logger.warning(f"Error processing price/rental for row: {row.name}")
                return 0
        
        processed_df['price'] = processed_df.apply(merge_price_rental, axis=1)
        logger.info("Successfully merged price and rental columns")
        
    except Exception as e:
        logger.error(f"Error in merging price/rental: {e}")
        # Continue processing with original price column
    
    try:
        # 2. Convert transactionDate to yyyy-mm-dd format
        def convert_transaction_date(date_str):
            """Convert date format to yyyy-mm-dd"""
            try:
                if pd.isna(date_str) or date_str == '':
                    return ''

                # Parse the date as dd/mm/yyyy format (dayfirst=True)
                dt = pd.to_datetime(date_str, dayfirst=True)
                return dt.strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                logger.warning(f"Could not convert date: {date_str}")
                return str(date_str)  # Return original if conversion fails

        processed_df['transactionDate'] = processed_df['transactionDate'].apply(convert_transaction_date)
        logger.info("Successfully converted transactionDate format")
        
    except Exception as e:
        logger.error(f"Error in date conversion: {e}")
    
    try:
        # 3. Handle unit column to prevent Excel date conversion
        def format_unit_column(unit_val):
            """Format unit to prevent Excel auto-conversion to dates"""
            try:
                if pd.isna(unit_val) or unit_val == '' or unit_val == '--':
                    return unit_val
                
                # Convert to string and add apostrophe prefix to prevent Excel date conversion
                unit_str = str(unit_val)
                
                # If it looks like it might be converted to a date by Excel (contains numbers and dashes/slashes)
                if re.search(r'\d+[-/]\d+', unit_str) or re.search(r'^\d+-\w+$', unit_str):
                    return f"'{unit_str}"  # Add apostrophe prefix
                
                return unit_str
            except (ValueError, TypeError):
                return str(unit_val) if unit_val is not None else ''
        
        processed_df['unit'] = processed_df['unit'].apply(format_unit_column)
        logger.info("Successfully formatted unit column")
        
    except Exception as e:
        logger.error(f"Error in unit column formatting: {e}")
    
    try:
        # 4. Extract year and age from completion_year
        def extract_year_age(completion_str):
            """Extract year and age from completion_year string"""
            try:
                if pd.isna(completion_str) or completion_str == '':
                    return None, None
                
                # Pattern: "1980 (45 Year(s))"
                match = re.search(r'(\d{4})\s*\((\d+)\s*Year', str(completion_str))
                if match:
                    year = int(match.group(1))
                    age = int(match.group(2))
                    return year, age
                else:
                    # Try to extract just the year if pattern doesn't match
                    year_match = re.search(r'(\d{4})', str(completion_str))
                    if year_match:
                        year = int(year_match.group(1))
                        # Calculate age based on current year (2025)
                        age = datetime.datetime.now().year - year
                        return year, age
                    
                return None, None
            except (ValueError, TypeError):
                return None, None
        
        # Apply the extraction function
        year_age_results = processed_df['completion_year'].apply(extract_year_age)
        processed_df['completion_year'] = [result[0] for result in year_age_results]
        processed_df['age'] = [result[1] for result in year_age_results]
        
        # Ensure correct data types
        processed_df['completion_year'] = pd.to_numeric(processed_df['completion_year'], errors='coerce').astype('Int64')
        processed_df['age'] = pd.to_numeric(processed_df['age'], errors='coerce').astype('float64')
        
        logger.info("Successfully extracted year and age from completion_year")
        
    except Exception as e:
        logger.error(f"Error in completion_year processing: {e}")
        
    try:
        # 5. Convert transactionType codes to readable labels
        def convert_transaction_type(trans_type):
            """Convert transaction type codes to readable labels"""
            try:
                if pd.isna(trans_type) or trans_type == '':
                    return ''
                
                trans_type_str = str(trans_type).upper().strip()
                
                if trans_type_str == 'R':
                    return 'RENT'
                elif trans_type_str == 'S':
                    return 'SALE'
                else:
                    return trans_type_str  # Return original if not R or S
            except (ValueError, TypeError):
                logger.warning(f"Error processing transaction type: {trans_type}")
                return str(trans_type) if trans_type is not None else ''
        
        processed_df['transactionType'] = processed_df['transactionType'].apply(convert_transaction_type)
        logger.info("Successfully converted transactionType codes to labels")
        
    except Exception as e:
        logger.error(f"Error in transactionType conversion: {e}")

    
    # 6. Placeholder for dropping unwanted columns
    # TODO: Customize this list based on your requirements
    columns_to_drop = [
        'rental',  
        'centabldg',
        'deptDisplayName',
        'AreaCode',
        'pricePostTypeDisplayName',
        'rentPostTypeDisplayName',
        'avgPrice',
        'avgRental',
        'propertyId',
        'district',
        'usage',
        'title_status',
        'transportation',
        'ac_system',
        'lifts',
        'property_id',
        'building_name',
        'zone',
        '_merge_status',
    ]

    try:
        # Drop columns that exist in the dataframe
        existing_cols_to_drop = [col for col in columns_to_drop if col in processed_df.columns]
        if existing_cols_to_drop:
            processed_df = processed_df.drop(columns=existing_cols_to_drop)
            logger.info(f"Dropped {len(existing_cols_to_drop)} columns")
    except Exception as e:
        logger.error(f"Error dropping columns: {e}")
    '''
    # 7. Placeholder for column reordering
    # TODO: Customize this order based on your requirements
    desired_column_order = [
        # Primary transaction information
        'id',
        'transactionDate',
        'transactionType',
        'price',
        
        # Property information
        'propertyNameEn',
        'propertyNameCn',
        'floor',
        'unit',
        'transactionArea',
        
        # Building information
        'completion_year',
        'age',
        'grade',
        'property_type',
        'total_floors',
        'floor_area',
        
        # Location information
        'district',
        'full_address',
        'Region',
        'District',
        'zone',
        
        # Additional details
        'developers',
        'management_company',
        'transportation',
        'ceiling_height',
        'ac_system',
        'lifts',
        'carpark',
        
        # Identifiers and metadata
        'propertyId',
        'property_id',
        'building_name',
        'AreaCode',
        '_merge_status',
        
        # Add remaining columns as needed...
    ]
    
    try:
        # Reorder columns - put specified columns first, then add remaining columns
        existing_ordered_cols = [col for col in desired_column_order if col in processed_df.columns]
        remaining_cols = [col for col in processed_df.columns if col not in existing_ordered_cols]
        final_column_order = existing_ordered_cols + remaining_cols
        
        processed_df = processed_df[final_column_order]
        logger.info(f"Reordered columns. Total columns: {len(final_column_order)}")
    except Exception as e:
        logger.error(f"Error reordering columns: {e}")'''
    
    # ============ MERGE AVG PRICE COLUMNS ============
    logger.info("💰 Merging avgPriceDisplayName and avgRentalDisplayName into one column...")
    
    def merge_avg_price(row):
        """Merge avgPriceDisplayName and avgRentalDisplayName based on transaction type"""
        try:
            avg_price = row.get('avgPriceDisplayName', '')
            avg_rental = row.get('avgRentalDisplayName', '')
            transaction_type = row.get('transactionType', '')
            
            # Use the appropriate column based on transaction type
            if transaction_type == 'SALE' and avg_price and str(avg_price).strip() not in ['', 'None', 'nan']:
                return avg_price
            elif transaction_type == 'RENT' and avg_rental and str(avg_rental).strip() not in ['', 'None', 'nan']:
                return avg_rental
            else:
                # Fallback: return whichever has a value
                if avg_price and str(avg_price).strip() not in ['', 'None', 'nan']:
                    return avg_price
                elif avg_rental and str(avg_rental).strip() not in ['', 'None', 'nan']:
                    return avg_rental
                else:
                    return 'None'
        except Exception as e:
            logger.debug(f"Error merging avg price: {str(e)}")
            return 'None'
    
    processed_df['avgPrice'] = processed_df.apply(merge_avg_price, axis=1)
    logger.info("   ✅ Merged avgPriceDisplayName and avgRentalDisplayName into avgPrice column")
    
    # ============ CLEAN GRADE COLUMN ============
    logger.info("🏢 Cleaning grade column...")
    
    def clean_grade(grade_value):
        """Remove 'Grade' suffix from grade values"""
        if pd.isna(grade_value) or not grade_value:
            return 'None'
        
        grade_str = str(grade_value).strip()
        if grade_str.lower().endswith(' grade'):
            return grade_str[:-6].strip()  # Remove ' Grade'
        elif grade_str.lower().endswith('grade'):
            return grade_str[:-5].strip()  # Remove 'Grade'
        else:
            return grade_str
    
    if 'grade' in processed_df.columns:
        processed_df['grade'] = processed_df['grade'].apply(clean_grade)
        logger.info("   ✅ Cleaned grade column")
    
    # ============ CLEAN PROPERTY USAGE ============
    logger.info("🏪 Cleaning property usage column...")
    
    def clean_property_usage(usage_value):
        """Convert 'office' to 'Commercial', keep others unchanged"""
        if pd.isna(usage_value) or not usage_value:
            return 'None'
        
        usage_str = str(usage_value).strip().lower()
        if usage_str == 'office':
            return 'Commercial'
        else:
            return str(usage_value).strip()
    
    # Drop propertyUsageEn and keep propertyUsageDisplayName
    if 'propertyUsageEn' in processed_df.columns:
        processed_df = processed_df.drop(columns=['propertyUsageEn'])
        logger.info("   🗑️ Removed propertyUsageEn column")
    
    if 'propertyUsageDisplayName' in processed_df.columns:
        processed_df['propertyUsageDisplayName'] = processed_df['propertyUsageDisplayName'].apply(clean_property_usage)
        logger.info("   ✅ Cleaned propertyUsageDisplayName column")
    
    # ============ PROPERTY TYPE CLEANING (Rename Commercial to Office) ============
    if 'property_type' in processed_df.columns:
        # Rename Commercial to Office
        processed_df['property_type'] = processed_df['property_type'].replace('Commercial', 'Office')
        logger.info("   ✅ Renamed Commercial to Office in property_type column")
    
    # ============ DATASOURCE COLUMN ============
    logger.info("📊 Adding Datasource column...")
    processed_df['Datasource'] = 'Centaline'
    
    # ============ FILL NONE VALUES ============
    logger.info("🔄 Filling empty cells with 'None'...")
    
    def fill_none_values(df):
        """Fill empty cells and standardize None values - simplified to avoid Parquet type issues"""
        
        for col in df.columns:
            # Convert everything to string to avoid complex type conflicts
            df[col] = df[col].astype(str)
            df[col] = df[col].replace(['', ' ', 'none', 'None', 'nan', '--', 'NULL', 'null', 'N/A'], 'None')
            # Fill actual NaN values with 'None'
            df[col] = df[col].fillna('None')
        return df
    
    processed_df = fill_none_values(processed_df)
    logger.info("   ✅ Filled empty values with 'None'")
    
    logger.info(f"Processing completed. Final shape: {processed_df.shape}")
    return processed_df

############################## 2. Centaline Oir End ##############################

############################## 3. Midland Res Start ##############################

def cleanse_midland_res(
    df: pd.DataFrame
    ) -> pd.DataFrame:
    """
    Final data cleaning and processing for the merged property data
    
    Args:
        df: Merged dataframe from transactions and estates
        
    Returns:
        pd.DataFrame: Cleaned and processed dataframe
    """
    try:
        # Make a copy to avoid modifying the original dataframe
        processed_df = df.copy()
        
        # 1. Process date columns - fix timezone issues and convert to dd/mm/yyyy format
        date_columns = [
            'building_first_op_date', 'tx_date', 'last_tx_date', 
            'update_date', 'first_op_date', 'market_stat_monthly_0_date'
        ]
        
        for col in date_columns:
            if col in processed_df.columns:
                try:
                    # Convert to datetime first
                    processed_df[col] = pd.to_datetime(processed_df[col], errors='coerce')
                    
                    # Fix timezone issues (convert UTC to HK time if needed)
                    if processed_df[col].dt.tz is not None:
                        # Convert from UTC to HK time (UTC+8)
                        processed_df[col] = processed_df[col].dt.tz_convert('Asia/Hong_Kong')
                    else:
                        # Assume it's already in HK time, just localize
                        processed_df[col] = processed_df[col].dt.tz_localize('Asia/Hong_Kong')
                    
                    # Convert to date only (remove time)
                    processed_df[col] = processed_df[col].dt.date
                    
                    # Convert to yyyy-mm-dd format
                    processed_df[col] = processed_df[col].apply(
                        lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) and hasattr(x, 'strftime') else None
                    )

                    logger.info(f"✅ Fixed {col} timezone and converted to yyyy-mm-dd format")
                except (ValueError, TypeError) as e:
                    logger.warning(f"Error processing date column {col}: {e}")
                    # Keep original values if processing fails
                    continue
        
        # 2. Add age column based on building_first_op_date
        try:
            # Check for building_first_op_date first, then fallback to first_op_date
            date_col_for_age = None
            if 'building_first_op_date' in processed_df.columns:
                date_col_for_age = 'building_first_op_date'
            elif 'first_op_date' in processed_df.columns:
                date_col_for_age = 'first_op_date'
            
            if date_col_for_age:
                processed_df['age'] = None
                
                # Calculate age only for non-null date values (excluding 'None' strings)
                mask = (processed_df[date_col_for_age].notna()) & (processed_df[date_col_for_age] != 'None')
                if mask.any():
                    # Convert string dates back to datetime for age calculation
                    op_dates = pd.to_datetime(processed_df.loc[mask, date_col_for_age], errors='coerce')
                    ages = (pd.Timestamp.now() - op_dates).dt.days / 365.25
                    processed_df.loc[mask, 'age'] = ages.round(1)
                    
                    # Ensure age column has correct data type
                    processed_df['age'] = pd.to_numeric(processed_df['age'], errors='coerce').astype('float64')
                    
                    logger.info(f"✅ Age calculated for {mask.sum()} records using {date_col_for_age}")
                else:
                    logger.warning(f"No valid dates found in {date_col_for_age} for age calculation")
            else:
                logger.warning("Neither building_first_op_date nor first_op_date column found, skipping age calculation")
                
        except (ValueError, TypeError) as e:
            logger.warning(f"Error calculating age column: {e}")
            processed_df['age'] = None
        
        # 3. Add tx_type mapping: S->SALE, L->RENT
        try:
            if 'tx_type' in processed_df.columns:
                # Clean and standardize values
                processed_df['tx_type'] = (
                    processed_df['tx_type']
                    .astype(str)
                    .str.strip()
                    .str.upper()
                )
                
                # Perform mapping
                tx_type_mapping = {'S': 'SALE', 'L': 'RENT'}
                processed_df['tx_type'] = processed_df['tx_type'].map(tx_type_mapping)
                
                # Log results
                logger.info(f"Final tx_type values: {processed_df['tx_type'].value_counts().to_dict()}")
                
        except (ValueError, TypeError) as e:
            logger.warning(f"Error mapping tx_type column: {e}")

        
        # 4. Drop unwanted columns (placeholder - you can modify this list)
        columns_to_drop = [
            'sm_district', 'int_district_id_trans', 'int_district', 
            'int_sm_district_id_trans', 'int_sm_district', 'estate_id',
            'building_id', 'unit', 'floor_level_id', 'holding_period',
            'gain', 'transaction_type', 'id', 'name', 'amenities',
            'housing_type', 'sm_district_name', 'region_name_estate', 
            'subregion_name', 'district_name', 'int_district_id_estate',
            'int_district_name', 'int_sm_district_id_estate',
            'int_sm_district_name', 'location_lat', 'location_lon',
            'parent_estate_id', 'parent_estate_name', 
            'property_stat_sell_count', 'property_stat_rent_count',
            'market_stat_total_tx_amount',
            'market_stat_monthly_0_avg_net_ft_price',
            'market_stat_monthly_1_avg_net_ft_price',
            'market_stat_monthly_2_avg_net_ft_price',
            'market_stat_monthly_3_avg_net_ft_price',
            'market_stat_monthly_4_avg_net_ft_price',
            'market_stat_monthly_5_avg_net_ft_price',
            'market_stat_monthly_6_avg_net_ft_price',
            'market_stat_monthly_7_avg_net_ft_price',
            'market_stat_monthly_8_avg_net_ft_price',
            'market_stat_monthly_9_avg_net_ft_price',
            'market_stat_monthly_10_avg_net_ft_price',
            'market_stat_monthly_11_date',
            'market_stat_monthly_11_avg_net_ft_price', 'tags', 'update_date',
            'tx_history_url_desc', 'market_stat_yearly_tx_count',
            'market_stat_yearly_total_tx_amount', 'market_stat_yearly_net_ft_price',
            'market_stat_yearly_net_ft_price_chg', 'market_stat_yearly_tx_count', 
            'market_stat_yearly_total_tx_amount', 'market_stat_yearly_net_ft_price'
        ]
        
        # Drop columns that exist in the dataframe
        existing_columns_to_drop = [col for col in columns_to_drop if col in processed_df.columns]
        if existing_columns_to_drop:
            processed_df = processed_df.drop(columns=existing_columns_to_drop)
            logger.info(f"Dropped {len(existing_columns_to_drop)} unwanted columns")
        
        # 5. Column reordering placeholder
        # TODO: Add your preferred column order here
        # Example:
        # preferred_order = ['estate', 'building', 'price', 'area', 'tx_date', 'tx_type_description', 'age', ...]
        # existing_preferred = [col for col in preferred_order if col in processed_df.columns]
        # other_columns = [col for col in processed_df.columns if col not in preferred_order]
        # final_order = existing_preferred + other_columns
        # processed_df = processed_df[final_order]
        
        # ============ LOCATION COLUMN CLEANING ============
        logger.info("📍 Cleaning location column...")
        
        # Convert location column to string format to avoid mixed data types
        if 'location' in processed_df.columns:
            try:
                def clean_location(loc):
                    """Convert location to string format"""
                    if pd.isna(loc) or not loc:
                        return 'None'
                    
                    try:
                        # If it's a dictionary with lat/lon, convert to string
                        if isinstance(loc, dict):
                            if 'lat' in loc and 'lon' in loc:
                                return f"{loc['lat']:.6f}, {loc['lon']:.6f}"
                            else:
                                return str(loc)
                        else:
                            return str(loc)
                    except:
                        return 'None'
                
                processed_df['location'] = processed_df['location'].apply(clean_location)
                logger.info(f"   ✅ Cleaned location column for {len(processed_df)} records")
            except Exception as e:
                logger.warning(f"   ⚠️ Error cleaning location column: {e}")
        
        # ============ ADDRESS EXTRACTION FROM URLS ============
        logger.info("📍 Extracting address information from URLs...")
        
        # Extract address information from url_desc_trans for Midland Residential
        if 'url_desc_trans' in processed_df.columns:
            try:
                def extract_address_from_url(url):
                    """Extract address information from Midland transaction URL"""
                    if pd.isna(url) or not url:
                        return None
                    
                    try:
                        # URL format: https://www.midland.com.hk/en/transaction/Hong-Kong-Island-Mid-Levels-West-Euston-Court-I20240802455
                        # Extract the location part between 'transaction/' and the last identifier
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
                    except:
                        return None
                
                processed_df['address'] = processed_df['url_desc_trans'].apply(extract_address_from_url)
                logger.info(f"   ✅ Extracted address from {processed_df['address'].notna().sum()} URLs")
            except Exception as e:
                logger.warning(f"   ⚠️ Error extracting address from URLs: {e}")
        
        # ============ DATASOURCE COLUMN ============
        logger.info("📊 Adding Datasource column...")
        processed_df['Datasource'] = 'Midland'
        
        # ============ FILL NONE VALUES ============
        logger.info("🔄 Filling empty cells with 'None'...")
        
        def fill_none_values(df):
            """Fill empty cells and standardize None values"""
            for col in df.columns:
                try:
                    # Handle numeric columns differently to avoid type conflicts
                    if col in ['price', 'last_price', 'area', 'net_area', 'unit_price_net', 'age', 'total_unit_count', 'total_block_count', 'primary_school_net', 'market_stat_yearly_total_tx_amount', 'market_stat_yearly_net_ft_price', 'market_stat_yearly_net_ft_price_chg']:
                        # For numeric columns, convert to numeric first, then fill NaN with 0.0 to keep numeric type
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        df[col] = df[col].fillna(0.0)
                    else:
                        # For non-numeric columns, replace empty strings, 'none', '--' with 'None'
                        df[col] = df[col].replace(['', ' ', 'none', 'None', '--', 'NULL', 'null', 'N/A'], 'None')
                        # Fill actual NaN values with 'None'
                        df[col] = df[col].fillna('None')
                except Exception as e:
                    logger.warning(f"Error processing column {col}: {e}")
                    # If there's an error, just fill with 'None' as string
                    df[col] = df[col].fillna('None')
            return df
        
        processed_df = fill_none_values(processed_df)
        logger.info("   ✅ Filled empty values with 'None'")
        
        logger.info(f"Final processed dataframe shape: {processed_df.shape}")
        return processed_df
        
    except Exception as e:
        logger.error(f"Error in process_final_data_cleaning: {e}")
        # Return processed dataframe if processing fails
        return processed_df


    
############################## 3. Midland Res End ##############################

############################## 4. Midland ICI Start ##############################
# Updated enrich_estate_data function
def cleanse_midland_ici(
    midland_ici_base: pd.DataFrame,
) -> pd.DataFrame:
    """
    Process Midland ICI data with cleaning, transformations, and feature engineering.
    
    Args:
        midland_ici_base (pd.DataFrame): Raw joined data from midland_ici_base
        
    Returns:
        pd.DataFrame: Processed and cleaned data
    """
    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta
    import logging
    
    logger = logging.getLogger(__name__)
    df = midland_ici_base.copy()
    
    try:
        logger.info(f"Starting data processing with {len(df)} records")
        
        # 1. Error handling - handle empty values and errors
        logger.info("Step 1: Handling empty values and errors")
        try:
            # Handle empty strings and convert to NaN for consistency
            df = df.replace(['', ' ', 'N/A', 'NULL', 'null'], np.nan)
            
            # Handle any potential data type errors
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str).replace('nan', np.nan)
            
            # NEW: Fill specific area columns with 'None' for null values and convert to string
            area_columns = [
                'area1', 'area_desc1', 'area2', 'area_desc2', 
                'area3', 'area_desc3', 'area4', 'area_desc4'
            ]

            for col in area_columns:
                if col in df.columns:
                    # Convert entire column to string first to handle mixed types
                    df[col] = df[col].astype(str)
                    # Replace 'nan' string with 'None'
                    df[col] = df[col].replace('nan', 'None')
                    # Also fill any remaining null values
                    df[col] = df[col].fillna('None')
                    #logger.info(f"Converted to string and filled null values with 'None' for column: {col}")

            logger.info("Area columns processed successfully")
            
        except Exception as e:
            logger.error(f"Error in step 1 - Error handling: {str(e)}")
            # Continue processing even if some error handling fails

        # 2. Process ics_type column (i=Industrial, c=Commercial, s=Retail)
        logger.info("Step 2: Processing ics_type column")
        try:
            if 'ics_type' in df.columns:
                # Map full words to proper format
                ics_type_mapping = {
                    'industrial': 'Industrial',
                    'commercial': 'Office', 
                    'retail': 'Retail',
                    # Also handle single letters if they exist
                    'i': 'Industrial',
                    'c': 'Office', 
                    's': 'Retail'
                }
                df['ics_type'] = df['ics_type'].str.lower().map(ics_type_mapping)
                df['ics_type'] = df['ics_type'].fillna('Unknown')
                logger.info("ics_type column processed successfully")
        except Exception as e:
            logger.error(f"Error processing ics_type column: {str(e)}")
        
        # 3. Placeholder for column rename and reorder
        logger.info("Step 3: Column rename and reorder (placeholder)")
        # TODO: Manually specify column renaming here
        column_rename_mapping = {}  # Add renaming rules here
        # Example: column_rename_mapping = {'old_name': 'new_name', 'temp_id': 'final_id'}
        
        if column_rename_mapping:
            df = df.rename(columns=column_rename_mapping)
            logger.info(f"Renamed columns: {column_rename_mapping}")
        
        # 4. Clean floor column - remove ** from values like **LOW**
        logger.info("Step 4: Cleaning floor column")
        try:
            if 'floor' in df.columns:
                df['floor'] = df['floor'].astype(str).str.replace('*', '', regex=False)
                df['floor'] = df['floor'].str.strip()
                logger.info("Floor column cleaned successfully")
        except Exception as e:
            logger.error(f"Error cleaning floor column: {str(e)}")
        
        # 5. ENHANCED: Fix flat column Excel date conversion issue AND add apostrophe prefix
        logger.info("Step 5: Fixing flat and streetno columns Excel conversion issues and adding apostrophe prefix")
        try:
            # Process both flat and streetno columns with the same logic
            columns_to_protect = ['flat', 'streetno']
            
            for col in columns_to_protect:
                if col in df.columns:
                    # Convert to string first
                    df[col] = df[col].astype(str)
                    
                    # Fix common Excel auto-conversions
                    # Pattern: 8-Oct, 9-Nov, etc. should become 8-10, 9-11, etc.
                    month_mapping = {
                        'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
                        'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
                        'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
                    }
                    
                    for month_name, month_num in month_mapping.items():
                        df[col] = df[col].str.replace(f'-{month_name}', f'-{month_num}', regex=False)
                    
                    # Add apostrophe prefix to prevent Excel auto-conversion
                    # Only add apostrophe if the value is not null/nan and not already prefixed
                    mask = (df[col].notna()) & (df[col] != 'nan') & (~df[col].str.startswith("'"))
                    df.loc[mask, col] = "'" + df.loc[mask, col].astype(str)
                    
                    logger.info(f"{col} column Excel conversion issues fixed and apostrophe prefix added")
            
        except Exception as e:
            logger.error(f"Error fixing flat and streetno columns: {str(e)}")
        
        # 6. Process tx_type and merge price columns
        logger.info("Step 6: Processing tx_type and merging price columns")
        try:
            # Process tx_type column (L=RENT, S=SALE)
            if 'tx_type' in df.columns:
                tx_type_mapping = {'L': 'RENT', 'S': 'SALE'}
                df['tx_type'] = df['tx_type'].map(tx_type_mapping).fillna(df['tx_type'])
                logger.info("tx_type column processed successfully")
            
            # ENHANCED: Merge sell and rent columns into price column
            price_cols = ['sell', 'rent']
            if all(col in df.columns for col in price_cols):
                # Convert columns to numeric, replacing non-numeric values with 0
                df['sell'] = pd.to_numeric(df['sell'], errors='coerce').fillna(0)
                df['rent'] = pd.to_numeric(df['rent'], errors='coerce').fillna(0)
                
                # Create price column using non-zero values
                df['price'] = np.where(
                    df['sell'] != 0, df['sell'], 
                    np.where(df['rent'] != 0, df['rent'], 0)
                )
                #logger.info("Price columns merged successfully into 'price' column")
            else:
                logger.warning(f"Not all required price columns found. Available: {[col for col in price_cols if col in df.columns]}")
            
            # ENHANCED: Merge ft_sell and ft_rent columns into price_per_feet column  
            ft_price_cols = ['ft_sell', 'ft_rent']
            if all(col in df.columns for col in ft_price_cols):
                # Convert columns to numeric, replacing non-numeric values with 0
                df['ft_sell'] = pd.to_numeric(df['ft_sell'], errors='coerce').fillna(0)
                df['ft_rent'] = pd.to_numeric(df['ft_rent'], errors='coerce').fillna(0)
                
                # Create price_per_feet column using non-zero values
                df['price_per_feet'] = np.where(
                    df['ft_sell'] != 0, df['ft_sell'],
                    np.where(df['ft_rent'] != 0, df['ft_rent'], 0)
                )
                #logger.info("Price per feet columns merged successfully into 'price_per_feet' column")
            else:
                logger.warning(f"Not all required ft_price columns found. Available: {[col for col in ft_price_cols if col in df.columns]}")
                
        except Exception as e:
            logger.error(f"Error processing tx_type and price columns: {str(e)}")
        
        # 7. Add age column based on Completion date
        logger.info("Step 7: Adding age column based on Completion date")
        try:
            current_year = datetime.now().year
            
            if 'Completion Date' in df.columns:
                df['completion_year'] = pd.to_datetime(df['Completion Date'], errors='coerce').dt.year
                df['age'] = current_year - df['completion_year']
                # Ensure age column has correct data type
                df['age'] = pd.to_numeric(df['age'], errors='coerce').astype('float64')
                logger.info(f"Age column added successfully using current year {current_year}")
            elif 'Completion' in df.columns:
                df['completion_year'] = pd.to_numeric(df['Completion'], errors='coerce')
                df['age'] = current_year - df['completion_year']
                # Ensure age column has correct data type
                df['age'] = pd.to_numeric(df['age'], errors='coerce').astype('float64')
                logger.info(f"Age column added successfully using current year {current_year}")

        except Exception as e:
            logger.error(f"Error adding age column: {str(e)}")
        
        # 8. Process date columns - convert format and add 1 day
        logger.info("Step 8: Processing date columns format conversion")
        try:
            # Define the 5 date columns to process
            date_columns = [
                'market_stat_monthly_0_date', 'first_op_date', 'update_date', 
                'tx_date', 'building_first_op_date'
            ]
            
            for col in date_columns:
                if col in df.columns:
                    # Convert from ISO format (2008-08-27T16:00:00.000Z) to dd/mm/yyyy and add 1 day
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    df[col] = df[col] + timedelta(days=1)  # Add 1 day
                    df[col] = df[col].dt.strftime('%Y-%m-%d')  # Format as yyyy-mm-dd
                    logger.info(f"Processed date column: {col}")
            
            logger.info("Date columns processed successfully")
        except Exception as e:
            logger.error(f"Error processing date columns: {str(e)}")
            
        # 9. Drop specified unwanted columns
        logger.info("Step 9: Dropping specified unwanted columns")
        columns_to_drop = [
            'dist_id', 'dist_code', 'building_id', 'pis_bldg_id', 
            'floor_zh', 'floor_en', 'rownum', 'id', 'Passenger Lift',
            'Cargo Lift', 'Car Park', 'Loading Area', 'Cargo Raised Floor',
            'Air Conditioning', 'Air Conditioning Opening Times',
            'Transport', 'has_building_match', "sell", "ft_sell", "rent", "ft_rent",
            'building_name_zh', 'building_name_en', 'Building Name', 'Type', 'Title',
            'Cargo', 'Ceiling Height(Approx.)', 'Raised Floor', 'completion_year'
        ]
        
        existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
        if existing_columns_to_drop:
            df = df.drop(columns=existing_columns_to_drop)
            logger.info(f"Dropped {len(existing_columns_to_drop)} columns")
        else:
            logger.info("No specified columns found to drop")
        
        # Final placeholder for column reordering
        logger.info("Final step: Column reordering (placeholder)")
        # TODO: Specify desired column order here
        desired_column_order = []  # Add column names in desired order
        # Example: desired_column_order = ['id', 'property_name', 'district', 'price', 'price_per_feet', 'age']
        
        if desired_column_order:
            # Reorder columns, keeping any extra columns at the end
            existing_desired_cols = [col for col in desired_column_order if col in df.columns]
            other_cols = [col for col in df.columns if col not in desired_column_order]
            df = df[existing_desired_cols + other_cols]
            logger.info("Columns reordered successfully")
        
        # ============ ADDRESS EXTRACTION FROM URLS ============
        logger.info("📍 Extracting address information from URLs...")
        
        # Extract address information from url_desc_trans for Midland Residential
        if 'url_desc_trans' in df.columns:
            try:
                def extract_address_from_url(url):
                    """Extract address information from Midland transaction URL"""
                    if pd.isna(url) or not url:
                        return None
                    
                    try:
                        # URL format: https://www.midland.com.hk/en/transaction/Hong-Kong-Island-Mid-Levels-West-Euston-Court-I20240802455
                        # Extract the location part between 'transaction/' and the last identifier
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
                    except:
                        return None
                
                df['address'] = df['url_desc_trans'].apply(extract_address_from_url)
                logger.info(f"   ✅ Extracted address from {df['address'].notna().sum()} URLs")
            except Exception as e:
                logger.warning(f"   ⚠️ Error extracting address from URLs: {e}")
        
        # ============ DATASOURCE COLUMN ============
        logger.info("📊 Adding Datasource column...")
        df['Datasource'] = 'Midland'
        
        # ============ FILL NONE VALUES ============
        logger.info("🔄 Filling empty cells with 'None'...")
        
        def fill_none_values(df):
            """Fill empty cells and standardize None values"""
            for col in df.columns:
                # Skip numeric columns (age, price, etc.) - keep them as numeric
                if col in ['age', 'price', 'unit_price_net', 'avgPrice']:
                    df[col] = df[col].fillna(0.0)
                else:
                    # Replace empty strings, 'none', '--' with 'None'
                    df[col] = df[col].replace(['', ' ', 'none', 'None', '--', 'NULL', 'null', 'N/A'], 'None')
                    # Fill actual NaN values with 'None'
                    df[col] = df[col].fillna('None')
            return df
        
        df = fill_none_values(df)
        logger.info("   ✅ Filled empty values with 'None'")
        
        logger.info(f"Data processing completed successfully. Final dataset has {len(df)} records and {len(df.columns)} columns")
        return df
        
    except Exception as e:
        logger.error(f"Critical error in data processing: {str(e)}")
        # Return original dataframe if processing fails completely
        return midland_ici_base



############################## 4. Midland ICI End ##############################

############################## 5. Final Merging ##############################
import re
import pandas as pd
from typing import Dict, Any

# Import the illegal characters pattern from openpyxl
try:
    from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
except ImportError:
    # Fallback if openpyxl is not available
    ILLEGAL_CHARACTERS_RE = re.compile(r'[\000-\010]|[\013-\014]|[\016-\037]')

def sanitize_worksheet_name(name: str) -> str:
    """
    Sanitize worksheet name to comply with Excel restrictions.
    """
    # Remove or replace invalid characters
    name = re.sub(r'[/\\?*:\[\]]', '_', name)
    
    # Remove leading/trailing apostrophes
    name = name.strip("'")
    
    # Ensure name is not empty
    if not name:
        name = "Sheet"
    
    # Truncate to 31 characters
    if len(name) > 31:
        name = name[:31]
    
    # Avoid reserved names
    if name.lower() == 'history':
        name = name + '_data'
    
    return name

def sanitize_dataframe_content(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove illegal characters from all string cells in the DataFrame and handle timezone issues.
    """
    df_copy = df.copy()
    
    # Remove timezone info from all datetime columns to avoid Excel compatibility issues
    for col in df_copy.columns:
        if df_copy[col].dtype == 'datetime64[ns, UTC]' or df_copy[col].dtype == 'datetime64[ns, Asia/Hong_Kong]':
            df_copy[col] = df_copy[col].dt.tz_localize(None)
        elif df_copy[col].dtype == 'object':
            # Check if object column contains datetime values
            try:
                # Try to convert to datetime with specific format to avoid warnings
                # First try dd/mm/yyyy format, then fallback to pandas inference
                temp_dt = pd.to_datetime(df_copy[col], format='%d/%m/%Y', errors='coerce')
                # If that fails, try other common formats
                if temp_dt.isna().all():
                    temp_dt = pd.to_datetime(df_copy[col], format='%Y-%m-%d', errors='coerce')
                # If still fails, use pandas inference but suppress warnings
                if temp_dt.isna().all():
                    import warnings
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        temp_dt = pd.to_datetime(df_copy[col], errors='coerce')
                
                if temp_dt.dt.tz is not None:
                    df_copy[col] = temp_dt.dt.tz_localize(None)
            except:
                pass
    
    def clean_cell(cell_value):
        if isinstance(cell_value, str):
            # Remove illegal characters using openpyxl's pattern
            return ILLEGAL_CHARACTERS_RE.sub('', cell_value)
        return cell_value
    
    # Apply sanitization to all cells
    return df_copy.map(clean_cell)

def merge_and_excel(
    cr: pd.DataFrame,
    co: pd.DataFrame, 
    mr: pd.DataFrame,
    mi: pd.DataFrame
) -> Dict[str, Dict[str, pd.DataFrame]]:
    import logging
    logger = logging.getLogger(__name__)
    
    """
    Process four input DataFrames, standardize date columns, and split into 
    four Excel files:
    - RE_residential_2020-2023
    - RE_commercial_2020-2023
    - RE_residential_2024-{current_year}
    - RE_commercial_2024-{current_year}
    
    Args:
        cr: Centaline Residential DataFrame
        co: Centaline OIR (commercial) DataFrame
        mr: Midland Residential DataFrame
        mi: Midland ICI (commercial) DataFrame
        
    Returns:
        Dictionary with four keys for the Excel outputs
    """
    
    current_year = datetime.now().year
    
    # Standardize date columns
    def standardize_dataframe(df, date_col='date'):
        df_copy = df.copy()
        df_copy['standard_date'] = pd.to_datetime(df_copy[date_col], format='%Y-%m-%d', errors='coerce')
        
        if df_copy['standard_date'].dt.tz is not None:
            df_copy['standard_date'] = df_copy['standard_date'].dt.tz_localize(None)
        
        df_copy['date'] = df_copy['standard_date']
        return df_copy
    
    # Process each dataframe
    df_cr_processed = standardize_dataframe(cr)
    df_co_processed = standardize_dataframe(co)
    df_mr_processed = standardize_dataframe(mr)
    df_mi_processed = standardize_dataframe(mi)
    
    # Split by date ranges: 2020-2023 and 2024-current
    def split_by_date_range(df, name):
        valid_dates = df[df['standard_date'].notna()]
        if not valid_dates.empty:
            min_year = valid_dates['standard_date'].dt.year.min()
            max_year = valid_dates['standard_date'].dt.year.max()
            logger.info(f"{name}: Date range {min_year}-{max_year}, Total records: {len(df)}")
        
        df_2020_2023 = df[
            (df['standard_date'].dt.year >= 2020) & 
            (df['standard_date'].dt.year <= 2023)
        ].copy()
        
        df_2024_current = df[
            df['standard_date'].dt.year >= 2024
        ].copy()
        
        logger.info(f"{name}: 2020-2023: {len(df_2020_2023)} records, 2024-{current_year}: {len(df_2024_current)} records")
        
        return df_2020_2023, df_2024_current
    
    # Prepare dataframe for Excel
    def prepare_for_excel(df: pd.DataFrame) -> pd.DataFrame:
        df_out = df.copy()
        df_out['date'] = pd.to_datetime(df_out['standard_date'], errors='coerce')
        
        if df_out['date'].dt.tz is not None:
            df_out['date'] = df_out['date'].dt.tz_localize(None)
        
        # Drop standard_date column
        if 'standard_date' in df_out.columns:
            df_out = df_out.drop(columns=['standard_date'])
        
        # Sort descending by date
        if 'date' in df_out.columns:
            df_out = df_out.sort_values(by='date', ascending=False, kind='mergesort').reset_index(drop=True)
        return df_out
    
    # Split residential data (Centaline + Midland)
    cr_2020_2023, cr_2024_current = split_by_date_range(df_cr_processed, "Centaline_Residential")
    mr_2020_2023, mr_2024_current = split_by_date_range(df_mr_processed, "Midland_Residential")
    
    # Split commercial data (Centaline OIR + Midland ICI)
    co_2020_2023, co_2024_current = split_by_date_range(df_co_processed, "Centaline_OIR")
    mi_2020_2023, mi_2024_current = split_by_date_range(df_mi_processed, "Midland_ICI")
    
    # Combine residential and commercial separately
    residential_2020_2023 = {
        sanitize_worksheet_name('Centaline_Res'): sanitize_dataframe_content(prepare_for_excel(cr_2020_2023)) if not cr_2020_2023.empty else None,
        sanitize_worksheet_name('Midland_Res'): sanitize_dataframe_content(prepare_for_excel(mr_2020_2023)) if not mr_2020_2023.empty else None
    }
    
    residential_2024_current = {
        sanitize_worksheet_name('Centaline_Res'): sanitize_dataframe_content(prepare_for_excel(cr_2024_current)) if not cr_2024_current.empty else None,
        sanitize_worksheet_name('Midland_Res'): sanitize_dataframe_content(prepare_for_excel(mr_2024_current)) if not mr_2024_current.empty else None
    }
    
    commercial_2020_2023 = {
        sanitize_worksheet_name('Centaline_OIR'): sanitize_dataframe_content(prepare_for_excel(co_2020_2023)) if not co_2020_2023.empty else None,
        sanitize_worksheet_name('Midland_ICI'): sanitize_dataframe_content(prepare_for_excel(mi_2020_2023)) if not mi_2020_2023.empty else None
    }
    
    commercial_2024_current = {
        sanitize_worksheet_name('Centaline_OIR'): sanitize_dataframe_content(prepare_for_excel(co_2024_current)) if not co_2024_current.empty else None,
        sanitize_worksheet_name('Midland_ICI'): sanitize_dataframe_content(prepare_for_excel(mi_2024_current)) if not mi_2024_current.empty else None
    }
    
    # Remove None entries
    residential_2020_2023 = {k: v for k, v in residential_2020_2023.items() if v is not None}
    residential_2024_current = {k: v for k, v in residential_2024_current.items() if v is not None}
    commercial_2020_2023 = {k: v for k, v in commercial_2020_2023.items() if v is not None}
    commercial_2024_current = {k: v for k, v in commercial_2024_current.items() if v is not None}
    
    return {
        'residential_2020_2023': residential_2020_2023,
        'commercial_2020_2023': commercial_2020_2023,
        'residential_2024_current': residential_2024_current,
        'commercial_2024_current': commercial_2024_current
    }

############################## COLUMN SELECTION FUNCTIONS ##############################

def select_centaline_res_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Select and reorder columns for Centaline Residential dataset
    
    Args:
        df: Input dataframe with all columns
        
    Returns:
        pd.DataFrame: Dataframe with selected columns only
    """
    logger.info("📋 Selecting columns for Centaline Residential...")
    
    # Current available columns from centaline_res_base (after enrich_estate_data)
    # Only select columns that actually exist in the data
    selected_columns = [
         'date',                    # Transaction date
         'region',                  # Region
         'district',                # District
         'subdistrict',             # Subdistrict
         'Name',                    # Property name from transaction data (always populated)
         'title_lg',                # Property title/location from transaction data
         'Tower',                   # Building tower
         'Floor',                   # Floor number
         'Flat',                    # Flat/unit number
         'transaction_type',        # Type of transaction
         'area',                    # Net area (from JavaScript nArea)
         'price',                   # Transaction price (from JavaScript)
         'ft_price',                # Net price per sqft (from JavaScript nUnitPrice)
         'g_area',                  # Gross area (from JavaScript gArea)
         'g_unit_price',            # Gross price per sqft (from JavaScript gUnitPrice)
         'year',                    # Completion year
         'age',                     # Building age
         'developer',               # Developer
         'source',                  # Data source (not 'Datasource')
         'address',                 # Property address
         'rooms',                   # Number of rooms
         'changes',                 # Price changes
         'property_type',           # Property type (not 'Type')
         'code',                    # Area code
         'building_code',           # Building code from JavaScript
    ]
    
    # Use the selected columns defined above
    # selected_columns = df.columns.tolist()  # Uncomment this line if you want all columns

    # Filter dataframe to selected columns
    available_columns = [col for col in selected_columns if col in df.columns]
    result_df = df[available_columns].copy()  # Use .copy() to avoid SettingWithCopyWarning
    
    # No need to rename or fill - Name column from transaction data is already complete!

    logger.info(f"✅ Selected {len(available_columns)} columns for Centaline Residential")
    logger.info(f"📊 Final shape: {result_df.shape}")

    return result_df

def select_centaline_oir_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Select and reorder columns for Centaline Office/Industrial/Retail dataset
    
    Args:
        df: Input dataframe with all columns
        
    Returns:
        pd.DataFrame: Dataframe with selected columns only
    """
    logger.info("📋 Selecting columns for Centaline Office/Industrial/Retail...")
    
    # Log available columns for debugging
    logger.info(f"  Available columns in input: {len(df.columns)}")
    
    # Core columns to select (filter to only those that exist)
    desired_columns = [
        'transactionDate',  # Will be renamed to 'date'
        'zoneEn',
        'districtNameEn',
        'propertyUsageDisplayName',
        'grade',
        'propertyNameEn',
        'propertyNameCn',
        'floor',
        'unit',
        'transactionType',
        'transactionArea',
        'price',
        'avgPrice',
        'full_address',
        'completion_year',
        'age',
        'source_url',
        'management_company',
        'developers',
        'carpark',
        'sourceDisplayName',
        'Datasource',
        'id',
        # Optional columns from hybrid matching
        'matched_building_name',
        'match_score',
        '_match_method',
        '_match_score',
    ]
    
    # Filter to only columns that actually exist in the dataframe
    available_cols = df.columns.tolist()
    selected_columns = [col for col in desired_columns if col in available_cols]
    
    # Log which columns are included/missing
    missing_cols = [col for col in desired_columns if col not in available_cols]
    if missing_cols:
        logger.info(f"  ⚠ Columns not found (will be skipped): {missing_cols}")
    
    logger.info(f"  ✓ Selected {len(selected_columns)} columns (out of {len(desired_columns)} desired)")
    
    # Select available columns
    result_df = df[selected_columns].copy()

    # Rename transactionDate to date
    if 'transactionDate' in result_df.columns:
        result_df = result_df.rename(columns={'transactionDate': 'date'})

    logger.info(f"✅ Column selection completed")
    logger.info(f"📊 Final shape: {result_df.shape}")

    return result_df

def select_midland_res_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Select and reorder columns for Midland Residential dataset
    
    Args:
        df: Input dataframe with all columns
        
    Returns:
        pd.DataFrame: Dataframe with selected columns only
    """
    logger.info("📋 Selecting columns for Midland Residential...")
    
    # Current available columns (commented for reference)
    # Uncomment and reorder the columns you want to keep
    selected_columns = [
        'tx_date',  # date
        'region_name_trans',  # region_name_trans
        'subregion',  # subregion
        'district',  # district
        'estate',  # estate
        'building',  # building
        'news_name',  # news_name
        'floor',  # floor
        'floor_level',  # floor_level
        'flat',  # flat
        'tx_type',  # tx_type
        'price',  # price
        'net_area',  # net_area
        'unit_price_net',  # unit_price_net
        'building_first_op_date',  # building_first_op_date
        'age',  # age
        'developer_name',  # developer_name
        'source',  # source
        'address',  # address
        'url_desc_trans',  # url_desc_trans
        'location',  # location
        'last_tx_date',  # last_tx_date
        'last_price',  # last_price
        'mkt_type',  # mkt_type
        'area',  # area
        'total_unit_count',  # total_unit_count
        'total_block_count',  # total_block_count
        'primary_school_net',  # primary_school_net
        'market_stat_monthly_0_date'  # market_stat_monthly_0_date
    ]
    
    # Use the selected columns defined above
    # selected_columns = df.columns.tolist()  # Uncomment this line if you want all columns
    
    # Filter dataframe to selected columns that actually exist
    available_columns = [col for col in selected_columns if col in df.columns]
    missing_columns = [col for col in selected_columns if col not in df.columns]
    
    if missing_columns:
        logger.warning(f"⚠️ Missing columns: {missing_columns}")
    
    result_df = df[available_columns]

    # Rename tx_date to date
    if 'tx_date' in result_df.columns:
        result_df = result_df.rename(columns={'tx_date': 'date'})

    logger.info(f"✅ Selected {len(available_columns)} columns for Midland Residential")
    logger.info(f"📊 Final shape: {result_df.shape}")

    return result_df

def select_midland_ici_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Select and reorder columns for Midland Office/Industrial/Retail dataset
    
    Args:
        df: Input dataframe with all columns
        
    Returns:
        pd.DataFrame: Dataframe with selected columns only
    """
    logger.info("📋 Selecting columns for Midland Office/Industrial/Retail...")
    
    # Current available columns (commented for reference)
    # Uncomment and reorder the columns you want to keep
    selected_columns = [
        'tx_date',  # date
        'dist_name_en',  # dist_name_en
        'ics_type',  # ics_type
        'Grade',  # Grade
        'eng_name',  # eng_name
        'chi_name',  # chi_name
        'floor',  # floor
        'flat',  # flat
        'tx_type',  # tx_type
        'area',  # area
        'price',  # price
        'price_per_feet',  # price_per_feet
        'street_name_zh',  # street_name_zh
        'street_name_en',  # street_name_en
        'streetno',  # streetno
        'Completion',  # Completion
        'age',  # age
        'area1',  # area1
        'area_desc1',  # area_desc1
        'area2',  # area2
        'area_desc2',  # area_desc2
        'area3',  # area3
        'area_desc3',  # area_desc3
        'area4',  # area_desc4
        'area_desc4',  # area_desc4
        'URL',  # URL
        'upload_source',  # upload_source
        'No. of Floors',  # No. of Floors
        'Management Fee (Approx. per sq. ft.)',  # Management Fee (Approx. per sq. ft.)
        'Floor Remark',  # Floor Remark
        'Management Company',  # Management Company
        'Datasource'  # Datasource
    ]
    
    # Use the selected columns defined above
    # selected_columns = df.columns.tolist()  # Uncomment this line if you want all columns
    
    # Ensure any missing columns exist to keep schema stable
    missing_columns = [col for col in selected_columns if col not in df.columns]
    if missing_columns:
        for col in missing_columns:
            df[col] = 'None'
        logger.warning(f"Missing columns filled with 'None': {missing_columns}")

    # Filter dataframe to selected columns
    result_df = df[selected_columns]

    # Rename tx_date to date
    if 'tx_date' in result_df.columns:
        result_df = result_df.rename(columns={'tx_date': 'date'})

    logger.info(f"✅ Selected {len(selected_columns)} columns for Midland ICI")
    logger.info(f"📊 Final shape: {result_df.shape}")

    return result_df
