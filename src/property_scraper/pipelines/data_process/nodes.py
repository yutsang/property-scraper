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
    
    if 'date' in df.columns:
        df['date'] = df['date'].apply(clean_date)
        logger.info(f"   ✅ Converted {df['date'].notna().sum()} dates successfully")
    
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
    
    # ============ PRECISE AGE CALCULATION WITH MONTH CONSIDERATION ============
    logger.info("🏗️ Step 5/6: Calculating precise building age from Occupation Permit with month precision...")
    
    def calculate_precise_age(occupation_permit):
        """Calculate precise building age from occupation permit date (format: 1985/9 or 2004/11) with month consideration"""
        try:
            if pd.isna(occupation_permit) or occupation_permit == '' or occupation_permit == '--':
                return None
                
            permit_str = str(occupation_permit).strip()
            if not permit_str or permit_str == '--':
                return None
            
            # Extract year and month from format like "1985/9" or "2004/11"
            import re
            match = re.match(r'(\d{4})/(\d{1,2})', permit_str)
            if match:
                permit_year = int(match.group(1))
                permit_month = int(match.group(2))
                
                # Current date (June 2025)
                current_year = 2025
                current_month = 6
                
                # Calculate age in years with month precision
                age_years = current_year - permit_year
                age_months = current_month - permit_month
                
                # If current month is before permit month, subtract one year
                if age_months < 0:
                    age_years -= 1
                
                # Ensure age is not negative
                return max(0, age_years)
            
            # Fallback: try to extract just the year
            year_match = re.match(r'(\d{4})', permit_str)
            if year_match:
                permit_year = int(year_match.group(1))
                current_year = 2025
                age = current_year - permit_year
                return max(0, age)
            
            return None
            
        except Exception as e:
            logger.debug(f"Error calculating precise age from '{occupation_permit}': {str(e)}")
            return None
    
    # Find the occupation permit column (could be named differently)
    occupation_permit_columns = [col for col in df.columns if 'occupation' in col.lower() and 'permit' in col.lower()]
    if occupation_permit_columns:
        permit_col = occupation_permit_columns[0]  # Use first match
        df['age'] = df[permit_col].apply(calculate_precise_age)
        logger.info(f"   ✅ Calculated precise age for {df['age'].notna().sum()} properties from {permit_col}")
        
        # Show some examples of the calculation
        #sample_data = df[[permit_col, 'age']].dropna().head(5)
        #if not sample_data.empty:
        #    logger.info("   📊 Sample age calculations:")
        #    for _, row in sample_data.iterrows():
        #        logger.info(f"      {row[permit_col]} → {row['age']} years old")
    else:
        # Try alternative column names
        alternative_cols = ['Occupation Permit', 'occupation_permit', 'permit_date']
        for col in alternative_cols:
            if col in df.columns:
                df['age'] = df[col].apply(calculate_precise_age)
                logger.info(f"   ✅ Calculated precise age for {df['age'].notna().sum()} properties from {col}")
                break
        else:
            logger.warning("   ⚠️ No occupation permit column found - age column not created")
    
    # ============ ADDRESS PARSING ============
    logger.info("🏢 Step 6/9: Parsing Tower/Block, Floor, and Flat from address...")
    
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
        address_components = df['address'].apply(parse_address_components).apply(pd.Series)
        df = pd.concat([df, address_components], axis=1)
        logger.info(f"   ✅ Parsed address components for {len(df)} records")
    
    # ============ TYPE CLASSIFICATION ============
    logger.info("🚗 Step 7/9: Creating Type column and parsing carpark details...")
    
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
            # Extract floor information for carparks
            floor_patterns = [
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
    
    if 'address' in df.columns:
        df['Type'] = df['address'].apply(classify_property_type)
        carpark_details = df['address'].apply(extract_carpark_details).apply(pd.Series)
        df = pd.concat([df, carpark_details], axis=1)
        logger.info(f"   ✅ Classified {(df['Type'] == 'Carpark').sum()} carpark records")
    
    # ============ DATASOURCE COLUMN ============
    logger.info("📊 Step 8/9: Adding Datasource column...")
    df['Datasource'] = 'Centaline'
    
    # ============ COLUMN REMOVAL ============
    logger.info("🗑️ Step 9/9: Removing unwanted columns...")
    
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
        'developer',
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
    logger.info("🔄 Final step: Filling empty cells with 'None'...")
    
    def fill_none_values(df):
        """Fill empty cells and standardize None values - simplified to avoid Parquet type issues"""
        
        for col in df.columns:
            # Convert everything to string to avoid complex type conflicts
            df[col] = df[col].astype(str)
            df[col] = df[col].replace(['', ' ', 'none', 'None', 'nan', '--', 'NULL', 'null', 'N/A'], 'None')
            # Fill actual NaN values with 'None'
            df[col] = df[col].fillna('None')
        return df
    
    df = fill_none_values(df)
    logger.info("   ✅ Filled empty values with 'None'")
    
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
        # 2. Convert transactionDate to dd/mm/yyyy format
        def convert_transaction_date(date_str):
            """Convert ISO date format to dd/mm/yyyy"""
            try:
                if pd.isna(date_str) or date_str == '':
                    return ''
                
                # Parse the ISO format date
                dt = pd.to_datetime(date_str)
                return dt.strftime('%d/%m/%Y')
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
        
        # 1. Process date columns - convert from ISO format to date only and add 1 day
        date_columns = [
            'building_first_op_date', 'tx_date', 'last_tx_date', 
            'update_date', 'first_op_date', 'market_stat_monthly_0_date'
        ]
        
        for col in date_columns:
            if col in processed_df.columns:
                try:
                    # Convert to datetime, add 1 day, then format as string to avoid Parquet issues
                    processed_df[col] = pd.to_datetime(processed_df[col], errors='coerce')
                    processed_df[col] = processed_df[col] + pd.Timedelta(days=1)
                    processed_df[col] = processed_df[col].dt.strftime('%Y-%m-%d')
                    # Replace NaT with 'None'
                    processed_df[col] = processed_df[col].replace('NaT', 'None')
                except (ValueError, TypeError) as e:
                    logger.warning(f"Error processing date column {col}: {e}")
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
                current_date = pd.Timestamp.now().date()
                processed_df['age'] = None
                
                # Calculate age only for non-null date values (excluding 'None' strings)
                mask = (processed_df[date_col_for_age].notna()) & (processed_df[date_col_for_age] != 'None')
                if mask.any():
                    # Convert string dates back to datetime for age calculation
                    op_dates = pd.to_datetime(processed_df.loc[mask, date_col_for_age], errors='coerce')
                    ages = (pd.Timestamp.now() - op_dates).dt.days / 365.25
                    processed_df.loc[mask, 'age'] = ages.round(1)
                    logger.info(f"Age calculated for {mask.sum()} records using {date_col_for_age}")
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
        
        # ============ DATASOURCE COLUMN ============
        logger.info("📊 Adding Datasource column...")
        processed_df['Datasource'] = 'Midland'
        
        # ============ FILL NONE VALUES ============
        logger.info("🔄 Filling empty cells with 'None'...")
        
        def fill_none_values(df):
            """Fill empty cells and standardize None values"""
            for col in df.columns:
                # Replace empty strings, 'none', '--' with 'None'
                df[col] = df[col].replace(['', ' ', 'none', 'None', '--', 'NULL', 'null', 'N/A'], 'None')
                # Fill actual NaN values with 'None'
                df[col] = df[col].fillna('None')
            return df
        
        processed_df = fill_none_values(processed_df)
        logger.info("   ✅ Filled empty values with 'None'")
        
        logger.info(f"Final processed dataframe shape: {processed_df.shape}")
        return processed_df
        
    except Exception as e:
        logger.error(f"Error in process_final_data_cleaning: {e}")
        # Return original dataframe if processing fails
        return df


    
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
                ics_type_mapping = {
                    'i': 'Industrial',
                    'c': 'Commercial', 
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
                # Convert entire column to string to ensure consistent data type
                df['age'] = df['age'].astype(str).replace('nan', 'None')
                logger.info(f"Age column added successfully using current year {current_year}")
            elif 'Completion' in df.columns:
                df['completion_year'] = pd.to_numeric(df['Completion'], errors='coerce')
                df['age'] = current_year - df['completion_year']
                # Convert entire column to string to ensure consistent data type
                df['age'] = df['age'].astype(str).replace('nan', 'None')
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
                    df[col] = df[col].dt.strftime('%d/%m/%Y')  # Format as dd/mm/yyyy
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
        
        # ============ DATASOURCE COLUMN ============
        logger.info("📊 Adding Datasource column...")
        df['Datasource'] = 'Midland'
        
        # ============ FILL NONE VALUES ============
        logger.info("🔄 Filling empty cells with 'None'...")
        
        def fill_none_values(df):
            """Fill empty cells and standardize None values"""
            for col in df.columns:
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
    Remove illegal characters from all string cells in the DataFrame.
    """
    def clean_cell(cell_value):
        if isinstance(cell_value, str):
            # Remove illegal characters using openpyxl's pattern
            return ILLEGAL_CHARACTERS_RE.sub('', cell_value)
        return cell_value
    
    # Apply sanitization to all cells
    return df.map(clean_cell)

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
    two Excel files based on year ranges with four tabs each.
    
    Args:
        input_df1-4: Input DataFrames from parquet files
        
    Returns:
        Dictionary with two keys for the Excel outputs, each containing
        a dictionary of tab names to DataFrames
    """
    
    # Define date column mapping for each input
    date_column_mapping = {
        'df_cr': 'date',     
        'df_co': 'transactionDate',          
        'df_mr': 'tx_date',         
        'df_mi': 'tx_date'           
    }
    
    # Define tab names with sanitization
    tab_names = {
        'df_cr': sanitize_worksheet_name('Centaline_Residential'),
        'df_co': sanitize_worksheet_name('Centaline_OIR'), 
        'df_mr': sanitize_worksheet_name('Midland_Residential'),
        'df_mi': sanitize_worksheet_name('Midland_ICI')
    }
    
    # Standardize date columns and add source identifier
    def standardize_dataframe(df, source_key):
        df_copy = df.copy()
        date_col = date_column_mapping[source_key]
        
        # Handle string dates with error handling for invalid values
        df_copy['standard_date'] = pd.to_datetime(df_copy[date_col], dayfirst=True, errors='coerce')
        #df_copy['source'] = source_key
        
        return df_copy
    
    # Process each dataframe
    df_cr_processed = standardize_dataframe(cr, 'df_cr')
    df_co_processed = standardize_dataframe(co, 'df_co')
    df_mr_processed = standardize_dataframe(mr, 'df_mr')
    df_mi_processed = standardize_dataframe(mi, 'df_mi')
    
    # Split each dataframe by date ranges
    def split_by_date_range(df, tab_name):
        # Debug date ranges
        valid_dates = df[df['standard_date'].notna()]
        if not valid_dates.empty:
            min_year = valid_dates['standard_date'].dt.year.min()
            max_year = valid_dates['standard_date'].dt.year.max()
            logger.info(f"{tab_name}: Date range {min_year}-{max_year}, Total records: {len(df)}")
            
            # Show year distribution
            year_counts = valid_dates['standard_date'].dt.year.value_counts().sort_index()
            logger.info(f"{tab_name}: Year distribution: {year_counts.head(10).to_dict()}")
        else:
            logger.warning(f"{tab_name}: No valid dates found!")
        
        df_2020_2022 = df[
            (df['standard_date'].dt.year >= 2020) & 
            (df['standard_date'].dt.year <= 2022)
        ].copy()
        
        df_2023_current = df[
            df['standard_date'].dt.year >= 2023
        ].copy()
        
        logger.info(f"{tab_name}: 2020-2022: {len(df_2020_2022)} records, 2023+: {len(df_2023_current)} records")
        
        return df_2020_2022, df_2023_current
    
    # Create dictionaries for Excel outputs
    excel_2020_2022 = {}
    excel_2023_current = {}
    
    # Process each dataframe and assign to appropriate tabs
    for df_processed, source_key in [
        (df_cr_processed, 'df_cr'),
        (df_co_processed, 'df_co'), 
        (df_mr_processed, 'df_mr'),
        (df_mi_processed, 'df_mi')
    ]:
        tab_name = tab_names[source_key]
        df_early, df_recent = split_by_date_range(df_processed, tab_name)
        
        # Excel row limit is ~1,048,576. Sample large datasets to fit within limits
        max_rows_per_sheet = 1000000  # Leave some buffer
        
        # Handle early period data
        if len(df_early) > max_rows_per_sheet:
            df_early_sampled = df_early.sample(n=max_rows_per_sheet, random_state=42)
            logger.warning(f"Sampled {tab_name} 2020-2022 from {len(df_early)} to {max_rows_per_sheet} rows")
        else:
            df_early_sampled = df_early
            
        # Handle recent period data  
        if len(df_recent) > max_rows_per_sheet:
            df_recent_sampled = df_recent.sample(n=max_rows_per_sheet, random_state=42)
            logger.warning(f"Sampled {tab_name} 2023-current from {len(df_recent)} to {max_rows_per_sheet} rows")
        else:
            df_recent_sampled = df_recent
        
        # Only add sheets with data to avoid "no visible sheets" error
        if not df_early_sampled.empty:
            excel_2020_2022[tab_name] = sanitize_dataframe_content(df_early_sampled)
        if not df_recent_sampled.empty:
            excel_2023_current[tab_name] = sanitize_dataframe_content(df_recent_sampled)
    
    return {
        'excel_2020_2022': excel_2020_2022,
        'excel_2023_current': excel_2023_current
    }
