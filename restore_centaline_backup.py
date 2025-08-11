#!/usr/bin/env python3
"""
Special script to restore Centaline Residential data from backup Excel file
and convert it back to the raw transaction format for pipeline processing.
"""

import pandas as pd
import logging
from datetime import datetime
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def restore_centaline_residential_backup():
    """
    Restore Centaline Residential data from backup Excel file to raw transaction format
    """
    
    backup_file = "data/03_primary/Combined_Dateset_2020_2022 copy.xlsx"
    output_file = "data/01_raw/centaline_res_trans_lv_0.parquet"
    
    logger.info(f"🔄 Starting Centaline Residential backup restoration...")
    logger.info(f"📂 Source: {backup_file}")
    logger.info(f"🎯 Target: {output_file}")
    
    try:
        # First, let's examine the Excel file structure
        logger.info("📋 Reading Excel file structure...")
        excel_file = pd.ExcelFile(backup_file)
        sheet_names = excel_file.sheet_names
        logger.info(f"📊 Available sheets: {sheet_names}")
        
        # Look for Centaline Residential sheet
        centaline_res_sheet = None
        for sheet in sheet_names:
            if 'centaline' in sheet.lower() and 'residential' in sheet.lower():
                centaline_res_sheet = sheet
                break
            elif 'centaline_res' in sheet.lower():
                centaline_res_sheet = sheet
                break
        
        if not centaline_res_sheet:
            # Try common variations
            for sheet in sheet_names:
                if 'centaline' in sheet.lower() and 'res' in sheet.lower():
                    centaline_res_sheet = sheet
                    break
        
        if not centaline_res_sheet:
            logger.error(f"❌ Could not find Centaline Residential sheet in: {sheet_names}")
            logger.info("📋 Let's examine all sheets to find the right one...")
            
            # Read first few rows of each sheet to identify the right one
            for sheet in sheet_names:
                logger.info(f"\n--- Sheet: {sheet} ---")
                try:
                    df_sample = pd.read_excel(backup_file, sheet_name=sheet, nrows=3)
                    logger.info(f"Columns: {list(df_sample.columns)}")
                    logger.info(f"Shape: {df_sample.shape}")
                except Exception as e:
                    logger.warning(f"Error reading sheet {sheet}: {e}")
            
            # For now, assume first sheet might be Centaline Residential
            centaline_res_sheet = sheet_names[0]
            logger.info(f"🎯 Using first sheet as fallback: {centaline_res_sheet}")
        
        logger.info(f"📖 Reading Centaline Residential data from sheet: {centaline_res_sheet}")
        
        # Read the data
        df = pd.read_excel(backup_file, sheet_name=centaline_res_sheet)
        logger.info(f"📊 Loaded backup data: {len(df):,} records")
        logger.info(f"📋 Columns: {list(df.columns)}")
        
        # Show some sample data
        logger.info("📝 Sample data (first 3 rows):")
        for i, row in df.head(3).iterrows():
            logger.info(f"Row {i}: {dict(row)}")
        
        # Check for date columns and date ranges
        date_columns = [col for col in df.columns if 'date' in col.lower()]
        logger.info(f"📅 Date columns found: {date_columns}")
        
        for date_col in date_columns:
            try:
                # Convert to datetime and check range
                df_temp = df.copy()
                df_temp[date_col] = pd.to_datetime(df_temp[date_col], errors='coerce')
                valid_dates = df_temp[date_col].dropna()
                
                if len(valid_dates) > 0:
                    min_date = valid_dates.min()
                    max_date = valid_dates.max()
                    logger.info(f"📅 {date_col}: {min_date} to {max_date}")
                    
                    # Check year distribution
                    year_counts = valid_dates.dt.year.value_counts().sort_index()
                    logger.info(f"📊 Year distribution: {year_counts.head(10).to_dict()}")
                    
            except Exception as e:
                logger.warning(f"Error analyzing date column {date_col}: {e}")
        
        # Convert the processed data back to raw transaction format
        # This means we need to reverse some of the processing steps
        
        logger.info("🔄 Converting processed data back to raw transaction format...")
        
        # Create the raw transaction dataframe
        # The key is to extract the core transaction fields that the pipeline expects
        
        raw_columns_mapping = {
            # Map processed column names back to raw format
            'date': 'date',
            'address': 'address', 
            'price': 'price',
            'area': 'area',
            'ft_price': 'ft_price',
            'rooms': 'rooms',
            'transaction_type': 'transaction_type',
            'agency': 'agency'
        }
        
        # Start with the backup data
        raw_df = df.copy()
        
        # Keep only the columns that exist and are needed for raw format
        available_columns = []
        for processed_col, raw_col in raw_columns_mapping.items():
            if processed_col in raw_df.columns:
                available_columns.append(processed_col)
            elif raw_col in raw_df.columns:
                available_columns.append(raw_col)
        
        logger.info(f"📋 Available columns for raw format: {available_columns}")
        
        # Keep only available columns
        if available_columns:
            raw_df = raw_df[available_columns]
        
        # Ensure we have the minimum required columns
        required_cols = ['date', 'address', 'price']
        missing_cols = [col for col in required_cols if col not in raw_df.columns]
        
        if missing_cols:
            logger.warning(f"⚠️ Missing required columns: {missing_cols}")
            # Try alternative column names
            for missing_col in missing_cols:
                alternatives = {
                    'date': ['Date', 'transaction_date', 'tx_date'],
                    'address': ['Address', 'property_address', 'location'],
                    'price': ['Price', 'transaction_price', 'amount']
                }
                
                if missing_col in alternatives:
                    for alt in alternatives[missing_col]:
                        if alt in df.columns:
                            raw_df[missing_col] = df[alt]
                            logger.info(f"✅ Mapped {alt} -> {missing_col}")
                            break
        
        # Save the restored raw data
        logger.info(f"💾 Saving restored raw transaction data to: {output_file}")
        
        # Create directory if needed
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Save as parquet
        raw_df.to_parquet(output_file, index=False)
        
        logger.info(f"✅ Successfully restored {len(raw_df):,} Centaline Residential transactions!")
        logger.info(f"📁 File saved: {output_file}")
        logger.info(f"📊 File size: {os.path.getsize(output_file) / (1024*1024):.1f} MB")
        
        # Verify the restoration
        logger.info("🔍 Verifying restored data...")
        verify_df = pd.read_parquet(output_file)
        logger.info(f"✅ Verification: {len(verify_df):,} records loaded successfully")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error during backup restoration: {str(e)}")
        import traceback
        logger.error(f"📋 Full traceback: {traceback.format_exc()}")
        return False

if __name__ == "__main__":
    success = restore_centaline_residential_backup()
    if success:
        print("\n🎉 Backup restoration completed successfully!")
        print("🚀 You can now run the Centaline Residential pipeline to process this data.")
    else:
        print("\n❌ Backup restoration failed. Please check the logs above.")
