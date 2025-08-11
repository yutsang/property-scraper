#!/usr/bin/env python3
"""
Debug script to trace what happens to our restored Centaline Residential data
through the pipeline and identify where the 2022 records are getting lost.
"""

import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_data_flow():
    """
    Analyze the data flow through the Centaline Residential pipeline
    to identify where the 127,860 2022 records are getting lost.
    """
    
    logger.info("🔍 Analyzing Centaline Residential data flow...")
    
    # 1. Check raw restored data
    logger.info("\n=== 1. RAW RESTORED DATA ===")
    try:
        raw_df = pd.read_parquet('data/01_raw/centaline_res_trans_lv_0.parquet')
        logger.info(f"📊 Raw data: {len(raw_df):,} records")
        
        raw_df['date'] = pd.to_datetime(raw_df['date'], errors='coerce')
        year_counts = raw_df['date'].dt.year.value_counts().sort_index()
        logger.info(f"📅 Raw year distribution: {year_counts.head().to_dict()}")
        
        # Check sample addresses
        logger.info("🏠 Sample addresses:")
        for i, addr in enumerate(raw_df['address'].head(3)):
            logger.info(f"   {i+1}. {addr}")
            
    except Exception as e:
        logger.error(f"❌ Error reading raw data: {e}")
    
    # 2. Check base data (after enrichment)
    logger.info("\n=== 2. BASE DATA (AFTER ENRICHMENT) ===")
    try:
        base_df = pd.read_parquet('data/02_intermediate/centaline_res_base.parquet')
        logger.info(f"📊 Base data: {len(base_df):,} records")
        
        base_df['date'] = pd.to_datetime(base_df['date'], errors='coerce')
        year_counts = base_df['date'].dt.year.value_counts().sort_index()
        logger.info(f"📅 Base year distribution: {year_counts.head().to_dict()}")
        
        # Check if our restored data is there
        sample_addresses = ['Townplace Kennedy Town Upper Floor Flat B', 
                          "Ivy On Belcher's Middle Floor Flat C",
                          'Luen Wai Apartment 12/F Flat 12']
        
        logger.info("🔍 Checking if our restored sample addresses are present:")
        for addr in sample_addresses:
            matches = base_df[base_df['address'].str.contains(addr.split()[0], na=False, case=False)]
            logger.info(f"   '{addr[:30]}...': {len(matches)} matches")
            if len(matches) > 0:
                match_dates = pd.to_datetime(matches['date'].iloc[0:3], errors='coerce')
                logger.info(f"      Dates: {match_dates.dt.date.tolist()}")
        
    except Exception as e:
        logger.error(f"❌ Error reading base data: {e}")
    
    # 3. Check processed data (after cleansing)
    logger.info("\n=== 3. PROCESSED DATA (AFTER CLEANSING) ===")
    try:
        processed_df = pd.read_parquet('data/03_primary/centaline_res.parquet')
        logger.info(f"📊 Processed data: {len(processed_df):,} records")
        
        processed_df['date'] = pd.to_datetime(processed_df['date'], errors='coerce')
        year_counts = processed_df['date'].dt.year.value_counts().sort_index()
        logger.info(f"📅 Processed year distribution: {year_counts.head().to_dict()}")
        
        # Check if our restored data survived
        logger.info("🔍 Checking if our restored sample addresses survived processing:")
        for addr in sample_addresses:
            matches = processed_df[processed_df['address'].str.contains(addr.split()[0], na=False, case=False)]
            logger.info(f"   '{addr[:30]}...': {len(matches)} matches")
            if len(matches) > 0:
                match_dates = pd.to_datetime(matches['date'].iloc[0:3], errors='coerce')
                logger.info(f"      Dates: {match_dates.dt.date.tolist()}")
        
    except Exception as e:
        logger.error(f"❌ Error reading processed data: {e}")
    
    # 4. Check for data source mixing
    logger.info("\n=== 4. DATA SOURCE ANALYSIS ===")
    try:
        # Check if base data has source information
        if 'source' in base_df.columns:
            source_counts = base_df['source'].value_counts()
            logger.info(f"📊 Base data sources: {source_counts.to_dict()}")
        
        # Check processing timestamp to see if data is mixed
        if 'processing_timestamp' in base_df.columns:
            timestamps = base_df['processing_timestamp'].value_counts()
            logger.info(f"📊 Processing timestamps: {len(timestamps)} unique timestamps")
            logger.info(f"    Latest: {timestamps.index[0] if len(timestamps) > 0 else 'None'}")
            
    except Exception as e:
        logger.error(f"❌ Error analyzing data sources: {e}")
    
    # 5. Summary and recommendations
    logger.info("\n=== 5. SUMMARY ===")
    raw_2022 = len(raw_df[raw_df['date'].dt.year == 2022]) if 'raw_df' in locals() else 0
    base_2022 = len(base_df[base_df['date'].dt.year == 2022]) if 'base_df' in locals() else 0
    processed_2022 = len(processed_df[processed_df['date'].dt.year == 2022]) if 'processed_df' in locals() else 0
    
    logger.info(f"📊 2022 Records Journey:")
    logger.info(f"   Raw: {raw_2022:,} records")
    logger.info(f"   Base: {base_2022:,} records ({(base_2022/raw_2022*100):.1f}% of raw)" if raw_2022 > 0 else "   Base: 0 records")
    logger.info(f"   Processed: {processed_2022:,} records ({(processed_2022/raw_2022*100):.1f}% of raw)" if raw_2022 > 0 else "   Processed: 0 records")
    
    if raw_2022 > base_2022:
        logger.warning("⚠️ Data loss during enrichment! Old data is being mixed with new data.")
        logger.info("💡 Recommendation: Clear intermediate data and re-run pipeline fresh")
    elif base_2022 > processed_2022:
        logger.warning("⚠️ Data loss during cleansing! Check for duplicate removal or filtering.")
        logger.info("💡 Recommendation: Check cleansing logic for aggressive filtering")

if __name__ == "__main__":
    analyze_data_flow()
