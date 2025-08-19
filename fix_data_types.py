#!/usr/bin/env python3
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fix_data_types():
    """Fix year and age column data types across all datasets"""
    
    datasets = ['centaline_res', 'centaline_oir', 'midland_res', 'midland_ici']
    
    for dataset in datasets:
        logger.info(f"🔧 Fixing data types for {dataset}...")
        
        # Load the dataset
        df = pd.read_parquet(f'data/03_primary/{dataset}.parquet')
        
        # Find year and age columns
        year_cols = [col for col in df.columns if 'year' in col.lower() and col not in ['market_stat_yearly_total_tx_amount', 'market_stat_yearly_net_ft_price', 'market_stat_yearly_net_ft_price_chg']]
        age_cols = [col for col in df.columns if col == 'age']
        
        logger.info(f"  Year columns: {year_cols}")
        logger.info(f"  Age columns: {age_cols}")
        
        # Fix year columns (convert to int)
        for col in year_cols:
            if col in df.columns:
                try:
                    # Convert to numeric, then to int
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    df[col] = df[col].astype('Int64')  # Use Int64 to handle NaN values
                    logger.info(f"  ✅ Fixed {col}: {df[col].dtype}")
                except Exception as e:
                    logger.error(f"  ❌ Error fixing {col}: {e}")
        
        # Fix age columns (convert to float)
        for col in age_cols:
            if col in df.columns:
                try:
                    # Convert to numeric, then to float
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    df[col] = df[col].astype('float64')
                    logger.info(f"  ✅ Fixed {col}: {df[col].dtype}")
                except Exception as e:
                    logger.error(f"  ❌ Error fixing {col}: {e}")
        
        # Save the updated dataset
        df.to_parquet(f'data/03_primary/{dataset}.parquet', index=False)
        logger.info(f"  💾 Saved updated {dataset} dataset")
        
        # Show sample values after fix
        for col in year_cols + age_cols:
            if col in df.columns:
                sample_values = df[col].dropna().head(3).tolist()
                logger.info(f"  📊 {col} sample after fix: {sample_values}")

if __name__ == "__main__":
    fix_data_types()
