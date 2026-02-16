"""
Utility functions for incremental scraping and date comparison.
Determines whether to run scraping nodes based on local file dates vs online data.
"""

import pandas as pd
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


def get_local_file_date_range(filepath: str) -> Optional[Tuple[datetime, datetime]]:
    """
    Get the date range from a local parquet or excel file.
    
    Args:
        filepath: Path to the local file
        
    Returns:
        Tuple of (min_date, max_date) or None if file doesn't exist
    """
    try:
        file_path = Path(filepath)
        if not file_path.exists():
            logger.info(f"File {filepath} does not exist")
            return None
        
        # Read the file based on extension
        if filepath.endswith('.parquet'):
            df = pd.read_parquet(filepath)
        elif filepath.endswith('.xlsx') or filepath.endswith('.xls'):
            df = pd.read_excel(filepath)
        elif filepath.endswith('.csv'):
            df = pd.read_csv(filepath)
        else:
            logger.warning(f"Unsupported file format: {filepath}")
            return None
        
        # Find date column (common date column names)
        date_columns = ['date', 'tx_date', 'transactionDate', 'Date', 'TX_DATE']
        date_col = None
        for col in date_columns:
            if col in df.columns:
                date_col = col
                break
        
        if date_col is None:
            logger.warning(f"No date column found in {filepath}")
            return None
        
        # Convert to datetime
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        valid_dates = df[df[date_col].notna()][date_col]
        
        if len(valid_dates) == 0:
            logger.warning(f"No valid dates found in {filepath}")
            return None
        
        min_date = valid_dates.min()
        max_date = valid_dates.max()
        
        logger.info(f"Local file {filepath}: Date range {min_date.date()} to {max_date.date()}")
        return (min_date, max_date)
        
    except Exception as e:
        logger.error(f"Error reading {filepath}: {e}")
        return None


def should_run_scraping(
    local_filepath: str,
    online_max_date: Optional[datetime] = None,
    force_refresh_days: int = 7
) -> bool:
    """
    Determine if scraping should run based on local file date vs online data.
    
    Args:
        local_filepath: Path to the local file
        online_max_date: Maximum date available online (if known)
        force_refresh_days: Force refresh if local data is older than this many days
        
    Returns:
        True if scraping should run, False otherwise
    """
    # Get local file date range
    local_dates = get_local_file_date_range(local_filepath)
    
    # If no local file exists, run scraping
    if local_dates is None:
        logger.info(f"No local file found at {local_filepath}, scraping required")
        return True
    
    local_min, local_max = local_dates
    
    # If online max date is provided, compare
    if online_max_date:
        if local_max < online_max_date:
            logger.info(f"Local data ({local_max.date()}) is older than online ({online_max_date.date()}), scraping required")
            return True
    
    # Check if local data is too old (force refresh)
    days_old = (datetime.now() - local_max).days
    if days_old > force_refresh_days:
        logger.info(f"Local data is {days_old} days old (threshold: {force_refresh_days}), scraping required")
        return True
    
    logger.info(f"Local data is up to date ({local_max.date()}), skipping scraping")
    return False


def get_incremental_date_range(
    local_filepath: str,
    lookback_days: int = 30
) -> Tuple[datetime, datetime]:
    """
    Get the date range for incremental scraping.
    
    Args:
        local_filepath: Path to the local file
        lookback_days: Number of days to look back from the latest local date
        
    Returns:
        Tuple of (start_date, end_date) for incremental scraping
    """
    local_dates = get_local_file_date_range(local_filepath)
    
    if local_dates is None:
        # No local data, scrape everything (e.g., last 2 years)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=730)  # 2 years
        logger.info(f"No local data, scraping full range: {start_date.date()} to {end_date.date()}")
        return (start_date, end_date)
    
    local_min, local_max = local_dates
    
    # Incremental scraping: start from (local_max - lookback_days) to now
    start_date = local_max - timedelta(days=lookback_days)
    end_date = datetime.now()
    
    logger.info(f"Incremental scraping range: {start_date.date()} to {end_date.date()}")
    return (start_date, end_date)


def compare_building_counts(
    local_filepath: str,
    online_building_count: int,
    area_column: str = 'district'
) -> dict:
    """
    Compare building counts between local file and online data by area.
    
    Args:
        local_filepath: Path to the local file
        online_building_count: Total building count available online
        area_column: Column name representing area/district
        
    Returns:
        Dictionary with comparison results
    """
    try:
        file_path = Path(local_filepath)
        if not file_path.exists():
            return {
                'status': 'missing',
                'local_count': 0,
                'online_count': online_building_count,
                'difference': online_building_count,
                'areas': {}
            }
        
        # Read the file
        if filepath.endswith('.parquet'):
            df = pd.read_parquet(local_filepath)
        elif filepath.endswith('.xlsx') or filepath.endswith('.xls'):
            df = pd.read_excel(local_filepath)
        elif filepath.endswith('.csv'):
            df = pd.read_csv(local_filepath)
        else:
            return {'status': 'unsupported_format'}
        
        local_count = len(df)
        difference = online_building_count - local_count
        
        # Count by area
        area_counts = {}
        if area_column in df.columns:
            area_counts = df[area_column].value_counts().to_dict()
        
        result = {
            'status': 'comparison_done',
            'local_count': local_count,
            'online_count': online_building_count,
            'difference': difference,
            'areas': area_counts,
            'needs_update': difference > 0
        }
        
        logger.info(f"Building count comparison: Local={local_count}, Online={online_building_count}, Diff={difference}")
        return result
        
    except Exception as e:
        logger.error(f"Error comparing building counts: {e}")
        return {'status': 'error', 'message': str(e)}
