"""
Date validation utility for property transaction data.
Validates date quality and detects anomalies.
"""

import pandas as pd
import logging
from datetime import datetime, date
from typing import Dict, Any, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)


def validate_transaction_dates(
    df: pd.DataFrame,
    date_column: str = 'transactionDate',
    date_format: str = '%d/%m/%Y',
    reference_date: date = None
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Validate transaction dates and add validation columns.
    
    Args:
        df: DataFrame with transaction data
        date_column: Name of the date column to validate
        date_format: Expected date format (default: dd/mm/yyyy)
        reference_date: Date to compare against (default: today)
    
    Returns:
        Tuple of (validated_df, validation_report)
    """
    
    if reference_date is None:
        reference_date = datetime.now().date()
    
    df_validated = df.copy()
    
    # Parse dates with explicit format
    df_validated['_parsed_date'] = pd.to_datetime(
        df_validated[date_column], 
        format=date_format, 
        errors='coerce'
    )
    
    # Add validation columns
    df_validated['_date_valid'] = df_validated['_parsed_date'].notna()
    df_validated['_is_future'] = df_validated['_parsed_date'] > pd.Timestamp(reference_date)
    df_validated['_is_very_old'] = df_validated['_parsed_date'] < pd.Timestamp('1990-01-01')
    df_validated['_days_from_ref'] = (
        pd.Timestamp(reference_date) - df_validated['_parsed_date']
    ).dt.days
    df_validated['_year'] = df_validated['_parsed_date'].dt.year
    
    # Calculate validation metrics
    total = len(df_validated)
    valid_count = df_validated['_date_valid'].sum()
    invalid_count = total - valid_count
    future_count = df_validated['_is_future'].sum()
    old_count = df_validated['_is_very_old'].sum()
    
    valid_df = df_validated[df_validated['_date_valid']]
    
    validation_report = {
        'total_records': total,
        'valid_dates': valid_count,
        'invalid_dates': invalid_count,
        'parse_rate_pct': (valid_count / total * 100) if total > 0 else 0,
        'future_dates': future_count,
        'very_old_dates': old_count,
        'min_date': valid_df['_parsed_date'].min() if len(valid_df) > 0 else None,
        'max_date': valid_df['_parsed_date'].max() if len(valid_df) > 0 else None,
        'reference_date': reference_date,
        'days_behind': (
            (pd.Timestamp(reference_date) - valid_df['_parsed_date'].max()).days 
            if len(valid_df) > 0 else None
        ),
        'year_distribution': valid_df['_year'].value_counts().to_dict() if len(valid_df) > 0 else {},
        'data_quality': 'EXCELLENT' if valid_count/total >= 0.99 and future_count == 0 else 
                       'GOOD' if valid_count/total >= 0.95 else 
                       'POOR'
    }
    
    # Log validation results
    logger.info("="*80)
    logger.info("DATE VALIDATION REPORT")
    logger.info("="*80)
    logger.info(f"Total records: {total:,}")
    logger.info(f"Valid dates: {valid_count:,} ({validation_report['parse_rate_pct']:.2f}%)")
    logger.info(f"Invalid dates: {invalid_count:,}")
    logger.info(f"Future dates: {future_count:,}")
    logger.info(f"Very old dates (before 1990): {old_count:,}")
    
    if validation_report['min_date'] and validation_report['max_date']:
        logger.info(f"Date range: {validation_report['min_date'].date()} to {validation_report['max_date'].date()}")
        logger.info(f"Days behind reference ({reference_date}): {validation_report['days_behind']}")
    
    logger.info(f"Data quality: {validation_report['data_quality']}")
    logger.info("="*80)
    
    # Warnings
    if future_count > 0:
        logger.warning(f"⚠️ ALERT: Found {future_count} future dates - investigate source data!")
        future_sample = df_validated[df_validated['_is_future']][
            [date_column, '_parsed_date', 'propertyNameEn', 'District']
        ].head(10)
        logger.warning(f"Sample future dates:\n{future_sample.to_string(index=False)}")
    
    if validation_report['parse_rate_pct'] < 99.0:
        logger.warning(f"⚠️ ALERT: Parse rate below 99% - some dates may be malformed!")
    
    return df_validated, validation_report


def should_run_transaction_node_with_validation(
    node_name: str,
    data_file_path: str,
    date_column: str = 'transactionDate',
    date_format: str = '%d/%m/%Y'
) -> Tuple[bool, Dict[str, Any]]:
    """
    Check if transaction node should run with comprehensive date validation.
    
    Returns:
        Tuple of (should_run: bool, validation_report: dict)
    """
    
    if not Path(data_file_path).exists():
        logger.info(f"Node '{node_name}' - no existing data file - will execute (initial scrape)")
        return True, {'reason': 'no_existing_data'}
    
    try:
        # Load data
        if data_file_path.endswith('.parquet'):
            df = pd.read_parquet(data_file_path)
        elif data_file_path.endswith('.csv'):
            df = pd.read_csv(data_file_path)
        else:
            logger.warning(f"Unsupported file format: {data_file_path}")
            return True, {'reason': 'unsupported_format'}
        
        if df.empty:
            logger.info(f"Node '{node_name}' - empty dataset - will execute")
            return True, {'reason': 'empty_dataset'}
        
        if date_column not in df.columns:
            logger.warning(f"Node '{node_name}' - no date column '{date_column}' - will execute")
            return True, {'reason': 'no_date_column'}
        
        # Validate dates
        df_validated, report = validate_transaction_dates(df, date_column, date_format)
        
        # Decision logic
        current_date = datetime.now().date()
        max_date = report['max_date'].date() if report['max_date'] else None
        
        if max_date is None:
            logger.info(f"Node '{node_name}' - no valid dates found - will execute")
            return True, report
        
        if max_date >= current_date:
            logger.info(f"Node '{node_name}' - dataset is UP TO DATE (max: {max_date}, current: {current_date}) - SKIP")
            logger.info(f"  Would fetch from {max_date + pd.Timedelta(days=1).to_pytimedelta()} onwards")
            return False, report
        else:
            days_behind = (current_date - max_date).days
            logger.info(f"Node '{node_name}' - dataset is {days_behind} days BEHIND (max: {max_date}, current: {current_date}) - RUN")
            logger.info(f"  Will fetch from {max_date + pd.Timedelta(days=1).to_pytimedelta()} to {current_date}")
            return True, report
            
    except Exception as e:
        logger.error(f"Error validating dates for '{node_name}': {e}")
        logger.info("Will execute node for safety")
        return True, {'reason': 'validation_error', 'error': str(e)}


def create_date_validation_report(data_file_path: str, output_file: str = None) -> pd.DataFrame:
    """
    Create a detailed date validation report for a data file.
    
    Args:
        data_file_path: Path to the parquet/csv file
        output_file: Optional path to save report
    
    Returns:
        DataFrame with validation report
    """
    
    df = pd.read_parquet(data_file_path) if data_file_path.endswith('.parquet') else pd.read_csv(data_file_path)
    
    # Try to find date column
    date_columns = ['transactionDate', 'date', 'tx_date', 'transaction_date']
    date_col = None
    
    for col in date_columns:
        if col in df.columns:
            date_col = col
            break
    
    if not date_col:
        logger.error("No date column found")
        return pd.DataFrame()
    
    # Validate
    df_validated, report = validate_transaction_dates(df, date_col)
    
    # Create summary report
    summary_data = {
        'metric': [
            'Total Records',
            'Valid Dates',
            'Invalid Dates',
            'Parse Rate %',
            'Future Dates',
            'Very Old Dates',
            'Min Date',
            'Max Date',
            'Days Behind',
            'Data Quality'
        ],
        'value': [
            f"{report['total_records']:,}",
            f"{report['valid_dates']:,}",
            f"{report['invalid_dates']:,}",
            f"{report['parse_rate_pct']:.2f}%",
            f"{report['future_dates']:,}",
            f"{report['very_old_dates']:,}",
            str(report['min_date'].date()) if report['min_date'] else 'N/A',
            str(report['max_date'].date()) if report['max_date'] else 'N/A',
            f"{report['days_behind']} days" if report['days_behind'] is not None else 'N/A',
            report['data_quality']
        ]
    }
    
    summary_df = pd.DataFrame(summary_data)
    
    if output_file:
        summary_df.to_csv(output_file, index=False)
        logger.info(f"Validation report saved to: {output_file}")
    
    return summary_df
