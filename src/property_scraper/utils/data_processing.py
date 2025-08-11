"""
Data processing utilities for property scraper pipelines.

This module provides common functions for data processing operations including:
- Data validation and quality checks
- Column mapping and transformation
- Data type conversion and standardization
- Parquet file operations
- Data merging and joining
"""

import pandas as pd  # type: ignore
import numpy as np  # type: ignore
import re
import logging
import os
from typing import Dict, Any, Optional, List, Union, Tuple, Set
from datetime import datetime

logger = logging.getLogger(__name__)

# ============ DATA VALIDATION UTILITIES ============

def validate_dataframe_structure(df: pd.DataFrame, required_columns: List[str], 
                                min_rows: int = 1) -> Dict[str, Any]:
    """
    Validate DataFrame structure and return validation results.
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        min_rows: Minimum number of rows required
        
    Returns:
        Dictionary with validation results
    """
    validation_results = {
        'is_valid': True,
        'errors': [],
        'warnings': [],
        'shape': df.shape,
        'columns': df.columns.tolist()
    }
    
    # Check if DataFrame is empty
    if df.empty:
        validation_results['is_valid'] = False
        validation_results['errors'].append("DataFrame is empty")
        return validation_results
    
    # Check minimum row count
    if len(df) < min_rows:
        validation_results['is_valid'] = False
        validation_results['errors'].append(f"DataFrame has {len(df)} rows, minimum required: {min_rows}")
    
    # Check required columns
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        validation_results['is_valid'] = False
        validation_results['errors'].append(f"Missing required columns: {missing_columns}")
    
    # Check for duplicate columns
    duplicate_columns = df.columns[df.columns.duplicated()].tolist()
    if duplicate_columns:
        validation_results['warnings'].append(f"Duplicate columns found: {duplicate_columns}")
    
    # Check data types
    type_info = {}
    for col in df.columns:
        type_info[col] = {
            'dtype': str(df[col].dtype),
            'null_count': df[col].isnull().sum(),
            'null_percentage': (df[col].isnull().sum() / len(df)) * 100
        }
    
    validation_results['column_info'] = type_info
    
    return validation_results

def check_data_quality(df: pd.DataFrame, critical_columns: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Perform comprehensive data quality checks.
    
    Args:
        df: DataFrame to check
        critical_columns: List of columns that cannot have missing values
        
    Returns:
        Dictionary with quality assessment results
    """
    quality_results = {
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'missing_data': {},
        'duplicate_rows': 0,
        'data_types': {},
        'quality_score': 0.0,
        'recommendations': []
    }
    
    if df.empty:
        quality_results['quality_score'] = 0.0
        quality_results['recommendations'].append("DataFrame is empty")
        return quality_results
    
    # Check missing data
    for col in df.columns:
        missing_count = df[col].isnull().sum()
        missing_percentage = (missing_count / len(df)) * 100
        quality_results['missing_data'][col] = {
            'count': missing_count,
            'percentage': missing_percentage
        }
        
        # Check critical columns
        if critical_columns and col in critical_columns and missing_count > 0:
            quality_results['recommendations'].append(f"Critical column '{col}' has {missing_count} missing values")
    
    # Check duplicates
    quality_results['duplicate_rows'] = df.duplicated().sum()
    if quality_results['duplicate_rows'] > 0:
        quality_results['recommendations'].append(f"Found {quality_results['duplicate_rows']} duplicate rows")
    
    # Check data types
    for col in df.columns:
        quality_results['data_types'][col] = str(df[col].dtype)
    
    # Calculate quality score (0-100)
    total_cells = len(df) * len(df.columns)
    missing_cells = df.isnull().sum().sum()
    duplicate_penalty = min(quality_results['duplicate_rows'] / len(df), 0.1)  # Max 10% penalty
    
    quality_results['quality_score'] = max(0, 100 - (missing_cells / total_cells * 100) - (duplicate_penalty * 100))
    
    # Add recommendations based on quality score
    if quality_results['quality_score'] < 70:
        quality_results['recommendations'].append("Data quality is below 70%. Consider data cleaning.")
    elif quality_results['quality_score'] < 85:
        quality_results['recommendations'].append("Data quality is moderate. Some cleaning may be beneficial.")
    
    return quality_results

def detect_outliers(df: pd.DataFrame, column: str, method: str = 'iqr') -> Dict[str, Any]:
    """
    Detect outliers in a numeric column.
    
    Args:
        df: DataFrame
        column: Column name to check for outliers
        method: Detection method ('iqr', 'zscore', 'percentile')
        
    Returns:
        Dictionary with outlier detection results
    """
    if column not in df.columns:
        return {'error': f"Column '{column}' not found in DataFrame"}
    
    if not pd.api.types.is_numeric_dtype(df[column]):
        return {'error': f"Column '{column}' is not numeric"}
    
    numeric_data = df[column].dropna()
    
    if len(numeric_data) == 0:
        return {'error': f"No numeric data found in column '{column}'"}
    
    outliers = pd.Series(dtype=bool)
    
    if method == 'iqr':
        Q1 = numeric_data.quantile(0.25)
        Q3 = numeric_data.quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        outliers = (numeric_data < lower_bound) | (numeric_data > upper_bound)
        
    elif method == 'zscore':
        z_scores = np.abs((numeric_data - numeric_data.mean()) / numeric_data.std())
        outliers = z_scores > 3
        
    elif method == 'percentile':
        lower_percentile = numeric_data.quantile(0.05)
        upper_percentile = numeric_data.quantile(0.95)
        outliers = (numeric_data < lower_percentile) | (numeric_data > upper_percentile)
    
    return {
        'method': method,
        'total_values': len(numeric_data),
        'outlier_count': outliers.sum(),
        'outlier_percentage': (outliers.sum() / len(numeric_data)) * 100,
        'outlier_indices': outliers[outliers].index.tolist(),
        'outlier_values': numeric_data[outliers].tolist()
    }

# ============ COLUMN MAPPING AND TRANSFORMATION ============

def apply_column_mapping(df: pd.DataFrame, mapping: Dict[str, str], 
                        drop_unmapped: bool = False) -> pd.DataFrame:
    """
    Apply column mapping to DataFrame.
    
    Args:
        df: Input DataFrame
        mapping: Dictionary mapping old column names to new names
        drop_unmapped: Whether to drop columns not in mapping
        
    Returns:
        DataFrame with mapped columns
    """
    df_mapped = df.copy()
    
    # Apply mapping for existing columns
    existing_mapping = {old: new for old, new in mapping.items() if old in df.columns}
    if existing_mapping:
        df_mapped = df_mapped.rename(columns=existing_mapping)
        logger.info(f"Applied column mapping: {existing_mapping}")
    
    # Drop unmapped columns if requested
    if drop_unmapped:
        mapped_columns = set(existing_mapping.values())
        unmapped_columns = [col for col in df_mapped.columns if col not in mapped_columns]
        if unmapped_columns:
            df_mapped = df_mapped.drop(columns=unmapped_columns)
            logger.info(f"Dropped unmapped columns: {unmapped_columns}")
    
    return df_mapped

def standardize_column_order(df: pd.DataFrame, preferred_order: List[str]) -> pd.DataFrame:
    """
    Standardize column order based on preferred order.
    
    Args:
        df: Input DataFrame
        preferred_order: List of column names in preferred order
        
    Returns:
        DataFrame with standardized column order
    """
    df_ordered = df.copy()
    
    # Get existing columns in preferred order
    existing_preferred = [col for col in preferred_order if col in df.columns]
    
    # Get remaining columns not in preferred order
    remaining_columns = [col for col in df.columns if col not in preferred_order]
    
    # Combine ordered columns
    final_order = existing_preferred + remaining_columns
    
    if final_order != df.columns.tolist():
        df_ordered = df_ordered[final_order]
        logger.info(f"Reordered columns: {len(existing_preferred)} in preferred order, {len(remaining_columns)} remaining")
    
    return df_ordered

def create_derived_columns(df: pd.DataFrame, derivation_rules: Dict[str, Any]) -> pd.DataFrame:
    """
    Create derived columns based on rules.
    
    Args:
        df: Input DataFrame
        derivation_rules: Dictionary of derivation rules
        
    Returns:
        DataFrame with derived columns
    """
    df_derived = df.copy()
    
    for new_col, rule in derivation_rules.items():
        try:
            if rule['type'] == 'calculation':
                # Simple calculation between columns
                df_derived[new_col] = df_derived.eval(rule['expression'])
            elif rule['type'] == 'condition':
                # Conditional logic
                df_derived[new_col] = np.where(
                    df_derived.eval(rule['condition']),
                    rule['if_true'],
                    rule['if_false']
                )
            elif rule['type'] == 'function':
                # Apply custom function
                df_derived[new_col] = df_derived[rule['column']].apply(rule['function'])
            
            logger.info(f"Created derived column: {new_col}")
            
        except Exception as e:
            logger.error(f"Error creating derived column '{new_col}': {e}")
    
    return df_derived

# ============ DATA TYPE CONVERSION ============

def convert_data_types(df: pd.DataFrame, type_mapping: Dict[str, str]) -> pd.DataFrame:
    """
    Convert data types based on mapping.
    
    Args:
        df: Input DataFrame
        type_mapping: Dictionary mapping column names to target types
        
    Returns:
        DataFrame with converted types
    """
    df_converted = df.copy()
    
    for col, target_type in type_mapping.items():
        if col not in df.columns:
            logger.warning(f"Column '{col}' not found for type conversion")
            continue
        
        try:
            if target_type == 'datetime':
                df_converted[col] = pd.to_datetime(df_converted[col], errors='coerce')
            elif target_type == 'numeric':
                df_converted[col] = pd.to_numeric(df_converted[col], errors='coerce')
            elif target_type == 'category':
                df_converted[col] = df_converted[col].astype('category')
            elif target_type == 'string':
                df_converted[col] = df_converted[col].astype('string')
            elif target_type == 'boolean':
                df_converted[col] = df_converted[col].astype('boolean')
            else:
                df_converted[col] = df_converted[col].astype(target_type)
            
            logger.info(f"Converted column '{col}' to {target_type}")
            
        except Exception as e:
            logger.error(f"Error converting column '{col}' to {target_type}: {e}")
    
    return df_converted

def standardize_text_columns(df: pd.DataFrame, text_columns: List[str],
                           operations: List[str] = None) -> pd.DataFrame:
    """
    Standardize text columns with common operations.
    
    Args:
        df: Input DataFrame
        text_columns: List of text column names
        operations: List of operations to apply ('lower', 'upper', 'title', 'strip', 'spaces')
        
    Returns:
        DataFrame with standardized text columns
    """
    if operations is None:
        operations = ['strip', 'spaces']
    
    df_standardized = df.copy()
    
    for col in text_columns:
        if col not in df.columns:
            logger.warning(f"Text column '{col}' not found")
            continue
        
        # Convert to string first
        df_standardized[col] = df_standardized[col].astype(str)
        
        # Apply operations
        for op in operations:
            if op == 'lower':
                df_standardized[col] = df_standardized[col].str.lower()
            elif op == 'upper':
                df_standardized[col] = df_standardized[col].str.upper()
            elif op == 'title':
                df_standardized[col] = df_standardized[col].str.title()
            elif op == 'strip':
                df_standardized[col] = df_standardized[col].str.strip()
            elif op == 'spaces':
                df_standardized[col] = df_standardized[col].str.replace(r'\s+', ' ', regex=True)
        
        logger.info(f"Standardized text column '{col}' with operations: {operations}")
    
    return df_standardized

# ============ PARQUET FILE OPERATIONS ============

def sanitize_parquet_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sanitize DataFrame columns for Parquet compatibility.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with sanitized columns
    """
    df_sanitized = df.copy()
    
    # Clean column names
    df_sanitized.columns = df_sanitized.columns.str.replace(r'[^a-zA-Z0-9_]', '_', regex=True)
    
    # Handle special columns that might cause issues
    for col in df_sanitized.columns:
        if df_sanitized[col].dtype == 'object':
            # Remove null bytes and control characters
            df_sanitized[col] = (
                df_sanitized[col]
                .astype(str)
                .str.replace(r'[\x00-\x1F\x7F-\x9F]', '', regex=True)
                .str.strip()
            )
    
    # Convert object columns to string type for better Parquet support
    object_columns = df_sanitized.select_dtypes(include=['object']).columns
    for col in object_columns:
        df_sanitized[col] = df_sanitized[col].astype('string')
    
    logger.info(f"Sanitized {len(df_sanitized.columns)} columns for Parquet compatibility")
    
    return df_sanitized

def safe_parquet_write(df: pd.DataFrame, file_path: str, 
                      create_backup: bool = True) -> bool:
    """
    Safely write DataFrame to Parquet with error handling.
    
    Args:
        df: DataFrame to write
        file_path: Output file path
        create_backup: Whether to create backup if file exists
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Create backup if file exists
        if create_backup and os.path.exists(file_path):
            backup_path = f"{file_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            os.rename(file_path, backup_path)
            logger.info(f"Created backup: {backup_path}")
        
        # Sanitize DataFrame
        df_sanitized = sanitize_parquet_columns(df)
        
        # Write to Parquet
        df_sanitized.to_parquet(file_path, engine='pyarrow', index=False)
        
        logger.info(f"Successfully wrote {len(df_sanitized)} rows to {file_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error writing Parquet file {file_path}: {e}")
        return False

def safe_parquet_read(file_path: str, columns: List[str] = None) -> Optional[pd.DataFrame]:
    """
    Safely read Parquet file with error handling.
    
    Args:
        file_path: Input file path
        columns: List of columns to read (None for all)
        
    Returns:
        DataFrame if successful, None otherwise
    """
    try:
        if not os.path.exists(file_path):
            logger.warning(f"Parquet file not found: {file_path}")
            return None
        
        df = pd.read_parquet(file_path, engine='pyarrow', columns=columns)
        
        logger.info(f"Successfully read {len(df)} rows from {file_path}")
        return df
        
    except Exception as e:
        logger.error(f"Error reading Parquet file {file_path}: {e}")
        return None

# ============ DATA MERGING AND JOINING ============

def smart_merge(left_df: pd.DataFrame, right_df: pd.DataFrame, 
               merge_config: Dict[str, Any]) -> pd.DataFrame:
    """
    Perform smart merge with validation and error handling.
    
    Args:
        left_df: Left DataFrame
        right_df: Right DataFrame
        merge_config: Merge configuration
        
    Returns:
        Merged DataFrame
    """
    try:
        # Extract merge parameters
        on = merge_config.get('on')
        left_on = merge_config.get('left_on')
        right_on = merge_config.get('right_on')
        how = merge_config.get('how', 'inner')
        
        # Validate merge keys
        if on:
            missing_left = [col for col in on if col not in left_df.columns]
            missing_right = [col for col in on if col not in right_df.columns]
            
            if missing_left:
                raise ValueError(f"Merge keys missing in left DataFrame: {missing_left}")
            if missing_right:
                raise ValueError(f"Merge keys missing in right DataFrame: {missing_right}")
        
        # Perform merge
        merged_df = pd.merge(left_df, right_df, on=on, left_on=left_on, 
                           right_on=right_on, how=how)
        
        logger.info(f"Merged DataFrames: {len(left_df)} + {len(right_df)} = {len(merged_df)} rows")
        
        return merged_df
        
    except Exception as e:
        logger.error(f"Error in smart_merge: {e}")
        return pd.DataFrame()

def deduplicate_dataframe(df: pd.DataFrame, subset: List[str] = None, 
                         keep: str = 'first') -> pd.DataFrame:
    """
    Remove duplicates from DataFrame with logging.
    
    Args:
        df: Input DataFrame
        subset: Columns to consider for duplicate detection
        keep: Which duplicate to keep ('first', 'last', False)
        
    Returns:
        DataFrame with duplicates removed
    """
    original_count = len(df)
    df_deduped = df.drop_duplicates(subset=subset, keep=keep)
    
    duplicates_removed = original_count - len(df_deduped)
    if duplicates_removed > 0:
        logger.info(f"Removed {duplicates_removed} duplicate rows")
    
    return df_deduped

def aggregate_data(df: pd.DataFrame, group_by: List[str], 
                  aggregations: Dict[str, Any]) -> pd.DataFrame:
    """
    Aggregate DataFrame with multiple operations.
    
    Args:
        df: Input DataFrame
        group_by: Columns to group by
        aggregations: Dictionary of column -> aggregation function
        
    Returns:
        Aggregated DataFrame
    """
    try:
        # Validate group_by columns
        missing_cols = [col for col in group_by if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Group by columns missing: {missing_cols}")
        
        # Perform aggregation
        agg_df = df.groupby(group_by).agg(aggregations).reset_index()
        
        # Flatten column names if needed
        if isinstance(agg_df.columns, pd.MultiIndex):
            agg_df.columns = ['_'.join(col).strip() for col in agg_df.columns.values]
        
        logger.info(f"Aggregated {len(df)} rows into {len(agg_df)} groups")
        
        return agg_df
        
    except Exception as e:
        logger.error(f"Error in aggregate_data: {e}")
        return pd.DataFrame()

# ============ UTILITY FUNCTIONS ============

def get_dataframe_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Get comprehensive summary of DataFrame.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Dictionary with DataFrame summary
    """
    summary = {
        'shape': df.shape,
        'columns': df.columns.tolist(),
        'dtypes': df.dtypes.to_dict(),
        'memory_usage': df.memory_usage(deep=True).sum(),
        'null_counts': df.isnull().sum().to_dict(),
        'duplicate_rows': df.duplicated().sum()
    }
    
    # Add numeric summary for numeric columns
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 0:
        summary['numeric_summary'] = df[numeric_cols].describe().to_dict()
    
    return summary

def profile_dataframe(df: pd.DataFrame, sample_size: int = 1000) -> Dict[str, Any]:
    """
    Profile DataFrame for analysis and debugging.
    
    Args:
        df: Input DataFrame
        sample_size: Number of sample rows to include
        
    Returns:
        Dictionary with profiling information
    """
    profile = {
        'basic_info': get_dataframe_summary(df),
        'sample_data': df.head(sample_size).to_dict('records') if len(df) > 0 else [],
        'column_profiles': {}
    }
    
    # Profile each column
    for col in df.columns:
        col_profile = {
            'dtype': str(df[col].dtype),
            'unique_count': df[col].nunique(),
            'null_count': df[col].isnull().sum(),
            'sample_values': df[col].dropna().head(5).tolist()
        }
        
        if pd.api.types.is_numeric_dtype(df[col]):
            col_profile['min'] = df[col].min()
            col_profile['max'] = df[col].max()
            col_profile['mean'] = df[col].mean()
            col_profile['std'] = df[col].std()
        
        profile['column_profiles'][col] = col_profile
    
    return profile 