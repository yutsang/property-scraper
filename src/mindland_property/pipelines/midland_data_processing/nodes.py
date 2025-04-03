# src/midland_property/pipelines/midland_data_processing/nodes.py
import pandas as pd

def merge_transactions_estates(transactions_df: pd.DataFrame, estates_df: pd.DataFrame) -> pd.DataFrame:
    return pd.merge(
        transactions_df,
        estates_df,
        left_on='estate_id',
        right_on='id',
        how='left',
        suffixes=('_trans', '_estate')
    )

def calculate_price_metrics(merged_df: pd.DataFrame) -> pd.DataFrame:
    merged_df['price_per_sqft'] = merged_df['price'] / merged_df['net_area']
    merged_df['price_variance'] = merged_df['price_per_sqft'] - merged_df['market_stat_net_ft_price']
    return merged_df
