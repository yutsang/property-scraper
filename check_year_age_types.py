#!/usr/bin/env python3
import pandas as pd

datasets = ['centaline_res', 'centaline_oir', 'midland_res', 'midland_ici']

print('Checking year and age columns across all datasets:')
print('=' * 60)

for dataset in datasets:
    print(f'\n{dataset.upper()}:')
    df = pd.read_parquet(f'data/03_primary/{dataset}.parquet')
    
    year_cols = [col for col in df.columns if 'year' in col.lower()]
    age_cols = [col for col in df.columns if 'age' in col.lower()]
    
    print(f'Year columns: {year_cols}')
    print(f'Age columns: {age_cols}')
    
    for col in year_cols + age_cols:
        if col in df.columns:
            sample_values = df[col].dropna().head(3).tolist()
            print(f'  {col}: {df[col].dtype} - Sample: {sample_values}')
    
    print(f'Total records: {len(df)}')
