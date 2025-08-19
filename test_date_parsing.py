#!/usr/bin/env python3
import pandas as pd

# Load the processed data
df = pd.read_parquet('data/03_primary/centaline_oir.parquet')
sample_dates = df['transactionDate'].dropna().head(10)

print('Sample dates from PROCESSED data:', sample_dates.tolist())
print('\nTesting if dates are now correct:')
print('Date\t\t\tInterpretation')
print('-' * 40)

for date in sample_dates:
    try:
        # Parse as dd/mm/yyyy
        dt = pd.to_datetime(date, format='%d/%m/%Y')
        print(f'{date}\t\t{dt.strftime("%d %B %Y")} ({dt.strftime("%A")})')
    except Exception as e:
        print(f'{date}\t\tError: {e}')

print('\nChecking for suspicious dates (where day > 12):')
suspicious_dates = []
for date in df['transactionDate'].dropna():
    try:
        day, month, year = date.split('/')
        if int(day) > 12:
            suspicious_dates.append(date)
    except:
        pass

if suspicious_dates:
    print(f'Found {len(suspicious_dates)} suspicious dates:')
    for date in suspicious_dates[:10]:  # Show first 10
        print(f'  {date}')
else:
    print('No suspicious dates found! All dates appear to be correctly formatted.')
