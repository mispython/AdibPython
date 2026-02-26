"""
EIMBISLX - Islamic Deposit Cleanup

Purpose: Clean future-dated records from Islamic deposit datasets

Process:
1. Read current REPTDATE
2. Filter out records with REPTDATE > current date
3. Replace datasets with cleaned versions

Datasets cleaned:
- DYIBU: Daily Islamic banking unit
- AWSA/B/C: Al-Wadiah savings (A/B/C tiers)
- AWCA/B: Al-Wadiah current (A/B)
- MUDH: Mudarabah deposits
"""

import polars as pl
from datetime import datetime
import os

# Directories
DEP_DIR = 'data/dep/'
ISLM_DIR = 'data/islm/'

print("EIMBISLX - Islamic Deposit Cleanup")

# Read REPTDATE
try:
    df_reptdate = pl.read_parquet(f'{DEP_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    
    mm = reptdate.month
    mm1 = mm + 1 if mm < 12 else 1
    reptmon = f'{mm1:02d}'
    edate = int(reptdate.strftime('%y%m%d'))
    
    print(f"REPTDATE: {reptdate.strftime('%d/%m/%Y')}")
    print(f"Month: {reptmon}")
    print(f"Cleaning records > {edate}")

except Exception as e:
    print(f"✗ ERROR: {e}")
    import sys
    sys.exit(1)

# Datasets to clean
datasets = ['DYIBU', 'AWSA', 'AWSB', 'AWSC', 'AWCA', 'AWCB', 'MUDH']

cleaned = 0
for ds in datasets:
    try:
        filepath = f'{ISLM_DIR}{ds}{reptmon}.parquet'
        
        if os.path.exists(filepath):
            # Read dataset
            df = pl.read_parquet(filepath)
            orig_count = len(df)
            
            # Filter: Keep only records <= REPTDATE
            df_clean = df.filter(pl.col('REPTDATE') <= reptdate)
            clean_count = len(df_clean)
            
            # Save cleaned dataset
            df_clean.write_parquet(filepath)
            
            removed = orig_count - clean_count
            print(f"✓ {ds}{reptmon}: {orig_count:,} → {clean_count:,} ({removed:,} removed)")
            cleaned += 1
        else:
            print(f"⚠ {ds}{reptmon}: Not found")
    
    except Exception as e:
        print(f"✗ {ds}{reptmon}: {e}")

print(f"\n✓ Cleaned {cleaned}/{len(datasets)} datasets")
print("✓ EIMBISLX Complete")
