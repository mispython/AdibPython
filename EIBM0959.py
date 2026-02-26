"""
EIBM0959 - Islamic Equity TMS NID Report

Purpose: Extract Islamic NID accounts from TMS (same as EIBM0958 but PIBB)

Input: EQUTMS file
Output: GLNID file (FM format)

Difference from EIBM0958:
- Entity: PIBB (Islamic) instead of PBB
- Branch: 3000 (Islamic treasury) instead of 7800
"""

import polars as pl
from datetime import datetime
import os

# Directories
BNM_DIR = 'data/bnm/'
INPUT_DIR = 'data/input/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIBM0959 - Islamic Equity TMS NID Report")

# Read REPTDATE
try:
    df_reptdate = pl.read_parquet(f'{BNM_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    
    mm = reptdate.month
    ym = f'{reptdate.year}{mm:02d}'

except Exception as e:
    print(f"✗ ERROR reading REPTDATE: {e}")
    import sys
    sys.exit(1)

# Read EQUTMS file
try:
    data = []
    with open(f'{INPUT_DIR}EQUTMS', 'r') as f:
        for line in f:
            portref = line[30:46].strip()
            
            # Filter: Portfolio starts with PFD, PLD, PSD, PZD, PDC
            if portref[:3] in ['PFD', 'PLD', 'PSD', 'PZD', 'PDC']:
                data.append({
                    'AGLFLD': 'PIBB',  # Islamic entity
                    'NOACCT': 1
                })
    
    df = pl.DataFrame(data) if data else pl.DataFrame([])
    print(f"✓ Filtered {len(df):,} Islamic NID accounts")

except Exception as e:
    print(f"✗ ERROR reading EQUTMS: {e}")
    import sys
    sys.exit(1)

# Summarize
if len(df) > 0:
    df_summary = df.group_by('AGLFLD').agg([
        pl.col('NOACCT').sum().alias('NOACCT')
    ])
    
    # Add fixed fields for Islamic
    df_summary = df_summary.with_columns([
        pl.lit('DPNIDS').alias('PRODUCX'),
        pl.lit('3000').alias('BRANCX'),  # Islamic branch
        pl.lit('EXT').alias('BGLFLD'),
        pl.lit('C_DEP').alias('CGLFLD'),
        pl.lit('ACTUAL').alias('DGLFLD'),
        pl.lit(ym).alias('YM')
    ])
else:
    print("⚠ No Islamic NID accounts found")
    df_summary = pl.DataFrame([])

# Generate GLNID file
if len(df_summary) > 0:
    cnt = len(df_summary)
    numac = df_summary['NOACCT'].sum()
    
    with open(f'{OUTPUT_DIR}GLNID', 'w') as f:
        for row in df_summary.iter_rows(named=True):
            line = (
                f"D,{row['AGLFLD']},EXT,{row['CGLFLD']},{row['DGLFLD']},,{row['BRANCX']},TREA,"
                f"{row['PRODUCX']},{row['YM']},{row['NOACCT']:>15},Y,,,,"
            )
            f.write(line + '\n')
        
        # Trailer
        f.write(f"T,{cnt:>10},{numac:>15},\n")
    
    print(f"✓ Generated GLNID: {cnt} records, {numac:,} accounts")
else:
    print("⚠ No output generated")

print("✓ EIBM0959 Complete")
