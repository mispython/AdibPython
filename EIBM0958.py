"""
EIBM0958 - Equity TMS NID Report

Purpose: Extract negotiable instrument deposit (NID) accounts from TMS

Input: EQUTMS file
Output: GLNID file (FM format)

Filter: Portfolio reference starts with PFD, PLD, PSD, PZD, or PDC
Report: Count of NID accounts
"""

import polars as pl
from datetime import datetime
import os

# Directories
BNM_DIR = 'data/bnm/'
INPUT_DIR = 'data/input/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIBM0958 - Equity TMS NID Report")

# Read REPTDATE
try:
    df_reptdate = pl.read_parquet(f'{BNM_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    
    mm = reptdate.month
    mm1 = mm - 1 if mm > 1 else 12
    
    reptmon = f'{mm:02d}'
    rdate = reptdate.strftime('%d%m%y')
    ryear = f'{reptdate.year % 100:02d}'
    reptyea4 = str(reptdate.year)
    rmonth = f'{mm:02d}'
    reptmon1 = f'{mm1:02d}'
    rday = f'{reptdate.day:02d}'
    reptday = f'{reptdate.day:02d}'
    
    ym = f'{reptyea4}{rmonth}'

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
                    'BRANCH': line[10:14].strip(),
                    'DEALREF': line[14:27].strip(),
                    'DEALTYPE': line[27:30].strip(),
                    'PORTREF': portref,
                    'DEPOTID': line[46:49].strip(),
                    'DEPOTFD': line[49:52].strip(),
                    'CUSTNO': line[52:58].strip(),
                    'CUSTLOC': line[58:61].strip(),
                    'CUSTNAME': line[61:96].strip(),
                    'AGLFLD': 'PBB',
                    'NOACCT': 1
                })
    
    df = pl.DataFrame(data) if data else pl.DataFrame([])
    print(f"✓ Filtered {len(df):,} NID accounts")

except Exception as e:
    print(f"✗ ERROR reading EQUTMS: {e}")
    import sys
    sys.exit(1)

# Summarize
if len(df) > 0:
    df_summary = df.group_by('AGLFLD').agg([
        pl.col('NOACCT').sum().alias('NOACCT')
    ])
    
    # Add fixed fields
    df_summary = df_summary.with_columns([
        pl.lit('DPNIDS').alias('PRODUCX'),
        pl.lit('7800').alias('BRANCX'),
        pl.lit('EXT').alias('BGLFLD'),
        pl.lit('C_DEP').alias('CGLFLD'),
        pl.lit('ACTUAL').alias('DGLFLD'),
        pl.lit(ym).alias('YM')
    ])
else:
    print("⚠ No NID accounts found")
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

print("✓ EIBM0958 Complete")
