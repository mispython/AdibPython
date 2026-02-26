"""
EIBM0961 - Equity TFX PMM Report

Purpose: Extract Private Money Market (PMM) deposits from TFX

Input: EQUTFX file (pipe-delimited)
Output: GLPMM file (FM format)

Filter: Deal type BCD or BCQ (Banker's Certificates)
"""

import polars as pl
from datetime import datetime
import os

# Directories
BNM_DIR = 'data/bnm/'
INPUT_DIR = 'data/input/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIBM0961 - Equity TFX PMM Report")

# Read REPTDATE
try:
    df_reptdate = pl.read_parquet(f'{BNM_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    ym = f'{reptdate.year}{reptdate.month:02d}'
except Exception as e:
    print(f"✗ ERROR reading REPTDATE: {e}")
    import sys
    sys.exit(1)

# Read EQUTFX file (pipe-delimited)
try:
    data = []
    with open(f'{INPUT_DIR}EQUTFX', 'r') as f:
        for line in f:
            parts = line.strip().split('|')
            if len(parts) > 7:
                ufclc = parts[7].strip()  # Customer location
                ufdlp = parts[2].strip()  # Deal type
                
                # Fix KL to KLC
                if ufclc == 'KL':
                    ufclc = 'KLC'
                
                # Filter: Deal type BCD or BCQ
                if ufdlp in ['BCD', 'BCQ']:
                    data.append({
                        'BRANCH': ufclc,
                        'DEALTYPE': ufdlp,
                        'AGLFLD': 'PBB',
                        'NOACCT': 1
                    })
    
    df = pl.DataFrame(data) if data else pl.DataFrame([])
    print(f"✓ Filtered {len(df):,} PMM accounts")

except Exception as e:
    print(f"✗ ERROR reading EQUTFX: {e}")
    import sys
    sys.exit(1)

# Summarize
if len(df) > 0:
    df_summary = df.group_by('AGLFLD').agg([
        pl.col('NOACCT').sum().alias('NOACCT')
    ])
    
    # Add fixed fields
    df_summary = df_summary.with_columns([
        pl.lit('DPPMMS').alias('PRODUCX'),
        pl.lit('7800').alias('BRANCX'),
        pl.lit('EXT').alias('BGLFLD'),
        pl.lit('C_DEP').alias('CGLFLD'),
        pl.lit('ACTUAL').alias('DGLFLD'),
        pl.lit(ym).alias('YM')
    ])
else:
    print("⚠ No PMM accounts found")
    df_summary = pl.DataFrame([])

# Generate GLPMM file
if len(df_summary) > 0:
    cnt = len(df_summary)
    numac = df_summary['NOACCT'].sum()
    
    with open(f'{OUTPUT_DIR}GLPMM', 'w') as f:
        for row in df_summary.iter_rows(named=True):
            line = (
                f"D,{row['AGLFLD']},EXT,{row['CGLFLD']},{row['DGLFLD']},,{row['BRANCX']},TREA,"
                f"{row['PRODUCX']},{row['YM']},{row['NOACCT']:>15},Y,,,,"
            )
            f.write(line + '\n')
        
        # Trailer
        f.write(f"T,{cnt:>10},{numac:>15},\n")
    
    print(f"✓ Generated GLPMM: {cnt} records, {numac:,} accounts")
else:
    print("⚠ No output generated")

print("✓ EIBM0961 Complete")
