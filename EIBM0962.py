"""
EIMB0962 - Islamic BCI Money Market Report

Purpose: Count Islamic BCI (Bank Commercial Investment) deals from equity TFX

Input: EQUTFX file
Output: GLPMM file (FM format)

Filter: DEALTYPE = 'BCI' (Bank Commercial Investment)
Entity: PIBB (Islamic)
Product: DPPMMS (Deposit Primary Money Market)
"""

import polars as pl
import os

# Directories
BNM_DIR = 'data/bnm/'
INPUT_DIR = 'data/input/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIMB0962 - Islamic BCI Money Market Report")

# Read REPTDATE
try:
    df_reptdate = pl.read_parquet(f'{BNM_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    ym = f'{reptdate.year}{reptdate.month:02d}'
except Exception as e:
    print(f"✗ ERROR: {e}")
    import sys
    sys.exit(1)

# Read EQUTFX file
try:
    data = []
    with open(f'{INPUT_DIR}EQUTFX', 'r') as f:
        for line in f:
            dealtype = line[14:17].strip()
            
            # Filter: BCI deals only
            if dealtype == 'BCI':
                data.append({'AGLFLD': 'PIBB', 'NOACCT': 1})
    
    df = pl.DataFrame(data) if data else pl.DataFrame([])
    print(f"✓ Filtered {len(df):,} BCI deals")
except Exception as e:
    print(f"✗ ERROR: {e}")
    import sys
    sys.exit(1)

# Summarize and output
if len(df) > 0:
    df_summary = df.group_by('AGLFLD').agg([pl.col('NOACCT').sum()])
    df_summary = df_summary.with_columns([
        pl.lit('DPPMMS').alias('PRODUCX'),
        pl.lit('3000').alias('BRANCX'),
        pl.lit(ym).alias('YM')
    ])
    
    cnt = len(df_summary)
    numac = df_summary['NOACCT'].sum()
    
    with open(f'{OUTPUT_DIR}GLPMM', 'w') as f:
        for row in df_summary.iter_rows(named=True):
            f.write(f"D,{row['AGLFLD']},EXT,C_DEP,ACTUAL,,{row['BRANCX']},TREA,"
                   f"{row['PRODUCX']},{row['YM']},{row['NOACCT']:>15},Y,,,,\n")
        f.write(f"T,{cnt:>10},{numac:>15},\n")
    
    print(f"✓ Generated GLPMM: {numac:,} accounts")
else:
    print("⚠ No BCI deals found")

print("✓ EIMB0962 Complete")
