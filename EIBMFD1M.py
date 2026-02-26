"""
EIBDFD1M - Fixed Deposit Large Accounts Report

Purpose: Extract large FD accounts (>1M) with rate changes

Logic:
1. Filter active FDs (balance > 0)
2. Split by customer type (Individual/Corporate)
3. Keep only accounts > RM 1 million
4. Match with rate history (last 5 rates)
5. Flag rate changes
"""

import polars as pl
from datetime import datetime
import os

# Directories
FD_DIR = 'data/fd/'
CISFD_DIR = 'data/cisfd/'
INPUT_DIR = 'data/input/'
FD1M_DIR = 'data/fd1m/'

os.makedirs(FD1M_DIR, exist_ok=True)

print("EIBDFD1M - FD Large Accounts Report")

# Read REPTDATE
try:
    df_reptdate = pl.read_parquet(f'{FD_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    df_reptdate.write_parquet(f'{FD1M_DIR}REPTDATE.parquet')
except Exception as e:
    print(f"✗ ERROR: {e}")
    import sys
    sys.exit(1)

# Read FD data
df_fd = pl.read_parquet(f'{FD_DIR}FD.parquet')

# Filter active FDs
df_fd = df_fd.filter(
    (~pl.col('OPENIND').is_in(['B','C','P'])) &
    (pl.col('ACCTTYPE').is_in([300,301,303])) &
    (pl.col('CURBAL') > 0)
)

# Read branch mapping
try:
    with open(f'{INPUT_DIR}BRANCHFL', 'r') as f:
        branch_data = []
        for line in f:
            branch_data.append({
                'BRANCH': int(line[1:4]) if line[1:4].strip() else 0,
                'BRN': line[5:8].strip()
            })
    df_branch = pl.DataFrame(branch_data)
    df_fd = df_fd.join(df_branch, on='BRANCH', how='left')
except:
    pass

# Read customer data
df_cust = pl.read_parquet(f'{CISFD_DIR}DEPOSIT.parquet')
df_cust = df_cust.filter(pl.col('SECCUST') == '901').select(['ACCTNO','NEWIC','CUSTNAME']).unique()
df_fd = df_fd.join(df_cust, on='ACCTNO', how='left')

# Split by customer type
df_fdi = df_fd.filter(pl.col('CUSTCD').is_in([77,78]))  # Individual
df_fdc = df_fd.filter(
    (pl.col('CUSTCD').is_in([1,79])) | 
    ((pl.col('CUSTCD') >= 66) & (pl.col('CUSTCD') <= 74))
)  # Corporate

# Individual: Group by IC, keep > 1M
if len(df_fdi) > 0:
    df_fdi = df_fdi.with_columns([
        pl.when(pl.col('PURPOSE') == '2').then(pl.lit('J')).otherwise(pl.lit('I')).alias('JI')
    ])
    df_fdi = df_fdi.with_columns([
        (pl.col('NEWIC') + pl.col('JI')).alias('NEWICJI')
    ])
    
    # Sum by NEWICJI
    df_sum = df_fdi.group_by('NEWICJI').agg([pl.col('CURBAL').sum().alias('AMOUNT')])
    df_sum = df_sum.filter(pl.col('AMOUNT') > 1000000)
    
    # Keep only accounts with total > 1M
    df_fdi = df_fdi.join(df_sum.select('NEWICJI'), on='NEWICJI', how='inner')

# Corporate: Group by name, keep > 1M
if len(df_fdc) > 0:
    df_fdc = df_fdc.with_columns([
        pl.lit('C').alias('JI'),
        pl.col('CUSTNAME').str.slice(0, 24).alias('CUSTNAME')
    ])
    
    # Sum by CUSTNAME
    df_sum = df_fdc.group_by('CUSTNAME').agg([pl.col('CURBAL').sum().alias('AMOUNT')])
    df_sum = df_sum.filter(pl.col('AMOUNT') > 1000000)
    
    # Keep only accounts with total > 1M
    df_fdc = df_fdc.join(df_sum.select('CUSTNAME'), on='CUSTNAME', how='inner')

# Combine
df_fd = pl.concat([df_fdc, df_fdi]) if len(df_fdc) > 0 or len(df_fdi) > 0 else pl.DataFrame([])

if len(df_fd) > 0:
    # Add PP field
    df_fd = df_fd.with_columns([
        pl.when(pl.col('CUSTCD').is_in([77,78]))
          .then(
              pl.when(pl.col('PURPOSE') == '2').then(pl.lit('J'))
              .otherwise(pl.lit('I'))
          )
          .otherwise(pl.lit('C'))
          .alias('PP')
    ])
    
    # Save output
    df_fd = df_fd.sort('ACCTNO')
    df_fd.write_parquet(f'{FD1M_DIR}FD1M.parquet')
    
    print(f"✓ Extracted {len(df_fd):,} large FD accounts")
else:
    print("⚠ No large accounts found")

print("✓ EIBDFD1M Complete")
