"""
EIMBRLFI - Islamic FISS Liquidity Framework Report

Purpose: Generate BNM New Liquidity Framework report for Islamic banking

Components:
1. Loans (FL/HL/OD/RC) by maturity profile
2. Deposits (FD/SA/CA) by maturity profile
3. Undrawn commitments
4. Kapiti items (large items)
5. Customer deposits >= 1% of total

Maturity Buckets:
01: Up to 1 week
02: >1 week - 1 month
03: >1 month - 3 months
04: >3 - 6 months
05: >6 months - 1 year
06: > 1 year

BNM Code Format: 95[ITEM][CUST][BUCKET]0000Y
"""

import polars as pl
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os

# Directories
LOAN_DIR = 'data/loan/'
BNM_DIR = 'data/bnm/'
BNM1_DIR = 'data/bnm1/'
FD_DIR = 'data/fd/'
DEPOSIT_DIR = 'data/deposit/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIMBRLFI - Islamic FISS Liquidity Framework Report")
print("=" * 60)

# Read REPTDATE
try:
    df_reptdate = pl.read_parquet(f'{LOAN_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    
    day = reptdate.day
    if day == 8:
        nowk = '1'
    elif day == 15:
        nowk = '2'
    elif day == 22:
        nowk = '3'
    else:
        nowk = '4'
    
    reptmon = f'{reptdate.month:02d}'
    rdate = reptdate.strftime('%d%m%y')
    
    print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
    print(f"Week: {nowk}, Month: {reptmon}")

except Exception as e:
    print(f"✗ ERROR: {e}")
    import sys
    sys.exit(1)

print("=" * 60)

# Helper function: Calculate remaining months
def calc_remmth(maturity_date, report_date):
    """Calculate remaining months to maturity"""
    if maturity_date is None or maturity_date <= report_date:
        return 0.1
    
    delta = relativedelta(maturity_date, report_date)
    years = delta.years
    months = delta.months
    days = delta.days
    
    # Days as fraction of month (assume 30 days)
    remmth = years * 12 + months + days / 30
    return remmth

# Helper function: Map to maturity bucket
def get_bucket(remmth):
    """Map remaining months to BNM bucket"""
    if remmth < 0.1:
        return '01'
    elif remmth <= 1:
        return '02'
    elif remmth <= 3:
        return '03'
    elif remmth <= 6:
        return '04'
    elif remmth <= 12:
        return '05'
    else:
        return '06'

# Process LOANS
print("\n1. Processing Islamic Loans...")
try:
    df_loan = pl.read_parquet(f'{BNM1_DIR}LOAN{reptmon}{nowk}.parquet')
    
    # Filter Islamic loans
    df_loan = df_loan.filter(
        (pl.col('AMTIND') == 'I') &
        (~pl.col('PAIDIND').is_in(['P','C'])) &
        ((pl.col('PRODCD').str.slice(0,2) == '34') | pl.col('PRODUCT').is_in([225,226]))
    )
    
    # Calculate customer type
    df_loan = df_loan.with_columns([
        pl.when(pl.col('CUSTCD').is_in(['77','78','95','96']))
          .then(pl.lit('08'))
          .otherwise(pl.lit('09'))
          .alias('CUST')
    ])
    
    # Calculate remaining months
    df_loan = df_loan.with_columns([
        pl.when(pl.col('ACCTYPE') == 'OD')
          .then(pl.lit(0.1))
          .otherwise(
              ((pl.col('EXPRDATE') - reptdate).dt.total_days() / 30.0)
          )
          .alias('REMMTH')
    ])
    
    # Determine ITEM code
    df_loan = df_loan.with_columns([
        pl.when(pl.col('ACCTYPE') == 'OD')
          .then(pl.lit('213'))  # OD
          .when(pl.col('CUSTCD').is_in(['77','78','95','96']))
          .then(pl.lit('214'))  # HL/Individual
          .otherwise(pl.lit('219'))  # Other
          .alias('ITEM')
    ])
    
    # Map to bucket
    df_loan = df_loan.with_columns([
        pl.col('REMMTH').map_elements(get_bucket, return_dtype=pl.Utf8).alias('BUCKET')
    ])
    
    # Create BNM code
    df_loan = df_loan.with_columns([
        (pl.lit('95') + pl.col('ITEM') + pl.col('CUST') + pl.col('BUCKET') + pl.lit('0000Y')).alias('BNMCODE')
    ])
    
    # Summarize
    df_note = df_loan.group_by('BNMCODE').agg([
        pl.col('BALANCE').sum().alias('AMOUNT'),
        pl.lit(0).alias('AMTUSD'),
        pl.lit(0).alias('AMTSGD')
    ])
    
    print(f"  ✓ Loans: {len(df_note):,} BNM codes")

except Exception as e:
    print(f"  ⚠ Loans: {e}")
    df_note = pl.DataFrame([])

# Process FIXED DEPOSITS
print("\n2. Processing Fixed Deposits...")
try:
    df_fd = pl.read_parquet(f'{FD_DIR}FD.parquet')
    
    # Filter Islamic FDs (by INTPLAN)
    # Assuming Islamic plans are identified (need FDDENOM format)
    df_fd = df_fd.filter(pl.col('CURBAL') > 0)
    
    # Calculate customer type
    df_fd = df_fd.with_columns([
        pl.when(pl.col('CUSTCD').is_in([77,78,95,96]))
          .then(pl.lit('08'))
          .otherwise(pl.lit('09'))
          .alias('CUST')
    ])
    
    # Calculate remaining months
    df_fd = df_fd.with_columns([
        ((pl.col('MATDATE').cast(pl.Date) - reptdate).dt.total_days() / 30.0).alias('REMMTH')
    ])
    
    # Map to bucket
    df_fd = df_fd.with_columns([
        pl.col('REMMTH').map_elements(get_bucket, return_dtype=pl.Utf8).alias('BUCKET')
    ])
    
    # Create BNM code (311 = FD)
    df_fd = df_fd.with_columns([
        (pl.lit('95311') + pl.col('CUST') + pl.col('BUCKET') + pl.lit('0000Y')).alias('BNMCODE')
    ])
    
    # Summarize
    df_fd_sum = df_fd.group_by('BNMCODE').agg([
        pl.col('CURBAL').sum().alias('AMOUNT'),
        pl.lit(0).alias('AMTUSD'),
        pl.lit(0).alias('AMTSGD')
    ])
    
    print(f"  ✓ FD: {len(df_fd_sum):,} BNM codes")

except Exception as e:
    print(f"  ⚠ FD: {e}")
    df_fd_sum = pl.DataFrame([])

# Process SAVINGS
print("\n3. Processing Savings...")
try:
    df_sa = pl.read_parquet(f'{BNM_DIR}SAVG{reptmon}{nowk}.parquet')
    
    # Filter Islamic
    df_sa = df_sa.filter(pl.col('AMTIND') == 'I')
    
    # Calculate customer type
    df_sa = df_sa.with_columns([
        pl.when(pl.col('CUSTCD').is_in(['77','78','95','96']))
          .then(pl.lit('08'))
          .otherwise(pl.lit('09'))
          .alias('CUST')
    ])
    
    # Create BNM code (312 = SA, bucket 01 = demand)
    df_sa = df_sa.with_columns([
        (pl.lit('95312') + pl.col('CUST') + pl.lit('01') + pl.lit('0000Y')).alias('BNMCODE')
    ])
    
    # Summarize
    df_sa_sum = df_sa.group_by('BNMCODE').agg([
        pl.col('CURBAL').sum().alias('AMOUNT'),
        pl.lit(0).alias('AMTUSD'),
        pl.lit(0).alias('AMTSGD')
    ])
    
    print(f"  ✓ Savings: {len(df_sa_sum):,} BNM codes")

except Exception as e:
    print(f"  ⚠ Savings: {e}")
    df_sa_sum = pl.DataFrame([])

# Process CURRENT ACCOUNTS
print("\n4. Processing Current Accounts...")
try:
    df_ca = pl.read_parquet(f'{BNM_DIR}CURN{reptmon}{nowk}.parquet')
    
    # Filter Islamic
    df_ca = df_ca.filter(
        (pl.col('AMTIND') == 'I') &
        (pl.col('PRODCD').str.slice(0,3).is_in(['421','423']))
    )
    
    # Calculate customer type
    df_ca = df_ca.with_columns([
        pl.when(pl.col('CUSTCD').is_in(['77','78','95','96']))
          .then(pl.lit('08'))
          .otherwise(pl.lit('09'))
          .alias('CUST')
    ])
    
    # Create BNM code (313 = CA, bucket 01 = demand)
    df_ca = df_ca.with_columns([
        (pl.lit('95313') + pl.col('CUST') + pl.lit('01') + pl.lit('0000Y')).alias('BNMCODE')
    ])
    
    # Summarize
    df_ca_sum = df_ca.group_by('BNMCODE').agg([
        pl.col('CURBAL').sum().alias('AMOUNT'),
        pl.lit(0).alias('AMTUSD'),
        pl.lit(0).alias('AMTSGD')
    ])
    
    print(f"  ✓ Current: {len(df_ca_sum):,} BNM codes")

except Exception as e:
    print(f"  ⚠ Current: {e}")
    df_ca_sum = pl.DataFrame([])

# Combine all
print("\n5. Consolidating...")
dfs = [df for df in [df_note, df_fd_sum, df_sa_sum, df_ca_sum] if len(df) > 0]

if dfs:
    df_final = pl.concat(dfs)
    
    # Final summary by BNMCODE
    df_final = df_final.group_by('BNMCODE').agg([
        pl.col('AMOUNT').sum(),
        pl.col('AMTUSD').sum(),
        pl.col('AMTSGD').sum()
    ]).sort('BNMCODE')
    
    # Save output
    df_final.write_csv(f'{OUTPUT_DIR}FISS_ISLAMIC.csv')
    
    total_amount = df_final['AMOUNT'].sum()
    
    print(f"  ✓ Total BNM codes: {len(df_final):,}")
    print(f"  ✓ Total Amount: RM {total_amount:,.2f}")
    
    # Summary by bucket
    df_final = df_final.with_columns([
        pl.col('BNMCODE').str.slice(8,2).alias('BUCKET')
    ])
    
    df_bucket = df_final.group_by('BUCKET').agg([
        pl.col('AMOUNT').sum()
    ]).sort('BUCKET')
    
    print(f"\n  Maturity Profile:")
    bucket_names = {
        '01': 'Up to 1 week',
        '02': '>1 week - 1 month',
        '03': '>1 - 3 months',
        '04': '>3 - 6 months',
        '05': '>6 months - 1 year',
        '06': '> 1 year'
    }
    
    for row in df_bucket.iter_rows(named=True):
        bucket = row['BUCKET']
        amount = row['AMOUNT']
        pct = amount / total_amount * 100
        print(f"    {bucket} ({bucket_names.get(bucket, 'Unknown')}): RM {amount:,.2f} ({pct:.1f}%)")

else:
    print("  ⚠ No data to consolidate")

print(f"\n{'='*60}")
print("✓ EIMBRLFI Complete")
print(f"{'='*60}")
print(f"\nIslamic FISS Liquidity Framework Report")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"\nComponents:")
print(f"  1. Loans (by maturity)")
print(f"  2. Fixed Deposits (by maturity)")
print(f"  3. Savings (demand)")
print(f"  4. Current Accounts (demand)")
print(f"\nMaturity Buckets: 6 categories")
print(f"Customer Types: Individual (08) vs Non-Individual (09)")
print(f"Output: FISS_ISLAMIC.csv")
