"""
EIIMRM04 - Islamic Loan Repricing Gap Report

Purpose: Interest rate risk analysis via repricing gap
- Fixed rate vs BLR (Base Lending Rate) loans
- Maturity buckets for repricing schedule
- Instalment vs repricing classification
- Weighted average yield calculation

Key Components:
1. Classify loans by interest type (Fixed/BLR)
2. Calculate repricing dates
3. Breakdown by maturity bucket
4. Weighted average yield calculation

Subtypes:
5 = Principal, 5.5 = WAREMM, 6 = Unearned Int, 7 = Accrued Int,
8 = Fee Amount, 9 = NPL, 11 = Instalment, 12 = Repricing, 13 = No-Reprice
"""

import polars as pl
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os

# Directories
LNNOTE_DIR = 'data/lnnote/'
BNM_DIR = 'data/bnm/'
OD_DIR = 'data/od/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIIMRM04 - Islamic Loan Repricing Gap Report")
print("=" * 60)

# Read REPTDATE
try:
    df_reptdate = pl.read_parquet(f'{LNNOTE_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    
    # Week determination
    day = reptdate.day
    if day == 8:
        nowk, wk1 = '1', '4'
    elif day == 15:
        nowk, wk1 = '2', '1'
    elif day == 22:
        nowk, wk1 = '3', '2'
    else:
        nowk, wk1 = '4', '3'
    
    reptmon = f'{reptdate.month:02d}'
    rdate = reptdate.strftime('%d%m%y')
    
    print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
    print(f"Week: {nowk}")

except Exception as e:
    print(f"✗ ERROR: {e}")
    import sys
    sys.exit(1)

print("=" * 60)

# Product formats (PBBLNFMT) - Simplified for Islamic products
ISLAMIC_PRODUCTS = list(range(110, 200)) + [131, 132, 720, 725]

# Helper: Calculate remaining months
def calc_remmth(maturity_date, report_date):
    """Calculate remaining months to maturity"""
    if maturity_date <= report_date:
        return 0
    delta = relativedelta(maturity_date, report_date)
    return delta.years * 12 + delta.months + delta.days / 30

# Helper: Categorize maturity
def categorize_remmth(remmth):
    """Map remaining months to bucket"""
    if remmth < 1:
        return '> 0-1 MTH'
    elif remmth < 24:
        m = int(remmth)
        return f'> {m}-{m+1} MTHS'
    elif remmth < 36:
        return '>2-3 YRS'
    elif remmth < 48:
        return '>3-4 YRS'
    elif remmth < 60:
        return '>4-5 YRS'
    else:
        return '>5 YRS'

# Process LOANS
print("\n1. Processing Islamic Loans...")
try:
    df_loan = pl.read_parquet(f'{BNM_DIR}LOAN{reptmon}{nowk}.parquet')
    
    # Filter Islamic loans
    df_loan = df_loan.filter(
        (pl.col('PRODCD').str.slice(0,2).is_in(['34','54'])) &
        (~pl.col('PRODUCT').is_in([700,705,380,381,128,130,500,520]))
    )
    
    print(f"  ✓ Loans: {len(df_loan):,} accounts")
    
    # Read supplementary data
    try:
        df_lnnote = pl.read_parquet(f'{LNNOTE_DIR}LNNOTE.parquet')
        df_loan = df_loan.join(df_lnnote, on=['ACCTNO','NOTENO'], how='left')
    except:
        print("  ⚠ LNNOTE not found, using loan data only")
    
    # Classify interest type
    df_loan = df_loan.with_columns([
        pl.when(
            (pl.col('NTINDEX').is_in([1,30,997])) |
            ((pl.col('ACCTYPE') == 'OD') & (pl.col('AMTIND') != 'I'))
        )
        .then(pl.lit('BLR'))
        .when(
            (pl.col('NTINDEX') != 1) |
            ((pl.col('ACCTYPE') == 'OD') & (pl.col('AMTIND') == 'I'))
        )
        .then(pl.lit('FIX'))
        .otherwise(pl.lit('OTH'))
        .alias('INTTYPE')
    ])
    
    # Override for specific products
    df_loan = df_loan.with_columns([
        pl.when(pl.col('PRODUCT').is_in([350,910,925,302,902,903,951]))
          .then(pl.lit('FIX'))
          .otherwise(pl.col('INTTYPE'))
          .alias('INTTYPE')
    ])
    
    # Calculate maturity and repricing
    df_loan = df_loan.with_columns([
        pl.when(pl.col('ACCTYPE') == 'LN')
          .then(pl.col('EXPRDATE'))
          .otherwise(pl.col('LMTEND'))
          .alias('MATDT')
    ])
    
    # Calculate remaining months
    df_loan = df_loan.with_columns([
        ((pl.col('MATDT') - reptdate).dt.total_days() / 30.0).alias('REMMTH')
    ])
    
    # Categorize
    df_loan = df_loan.with_columns([
        pl.col('REMMTH').map_elements(categorize_remmth, return_dtype=pl.Utf8).alias('REMMTH1')
    ])
    
    # Calculate yield
    df_loan = df_loan.with_columns([
        (pl.col('CURBAL') * pl.col('INTRATE')).alias('YIELD')
    ])

except Exception as e:
    print(f"  ✗ Loans: {e}")
    df_loan = pl.DataFrame([])
    import sys
    sys.exit(1)

# Create analysis datasets
print("\n2. Analyzing by subtypes...")

# Initialize results
results_fix = []
results_blr = []

# Subtype 5: Principal
df_principal = df_loan.with_columns([
    pl.lit(5).alias('SUBTYP'),
    pl.col('CURBAL').alias('AMOUNT'),
    (pl.col('CURBAL') * pl.col('INTRATE')).alias('YIELD')
])

# Subtype 5.5: WAREMM (Weighted Average Remaining Maturity)
df_waremm = df_loan.with_columns([
    pl.lit(5.5).alias('SUBTYP'),
    pl.col('CURBAL').alias('AMOUNT'),
    (pl.col('CURBAL') * pl.col('REMMTH')).alias('YIELD')
])

# Subtype 6: Unearned Interest
df_unearn = df_loan.with_columns([
    pl.lit(6).alias('SUBTYP'),
    pl.when(pl.col('NTINT') == 'A')
      .then(pl.col('INTAMT') - pl.col('INTEARN2') + pl.col('INTEARN3'))
      .otherwise(0)
      .alias('AMOUNT'),
    pl.lit(0.0).alias('YIELD')
])

# Subtype 7: Accrued Interest
df_acrint = df_loan.with_columns([
    pl.lit(7).alias('SUBTYP'),
    pl.when(pl.col('NTINT') == 'A')
      .then(pl.col('INTEARN'))
      .otherwise(pl.col('BALANCE') - pl.col('CURBAL') - pl.col('FEEAMT'))
      .alias('AMOUNT'),
    pl.lit(0.0).alias('YIELD')
])

# Subtype 8: Fee Amount
df_fee = df_loan.with_columns([
    pl.lit(8).alias('SUBTYP'),
    pl.col('FEEAMT').alias('AMOUNT'),
    pl.lit(0.0).alias('YIELD')
])

# Subtype 9: NPL
df_npl = df_loan.filter(pl.col('RISKRTE').is_in([1,2,3,4])).with_columns([
    pl.lit(9).alias('SUBTYP'),
    pl.col('CURBAL').alias('AMOUNT'),
    pl.lit(0.0).alias('YIELD')
])

# Subtype 11: Instalment (for term loans)
df_inst = df_loan.filter(pl.col('ACCTYPE') == 'LN').with_columns([
    pl.lit(11).alias('SUBTYP'),
    pl.col('PAYAMT').alias('AMOUNT'),
    (pl.col('PAYAMT') * pl.col('INTRATE')).alias('YIELD')
])

# Subtype 12: Repricing
df_repric = df_loan.filter(
    (pl.col('REPRICDT').is_not_null()) |
    (pl.col('PRODUCT').is_in([350,910,925,302,902,903,951])) |
    (pl.col('INTTYPE') == 'BLR')
).with_columns([
    pl.lit(12).alias('SUBTYP'),
    pl.col('CURBAL').alias('AMOUNT'),
    (pl.col('CURBAL') * pl.col('INTRATE')).alias('YIELD'),
    pl.when(pl.col('INTTYPE') == 'BLR')
      .then(pl.lit('> 0-1 MTH'))
      .otherwise(pl.col('REMMTH1'))
      .alias('REMMTH1')
])

# Subtype 13: No-Reprice
df_norepric = df_loan.filter(
    (pl.col('REPRICDT').is_null()) &
    (pl.col('NTINDEX') != 1) &
    (~pl.col('PRODUCT').is_in([350,910,925,302,902,903,951])) &
    (pl.col('INTTYPE') != 'BLR')
).with_columns([
    pl.lit(13).alias('SUBTYP'),
    pl.col('CURBAL').alias('AMOUNT'),
    (pl.col('CURBAL') * pl.col('INTRATE')).alias('YIELD')
])

# Combine all subtypes
df_all = pl.concat([
    df_principal, df_waremm, df_unearn, df_acrint,
    df_fee, df_npl, df_inst, df_repric, df_norepric
])

# Split by interest type
print("\n3. Summarizing by interest type...")

# Fixed Rate Loans
df_fix = df_all.filter(pl.col('INTTYPE') == 'FIX').group_by(['PRODUCT','SUBTYP','REMMTH1']).agg([
    pl.col('AMOUNT').sum(),
    pl.col('YIELD').sum()
])

df_fix = df_fix.with_columns([
    pl.when(pl.col('AMOUNT') > 0)
      .then(pl.col('YIELD') / pl.col('AMOUNT'))
      .otherwise(0)
      .alias('WAYLD')
])

# BLR Loans
df_blr = df_all.filter(pl.col('INTTYPE') == 'BLR').group_by(['PRODUCT','SUBTYP','REMMTH1']).agg([
    pl.col('AMOUNT').sum(),
    pl.col('YIELD').sum()
])

df_blr = df_blr.with_columns([
    pl.when(pl.col('AMOUNT') > 0)
      .then(pl.col('YIELD') / pl.col('AMOUNT'))
      .otherwise(0)
      .alias('WAYLD')
])

# Save outputs
df_fix.write_csv(f'{OUTPUT_DIR}REPRICING_FIXED.csv')
df_blr.write_csv(f'{OUTPUT_DIR}REPRICING_BLR.csv')

total_fix = df_fix['AMOUNT'].sum()
total_blr = df_blr['AMOUNT'].sum()

print(f"  ✓ Fixed Rate: RM {total_fix:,.2f}")
print(f"  ✓ BLR: RM {total_blr:,.2f}")
print(f"  ✓ Total: RM {total_fix + total_blr:,.2f}")

# Summary by subtype
print(f"\n  Breakdown by Subtype:")
subtype_names = {
    5: 'Principal',
    5.5: 'WAREMM(MTH)',
    6: 'Unearn Int',
    7: 'Accrued Int',
    8: 'Fee Amount',
    9: 'NPL',
    11: 'Instalment',
    12: 'Repricing',
    13: 'No-Reprice'
}

df_subtype = df_all.group_by('SUBTYP').agg([
    pl.col('AMOUNT').sum()
])

for row in df_subtype.iter_rows(named=True):
    subtyp = row['SUBTYP']
    amount = row['AMOUNT']
    name = subtype_names.get(subtyp, f'Type {subtyp}')
    print(f"    {name}: RM {amount:,.2f}")

print(f"\n{'='*60}")
print("✓ EIIMRM04 Complete")
print(f"{'='*60}")
print(f"\nIslamic Loan Repricing Gap Report")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"\nAnalysis Types:")
print(f"  - Fixed Rate Loans: Repricing based on contract terms")
print(f"  - BLR Loans: Immediate repricing (0-1 month)")
print(f"\nMaturity Buckets:")
print(f"  Monthly: > 0-1 MTH to > 23-24 MTHS")
print(f"  Yearly: >2-3 YRS to >5 YRS")
print(f"\nSubtypes:")
print(f"  Principal, WAREMM, Instalment, Repricing,")
print(f"  No-Reprice, Unearned Int, Accrued Int,")
print(f"  Fee Amount, NPL")
print(f"\nOutputs:")
print(f"  - REPRICING_FIXED.csv")
print(f"  - REPRICING_BLR.csv")
