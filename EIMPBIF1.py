"""
EIMPBIF1 - Factoring Report (Production Ready)

Purpose:
- Generate factoring business reports
- Summarize factoring disbursements, repayments, and balances
- Produce overall factoring report and SME-specific report

Factoring:
- Invoice financing product
- Business advances money against outstanding invoices
- Short-term working capital solution

Report Sections:
1. Overall Factoring Summary
   - All factoring accounts
   - Disbursements, repayments, balances
   - Account counts

2. SME Factoring Summary
   - Filtered by SME customer codes (41-54)
   - Same metrics as overall

Customer Codes (SME):
- 41, 42, 43, 44, 46, 47, 48, 49
- 51, 52, 53, 54

Week Assignment (same as EIBWWKLY):
- Day 8:  Week 1 (WK='1', WK1='4')
- Day 15: Week 2 (WK='2', WK1='1', WK2='2', WK3='1')
- Day 22: Week 3 (WK='3', WK1='2')
- Day 29+: Week 4 (WK='4', WK1='3', WK2='2', WK3='1')

Input:
- PBIF.REPTDATE: Report date
- PBIF dataset: Factoring data (from RDLMPBIF)

Output:
- Overall factoring summary report
- SME factoring summary report
"""

import polars as pl
from datetime import datetime
import os

# Directories
PBIF_DIR = 'data/pbif/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIMPBIF1 - Factoring Report")
print("=" * 60)

# Read REPTDATE
print("\nReading REPTDATE...")
try:
    df_reptdate = pl.read_parquet(f'{PBIF_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    
    day = reptdate.day
    month = reptdate.month
    year = reptdate.year
    
    # Determine week based on day
    if day == 8:
        sdd = 1
        wk = '1'
        wk1 = '4'
        wk2 = None
        wk3 = None
    elif day == 15:
        sdd = 9
        wk = '2'
        wk1 = '1'
        wk2 = None
        wk3 = None
    elif day == 22:
        sdd = 16
        wk = '3'
        wk1 = '2'
        wk2 = None
        wk3 = None
    else:  # Day 29-31
        sdd = 23
        wk = '4'
        wk1 = '3'
        wk2 = '2'
        wk3 = '1'
    
    # Month references
    if wk == '1':
        mm1 = month - 1 if month > 1 else 12
    else:
        mm1 = month
    
    mm2 = month - 1 if month > 1 else 12
    
    # Calculate start date
    sdate = datetime(year, month, sdd)
    
    # Format variables
    reptmon = f'{month:02d}'
    reptmon1 = f'{mm1:02d}'
    reptmon2 = f'{mm2:02d}'
    reptyear = f'{year % 100:02d}'
    reptday = f'{day:02d}'
    mdate = f'{year % 100:02d}{month:02d}{day:02d}'
    rdate = reptdate.strftime('%d%m%y')
    sdate_str = sdate.strftime('%d%m%y')
    
    print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
    print(f"Week: {wk}, Previous Week: {wk1}")

except Exception as e:
    print(f"  ⚠ REPTDATE: {e}")
    import sys
    sys.exit(1)

print("=" * 60)

# Read PBIF factoring data
print("\nReading factoring data...")
try:
    # In production, this would come from RDLMPBIF program
    # For now, we'll read from a parquet file
    df_pbif = pl.read_parquet(f'{PBIF_DIR}PBIF.parquet')
    
    # Add derived fields
    df_pbif = df_pbif.with_columns([
        pl.col('INLIMIT').alias('APPRLIMX'),
        pl.lit('FACTORING').alias('PRODESC'),
        pl.lit(0).alias('DISBNO'),
        pl.lit(0).alias('REPAYNO'),
        pl.when(pl.col('BALANCE') > 0).then(pl.lit(1)).otherwise(pl.lit(0)).alias('NOACCT')
    ])
    
    # Set disbursement flag
    df_pbif = df_pbif.with_columns([
        pl.when(pl.col('DISBURSE') > 0).then(pl.lit(1)).otherwise(pl.col('DISBNO')).alias('DISBNO')
    ])
    
    # Set repayment flag
    df_pbif = df_pbif.with_columns([
        pl.when(pl.col('REPAID') > 0).then(pl.lit(1)).otherwise(pl.col('REPAYNO')).alias('REPAYNO')
    ])
    
    print(f"  ✓ PBIF: {len(df_pbif):,} records")

except Exception as e:
    print(f"  ⚠ PBIF: {e}")
    import sys
    sys.exit(1)

# Summarize by PRODESC and CUSTCX
print("\nSummarizing by customer code...")
df_alm = df_pbif.group_by(['PRODESC', 'CUSTCX']).agg([
    pl.col('DISBURSE').sum().alias('DISBURSE'),
    pl.col('REPAID').sum().alias('REPAID'),
    pl.col('DISBNO').sum().alias('DISBNO'),
    pl.col('REPAYNO').sum().alias('REPAYNO'),
    pl.col('BALANCE').sum().alias('BALANCE'),
    pl.col('NOACCT').sum().alias('NOACCT')
])

print(f"  ✓ Customer summary: {len(df_alm):,} records")

# Overall summary
print("\nGenerating overall factoring summary...")
df_alm1 = df_alm.group_by('PRODESC').agg([
    pl.col('DISBURSE').sum().alias('DISBURSE'),
    pl.col('REPAID').sum().alias('REPAID'),
    pl.col('DISBNO').sum().alias('DISBNO'),
    pl.col('REPAYNO').sum().alias('REPAYNO'),
    pl.col('BALANCE').sum().alias('BALANCE'),
    pl.col('NOACCT').sum().alias('NOACCT')
])

print(f"\nFactoring Summary (Overall):")
print(f"  Product: FACTORING")
for row in df_alm1.iter_rows(named=True):
    print(f"  Disbursements: RM {row['DISBURSE']:,.2f} ({row['DISBNO']:,} accounts)")
    print(f"  Repayments:    RM {row['REPAID']:,.2f} ({row['REPAYNO']:,} accounts)")
    print(f"  Balance:       RM {row['BALANCE']:,.2f} ({row['NOACCT']:,} accounts)")

# SME factoring summary
print("\nFiltering SME customers...")
sme_codes = ['41', '42', '43', '44', '46', '47', '48', '49', 
             '51', '52', '53', '54']

df_almsme = df_alm.filter(pl.col('CUSTCX').is_in(sme_codes))
print(f"  ✓ SME records: {len(df_almsme):,}")

df_almsme_summary = df_almsme.group_by('PRODESC').agg([
    pl.col('DISBURSE').sum().alias('DISBURSE'),
    pl.col('REPAID').sum().alias('REPAID'),
    pl.col('DISBNO').sum().alias('DISBNO'),
    pl.col('REPAYNO').sum().alias('REPAYNO'),
    pl.col('BALANCE').sum().alias('BALANCE'),
    pl.col('NOACCT').sum().alias('NOACCT')
])

print(f"\nSME Factoring Summary:")
print(f"  Product: FACTORING (SME)")
for row in df_almsme_summary.iter_rows(named=True):
    print(f"  Disbursements: RM {row['DISBURSE']:,.2f} ({row['DISBNO']:,} accounts)")
    print(f"  Repayments:    RM {row['REPAID']:,.2f} ({row['REPAYNO']:,} accounts)")
    print(f"  Balance:       RM {row['BALANCE']:,.2f} ({row['NOACCT']:,} accounts)")

# Generate CSV reports
print("\nGenerating CSV reports...")

# Overall report
df_alm1.write_csv(f'{OUTPUT_DIR}FACTORING_{reptmon}{reptyear}.csv')
print(f"  ✓ FACTORING_{reptmon}{reptyear}.csv")

# SME report
df_almsme_summary.write_csv(f'{OUTPUT_DIR}SME_FACTORING_{reptmon}{reptyear}.csv')
print(f"  ✓ SME_FACTORING_{reptmon}{reptyear}.csv")

print(f"\n{'='*60}")
print(f"✓ EIMPBIF1 Complete!")
print(f"{'='*60}")
print(f"\nFactoring Report Summary:")
print(f"  Report Date: {reptmon}/{reptyear}")
print(f"  Week: {wk}")
print(f"\nReports Generated:")
print(f"  1. Overall Factoring Summary")
print(f"     - All factoring accounts")
print(f"     - Total disbursements, repayments, balances")
print(f"  2. SME Factoring Summary")
print(f"     - Customer codes: {', '.join(sme_codes)}")
print(f"     - SME-specific metrics")
print(f"\nMetrics:")
print(f"  DISBURSE: Total disbursements (new factoring)")
print(f"  REPAID:   Total repayments (collections)")
print(f"  BALANCE:  Outstanding balance")
print(f"  DISBNO:   Number of disbursement accounts")
print(f"  REPAYNO:  Number of repayment accounts")
print(f"  NOACCT:   Number of accounts with balance")
