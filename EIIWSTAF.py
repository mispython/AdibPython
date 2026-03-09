"""
EIIWSTAF - Islamic Staff Loan Weekly Report (PIBB)

Purpose: Weekly staff loan activity reports for Islamic banking
- Settled loans for the week
- New loans for the week
- Migration loans (restructured/converted)
- Full release loans

Filter: Staff loans only (COSTCTR 3000-3999 for PIBB, 8044 for PBB)
Output: 4 separate reports with summary totals
"""

import polars as pl
from datetime import datetime, timedelta
import os

# Directories
MNILN_DIR = 'data/mniln/'
IMNILN_DIR = 'data/imniln/'
PAY_DIR = 'data/pay/'
IPAY_DIR = 'data/ipay/'
LNHIST_DIR = 'data/lnhist/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LNHIST_DIR, exist_ok=True)

print("EIIWSTAF - Islamic Staff Loan Weekly Report")
print("=" * 60)

# Read REPTDATE
try:
    df_reptdate = pl.read_parquet(f'{MNILN_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    
    # Week determination
    day = reptdate.day
    if day == 8:
        sdd, nowk = 1, '01'
    elif day == 15:
        sdd, nowk = 9, '02'
    elif day == 22:
        sdd, nowk = 16, '03'
    else:
        sdd, nowk = 23, '04'
    
    # Date calculations
    mmp = reptdate.month
    yyp = reptdate.year
    pdate = datetime(yyp, mmp, 1)
    sdate = datetime(yyp, mmp, sdd)
    edate = reptdate
    
    # Previous month calculation
    prevdate = pdate - timedelta(days=1)
    
    rdate = reptdate.strftime('%d/%m/%Y')
    reptmon = f'{mmp:02d}'
    reptday = day
    reptmth = mmp
    reptyear = yyp
    
    print(f"Report Date: {rdate}")
    print(f"Week: {nowk}, Period: {sdate.strftime('%d/%m/%Y')} to {edate.strftime('%d/%m/%Y')}")

except Exception as e:
    print(f"✗ ERROR: {e}")
    import sys
    sys.exit(1)

print("=" * 60)

# Process PBB Staff Loans
print("\n1. Processing PBB Staff Loans...")
try:
    df_lnnote_pbb = pl.read_parquet(f'{MNILN_DIR}LNNOTE.parquet')
    df_lncomm_pbb = pl.read_parquet(f'{MNILN_DIR}LNCOMM.parquet')
    
    # Filter staff loans
    df_lnnote_pbb = df_lnnote_pbb.filter(
        ((pl.col('LOANTYPE') <= 61) | pl.col('LOANTYPE').is_in([100,102,103,104,105])) &
        (pl.col('COSTCTR') == 8044)
    )
    
    # Merge with commitment
    df_pbb = df_lnnote_pbb.join(df_lncomm_pbb, on=['ACCTNO','COMMNO'], how='left')
    
    # Calculate approved limit
    df_pbb = df_pbb.with_columns([
        pl.when(pl.col('COMMNO') > 0)
          .then(
              pl.when(pl.col('REVOVLI') == 'N')
                .then(pl.col('CORGAMT'))
                .otherwise(pl.col('CCURAMT'))
          )
          .otherwise(pl.col('ORGBAL'))
          .alias('APPRLIMT')
    ])
    
    print(f"  ✓ PBB Staff Loans: {len(df_pbb):,} accounts")

except Exception as e:
    print(f"  ⚠ PBB: {e}")
    df_pbb = pl.DataFrame([])

# Process PIBB (Islamic) Staff Loans
print("\n2. Processing PIBB Staff Loans...")
try:
    df_lnnote_pibb = pl.read_parquet(f'{IMNILN_DIR}LNNOTE.parquet')
    df_lncomm_pibb = pl.read_parquet(f'{IMNILN_DIR}LNCOMM.parquet')
    
    # Filter Islamic staff loans
    df_lnnote_pibb = df_lnnote_pibb.filter(
        ((pl.col('LOANTYPE') <= 61) | pl.col('LOANTYPE').is_in([100,102,103,104,105])) &
        (pl.col('COSTCTR') >= 3000) &
        (pl.col('COSTCTR') <= 3999)
    )
    
    # Merge with commitment
    df_pibb = df_lnnote_pibb.join(df_lncomm_pibb, on=['ACCTNO','COMMNO'], how='left')
    
    # Calculate approved limit
    df_pibb = df_pibb.with_columns([
        pl.when(pl.col('COMMNO') > 0)
          .then(
              pl.when(pl.col('REVOVLI') == 'N')
                .then(pl.col('CORGAMT'))
                .otherwise(pl.col('CCURAMT'))
          )
          .otherwise(pl.col('ORGBAL'))
          .alias('APPRLIMT')
    ])
    
    print(f"  ✓ PIBB Staff Loans: {len(df_pibb):,} accounts")

except Exception as e:
    print(f"  ⚠ PIBB: {e}")
    df_pibb = pl.DataFrame([])

# Combine PBB + PIBB
if len(df_pbb) > 0 or len(df_pibb) > 0:
    df_loan = pl.concat([df for df in [df_pbb, df_pibb] if len(df) > 0])
    print(f"\n  ✓ Total Staff Loans: {len(df_loan):,} accounts")
else:
    print("  ✗ No staff loans found")
    import sys
    sys.exit(1)

# Add derived fields
df_loan = df_loan.with_columns([
    pl.lit(1).alias('NOOFAC')
])

# Report 1: SETTLED LOANS
print("\n3. Generating Settled Loans Report...")
df_settled = df_loan.filter(
    (pl.col('PAIDIND').is_in(['P','C'])) &
    (pl.col('LASTTRAN').dt.day() >= sdd) &
    (pl.col('LASTTRAN').dt.day() <= reptday) &
    (pl.col('LASTTRAN').dt.month() == reptmth) &
    (pl.col('LASTTRAN').dt.year() == reptyear)
)

df_settled = df_settled.with_columns([
    pl.when(pl.col('LSTTRNCD') == 652)
      .then(pl.lit('LAST TRANCODE EQ 652'))
      .otherwise(pl.lit('LAST TRANCODE NE 652'))
      .alias('LSTRNDSC')
])

if len(df_settled) > 0:
    df_settled.write_csv(f'{OUTPUT_DIR}STAFF_SETTLED.csv')
    total_settled = df_settled['APPRLIMT'].sum()
    count_settled = len(df_settled)
    print(f"  ✓ Settled: {count_settled:,} accounts, RM {total_settled:,.2f}")
else:
    print("  ⚠ No settled loans this week")

# Report 2 & 3: NEW & MIGRATION LOANS
print("\n4. Identifying New & Migration Loans...")

# Read historical base
try:
    df_hist = pl.read_parquet(f'{LNHIST_DIR}ISBASE.parquet')
except:
    df_hist = pl.DataFrame({'ACCTNO': [], 'NOTENO': []})

# New releases (not in history)
df_new = df_loan.join(
    df_hist.select(['ACCTNO','NOTENO']), 
    on=['ACCTNO','NOTENO'], 
    how='anti'
)

# Filter by date range
df_new = df_new.filter(
    ((pl.col('ISSDTE') >= pdate) & (pl.col('ISSDTE') <= edate)) |
    ((pl.col('FULRELDTE') >= pdate) & (pl.col('FULRELDTE') <= edate) & 
     (pl.col('FLAG1') == 'M') & (pl.col('RESTIND') == 'M'))
)

# Classify: Migration vs New
df_migration = df_new.filter(
    (pl.col('FLAG1') == 'M') & (pl.col('RESTIND') == 'M')
)

df_new_only = df_new.filter(
    (pl.col('FLAG1') != 'M') | (pl.col('RESTIND') != 'M')
)

# Read payment data
try:
    df_pay_pbb = pl.read_parquet(f'{PAY_DIR}LNPAY{nowk}.parquet')
    df_pay_pibb = pl.read_parquet(f'{IPAY_DIR}ILNPAY{nowk}.parquet')
    df_pay = pl.concat([df_pay_pbb, df_pay_pibb]).filter(pl.col('PAYAMT') != 0)
    
    # Format payment effective date
    df_pay = df_pay.with_columns([
        (pl.lit('99/') + 
         pl.col('EFFDATE').dt.strftime('%m/') + 
         pl.col('EFFDATE').dt.strftime('%y')).alias('PAYEFF')
    ])
    
    df_pay = df_pay.select(['ACCTNO','NOTENO','PAYEFF','PAYAMT']).unique()
    
    # Merge payments
    df_new_only = df_new_only.join(df_pay, on=['ACCTNO','NOTENO'], how='left')
    df_migration = df_migration.join(df_pay, on=['ACCTNO','NOTENO'], how='left')
    
except:
    print("  ⚠ Payment data not found")

if len(df_new_only) > 0:
    df_new_only.write_csv(f'{OUTPUT_DIR}STAFF_NEW.csv')
    count_new = len(df_new_only)
    total_new = df_new_only['APPRLIMT'].sum()
    print(f"  ✓ New: {count_new:,} accounts, RM {total_new:,.2f}")
else:
    print("  ⚠ No new loans this week")

if len(df_migration) > 0:
    df_migration.write_csv(f'{OUTPUT_DIR}STAFF_MIGRATION.csv')
    count_mig = len(df_migration)
    total_mig = df_migration['APPRLIMT'].sum()
    print(f"  ✓ Migration: {count_mig:,} accounts, RM {total_mig:,.2f}")
else:
    print("  ⚠ No migration loans this week")

# Report 4: FULL RELEASE LOANS
print("\n5. Identifying Full Release Loans...")
df_fullrel = df_new.filter(
    (pl.col('FLAG1') == 'M') & (pl.col('RESTIND') == 'M')
)

if len(df_fullrel) > 0:
    df_fullrel = df_fullrel.join(df_pay, on=['ACCTNO','NOTENO'], how='left')
    df_fullrel.write_csv(f'{OUTPUT_DIR}STAFF_FULLRELEASE.csv')
    count_rel = len(df_fullrel)
    total_rel = df_fullrel['APPRLIMT'].sum()
    print(f"  ✓ Full Release: {count_rel:,} accounts, RM {total_rel:,.2f}")
else:
    print("  ⚠ No full release loans this week")

# Update historical base
if len(df_new) > 0:
    df_new_base = df_new.select(['ACCTNO','NOTENO'])
    
    if len(df_hist) > 0:
        df_hist_updated = pl.concat([df_hist, df_new_base]).unique()
    else:
        df_hist_updated = df_new_base
    
    df_hist_updated.write_parquet(f'{LNHIST_DIR}ISBASE.parquet')
    print(f"\n  ✓ Updated historical base: {len(df_hist_updated):,} total accounts")

print(f"\n{'='*60}")
print("✓ EIIWSTAF Complete")
print(f"{'='*60}")
print(f"\nIslamic Staff Loan Weekly Report")
print(f"Report Date: {rdate}")
print(f"Week: {nowk}")
print(f"\nReports Generated:")
print(f"  1. Settled Loans - Paid off this week")
print(f"  2. New Loans - Fresh disbursements")
print(f"  3. Migration Loans - Restructured/converted")
print(f"  4. Full Release Loans - Fully disbursed")
print(f"\nFilter:")
print(f"  PBB Staff: COSTCTR = 8044")
print(f"  PIBB Staff: COSTCTR 3000-3999")
print(f"\nOutputs:")
print(f"  - STAFF_SETTLED.csv")
print(f"  - STAFF_NEW.csv")
print(f"  - STAFF_MIGRATION.csv")
print(f"  - STAFF_FULLRELEASE.csv")
