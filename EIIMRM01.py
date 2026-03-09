"""
EIIMRM01 - Islamic Time to Maturity Report

Purpose: Risk Management report showing deposit maturity profiles
- Fixed Deposits by remaining maturity (0-60 months)
- Savings and Current accounts
- Weighted Average Cost calculation
- Split: Conventional vs SPTF (Shariah-compliant)

Output: Time to maturity analysis for Islamic deposits
"""

import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os

# Directories
BNM_DIR = 'data/bnm/'
FD_DIR = 'data/fd/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIIMRM01 - Islamic Time to Maturity Report")
print("=" * 60)

# Get environment variables
reptmon = os.environ.get('REPTMON', '12')
nowk = os.environ.get('NOWK', '4')
rdate = os.environ.get('RDATE', datetime.today().strftime('%d%m%y'))

# Read REPTDATE
try:
    df_reptdate = pl.read_parquet(f'{BNM_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
except:
    reptdate = datetime.today()

print("=" * 60)

# Format mappings (PBBDPFMT)
FDPROD = {  # FD product to BIC code
    470: '42630', 471: '42630', 472: '42630',  # FCY FD
    # Add more mappings as needed
}

TERMFMT = {  # Interest plan to term in months
    470: 1, 471: 1, 476: 1, 477: 1, 482: 1, 483: 1,
    472: 3, 473: 3, 478: 3, 479: 3, 484: 3, 485: 3,
    474: 6, 475: 6, 480: 6, 481: 6, 486: 6, 487: 6
}

SAPROD = {100: '42100', 101: '42101', 204: '42204', 214: '42214', 215: '42215'}
CAPROD = {1: '42310', 101: '42310', 150: '42180', 166: '42180', 177: '42180'}

# Helper: Calculate remaining months
def calc_remmth(maturity_date, report_date):
    """Calculate remaining months to maturity"""
    if maturity_date <= report_date:
        return 99  # Overdue
    delta = relativedelta(maturity_date, report_date)
    return delta.years * 12 + delta.months + delta.days / 30

# Helper: Categorize remaining months
def categorize_remmth(remmth):
    """Map remaining months to display category"""
    if remmth == 99:
        return 'OVERDUE FD'
    elif remmth <= 0:
        return ''
    elif remmth <= 1:
        return '> 0-1 MTH'
    elif remmth <= 24:
        m = int(remmth)
        return f'> {m}-{m+1} MTHS'
    elif remmth <= 36:
        return '>2-3 YRS'
    elif remmth <= 48:
        return '>3-4 YRS'
    elif remmth <= 60:
        return '>4-5 YRS'
    else:
        return ''

# Process FIXED DEPOSITS
print("\n1. Processing Fixed Deposits...")
try:
    df_fd = pl.read_parquet(f'{FD_DIR}FD.parquet')
    
    # Filter active FDs
    df_fd = df_fd.filter(
        (pl.col('OPENIND').is_in(['O','D'])) &
        (pl.col('CURBAL') > 0)
    )
    
    # Determine product type
    df_fd = df_fd.with_columns([
        pl.col('INTPLAN').map_elements(lambda x: FDPROD.get(x, '42100'), return_dtype=pl.Utf8).alias('BNMCODE'),
        pl.col('INTPLAN').map_elements(lambda x: TERMFMT.get(x, 12), return_dtype=pl.Int64).alias('TERM')
    ])
    
    df_fd = df_fd.with_columns([
        pl.when(pl.col('BNMCODE') == '42630')
          .then(pl.lit('FIXED DEPT(FCY)'))
          .otherwise(pl.lit('FIXED DEPT(RM)'))
          .alias('PRODTYP')
    ])
    
    # Calculate remaining maturity
    df_fd = df_fd.with_columns([
        pl.col('MATDATE').cast(pl.Date).alias('MATDT')
    ])
    
    df_fd = df_fd.with_columns([
        ((pl.col('MATDT') - reptdate).dt.total_days() / 30.0).alias('REMMTH')
    ])
    
    # Determine subtype and subtitle
    df_fd = df_fd.with_columns([
        pl.when(pl.col('BNMCODE') == '42132')
          .then(pl.lit('SPTF'))
          .otherwise(pl.lit('CONVENTIONAL'))
          .alias('SUBTYP'),
        pl.when((pl.col('OPENIND') == 'D') | (pl.col('MATDT') < reptdate))
          .then(pl.lit('B'))  # Overdue
          .otherwise(pl.lit('A'))  # Remaining maturity
          .alias('SUBTTL')
    ])
    
    # Mark new FDs (within 1 month of term)
    df_fd = df_fd.with_columns([
        ((pl.col('TERM') - pl.col('REMMTH')) < 1).alias('IS_NEW')
    ])
    
    # Calculate cost and weighted metrics
    df_fd = df_fd.with_columns([
        (pl.col('CURBAL') * pl.col('RATE')).alias('COST'),
        (pl.col('CURBAL') * pl.col('REMMTH')).alias('REMM')
    ])
    
    # Categorize REMMTH
    df_fd = df_fd.with_columns([
        pl.col('REMMTH').map_elements(categorize_remmth, return_dtype=pl.Utf8).alias('REMMTH1')
    ])
    
    print(f"  ✓ FD: {len(df_fd):,} accounts")

except Exception as e:
    print(f"  ✗ FD: {e}")
    df_fd = pl.DataFrame([])

# Process SAVINGS
print("\n2. Processing Savings...")
try:
    df_sa = pl.read_parquet(f'{BNM_DIR}SAVING.parquet')
    
    df_sa = df_sa.filter(
        (~pl.col('OPENIND').is_in(['B','C','P'])) &
        (pl.col('CURBAL') >= 0)
    )
    
    # Determine subtype
    df_sa = df_sa.with_columns([
        pl.lit('SAVINGS DEPOSIT').alias('PRODTYP'),
        pl.when(pl.col('PRODUCT').is_in([204,214,215]))
          .then(pl.lit('SPTF'))
          .otherwise(pl.lit('CONVENTIONAL'))
          .alias('SUBTYP'),
        pl.when(pl.col('PRODUCT').is_in([204,214,215]))
          .then(pl.lit('D2'))  # Wadiah Saving
          .otherwise(pl.lit('D1'))  # Normal Saving
          .alias('SUBTTL'),
        pl.lit(0.0).alias('REMMTH'),
        (pl.col('CURBAL') * pl.col('INTRATE')).alias('COST'),
        (pl.col('CURBAL') * pl.lit(0.0)).alias('REMM')
    ])
    
    df_sa = df_sa.rename({'CURBAL': 'AMOUNT'})
    
    print(f"  ✓ Savings: {len(df_sa):,} accounts")

except Exception as e:
    print(f"  ✗ Savings: {e}")
    df_sa = pl.DataFrame([])

# Process CURRENT ACCOUNTS
print("\n3. Processing Current Accounts...")
try:
    df_ca = pl.read_parquet(f'{BNM_DIR}CURRENT.parquet')
    
    df_ca = df_ca.filter(
        (~pl.col('OPENIND').is_in(['B','C','P']))
    )
    
    # Classify current accounts
    df_ca = df_ca.with_columns([
        pl.when(pl.col('PRODUCT').is_in([101,103,161,163]))
          .then(pl.lit('F1/F2'))  # Interest-bearing Gov/Housing
          .when(pl.col('PRODUCT').is_in([150,151,152,181]))
          .then(pl.lit('ACE'))  # ACE accounts
          .when(pl.col('PRODUCT').is_in([60,61,62,63,64,160,162,164,165,166,182]))
          .then(pl.lit('E2'))  # Wadiah Current
          .when((pl.col('PRODUCT') >= 400) & (pl.col('PRODUCT') <= 410))
          .then(pl.lit('E3'))  # FCY Current
          .when(pl.col('PRODUCT').is_in([104,105,177,189,190,178]))
          .then(pl.lit('VOSTRO'))  # Vostro/Special
          .when(pl.col('CURBAL') <= 0)
          .then(pl.lit('E4'))  # OD
          .otherwise(pl.lit('E1'))  # Normal Current
          .alias('SUBTTL')
    ])
    
    # Calculate cost
    df_ca = df_ca.with_columns([
        pl.lit('DEMAND DEPOSIT').alias('PRODTYP'),
        pl.lit('CONVENTIONAL').alias('SUBTYP'),
        pl.lit(0.0).alias('REMMTH'),
        (pl.col('CURBAL') * pl.col('INTRATE')).alias('COST'),
        (pl.col('CURBAL') * pl.lit(0.0)).alias('REMM')
    ])
    
    df_ca = df_ca.rename({'CURBAL': 'AMOUNT'})
    
    print(f"  ✓ Current: {len(df_ca):,} accounts")

except Exception as e:
    print(f"  ✗ Current: {e}")
    df_ca = pl.DataFrame([])

# Combine all deposits
print("\n4. Consolidating...")
dfs = [df for df in [df_fd.select(['PRODTYP','SUBTYP','SUBTTL','REMMTH','AMOUNT','COST','REMM']),
                     df_sa.select(['PRODTYP','SUBTYP','SUBTTL','REMMTH','AMOUNT','COST','REMM']),
                     df_ca.select(['PRODTYP','SUBTYP','SUBTTL','REMMTH','AMOUNT','COST','REMM'])]
       if len(df) > 0]

if dfs:
    df_dep = pl.concat(dfs)
    
    # Calculate weighted average cost
    df_dep = df_dep.with_columns([
        pl.when(pl.col('AMOUNT') > 0)
          .then(pl.col('COST') / pl.col('AMOUNT'))
          .otherwise(0)
          .alias('WACOST'),
        pl.when(pl.col('AMOUNT') > 0)
          .then(pl.col('REMM') / pl.col('AMOUNT'))
          .otherwise(0)
          .alias('WAREMM'),
        (pl.col('AMOUNT') / 1000).round(0).alias('AMOUNT_K')  # Convert to thousands
    ])
    
    # Summarize
    df_summary = df_dep.group_by(['PRODTYP','SUBTYP','SUBTTL']).agg([
        pl.col('AMOUNT_K').sum().alias('AMOUNT'),
        pl.col('COST').sum(),
        pl.col('REMM').sum()
    ])
    
    df_summary = df_summary.with_columns([
        pl.when(pl.col('AMOUNT') > 0)
          .then(pl.col('COST') / (pl.col('AMOUNT') * 1000))
          .otherwise(0)
          .alias('WACOST'),
        pl.when(pl.col('AMOUNT') > 0)
          .then(pl.col('REMM') / (pl.col('AMOUNT') * 1000))
          .otherwise(0)
          .alias('WAREMM')
    ])
    
    # Save output
    df_summary.write_csv(f'{OUTPUT_DIR}MATURITY_PROFILE.csv')
    
    total = df_summary['AMOUNT'].sum()
    
    print(f"  ✓ Total deposits: RM {total:,.0f}K")
    
    # Summary by product type
    print(f"\n  Summary by Product Type:")
    df_prod = df_summary.group_by('PRODTYP').agg([
        pl.col('AMOUNT').sum()
    ])
    
    for row in df_prod.iter_rows(named=True):
        print(f"    {row['PRODTYP']}: RM {row['AMOUNT']:,.0f}K")

else:
    print("  ⚠ No data to process")

print(f"\n{'='*60}")
print("✓ EIIMRM01 Complete")
print(f"{'='*60}")
print(f"\nIslamic Time to Maturity Report")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"\nDeposit Types:")
print(f"  - Fixed Deposits (RM & FCY)")
print(f"  - Savings (Conventional & SPTF)")
print(f"  - Current Accounts (Various types)")
print(f"\nMaturity Buckets:")
print(f"  - Monthly: 0-24 months")
print(f"  - Yearly: 2-5 years")
print(f"  - Special: Overdue, New FDs")
print(f"\nMetrics:")
print(f"  - Balance Outstanding (RM'000)")
print(f"  - W.A. Cost % (Weighted Average Cost)")
print(f"  - Remaining Maturity (months)")
