"""
EIIMRM03 - Islamic SPTF Fixed Deposit Maturity Report

Purpose: Time to maturity for SPTF (Shariah) FDs by customer type
- Filter: SPTF FDs only (BNMCODE='42132')
- Split: Individuals vs Non-Individuals
- Maturity profile by month (0-60 months)

Difference from EIIMRM01:
- EIIMRM01: All deposits (FD/SA/CA)
- EIIMRM03: SPTF FDs only, by customer type

Customer Types:
- INDIVIDUALS: CUSTCD in (77, 78, 95, 96)
- NON-INDIVIDUALS: All others
"""

import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os

# Directories
FD_DIR = 'data/fd/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIIMRM03 - Islamic SPTF FD Maturity by Customer Type")
print("=" * 60)

# Read REPTDATE
try:
    df_reptdate = pl.read_parquet(f'{FD_DIR}REPTDATE.parquet')
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
    
    rdate = reptdate.strftime('%d%m%y')
    print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")

except Exception as e:
    print(f"✗ ERROR: {e}")
    import sys
    sys.exit(1)

print("=" * 60)

# Format mappings (PBBDPFMT)
FDPROD = {
    # SPTF FD code
    300: '42132',  # Example SPTF plan
}

TERMFMT = {
    470: 1, 471: 1, 476: 1, 477: 1, 482: 1, 483: 1,
    472: 3, 473: 3, 478: 3, 479: 3, 484: 3, 485: 3,
    474: 6, 475: 6, 480: 6, 481: 6, 486: 6, 487: 6
}

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
print("\n1. Processing SPTF Fixed Deposits...")
try:
    df_fd = pl.read_parquet(f'{FD_DIR}FD.parquet')
    
    # Apply FDPROD format to get BNMCODE
    df_fd = df_fd.with_columns([
        pl.col('INTPLAN').map_elements(
            lambda x: FDPROD.get(x, '42100'), 
            return_dtype=pl.Utf8
        ).alias('BNMCODE')
    ])
    
    # Filter: SPTF only (BNMCODE='42132')
    df_fd = df_fd.filter(pl.col('BNMCODE') == '42132')
    
    # Filter active FDs
    df_fd = df_fd.filter(
        (pl.col('OPENIND').is_in(['O','D'])) &
        (pl.col('CURBAL') > 0)
    )
    
    print(f"  ✓ SPTF FDs: {len(df_fd):,} accounts")
    
    # Classify customer type
    df_fd = df_fd.with_columns([
        pl.when(pl.col('CUSTCD').is_in([77, 78, 95, 96]))
          .then(pl.lit('  INDIVIDUALS  '))
          .otherwise(pl.lit('NON-INDIVIDUALS'))
          .alias('TYPE')
    ])
    
    # Determine product type (RM vs FCY)
    df_fd = df_fd.with_columns([
        pl.when(pl.col('BNMCODE') == '42630')
          .then(pl.lit('FIXED DEPT(FCY)'))
          .otherwise(pl.lit('FIXED DEPT(RM)'))
          .alias('PRODTYP'),
        pl.col('INTPLAN').map_elements(
            lambda x: TERMFMT.get(x, 12), 
            return_dtype=pl.Int64
        ).alias('TERM')
    ])
    
    # Calculate maturity date and remaining months
    df_fd = df_fd.with_columns([
        pl.col('MATDATE').cast(pl.Date).alias('MATDT')
    ])
    
    df_fd = df_fd.with_columns([
        ((pl.col('MATDT') - reptdate).dt.total_days() / 30.0).alias('REMMTH')
    ])
    
    # Classify by subtitle
    df_fd = df_fd.with_columns([
        pl.when((pl.col('OPENIND') == 'D') | (pl.col('MATDT') < reptdate))
          .then(pl.lit('B'))  # Overdue
          .otherwise(pl.lit('A'))  # Remaining maturity
          .alias('SUBTTL'),
        pl.lit('SPTF').alias('SUBTYP')
    ])
    
    # Calculate cost and weighted metrics
    df_fd = df_fd.with_columns([
        (pl.col('CURBAL') * pl.col('RATE')).alias('COST'),
        (pl.col('CURBAL') * pl.col('REMMTH')).alias('REMM')
    ])
    
    # Categorize REMMTH
    df_fd = df_fd.with_columns([
        pl.col('REMMTH').map_elements(
            categorize_remmth, 
            return_dtype=pl.Utf8
        ).alias('REMMTH1')
    ])
    
    # Rename for output
    df_fd = df_fd.rename({'CURBAL': 'AMOUNT'})
    
    # Separate: Regular, Overdue, New
    df_fd_regular = df_fd.filter(pl.col('SUBTTL') == 'A')
    df_fd_overdue = df_fd.filter(pl.col('SUBTTL') == 'B').with_columns([
        pl.lit(99.0).alias('REMMTH')
    ])
    
    # New FDs (within 1 month of term)
    df_fd_new = df_fd_regular.filter(
        (pl.col('TERM') - pl.col('REMMTH')) < 1
    ).with_columns([
        pl.lit('C').alias('SUBTTL'),
        (pl.col('TERM') - 0.5).alias('REMMTH')
    ])
    
    # Combine all
    df_all = pl.concat([df_fd_regular, df_fd_overdue, df_fd_new])

except Exception as e:
    print(f"  ✗ FD: {e}")
    df_all = pl.DataFrame([])
    import sys
    sys.exit(1)

# Summarize
print("\n2. Summarizing by customer type...")
if len(df_all) > 0:
    # Group by TYPE, PRODTYP, SUBTYP, SUBTTL, REMMTH1
    df_summary = df_all.group_by(['TYPE', 'PRODTYP', 'SUBTYP', 'SUBTTL', 'REMMTH1']).agg([
        pl.col('AMOUNT').sum(),
        pl.col('COST').sum(),
        pl.col('REMM').sum()
    ])
    
    # Calculate weighted averages
    df_summary = df_summary.with_columns([
        pl.when(pl.col('AMOUNT') > 0)
          .then(pl.col('COST') / pl.col('AMOUNT'))
          .otherwise(0)
          .alias('WACOST'),
        pl.when(pl.col('AMOUNT') > 0)
          .then(pl.col('REMM') / pl.col('AMOUNT'))
          .otherwise(0)
          .alias('WAREMM'),
        (pl.col('AMOUNT') / 1000).round(0).alias('AMOUNT_K')
    ])
    
    # Create totals by TYPE
    df_type_total = df_summary.group_by(['TYPE', 'PRODTYP', 'SUBTTL', 'REMMTH1']).agg([
        pl.col('AMOUNT_K').sum().alias('AMOUNT'),
        pl.col('COST').sum(),
        pl.col('REMM').sum()
    ])
    
    df_type_total = df_type_total.with_columns([
        pl.when(pl.col('AMOUNT') > 0)
          .then(pl.col('COST') / (pl.col('AMOUNT') * 1000))
          .otherwise(0)
          .alias('WACOST'),
        pl.when(pl.col('AMOUNT') > 0)
          .then(pl.col('REMM') / (pl.col('AMOUNT') * 1000))
          .otherwise(0)
          .alias('WAREMM'),
        pl.lit('TOTAL').alias('SUBTYP')
    ])
    
    # Grand total
    df_grand_total = df_summary.group_by(['PRODTYP', 'SUBTTL', 'REMMTH1']).agg([
        pl.col('AMOUNT_K').sum().alias('AMOUNT'),
        pl.col('COST').sum(),
        pl.col('REMM').sum()
    ])
    
    df_grand_total = df_grand_total.with_columns([
        pl.when(pl.col('AMOUNT') > 0)
          .then(pl.col('COST') / (pl.col('AMOUNT') * 1000))
          .otherwise(0)
          .alias('WACOST'),
        pl.when(pl.col('AMOUNT') > 0)
          .then(pl.col('REMM') / (pl.col('AMOUNT') * 1000))
          .otherwise(0)
          .alias('WAREMM'),
        pl.lit('TOTAL').alias('SUBTYP'),
        pl.lit('TOTAL').alias('TYPE')
    ])
    
    # Combine all levels
    df_final = pl.concat([
        df_summary.select(['TYPE', 'PRODTYP', 'SUBTYP', 'SUBTTL', 'REMMTH1', 'AMOUNT_K', 'WACOST', 'WAREMM'])
                   .rename({'AMOUNT_K': 'AMOUNT'}),
        df_type_total.select(['TYPE', 'PRODTYP', 'SUBTYP', 'SUBTTL', 'REMMTH1', 'AMOUNT', 'WACOST', 'WAREMM']),
        df_grand_total.select(['TYPE', 'PRODTYP', 'SUBTYP', 'SUBTTL', 'REMMTH1', 'AMOUNT', 'WACOST', 'WAREMM'])
    ])
    
    # Save output
    df_final.write_csv(f'{OUTPUT_DIR}SPTF_MATURITY_BY_TYPE.csv')
    
    total_amount = df_final.filter(pl.col('TYPE') == 'TOTAL')['AMOUNT'].sum()
    
    print(f"  ✓ Total SPTF FDs: RM {total_amount:,.0f}K")
    
    # Summary by customer type
    print(f"\n  Summary by Customer Type:")
    df_type_sum = df_final.filter(
        (pl.col('SUBTYP') == 'TOTAL') & 
        (pl.col('TYPE') != 'TOTAL') &
        (pl.col('REMMTH1') == 'SUB-TOTAL')
    ).group_by('TYPE').agg([
        pl.col('AMOUNT').sum()
    ])
    
    for row in df_type_sum.iter_rows(named=True):
        pct = row['AMOUNT'] / total_amount * 100 if total_amount > 0 else 0
        print(f"    {row['TYPE']}: RM {row['AMOUNT']:,.0f}K ({pct:.1f}%)")

else:
    print("  ⚠ No SPTF FDs found")

print(f"\n{'='*60}")
print("✓ EIIMRM03 Complete")
print(f"{'='*60}")
print(f"\nIslamic SPTF FD Maturity Report")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"\nFilter: SPTF Fixed Deposits only (BNMCODE='42132')")
print(f"\nCustomer Types:")
print(f"  INDIVIDUALS: CUSTCD 77, 78, 95, 96")
print(f"  NON-INDIVIDUALS: All others")
print(f"\nMaturity Categories:")
print(f"  A - REMAINING MATURITY: Active FDs")
print(f"  B - OVERDUE FD: Past maturity")
print(f"  C - NEW FD FOR THE MONTH: Recently placed")
print(f"\nOutput: SPTF_MATURITY_BY_TYPE.csv")
