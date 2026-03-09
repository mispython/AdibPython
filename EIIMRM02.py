"""
EIIMRM02 - Islamic Time to Maturity Report (by Customer Type)

Purpose: Risk Management maturity profile split by customer type
- Fixed Deposits by remaining maturity (1-60 months)
- Split: Individuals vs Non-Individuals
- Split: Conventional vs SPTF (Shariah-compliant)
- Weighted Average Cost and Original Maturity

Key Difference from EIIMRM01:
- EIIMRM01: Includes SA/CA, no customer type split
- EIIMRM02: FD only, split by customer type (Individuals/Non-Individuals)

Output: Maturity analysis for Islamic FD by customer type
"""

import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os

# Directories
FD_DIR = 'data/fd/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIIMRM02 - Islamic Maturity Report (by Customer Type)")
print("=" * 60)

# Get environment variables
rdate = os.environ.get('RDATE', datetime.today().strftime('%d%m%y'))

# Read REPTDATE
try:
    df_reptdate = pl.read_parquet(f'{FD_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
except:
    reptdate = datetime.today()

print("=" * 60)

# Format mappings (PBBDPFMT)
FDPROD = {
    470: '42630', 471: '42630', 472: '42630', 473: '42630',
    474: '42630', 475: '42630',  # FCY FD
}

TERMFMT = {
    470: 1, 471: 1, 476: 1, 477: 1, 482: 1, 483: 1,
    488: 1, 489: 1, 494: 1, 495: 1, 548: 1, 549: 1, 554: 1, 555: 1,
    472: 3, 473: 3, 478: 3, 479: 3, 484: 3, 485: 3,
    490: 3, 491: 3, 496: 3, 497: 3, 550: 3, 551: 3, 556: 3, 557: 3,
    474: 6, 475: 6, 480: 6, 481: 6, 486: 6, 487: 6,
    492: 6, 493: 6, 498: 6, 499: 6, 552: 6, 553: 6, 558: 6, 559: 6
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
    elif remmth <= 24:
        m = int(remmth)
        return f'{m:2d} MONTHS'
    elif remmth <= 36:
        return '>2-3 YRS  '
    elif remmth <= 48:
        return '>3-4 YRS  '
    elif remmth <= 60:
        return '>4-5 YRS  '
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
    
    # Determine product type and term
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
    
    # Determine customer type
    df_fd = df_fd.with_columns([
        pl.when(pl.col('CUSTCD').is_in([76,77,78,95,96]))
          .then(pl.lit('  INDIVIDUALS  '))
          .otherwise(pl.lit('NON-INDIVIDUALS'))
          .alias('TYPE')
    ])
    
    # Calculate maturity date
    df_fd = df_fd.with_columns([
        pl.col('MATDATE').cast(pl.Date).alias('MATDT')
    ])
    
    # Calculate remaining months
    df_fd = df_fd.with_columns([
        ((pl.col('MATDT') - reptdate).dt.total_days() / 30.0).alias('REMMT1')
    ])
    
    # Determine subtype and subtitle
    df_fd = df_fd.with_columns([
        pl.when(pl.col('BNMCODE') == '42132')
          .then(pl.lit('SPTF'))
          .otherwise(pl.lit('CONVENTIONAL'))
          .alias('SUBTYP'),
        pl.when((pl.col('OPENIND') == 'D') | (pl.col('MATDT') < reptdate))
          .then(pl.lit('B'))  # Overdue
          .otherwise(pl.lit('A'))  # Original maturity
          .alias('SUBTTL'),
        pl.when((pl.col('OPENIND') == 'D') | (pl.col('MATDT') < reptdate))
          .then(pl.lit(99))  # Overdue marker
          .otherwise(pl.col('TERM'))  # Use term for active
          .alias('REMMTH')
    ])
    
    # Calculate cost and origin (weighted maturity)
    df_fd = df_fd.with_columns([
        (pl.col('CURBAL') * pl.col('RATE')).alias('COST'),
        (pl.col('CURBAL') * pl.col('REMMTH')).alias('ORIGIN')
    ])
    
    # Rename amount
    df_fd = df_fd.rename({'CURBAL': 'AMOUNT'})
    
    # Create datasets based on subtitle
    df_td = df_fd.filter(pl.col('SUBTTL') == 'B')  # Overdue
    df_fd_active = df_fd.filter(pl.col('SUBTTL') == 'A')  # Active
    
    # Identify new FDs (within 1 month of term)
    df_fdn = df_fd_active.filter(
        (pl.col('TERM') - pl.col('REMMT1')) < 1
    ).with_columns([
        pl.lit('C').alias('SUBTTL')  # New FD
    ])
    
    print(f"  ✓ Active FD: {len(df_fd_active):,}")
    print(f"  ✓ Overdue FD: {len(df_td):,}")
    print(f"  ✓ New FD: {len(df_fdn):,}")

except Exception as e:
    print(f"  ✗ FD: {e}")
    df_td = pl.DataFrame([])
    df_fd_active = pl.DataFrame([])
    df_fdn = pl.DataFrame([])

# Combine all FD datasets
print("\n2. Consolidating...")
dfs = [df for df in [df_td, df_fd_active, df_fdn] if len(df) > 0]

if dfs:
    df_dep = pl.concat(dfs)
    
    # Summarize by category
    df_summary = df_dep.group_by(['TYPE','PRODTYP','SUBTTL','REMMTH','SUBTYP']).agg([
        pl.col('AMOUNT').sum(),
        pl.col('COST').sum(),
        pl.col('ORIGIN').sum()
    ])
    
    # Categorize REMMTH for display
    df_summary = df_summary.with_columns([
        pl.col('REMMTH').map_elements(categorize_remmth, return_dtype=pl.Utf8).alias('REMMTH1')
    ])
    
    # Sort
    df_summary = df_summary.sort(['PRODTYP','SUBTTL','SUBTYP','TYPE','REMMTH1'])
    
    # Create dummy rows for all months (1-60)
    print("\n3. Generating complete maturity buckets...")
    dummy_data = []
    for prodtyp in df_summary['PRODTYP'].unique():
        for subttl in ['A', 'C']:  # Active and New FD
            for subtyp in ['CONVENTIONAL', 'SPTF']:
                for typ in ['  INDIVIDUALS  ', 'NON-INDIVIDUALS']:
                    for month in range(1, 61):
                        dummy_data.append({
                            'PRODTYP': prodtyp,
                            'SUBTTL': subttl,
                            'SUBTYP': subtyp,
                            'TYPE': typ,
                            'REMMTH': month,
                            'REMMTH1': categorize_remmth(month)
                        })
    
    if dummy_data:
        df_dummy = pl.DataFrame(dummy_data).unique()
        
        # Merge with actual data
        df_final = df_dummy.join(
            df_summary.select(['PRODTYP','SUBTTL','SUBTYP','TYPE','REMMTH1','AMOUNT','COST','ORIGIN']),
            on=['PRODTYP','SUBTTL','SUBTYP','TYPE','REMMTH1'],
            how='left'
        )
        
        # Fill nulls
        df_final = df_final.with_columns([
            pl.col('AMOUNT').fill_null(0),
            pl.col('COST').fill_null(0),
            pl.col('ORIGIN').fill_null(0)
        ])
    else:
        df_final = df_summary
    
    # Calculate weighted averages
    df_final = df_final.with_columns([
        pl.when(pl.col('AMOUNT') > 0)
          .then(pl.col('COST') / pl.col('AMOUNT'))
          .otherwise(0)
          .alias('WACOST'),
        pl.when(pl.col('AMOUNT') > 0)
          .then(pl.col('ORIGIN') / pl.col('AMOUNT'))
          .otherwise(0)
          .alias('WAORIG'),
        (pl.col('AMOUNT') / 1000).round(0).alias('AMOUNT_K')
    ])
    
    # Generate totals
    print("\n4. Generating totals...")
    
    # Subtype total
    df_subtype_total = df_final.group_by(['TYPE','PRODTYP','SUBTTL','REMMTH1']).agg([
        pl.col('AMOUNT_K').sum().alias('AMOUNT'),
        pl.col('COST').sum(),
        pl.col('ORIGIN').sum()
    ]).with_columns([
        pl.lit('TOTAL').alias('SUBTYP'),
        pl.when(pl.col('AMOUNT') > 0)
          .then(pl.col('COST') / (pl.col('AMOUNT') * 1000))
          .otherwise(0)
          .alias('WACOST'),
        pl.when(pl.col('AMOUNT') > 0)
          .then(pl.col('ORIGIN') / (pl.col('AMOUNT') * 1000))
          .otherwise(0)
          .alias('WAORIG')
    ])
    
    # Type total
    df_type_total = df_final.group_by(['SUBTYP','PRODTYP','SUBTTL','REMMTH1']).agg([
        pl.col('AMOUNT_K').sum().alias('AMOUNT'),
        pl.col('COST').sum(),
        pl.col('ORIGIN').sum()
    ]).with_columns([
        pl.lit('TOTAL').alias('TYPE'),
        pl.when(pl.col('AMOUNT') > 0)
          .then(pl.col('COST') / (pl.col('AMOUNT') * 1000))
          .otherwise(0)
          .alias('WACOST'),
        pl.when(pl.col('AMOUNT') > 0)
          .then(pl.col('ORIGIN') / (pl.col('AMOUNT') * 1000))
          .otherwise(0)
          .alias('WAORIG')
    ])
    
    # Grand total
    df_grand_total = df_final.group_by(['PRODTYP','SUBTTL','SUBTYP','TYPE']).agg([
        pl.col('AMOUNT_K').sum().alias('AMOUNT'),
        pl.col('COST').sum(),
        pl.col('ORIGIN').sum()
    ]).with_columns([
        pl.lit('SUB-TOTAL').alias('REMMTH1'),
        pl.when(pl.col('AMOUNT') > 0)
          .then(pl.col('COST') / (pl.col('AMOUNT') * 1000))
          .otherwise(0)
          .alias('WACOST'),
        pl.when(pl.col('AMOUNT') > 0)
          .then(pl.col('ORIGIN') / (pl.col('AMOUNT') * 1000))
          .otherwise(0)
          .alias('WAORIG')
    ])
    
    # Combine all
    df_output = pl.concat([
        df_final.select(['PRODTYP','SUBTTL','SUBTYP','TYPE','REMMTH1','AMOUNT_K','WACOST','WAORIG']).rename({'AMOUNT_K':'AMOUNT'}),
        df_subtype_total.select(['PRODTYP','SUBTTL','SUBTYP','TYPE','REMMTH1','AMOUNT','WACOST','WAORIG']),
        df_type_total.select(['PRODTYP','SUBTTL','SUBTYP','TYPE','REMMTH1','AMOUNT','WACOST','WAORIG']),
        df_grand_total.select(['PRODTYP','SUBTTL','SUBTYP','TYPE','REMMTH1','AMOUNT','WACOST','WAORIG'])
    ])
    
    # Filter out empty types
    df_output = df_output.filter(pl.col('TYPE') != '               ')
    
    # Save
    df_output.write_csv(f'{OUTPUT_DIR}MATURITY_CUSTOMER_TYPE.csv')
    
    total = df_output.filter(pl.col('REMMTH1') == 'SUB-TOTAL')['AMOUNT'].sum()
    
    print(f"  ✓ Total FD: RM {total:,.0f}K")
    
    # Summary by customer type
    print(f"\n  Summary by Customer Type:")
    df_cust = df_output.filter(pl.col('REMMTH1') == 'SUB-TOTAL').group_by('TYPE').agg([
        pl.col('AMOUNT').sum()
    ])
    
    for row in df_cust.iter_rows(named=True):
        print(f"    {row['TYPE']}: RM {row['AMOUNT']:,.0f}K")

else:
    print("  ⚠ No FD data to process")

print(f"\n{'='*60}")
print("✓ EIIMRM02 Complete")
print(f"{'='*60}")
print(f"\nIslamic Maturity Report (by Customer Type)")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"\nCustomer Types:")
print(f"  - Individuals (CUSTCD: 76,77,78,95,96)")
print(f"  - Non-Individuals (All others)")
print(f"\nProduct Types:")
print(f"  - Conventional FD")
print(f"  - SPTF FD (Shariah-compliant)")
print(f"\nMaturity Categories:")
print(f"  A - ORIGINAL MATURITY (Active FDs)")
print(f"  B - OVERDUE FD (Past maturity)")
print(f"  C - NEW FD FOR THE MONTH (Recent placements)")
print(f"\nMetrics:")
print(f"  - Balance Outstanding (RM'000)")
print(f"  - W.A. Cost % (Weighted Average Cost)")
print(f"  - Remaining Maturity (WAORIG)")
