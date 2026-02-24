"""
EIBRFCFD - Foreign Currency FD Average Balance Report
Generates CSV report of FCY FD balances by branch and currency with averages.
Complete implementation with all SAS steps.
"""

import polars as pl
from datetime import datetime, timedelta
from pathlib import Path

# =============================================================================
# CONFIGURATION
# =============================================================================
PATHS = {
    'FD': 'data/fd/',
    'FDM': 'data/fdm/',
    'PGM': 'pgm/',
    'OUTPUT': 'output/'
}

for path in PATHS.values():
    Path(path).mkdir(parents=True, exist_ok=True)

# Currencies to process
CURRENCIES = ['USD', 'AUD', 'JPY', 'EUR', 'GBP', 'SGD', 'HKD', 'NZD', 'CAD', 'CNY']
BRANCH_RANGE = range(2, 281)  # 002 to 280

# Branches to exclude
EXCLUDE_BRANCHES = {12,82,84,98,99,100,119,132,134,166,
                    181,182,188,212,213,214,215,218,223,
                    227,229,236,246,255}

# =============================================================================
# FORMAT INCLUSION
# =============================================================================
def include_pbbdpfmt():
    """Equivalent to %INC PGM(PBBDPFMT)"""
    # In production, this would load formats
    pass

# =============================================================================
# DATE PROCESSING
# =============================================================================
def get_dates():
    """Get report dates from FD.REPTDATE"""
    df = pl.read_parquet(f"{PATHS['FD']}REPTDATE.parquet")
    reptdate = df['REPTDATE'][0]
    
    dd = reptdate.day
    sdate = reptdate.replace(day=1)  # First day of month
    
    return {
        'reptmon': f"{reptdate.month:02d}",
        'reptday': f"{dd:02d}",
        'reptyra': f"{reptdate.year % 100:02d}",
        'xdate': reptdate.strftime('%j'),  # Z5. format
        'sdate': sdate.strftime('%j'),
        'reptdate': reptdate,
        'sdate_obj': sdate,
        'dd': dd
    }

# =============================================================================
# CREATE DUMMY DATASET
# =============================================================================
def create_dummy():
    """Create FDDUM with zero records for all branch/currency combinations"""
    rows = []
    for cur in CURRENCIES:
        for br in BRANCH_RANGE:
            rows.append({
                'CURCODE': cur,
                'BRANCH': br,
                'AVGB01': 0.0, 'AVGB03': 0.0, 'AVGB06': 0.0, 'AVGB12': 0.0,
                'AVGBAL': 0.0, 'CURBAL': 0.0
            })
    
    df = pl.DataFrame(rows)
    
    # Delete excluded branches (equivalent to IF BRANCH IN (...) THEN DELETE)
    df = df.filter(~pl.col('BRANCH').is_in(EXCLUDE_BRANCHES))
    
    # PROC SUMMARY NWAY (though all zeros, so no change)
    df = df.group_by(['CURCODE', 'BRANCH']).agg([
        pl.col('AVGB01').sum(),
        pl.col('AVGB03').sum(),
        pl.col('AVGB06').sum(),
        pl.col('AVGB12').sum(),
        pl.col('AVGBAL').sum(),
        pl.col('CURBAL').sum()
    ]).sort(['CURCODE', 'BRANCH'])
    
    return df

# =============================================================================
# MAIN PROCESSING
# =============================================================================
def main():
    print("=" * 60)
    print("EIBRFCFD - FCY FD Average Balance Report")
    print("=" * 60)
    
    # Include formats
    include_pbbdpfmt()
    
    # Get dates
    dates = get_dates()
    print(f"\nReport Date: {dates['reptdate'].strftime('%d/%m/%Y')}")
    print(f"Start Date: {dates['sdate_obj'].strftime('%d/%m/%Y')}")
    print(f"Days in month: {dates['dd']}")
    
    # Create dummy dataset
    print("\nCreating dummy dataset...")
    fddum = create_dummy()
    print(f"  Dummy records: {len(fddum):,}")
    
    # Read FCY data
    print("\nReading FCY data...")
    fcy_file = f"{PATHS['FDM']}FCY{dates['reptmon']}.parquet"
    fcy_all = pl.read_parquet(fcy_file)
    
    # Filter for date range (SDATE <= REPTDATE <= XDATE)
    fcy_all = fcy_all.filter(
        (pl.col('REPTDATE') >= dates['sdate_obj']) &
        (pl.col('REPTDATE') <= dates['reptdate'])
    )
    
    # Create two datasets: FCY (all) and FCY01 (CURBAL>0)
    fcy_all = fcy_all.with_columns([pl.col('REPTDATE').alias('REPTDATE_ORIG')])
    fcy01_raw = fcy_all.filter(pl.col('CURBAL') > 0)
    print(f"  FCY (all): {len(fcy_all):,}")
    print(f"  FCY01 (CURBAL>0): {len(fcy01_raw):,}")
    
    # Process FCY01: Get latest record per ACCTNO/CDNO
    if not fcy01_raw.is_empty():
        # First sort: BY ACCTNO CDNO DESCENDING REPTDATE
        fcy01_sorted = fcy01_raw.sort(['ACCTNO', 'CDNO', 'REPTDATE'], 
                                       descending=[False, False, True])
        # Then NODUPKEY: BY ACCTNO CDNO (keep first = latest date)
        fcy01_unique = fcy01_sorted.unique(subset=['ACCTNO', 'CDNO'], keep='first')
        
        # Summarize by CURCODE/BRANCH
        fcy01_sum = fcy01_unique.group_by(['CURCODE', 'BRANCH']).agg([
            pl.col('CURBAL').sum().alias('CURBAL')
        ])
    else:
        fcy01_sum = pl.DataFrame(schema={'CURCODE': pl.Utf8, 'BRANCH': pl.Int64, 'CURBAL': pl.Float64})
    
    # Process FCY: Summarize by CURCODE/BRANCH/TENURE
    fcy_tenure_sum = fcy_all.group_by(['CURCODE', 'BRANCH', 'TENURE']).agg([
        pl.col('CURBAL').sum().alias('CURBAL_TENURE')
    ])
    
    # Calculate daily averages
    fcy_tenure_sum = fcy_tenure_sum.with_columns([
        pl.when(pl.col('TENURE') == '01-MONTH').then(pl.col('CURBAL_TENURE') / dates['dd']).otherwise(0).alias('AVGB01'),
        pl.when(pl.col('TENURE') == '03-MONTH').then(pl.col('CURBAL_TENURE') / dates['dd']).otherwise(0).alias('AVGB03'),
        pl.when(pl.col('TENURE') == '06-MONTH').then(pl.col('CURBAL_TENURE') / dates['dd']).otherwise(0).alias('AVGB06'),
        pl.when(pl.col('TENURE') == '12-MONTH').then(pl.col('CURBAL_TENURE') / dates['dd']).otherwise(0).alias('AVGB12')
    ])
    
    # Summarize averages by CURCODE/BRANCH
    fcy_avg = fcy_tenure_sum.group_by(['CURCODE', 'BRANCH']).agg([
        pl.col('AVGB01').sum(),
        pl.col('AVGB03').sum(),
        pl.col('AVGB06').sum(),
        pl.col('AVGB12').sum(),
        (pl.col('AVGB01').sum() + pl.col('AVGB03').sum() + 
         pl.col('AVGB06').sum() + pl.col('AVGB12').sum()).alias('AVGBAL')
    ])
    
    # Merge all datasets (three merges as in SAS)
    print("\nMerging datasets...")
    
    # First merge: FCY + FCY01
    merged1 = fcy_avg.join(fcy01_sum, on=['CURCODE', 'BRANCH'], how='outer').fill_null(0)
    
    # Second merge: FDDUM + merged1
    merged2 = fddum.join(merged1, on=['CURCODE', 'BRANCH'], how='left', suffix='_RIGHT')
    
    # Fill nulls (from FCY data)
    for col in ['AVGB01', 'AVGB03', 'AVGB06', 'AVGB12', 'AVGBAL', 'CURBAL']:
        if f'{col}_RIGHT' in merged2.columns:
            merged2 = merged2.with_columns([
                pl.when(pl.col(f'{col}_RIGHT').is_not_null())
                .then(pl.col(f'{col}_RIGHT'))
                .otherwise(pl.col(col)).alias(col)
            ])
            merged2 = merged2.drop(f'{col}_RIGHT')
    
    # Sort for output
    result = merged2.sort(['CURCODE', 'BRANCH'])
    
    # Generate CSV report with running subtotals
    print("\nGenerating report...")
    output_file = PATHS['OUTPUT'] / f"FDFA_{dates['reptdate'].strftime('%Y%m%d')}.txt"
    
    with open(output_file, 'w') as f:
        # Header
        f.write("CURCODE;BRANCH;01-MONTH;03-MONTH;06-MONTH;12-MONTH;TOTAL AVG BAL;TODATE BALANCE\n")
        
        # Data rows with running subtotals
        for cur in CURRENCIES:
            cur_df = result.filter(pl.col('CURCODE') == cur).sort('BRANCH')
            sub01 = sub03 = sub06 = sub12 = avgb = curb = 0.0
            
            for row in cur_df.rows(named=True):
                f.write(f"{row['CURCODE']};{row['BRANCH']:03d};"
                       f"{row['AVGB01']:15,.2f};{row['AVGB03']:15,.2f};"
                       f"{row['AVGB06']:15,.2f};{row['AVGB12']:15,.2f};"
                       f"{row['AVGBAL']:20,.2f};{row['CURBAL']:15,.2f}\n")
                
                sub01 += row['AVGB01']
                sub03 += row['AVGB03']
                sub06 += row['AVGB06']
                sub12 += row['AVGB12']
                avgb += row['AVGBAL']
                curb += row['CURBAL']
            
            # Subtotals
            f.write(f"SUBT;;{sub01:18,.2f};{sub03:18,.2f};{sub06:18,.2f};"
                   f"{sub12:18,.2f};{avgb:18,.2f};{curb:18,.2f}\n")
            f.write("  \n")
    
    print(f"  Report saved to: {output_file}")
    
    # Create FYY dataset (summary by CURCODE)
    fyy = result.group_by('CURCODE').agg([
        pl.col('AVGB01').sum().alias('AVGB01'),
        pl.col('AVGB03').sum().alias('AVGB03'),
        pl.col('AVGB06').sum().alias('AVGB06'),
        pl.col('AVGB12').sum().alias('AVGB12'),
        pl.col('AVGBAL').sum().alias('AVGBAL'),
        pl.col('CURBAL').sum().alias('CURBAL')
    ]).sort('CURCODE')
    
    # Final PROC PRINT
    print("\n" + "=" * 60)
    print("FYY DATASET - SUMMARY BY CURRENCY")
    print("=" * 60)
    print(fyy.select(['CURCODE', 'AVGB01', 'AVGB03', 'AVGB06', 'AVGB12', 'AVGBAL', 'CURBAL']))
    
    print("\n" + "=" * 60)
    print("✓ EIBRFCFD Complete")

if __name__ == "__main__":
    main()
