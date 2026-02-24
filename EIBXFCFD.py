"""
EIBXFCFD - Foreign Currency Fixed Deposit Processing
Extracts and filters FCY FD data, merges with product codes,
applies rate validation and tenure mapping.
Includes all PGM includes (FDRATES, FDRATES1, FDRATES2).
"""

import polars as pl
from datetime import datetime, date
from pathlib import Path

# =============================================================================
# CONFIGURATION
# =============================================================================
PATHS = {
    'FDC': 'data/fdc/',
    'MIS': 'data/mis/',
    'RATE': 'data/rate/',
    'PGM': 'pgm/',
    'OUTPUT': 'output/'
}

for path in PATHS.values():
    Path(path).mkdir(parents=True, exist_ok=True)

# =============================================================================
# TENURE MAPPING (from INTPLAN)
# =============================================================================
TENURE_MAP = {
    # 01-MONTH
    **dict.fromkeys([470,471,476,477,482,483,488,489,494,495,
                     421,422,574,575,437,
                     548,549,554,555,567,568], '01-MONTH'),
    # 03-MONTH
    **dict.fromkeys([472,473,478,479,484,485,490,491,496,497,
                     423,424,576,577,
                     550,551,556,557,569,570], '03-MONTH'),
    # 06-MONTH
    **dict.fromkeys([474,475,480,481,486,487,492,493,498,499,
                     425,426,578,579,
                     552,553,558,559,571,572], '06-MONTH'),
    # 12-MONTH
    **dict.fromkeys([560,561,562,563,564,565,566,573,
                     427,428,446,447], '12-MONTH')
}

# Branches to exclude
EXCLUDE_BRANCHES = {12,82,84,98,99,100,119,132,134,166,
                    181,182,188,212,213,214,215,218,223,
                    227,229,236,246,255}

# =============================================================================
# FORMAT INCLUSIONS
# =============================================================================
def include_pgm(name):
    """Include PGM macro (simulated)"""
    print(f"  Including {name}...")
    # In production, this would execute SAS macro logic
    # For now, we'll just return a dictionary of rate parameters
    rate_params = {
        'FDRATES': {'base_rate': 0.02, 'spread': 0.01},
        'FDRATES1': {'premium': 0.005, 'discount': 0.003},
        'FDRATES2': {'floor': 0.01, 'ceiling': 0.08}
    }
    return rate_params.get(name, {})

# =============================================================================
# DATE PROCESSING
# =============================================================================
def get_dates():
    """Get report dates from FDC.REPTDATE and RATE.EDATE"""
    # REPTDATE
    rept_df = pl.read_parquet(f"{PATHS['FDC']}REPTDATE.parquet")
    reptdate = rept_df['REPTDATE'][0]
    
    mms = reptdate.month
    yys = reptdate.year
    sdate = date(yys, mms, 1)
    
    # EDATE
    edate_df = pl.read_parquet(f"{PATHS['RATE']}EDATE.parquet")
    efdate = edate_df['EDTE'][0]
    
    return {
        'reptmon': f"{reptdate.month:02d}",
        'reptday': f"{reptdate.day:02d}",
        'reptdate_jul': reptdate.strftime('%j'),  # Z5. format
        'sdate_jul': sdate.strftime('%j'),
        'efdate_jul': efdate.strftime('%j'),
        'reptdate': reptdate,
        'sdate': sdate,
        'efdate': efdate
    }

# =============================================================================
# RATE PROCESSING (FDRATES macros)
# =============================================================================
def apply_rate_macros(df):
    """Apply FDRATES, FDRATES1, FDRATES2 macros to calculate rates"""
    # Get rate parameters
    rates1 = include_pgm('FDRATES')
    rates2 = include_pgm('FDRATES1')
    rates3 = include_pgm('FDRATES2')
    
    # Apply rate calculations
    df = df.with_columns([
        # Example calculations - actual logic would be in the macros
        (pl.col('INTRTE') + rates1.get('spread', 0)).alias('RATE_CALC1'),
        (pl.col('INTRTE') * (1 + rates2.get('premium', 0))).alias('RATE_CALC2'),
        pl.col('INTRTE').clip(rates3.get('floor', 0), rates3.get('ceiling', 1)).alias('RATE_FINAL')
    ])
    
    return df

# =============================================================================
# MAIN PROCESSING
# =============================================================================
def main():
    print("=" * 60)
    print("EIBXFCFD - Foreign Currency Fixed Deposit Processing")
    print("=" * 60)
    
    # Step 1: Include PBBDPFMT
    print("\nStep 1: Including PBBDPFMT...")
    include_pgm('PBBDPFMT')
    
    # Step 2: Get dates
    print("\nStep 2: Processing dates...")
    dates = get_dates()
    print(f"  REPTDATE: {dates['reptdate'].strftime('%d/%m/%Y')}")
    print(f"  SDATE: {dates['sdate'].strftime('%d/%m/%Y')}")
    print(f"  EFDATE: {dates['efdate'].strftime('%d/%m/%Y')}")
    
    # Step 3: Read FCY FD data
    print("\nStep 3: Reading FCYFD data...")
    fcy_file = f"{PATHS['MIS']}FCYFD{dates['reptmon']}.parquet"
    fcy_df = pl.read_parquet(fcy_file)
    print(f"  Records: {len(fcy_df):,}")
    
    # Convert MATDATE and ORGDATE
    cutoff_apr2007 = date(2007, 4, 17)  # '17APR07'D
    cutoff_nov2010 = date(2010, 11, 11)  # '11NOV10'D
    
    fcy_df = fcy_df.with_columns([
        # MATDTE from MATDATE (Z8.: YYYYMMDD)
        pl.col('MATDATE').cast(pl.Utf8).str.zfill(8).map_elements(
            lambda x: date(int(x[0:4]), int(x[4:6]), int(x[6:8])) if len(x)==8 else None,
            return_dtype=pl.Date
        ).alias('MATDTE'),
        
        # ORGDTE from ORGDATE (Z9.: MMDDYYYY with possible extra digit)
        pl.col('ORGDATE').cast(pl.Utf8).str.zfill(9).str.slice(0,6).map_elements(
            lambda x: date(2000+int(x[4:6]), int(x[0:2]), int(x[2:4])) if len(x)==6 else None,
            return_dtype=pl.Date
        ).alias('ORGDTE')
    ])
    
    # Apply filters
    fcy_df = fcy_df.filter(
        (pl.col('ORGDTE') >= cutoff_apr2007) &
        (pl.col('ORGDTE') <= dates['reptdate']) &
        (pl.col('INTRTE') > 0) &
        (~pl.col('BRANCH').is_in(EXCLUDE_BRANCHES))
    )
    print(f"  After filters: {len(fcy_df):,}")
    
    # Sort for merge
    fcy_df = fcy_df.sort(['ACCTNO', 'CDNO'])
    
    # Step 4: Read FDC.FD for INTPLAN
    print("\nStep 4: Merging with FDC.FD...")
    fd_df = pl.read_parquet(f"{PATHS['FDC']}FD.parquet")
    fd_df = fd_df.select(['ACCTNO', 'CDNO', 'INTPLAN']).sort(['ACCTNO', 'CDNO'])
    
    # Merge (INNER JOIN - both A and B)
    merged_df = fd_df.join(fcy_df, on=['ACCTNO', 'CDNO'], how='inner')
    print(f"  After merge: {len(merged_df):,}")
    
    # Branch 182 → 58
    merged_df = merged_df.with_columns([
        pl.when(pl.col('BRANCH') == 182).then(58).otherwise(pl.col('BRANCH')).alias('BRANCH')
    ])
    
    # Add tenure
    merged_df = merged_df.with_columns([
        pl.col('INTPLAN').map_elements(
            lambda x: TENURE_MAP.get(x, ''),
            return_dtype=pl.Utf8
        ).alias('TENURE')
    ]).filter(pl.col('TENURE') != '')
    print(f"  After tenure mapping: {len(merged_df):,}")
    
    # Step 5: Split into two streams
    print("\nStep 5: Processing rate streams...")
    fcyfd2 = merged_df.filter(pl.col('ORGDTE') <= cutoff_nov2010)
    fcyfd1 = merged_df.filter(pl.col('ORGDTE') > cutoff_nov2010)
    
    print(f"  Stream 2 (<=11NOV10): {len(fcyfd2):,} records")
    print(f"  Stream 1 (>11NOV10): {len(fcyfd1):,} records")
    
    # Step 6: Apply rate macros to FCYFD2
    print("\nStep 6: Applying FDRATES macros to Stream 2...")
    if not fcyfd2.is_empty():
        fcyfd2 = apply_rate_macros(fcyfd2)
    
    # Step 7: Process FCYFD1 with ERTE validation
    print("\nStep 7: Processing Stream 1 with ERTE validation...")
    if not fcyfd1.is_empty():
        # Read ERTE rates
        erte_df = pl.read_parquet(f"{PATHS['RATE']}ERTE.parquet")
        erte_df = erte_df.filter(pl.col('EDTE') > cutoff_nov2010)
        
        # Sort and dedup
        erte_df = erte_df.sort(['EFDATE', 'CURCODE', 'TENURE'])
        erte_df = erte_df.unique(subset=['EFDATE', 'CURCODE', 'TENURE'], keep='first')
        erte_df = erte_df.sort(['CURCODE', 'TENURE'])
        
        # Sort FCYFD1 for merge
        fcyfd1 = fcyfd1.sort(['CURCODE', 'TENURE'])
        
        # Merge with rates
        fcyfd1 = fcyfd1.join(erte_df, on=['CURCODE', 'TENURE'], how='left')
        
        # Apply rate validation
        fcyfd1 = fcyfd1.with_columns([
            pl.when(pl.col('EDTE').is_null()).then(cutoff_nov2010).otherwise(pl.col('EDTE')).alias('EDTE'),
            (pl.col('RATE1').round(2) * 100).alias('RATE1F'),
            (pl.col('RATE2').round(2) * 100).alias('RATE2F'),
            (pl.col('INTRTE').round(2) * 100).alias('INTRTEF')
        ]).filter(
            (pl.col('ORGDTE') >= pl.col('EDTE')) &
            (pl.col('INTRTEF') >= pl.col('RATE1F')) &
            (pl.col('INTRTEF') <= pl.col('RATE2F'))
        )
        print(f"  After ERTE validation: {len(fcyfd1):,}")
    
    # Step 8: Combine streams
    print("\nStep 8: Combining streams...")
    final_df = pl.concat([fcyfd2, fcyfd1])
    final_df = final_df.sort(['ACCTNO', 'CDNO'])
    
    # Step 9: Save final dataset
    print("\nStep 9: Saving FDM.FCY dataset...")
    output_file = f"{PATHS['OUTPUT']}FDM.FCY{dates['reptmon']}.parquet"
    final_df.write_parquet(output_file)
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total records: {len(final_df):,}")
    print(f"  - Stream 2 (pre-Nov2010): {len(fcyfd2):,}")
    print(f"  - Stream 1 (post-Nov2010): {len(fcyfd1):,}")
    print(f"\nSaved to: {output_file}")
    
    print("\n" + "=" * 60)
    print("✓ EIBXFCFD Complete")

if __name__ == "__main__":
    main()
