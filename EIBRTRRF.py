import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
from npgs_reports import npgs5rpt  # Use shared report module

def eibrtrrf():
    base = Path.cwd()
    bnm_path = base / "BNM"
    npgs_path = base / "NPGS"
    
    # REPTDATE processing (same pattern)
    reptdate_df = pl.read_parquet(bnm_path / "REPTDATE.parquet")
    reptdate = reptdate_df["REPTDATE"][0]
    
    mm = reptdate.month
    mm1 = mm - 1 if mm > 1 else 12
    
    reptmon = f"{mm:02d}"
    reptmon1 = f"{mm1:02d}"
    reptyear = str(reptdate.year)
    reptday = f"{reptdate.day:02d}"
    rdate = reptdate.strftime("%d%m%y")
    ndate = f"{reptdate.day:02d}{reptdate.month:02d}"
    
    print(f"REPTMON: {reptmon}, RDATE: {rdate}")
    
    # Read LNTRRF data
    try:
        ln_df = pl.read_parquet(npgs_path / f"LNTRRF{reptmon}.parquet")
    except:
        print(f"File not found: NPGS/LNTRRF{reptmon}.parquet")
        return
    
    # Create TRRF dataset
    trrf_df = ln_df.filter(ln_df["CVAR13"].str.strip_chars() != "")
    trrf_df = trrf_df.with_columns(
        pl.col("CVAR13").alias("NDATE"),
        pl.col("CVAR12").alias("STATUS")
    ).select(["CVAR01", "CVAR06", "STATUS", "NDATE"])
    
    trrf_df.write_parquet(npgs_path / "TRRF.parquet")
    
    # Process NPGS data
    npgs_df = ln_df.with_columns(
        pl.lit(" " * 10).alias("CVARX1"),
        pl.lit(" " * 10).alias("CVARX2"),
        pl.lit(" " * 4).alias("CVARX3"),
        pl.when(pl.col("CVAR12") == "NPL").then("NP").otherwise("AP").alias("CVAR12A")
    )
    
    # Process SCH=7Q and SCH=8Q separately (same logic, different filter)
    process_scheme(npgs_df, "7Q", rdate, base)
    process_scheme(npgs_df, "8Q", rdate, base)
    
    print(f"Processing complete for SCH=7Q and SCH=8Q")

def process_scheme(npgs_df, scheme, rdate, base):
    """Process a specific scheme (7Q or 8Q)"""
    # Filter for specific scheme
    npgs5_df = npgs_df.filter(
        (pl.col("CVAR02") == scheme) &
        (pl.col("NATGUAR") == "06") &
        (pl.col("CINSTCL") == "18")
    )
    
    if npgs5_df.is_empty():
        print(f"No data for SCH={scheme}")
        return
    
    # Calculate CVARX2 (NPL notification date) - same as SCH=101 logic
    def calculate_npl_date(cvar13):
        if cvar13 and cvar13.strip():
            try:
                # Parse DDMMYY10 format (likely DD/MM/YYYY)
                npl_date = datetime.strptime(cvar13, "%d/%m/%Y")
                # Beginning of next month + 6 days
                next_month = (npl_date.replace(day=1) + timedelta(days=32)).replace(day=1)
                return (next_month + timedelta(days=6)).strftime("%d/%m/%Y")
            except:
                return " " * 10
        return " " * 10
    
    # Apply NPL logic if CVAR12='NPL'
    npgs5_df = npgs5_df.with_columns(
        pl.when(pl.col("CVAR12") == "NPL")
        .then(pl.col("CVAR13").map_elements(calculate_npl_date, return_dtype=pl.Utf8))
        .otherwise(pl.lit(" " * 10))
        .alias("CVARX2"),
        
        pl.when(pl.col("CVAR12") == "NPL").then("CFBS").otherwise(" " * 4).alias("CVARX3"),
        
        # CVARX5: disbursement date
        pl.when((pl.col("CVAR05") != 0) & (pl.col("CVAR05").is_not_null()))
        .then(pl.col("CVAR05").cast(pl.Int64).cast(pl.Utf8).str.str_pad(10, '0'))
        .otherwise(pl.lit(" " * 10))
        .alias("CVARX5"),
        
        # Handle missing values
        pl.when(pl.col("CVAR17").is_null()).then(0.0).otherwise(pl.col("CVAR17")).alias("CVAR17"),
        pl.when(pl.col("ACCRUAL").is_null()).then(0.0).otherwise(pl.col("ACCRUAL")).alias("ACCRUALX")
    )
    
    npgs5_df = npgs5_df.sort(["CVAR01", "CVAR06"])
    
    # Write output file (SC7QT.csv or SC8QT.csv)
    output_file = base / f"SC{scheme.replace('Q', '')}T.csv"
    
    # Select columns in exact order from SAS PUT statement
    output_cols = ['CVAR02', 'CVAR03', 'CVAR04', 'CVAR06', 'CVARX5',
                   'CVAR08', 'CVAR16', 'CVAR09', 'CVAR17', 'ACCRUALX',
                   'CVAR11', 'CVAR12A', 'CVAR13', 'CVARX2', 'CVARX3', 'CVAR01']
    
    # Write CSV with semicolon delimiters
    npgs5_df.select([c for c in output_cols if c in npgs5_df.columns]) \
           .write_csv(output_file, separator=";")
    
    # Generate report using shared NPGS5RPT module
    print("=" * 60)
    print("PUBLIC BANK BERHAD")
    print(f"DETAIL OF ACCTS (SCH={scheme}) FOR SUBMISSION TO CGC @ {rdate}")
    print("=" * 60)
    
    # Use the shared report module
    report_file = base / f"SC{scheme.replace('Q', '')}R.txt"
    npgs5rpt(npgs5_df, rdate, report_file)
    
    print(f"  SCH={scheme}: {len(npgs5_df)} records -> {output_file.name}, {report_file.name}")

# Alternative: Process both schemes in one function if they share more logic
def process_all_schemes():
    """Process both 7Q and 8Q together"""
    base = Path.cwd()
    bnm_path = base / "BNM"
    npgs_path = base / "NPGS"
    
    # Get date variables
    reptdate_df = pl.read_parquet(bnm_path / "REPTDATE.parquet")
    reptdate = reptdate_df["REPTDATE"][0]
    reptmon = f"{reptdate.month:02d}"
    rdate = reptdate.strftime("%d%m%y")
    
    # Read data
    ln_df = pl.read_parquet(npgs_path / f"LNTRRF{reptmon}.parquet")
    
    # Process NPGS data once
    npgs_df = ln_df.with_columns(
        pl.lit(" " * 10).alias("CVARX1"),
        pl.lit(" " * 10).alias("CVARX2"),
        pl.lit(" " * 4).alias("CVARX3"),
        pl.when(pl.col("CVAR12") == "NPL").then("NP").otherwise("AP").alias("CVAR12A")
    )
    
    # Process each scheme
    for scheme in ["7Q", "8Q"]:
        scheme_df = npgs_df.filter(
            (pl.col("CVAR02") == scheme) &
            (pl.col("NATGUAR") == "06") &
            (pl.col("CINSTCL") == "18")
        )
        
        if not scheme_df.is_empty():
            # Apply transformations
            scheme_df = apply_npl_logic(scheme_df)
            
            # Save output
            scheme_df.select(['CVAR02','CVAR03','CVAR04','CVAR06','CVARX5',
                             'CVAR08','CVAR16','CVAR09','CVAR17','ACCRUALX',
                             'CVAR11','CVAR12A','CVAR13','CVARX2','CVARX3','CVAR01']) \
                    .write_csv(base / f"SC{scheme.replace('Q', '')}T.csv", separator=";")
            
            # Generate report
            print(f"\nSCH={scheme} Report:")
            npgs5rpt(scheme_df, rdate, base / f"SC{scheme.replace('Q', '')}R.txt")

def apply_npl_logic(df):
    """Apply NPL date calculations (same as SCH=101)"""
    def calculate_npl_date(cvar13):
        if cvar13 and cvar13.strip():
            try:
                npl_date = datetime.strptime(cvar13, "%d/%m/%Y")
                next_month = (npl_date.replace(day=1) + timedelta(days=32)).replace(day=1)
                return (next_month + timedelta(days=6)).strftime("%d/%m/%Y")
            except:
                return " " * 10
        return " " * 10
    
    return df.with_columns(
        pl.when(pl.col("CVAR12") == "NPL")
        .then(pl.col("CVAR13").map_elements(calculate_npl_date, return_dtype=pl.Utf8))
        .otherwise(pl.lit(" " * 10))
        .alias("CVARX2"),
        
        pl.when(pl.col("CVAR12") == "NPL").then("CFBS").otherwise(" " * 4).alias("CVARX3"),
        
        pl.when((pl.col("CVAR05") != 0) & (pl.col("CVAR05").is_not_null()))
        .then(pl.col("CVAR05").cast(pl.Utf8).str.str_pad(10, '0'))
        .otherwise(pl.lit(" " * 10))
        .alias("CVARX5"),
        
        pl.when(pl.col("CVAR17").is_null()).then(0.0).otherwise(pl.col("CVAR17")).alias("CVAR17"),
        pl.when(pl.col("ACCRUAL").is_null()).then(0.0).otherwise(pl.col("ACCRUAL")).alias("ACCRUALX")
    ).sort(["CVAR01", "CVAR06"])

if __name__ == "__main__":
    eibrtrrf()
