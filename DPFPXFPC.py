import polars as pl
import pyreadstat
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from NPGS5RPT import npgs5_report  # Use shared report module
import saspy

def read_sas7bdat(file_path):
    """Read SAS dataset and convert to Polars DataFrame with lowercase column names"""
    df, meta = pyreadstat.read_sas7bdat(str(file_path))
    # Convert to Polars and lowercase column names
    pl_df = pl.from_pandas(df)
    pl_df = pl_df.rename({col: col.lower() for col in pl_df.columns})
    return pl_df

def write_sas7bdat(df, output_path):
    """Write Polars DataFrame to SAS7BDAT format using saspy"""
    try:
        # Convert Polars to Pandas for saspy
        pdf = df.to_pandas()
        # Ensure column names are valid SAS names (uppercase for SAS convention)
        pdf.columns = [col.upper() for col in pdf.columns]
        
        # Use saspy to write SAS dataset
        sas = saspy.SASsession(cfgname='default')
        
        # Create a temporary pandas dataframe in SAS
        sas.df2sd(pdf, table='_temp_table', libpath=str(output_path.parent))
        
        # Copy to final name
        sas.submit(f"""
            libname mylib '{output_path.parent}';
            
            DATA mylib.{output_path.stem.lower()};
                SET work._temp_table;
            RUN;
            
            PROC DATASETS LIB=work NOLIST;
                DELETE _temp_table;
            RUN;
            QUIT;
        """)
        
        sas.endsas()
        print(f"  SAS dataset written: {output_path}")
    except Exception as e:
        print(f"  Warning: Could not write SAS dataset: {e}")
        print(f"  Continuing with other output formats...")

def eibrtrrf():
    npgs_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRTRRF")
    output = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRTRRF")
    
    # Create output directory if it doesn't exist
    output.mkdir(parents=True, exist_ok=True)
    
    # Get previous day's date
    reptdate = datetime.now() - timedelta(days=1)
    
    mm = reptdate.month
    mm1 = mm - 1 if mm > 1 else 12
    
    reptmon = f"{mm:02d}"
    reptmon1 = f"{mm1:02d}"
    reptyear = str(reptdate.year)
    reptday = f"{reptdate.day:02d}"
    rdate = reptdate.strftime("%d%m%y")
    ndate = f"{reptdate.day:02d}{reptdate.month:02d}"
    
    print(f"REPTMON: {reptmon}, RDATE: {rdate}")
    
    # Read LNTRRF data from SAS7BDAT
    try:
        lntrrf_file = npgs_path / f"lntrrf{reptmon.lower()}.sas7bdat"
        ln_df = read_sas7bdat(lntrrf_file)
        print(f"Read {len(ln_df)} records from {lntrrf_file.name}")
        print(f"Available columns: {ln_df.columns}")
    except Exception as e:
        print(f"File not found or error: {lntrrf_file}")
        print(f"Error: {e}")
        return
    
    # Create TRRF dataset
    trrf_df = ln_df.filter(pl.col("cvar13").str.strip_chars() != "")
    trrf_df = trrf_df.with_columns(
        pl.col("cvar13").alias("ndate"),
        pl.col("cvar12").alias("status")
    ).select(["cvar01", "cvar06", "status", "ndate"])
    
    # Write TRRF in multiple formats
    trrf_df.write_parquet(npgs_path / "trrf.parquet")
    trrf_df.write_csv(npgs_path / "trrf.txt", separator=";")
    trrf_df.to_pandas().to_csv(npgs_path / "trrf.csv", index=False)
    
    print(f"TRRF dataset created with {len(trrf_df)} records")
    
    # Process NPGS data
    npgs_df = ln_df.with_columns(
        pl.lit(" " * 10).alias("cvarx1"),
        pl.lit(" " * 10).alias("cvarx2"),
        pl.lit(" " * 4).alias("cvarx3"),
        # Fix: Use proper string comparison for cvar12
        pl.when(pl.col("cvar12").str.strip_chars() == "NPL")
        .then(pl.lit("NP"))
        .otherwise(pl.lit("AP"))
        .alias("cvar12a")
    )
    
    print(f"NPGS dataset processed with {len(npgs_df)} records")
    
    # Process SCH=7Q and SCH=8Q separately
    process_scheme(npgs_df, "7q", rdate, output)
    process_scheme(npgs_df, "8q", rdate, output)
    
    print(f"Processing complete for SCH=7Q and SCH=8Q")

def process_scheme(npgs_df, scheme, rdate, base):
    """Process a specific scheme (7Q or 8Q)"""
    # Filter for specific scheme
    npgs5_df = npgs_df.filter(
        (pl.col("cvar02").str.strip_chars().str.to_lowercase() == scheme.lower()) &
        (pl.col("natguar").str.strip_chars() == "06") &
        (pl.col("cinstcl").str.strip_chars() == "18")
    )
    
    if npgs5_df.is_empty():
        print(f"No data for SCH={scheme}")
        return
    
    print(f"Processing {len(npgs5_df)} records for SCH={scheme}")
    
    # Calculate CVARX2 (NPL notification date)
    def calculate_npl_date(cvar13):
        if cvar13 and str(cvar13).strip():
            try:
                # Try different date formats
                date_str = str(cvar13).strip()
                date_formats = ["%d/%m/%Y", "%d-%m-%Y", "%d%m%Y", "%Y-%m-%d", "%d/%m/%y"]
                npl_date = None
                for fmt in date_formats:
                    try:
                        npl_date = datetime.strptime(date_str, fmt)
                        break
                    except:
                        continue
                
                if npl_date:
                    # Beginning of next month + 6 days
                    next_month = (npl_date.replace(day=1) + timedelta(days=32)).replace(day=1)
                    return (next_month + timedelta(days=6)).strftime("%d/%m/%Y")
            except:
                return " " * 10
        return " " * 10
    
    # Apply transformations
    npgs5_df = npgs5_df.with_columns(
        # Fix: Proper string comparison for cvar12
        pl.when(pl.col("cvar12").str.strip_chars() == "NPL")
        .then(pl.col("cvar13").map_elements(calculate_npl_date, return_dtype=pl.Utf8))
        .otherwise(pl.lit(" " * 10))
        .alias("cvarx2"),
        
        # Fix: Proper string comparison for cvar12
        pl.when(pl.col("cvar12").str.strip_chars() == "NPL")
        .then(pl.lit("CFBS"))
        .otherwise(pl.lit(" " * 4))
        .alias("cvarx3"),
        
        # CVARX5: disbursement date
        pl.when((pl.col("cvar05") != 0) & (pl.col("cvar05").is_not_null()))
        .then(pl.col("cvar05").cast(pl.Int64).cast(pl.Utf8).str.zfill(10))
        .otherwise(pl.lit(" " * 10))
        .alias("cvarx5"),
        
        # Handle missing values - directly replace cvar17
        pl.when(pl.col("cvar17").is_null()).then(0.0).otherwise(pl.col("cvar17")).alias("cvar17"),
        pl.when(pl.col("accrual").is_null()).then(0.0).otherwise(pl.col("accrual")).alias("accrualx")
    )
    
    npgs5_df = npgs5_df.sort(["cvar01", "cvar06"])
    
    # Select columns in exact order
    output_cols = ['cvar02', 'cvar03', 'cvar04', 'cvar06', 'cvarx5',
                   'cvar08', 'cvar16', 'cvar09', 'cvar17', 'accrualx',
                   'cvar11', 'cvar12a', 'cvar13', 'cvarx2', 'cvarx3', 'cvar01']
    
    # Ensure all output columns exist
    existing_cols = [c for c in output_cols if c in npgs5_df.columns]
    output_df = npgs5_df.select(existing_cols)
    
    # Generate output files in multiple formats
    scheme_short = scheme.replace('q', '')
    
    # Write CSV (text) file
    output_file = base / f"sc{scheme_short}t.csv"
    output_df.write_csv(output_file, separator=";")
    print(f"  CSV written: {output_file}")
    
    # Write text file
    text_file = base / f"sc{scheme_short}t.txt"
    output_df.to_pandas().to_csv(text_file, sep=';', index=False)
    print(f"  Text file written: {text_file}")
    
    # Write Parquet file
    parquet_file = base / f"sc{scheme_short}t.parquet"
    output_df.write_parquet(parquet_file)
    print(f"  Parquet written: {parquet_file}")
    
    # Write SAS7BDAT file (optional, wrapped in try/except)
    try:
        sas_file = base / f"sc{scheme_short}t.sas7bdat"
        write_sas7bdat(output_df, sas_file)
    except Exception as e:
        print(f"  Warning: Could not write SAS dataset: {e}")
    
    # Generate report using shared NPGS5RPT module
    print("=" * 60)
    print("PUBLIC BANK BERHAD")
    print(f"DETAIL OF ACCTS (SCH={scheme.upper()}) FOR SUBMISSION TO CGC @ {rdate}")
    print("=" * 60)
    
    # Use the shared report module - Fixed function call
    report_file = base / f"sc{scheme_short}r.txt"
    
    # Try different function signatures for npgs5_report
    try:
        # Try with 4 arguments (df, rdate, title2, report_file)
        title2 = f"SCHEME {scheme.upper()} - AS AT {rdate}"
        npgs5_report(npgs5_df, rdate, title2, report_file)
    except TypeError:
        try:
            # Try with 3 arguments (df, rdate, report_file) - original call
            npgs5_report(npgs5_df, rdate, report_file)
        except TypeError as e:
            print(f"  Warning: Could not generate report: {e}")
            print(f"  Please check the npgs5_report function signature")
            # Create a simple text report as fallback
            with open(report_file, 'w') as f:
                f.write("=" * 60 + "\n")
                f.write("PUBLIC BANK BERHAD\n")
                f.write(f"DETAIL OF ACCTS (SCH={scheme.upper()}) FOR SUBMISSION TO CGC @ {rdate}\n")
                f.write("=" * 60 + "\n")
                f.write(f"Total records: {len(npgs5_df)}\n")
                f.write("\nSample data:\n")
                f.write(npgs5_df.head(10).to_pandas().to_string())
    
    print(f"  SCH={scheme.upper()}: {len(npgs5_df)} records")
    print(f"    Output files: {output_file.name}, {text_file.name}, {parquet_file.name}")
    if 'sas_file' in locals():
        print(f"    SAS file: {sas_file.name}")
    print(f"    Report: {report_file.name}")

def process_all_schemes():
    """Process both 7Q and 8Q together"""
    base = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRTRRF")
    npgs_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRTRRF")
    
    # Create output directory if it doesn't exist
    base.mkdir(parents=True, exist_ok=True)
    
    # Get previous day's date
    reptdate = datetime.now() - timedelta(days=1)
    reptmon = f"{reptdate.month:02d}"
    rdate = reptdate.strftime("%d%m%y")
    
    # Read data from SAS7BDAT
    lntrrf_file = npgs_path / f"lntrrf{reptmon.lower()}.sas7bdat"
    ln_df = read_sas7bdat(lntrrf_file)
    
    # Process NPGS data once
    npgs_df = ln_df.with_columns(
        pl.lit(" " * 10).alias("cvarx1"),
        pl.lit(" " * 10).alias("cvarx2"),
        pl.lit(" " * 4).alias("cvarx3"),
        # Fix: Proper string comparison
        pl.when(pl.col("cvar12").str.strip_chars() == "NPL")
        .then(pl.lit("NP"))
        .otherwise(pl.lit("AP"))
        .alias("cvar12a")
    )
    
    # Process each scheme
    for scheme in ["7q", "8q"]:
        scheme_df = npgs_df.filter(
            (pl.col("cvar02").str.strip_chars().str.to_lowercase() == scheme.lower()) &
            (pl.col("natguar").str.strip_chars() == "06") &
            (pl.col("cinstcl").str.strip_chars() == "18")
        )
        
        if not scheme_df.is_empty():
            # Apply transformations
            scheme_df = apply_npl_logic(scheme_df)
            
            # Select columns and save outputs
            output_cols = ['cvar02','cvar03','cvar04','cvar06','cvarx5',
                          'cvar08','cvar16','cvar09','cvar17','accrualx',
                          'cvar11','cvar12a','cvar13','cvarx2','cvarx3','cvar01']
            existing_cols = [c for c in output_cols if c in scheme_df.columns]
            output_df = scheme_df.select(existing_cols)
            
            scheme_short = scheme.replace('q', '')
            
            # Write multiple formats
            output_df.write_csv(base / f"sc{scheme_short}t.csv", separator=";")
            output_df.to_pandas().to_csv(base / f"sc{scheme_short}t.txt", sep=';', index=False)
            output_df.write_parquet(base / f"sc{scheme_short}t.parquet")
            
            # Write SAS7BDAT with error handling
            try:
                write_sas7bdat(output_df, base / f"sc{scheme_short}t.sas7bdat")
            except Exception as e:
                print(f"Warning: Could not write SAS dataset for {scheme}: {e}")
            
            # Generate report with error handling
            report_file = base / f"sc{scheme_short}r.txt"
            try:
                title2 = f"SCHEME {scheme.upper()} - AS AT {rdate}"
                npgs5_report(scheme_df, rdate, title2, report_file)
            except TypeError:
                try:
                    npgs5_report(scheme_df, rdate, report_file)
                except TypeError as e:
                    print(f"Warning: Could not generate report: {e}")
                    # Create a simple text report as fallback
                    with open(report_file, 'w') as f:
                        f.write("=" * 60 + "\n")
                        f.write("PUBLIC BANK BERHAD\n")
                        f.write(f"DETAIL OF ACCTS (SCH={scheme.upper()}) FOR SUBMISSION TO CGC @ {rdate}\n")
                        f.write("=" * 60 + "\n")
                        f.write(f"Total records: {len(scheme_df)}\n")
            
            print(f"\nSCH={scheme.upper()} Report:")
            print(f"Report written to: {report_file}")

def apply_npl_logic(df):
    """Apply NPL date calculations"""
    def calculate_npl_date(cvar13):
        if cvar13 and str(cvar13).strip():
            try:
                # Try multiple date formats
                date_str = str(cvar13).strip()
                date_formats = ["%d/%m/%Y", "%d-%m-%Y", "%d%m%Y", "%Y-%m-%d", "%d/%m/%y"]
                npl_date = None
                for fmt in date_formats:
                    try:
                        npl_date = datetime.strptime(date_str, fmt)
                        break
                    except:
                        continue
                
                if npl_date:
                    next_month = (npl_date.replace(day=1) + timedelta(days=32)).replace(day=1)
                    return (next_month + timedelta(days=6)).strftime("%d/%m/%Y")
            except:
                return " " * 10
        return " " * 10
    
    return df.with_columns(
        # Fix: Proper string comparison
        pl.when(pl.col("cvar12").str.strip_chars() == "NPL")
        .then(pl.col("cvar13").map_elements(calculate_npl_date, return_dtype=pl.Utf8))
        .otherwise(pl.lit(" " * 10))
        .alias("cvarx2"),
        
        # Fix: Proper string comparison
        pl.when(pl.col("cvar12").str.strip_chars() == "NPL")
        .then(pl.lit("CFBS"))
        .otherwise(pl.lit(" " * 4))
        .alias("cvarx3"),
        
        pl.when((pl.col("cvar05") != 0) & (pl.col("cvar05").is_not_null()))
        .then(pl.col("cvar05").cast(pl.Utf8).str.zfill(10))
        .otherwise(pl.lit(" " * 10))
        .alias("cvarx5"),
        
        # Fix: Direct replacement without creating duplicate
        pl.when(pl.col("cvar17").is_null()).then(0.0).otherwise(pl.col("cvar17")).alias("cvar17"),
        pl.when(pl.col("accrual").is_null()).then(0.0).otherwise(pl.col("accrual")).alias("accrualx")
    ).sort(["cvar01", "cvar06"])

if __name__ == "__main__":
    eibrtrrf()
