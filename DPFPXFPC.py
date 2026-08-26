import polars as pl
import pandas as pd
import pyreadstat
from pathlib import Path
from datetime import datetime, timedelta
import sys
import saspy
from NPGS3RPT import npgs3_report
from NPGS4RPT import npgs4_report
from NPGS5RPT import npgs5_report

def eibrsmez():
    npgs_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRSMEZ")
    output = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRSMEZ")
    npgs_path.mkdir(exist_ok=True)
    output.mkdir(exist_ok=True)
    
    # Initialize SAS session
    sas = saspy.SASsession(cfgname='default')
    
    # Step 1: Calculate report date (using datetime - 1 day instead of REPTDATE)
    reptdate = datetime.now() - timedelta(days=1)
    
    mm = reptdate.month
    mm1 = mm - 1 if mm > 1 else 12
    
    # SAS CALL SYMPUT equivalents
    reptmon = f"{mm:02d}"
    reptmon1 = f"{mm1:02d}"
    reptyear = str(reptdate.year)
    reptday = f"{reptdate.day:02d}"
    rdate = reptdate.strftime("%d%m%y")
    ndate = f"{reptdate.day:02d}{reptdate.month:02d}"
    
    print(f"REPTMON: {reptmon}, REPTMON1: {reptmon1}, RDATE: {rdate}")
    
    # Step 2: Read SMEZ data from SAS7BDAT files using pyreadstat
    print("Reading SAS7BDAT files...")
    ln_df_pandas, ln_meta = pyreadstat.read_sas7bdat(
        npgs_path / f"lnsmez{reptmon.lower()}.sas7bdat"
    )
    dp_df_pandas, dp_meta = pyreadstat.read_sas7bdat(
        npgs_path / f"dpsmez{reptmon.lower()}.sas7bdat"
    )
    
    # Convert to Polars DataFrames with lowercase column names
    ln_df = pl.from_pandas(ln_df_pandas)
    dp_df = pl.from_pandas(dp_df_pandas)
    
    # Convert all column names to lowercase
    ln_df = ln_df.rename({col: col.lower() for col in ln_df.columns})
    dp_df = dp_df.rename({col: col.lower() for col in dp_df.columns})
    
    # Find all unique columns from both DataFrames (SAS SET behavior)
    all_columns = list(dict.fromkeys(ln_df.columns + dp_df.columns))  # preserves order, removes duplicates
    
    # Add missing columns with null values to each DataFrame
    for col in all_columns:
        if col not in ln_df.columns:
            ln_df = ln_df.with_columns(pl.lit(None).alias(col))
        if col not in dp_df.columns:
            dp_df = dp_df.with_columns(pl.lit(None).alias(col))
    
    # Ensure both DataFrames have columns in the same order
    ln_df = ln_df.select(all_columns)
    dp_df = dp_df.select(all_columns)
    
    print(f"LN columns: {len(ln_df.columns)}, DP columns: {len(dp_df.columns)}")
    print(f"All columns: {all_columns}")
    
    # Step 3: Create SMEZ dataset
    smez_df = pl.concat([ln_df, dp_df])
    
    # IF CVAR13 NE '         ' (9 spaces)
    smez_df = smez_df.filter(smez_df["cvar13"].str.strip_chars() != "")
    smez_df = smez_df.with_columns(
        pl.col("cvar13").alias("ndate"),
        pl.col("cvar12").alias("status")
    ).select(["cvar01", "cvar06", "status", "ndate"])
    
    # Write SMEZ to multiple formats
    smez_df.write_parquet(output / "smez.parquet")
    smez_df.write_csv(output / "smez.txt", separator="|")
    
    # Write to SAS7BDAT using saspy
    smez_pandas = smez_df.to_pandas()
    sas.df2sd(smez_pandas, table='smez', libref='work')
    
    # Step 4: NPGS base dataset (use the aligned dataframes)
    npgs_df = pl.concat([ln_df, dp_df])
    npgs_df = npgs_df.with_columns(
        pl.lit(" " * 10).alias("cvarx1"),  # 10 spaces
        pl.lit(" " * 10).alias("cvarx2"),
        pl.lit(" " * 4).alias("cvarx3"),
        pl.when(pl.col("cvar12") == "NPL").then("NP").otherwise("AP").alias("cvar12a")
    )
    
    # Step 5: SC93 processing (CVAR02='93')
    npgs3_df = npgs_df.filter(
        (pl.col("cvar02") == "93") &
        (pl.col("natguar") == "06") &
        (pl.col("cinstcl") == "18")
    )
    
    npgs3_df = npgs3_df.with_columns(
        pl.lit(" " * 10).alias("cvarxx"),
        pl.col("accrual").alias("accrualx")
    )
    
    npgs3_df = npgs3_df.sort(["cvar01", "cvar06"])
    
    # Write SC93T file with SAS PUT format
    with open(output / "sc93t.txt", 'w') as f:
        for row in npgs3_df.iter_rows(named=True):
            # Handle potential None values
            def safe_str(val, default=""):
                return str(val).strip() if val is not None else default
            
            def safe_float(val, default=0.0):
                return float(val) if val is not None else default
            
            # SAS PUT with @001 positions and +(-1) for semicolons
            line = (
                f"{safe_str(row.get('cvar02'))};{safe_str(row.get('cvar03'))};{safe_str(row.get('cvar04'))};"
                f"{safe_str(row.get('cvar06'))};{safe_float(row.get('cvar08')):.2f};{safe_float(row.get('cvar09')):.2f};"
                f"{safe_float(row.get('curbal')):.2f};{safe_float(row.get('accrualx')):.2f};{safe_str(row.get('cvar11'))};"
                f"{safe_str(row.get('cvar12a'))};{safe_str(row.get('cvar13'))};{safe_str(row.get('cvarx2'))};"
                f"{safe_str(row.get('cvarx3'))};{safe_str(row.get('cvar01'))};{safe_str(row.get('tranche'))};"
            )
            f.write(line + "\n")
    
    # Write SC93T to multiple formats
    npgs3_df.write_parquet(output / "sc93t.parquet")
    
    # Write to SAS7BDAT using saspy
    npgs3_pandas = npgs3_df.to_pandas()
    sas.df2sd(npgs3_pandas, table='sc93t', libref='work')
    
    # Generate SC93 Report
    print("=" * 60)
    print("PUBLIC BANK BERHAD")
    print(f"DETAIL OF ACCTS (SCH=93) FOR SUBMISSION TO CGC @ {rdate}")
    print("=" * 60)
    
    npgs3_report(
        df=npgs3_df,
        report_path=str(output / "sc93r.txt"),
        title1="PUBLIC BANK BERHAD",
        title2=f"DETAIL OF ACCTS (SCH=93) FOR SUBMISSION TO CGC @ {rdate}"
    )
    
    # Step 6: SC94 processing (CVAR02='94')
    npgs4_df = npgs_df.filter(
        (pl.col("cvar02") == "94") &
        (pl.col("natguar") == "06") &
        (pl.col("cinstcl") == "18")
    )
    
    npgs4_df = npgs4_df.with_columns(
        pl.lit(" " * 10).alias("cvarxx")
    )
    
    npgs4_df = npgs4_df.sort(["cvar01", "cvar06"])
    
    # Write SC94T with exact SAS fixed positions
    with open(output / "sc94t.txt", 'w') as f:
        for row in npgs4_df.iter_rows(named=True):
            # Handle potential None values
            def safe_str(val, default=""):
                return str(val).strip() if val is not None else default
            
            def safe_float(val, default=0.0):
                return float(val) if val is not None else default
            
            # Exact SAS @ positions converted to Python string formatting
            line = f"{safe_str(row.get('cvar02')):2s};{safe_str(row.get('cvar03')):15s};{safe_str(row.get('cvar04')):100s};" \
                   f"{'':10s};{safe_float(row.get('cvar06')):10.0f};{safe_float(row.get('cvar08')):10.2f};" \
                   f"{safe_float(row.get('cvar09')):10.2f};{safe_float(row.get('curbal')):10.2f};{safe_float(row.get('accrual')):10.2f};" \
                   f"{safe_float(row.get('cvar11')):2.0f};{safe_str(row.get('cvar12a')):2s};{safe_str(row.get('cvar13')):10s};" \
                   f"{safe_str(row.get('cvarx2')):10s};{safe_str(row.get('cvarx3')):4s};{safe_float(row.get('cvar01')):10.0f};" \
                   f"{safe_str(row.get('tranche')):8s};"
            f.write(line + "\n")
    
    # Write SC94T to multiple formats
    npgs4_df.write_parquet(output / "sc94t.parquet")
    
    # Write to SAS7BDAT using saspy
    npgs4_pandas = npgs4_df.to_pandas()
    sas.df2sd(npgs4_pandas, table='sc94t', libref='work')
    
    # Generate SC94 Report
    print("=" * 60)
    print("PUBLIC BANK BERHAD")
    print(f"DETAIL OF ACCTS (SCH=94) FOR SUBMISSION TO CGC @ {rdate}")
    print("=" * 60)
    
    npgs4_report(
        df=npgs4_df,
        report_path=str(output / "sc94r.txt"),
        title1="PUBLIC BANK BERHAD",
        title2=f"DETAIL OF ACCTS (SCH=94) FOR SUBMISSION TO CGC @ {rdate}"
    )
    
    # Step 7: SC101 processing (CVAR02='101')
    npgs5_df = npgs_df.filter(
        (pl.col("cvar02") == "101") &
        (pl.col("natguar") == "06") &
        (pl.col("cinstcl") == "18")
    )
    
    # Handle NPL date calculations (SAS INTNX)
    def calculate_npl_date(cvar13):
        if cvar13 and cvar13.strip():
            try:
                # Parse DDMMYY10 format (10 chars, likely DD/MM/YYYY)
                npl_date = datetime.strptime(cvar13, "%d/%m/%Y")
                # INTNX('MONTH', date, 1, 'B') = beginning of next month
                next_month = (npl_date.replace(day=1) + timedelta(days=32)).replace(day=1)
                # Add 6 days
                return (next_month + timedelta(days=6)).strftime("%d/%m/%Y")
            except:
                return " " * 10
        return " " * 10
    
    npgs5_df = npgs5_df.with_columns(
        # Handle CVARX2 (NPL notification date)
        pl.when(pl.col("cvar12") == "NPL")
        .then(pl.col("cvar13").map_elements(calculate_npl_date, return_dtype=pl.Utf8))
        .otherwise(pl.lit(" " * 10))
        .alias("cvarx2"),
        
        # Handle CVARX3 (NPL reason)
        pl.when(pl.col("cvar12") == "NPL").then("CFBS").otherwise(" " * 4).alias("cvarx3"),
        
        # Handle CVARX5 (disbursement date)
        pl.when((pl.col("cvar05") != 0) & (pl.col("cvar05").is_not_null()))
        .then(pl.col("cvar05").cast(pl.Int64).cast(pl.Utf8).str.str_pad(10, '0'))
        .otherwise(pl.lit(" " * 10))
        .alias("cvarx5"),
        
        # Handle missing values
        pl.when(pl.col("cvar17").is_null()).then(0.0).otherwise(pl.col("cvar17")).alias("cvar17"),
        pl.when(pl.col("accrual").is_null()).then(0.0).otherwise(pl.col("accrual")).alias("accrualx")
    )
    
    npgs5_df = npgs5_df.sort(["cvar01", "cvar06"])
    
    # Write SC101T with DLM=';' DSD (comma-separated with semicolons)
    npgs5_df.select([
        "cvar02", "cvar03", "cvar04", "cvar06", "cvarx5",
        "cvar16", "cvar08", "cvar09", "cvar17", "accrualx",
        "cvar10", "cvar11", "cvar12a", "cvar13", "cvarx2", 
        "cvarx3", "cvar01"
    ]).write_csv(output / "sc101t.txt", separator=";")
    
    # Write SC101T to multiple formats
    npgs5_df.write_parquet(output / "sc101t.parquet")
    
    # Write to SAS7BDAT using saspy
    npgs5_pandas = npgs5_df.to_pandas()
    sas.df2sd(npgs5_pandas, table='sc101t', libref='work')
    
    # Generate SC101 Report
    print("=" * 60)
    print("PUBLIC BANK BERHAD")
    print(f"DETAIL OF ACCTS (SCH=101) FOR SUBMISSION TO CGC @ {rdate}")
    print("=" * 60)
    
    npgs5_report(
        df=npgs5_df,
        report_path=str(output / "sc101r.txt"),
        title1="PUBLIC BANK BERHAD",
        title2=f"DETAIL OF ACCTS (SCH=101) FOR SUBMISSION TO CGC @ {rdate}"
    )
    
    # Export all datasets to SAS7BDAT format using saspy
    print("Exporting datasets to SAS7BDAT format...")
    sas.submit("""
        libname output "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRSMEZ";
        data output.smez; set work.smez; run;
        data output.sc93t; set work.sc93t; run;
        data output.sc94t; set work.sc94t; run;
        data output.sc101t; set work.sc101t; run;
    """)
    
    # Close SAS session
    sas.endsas()
    
    print("\nProcessing complete. Files created in multiple formats:")
    print("- Text files: smez.txt, sc93t.txt, sc94t.txt, sc101t.txt")
    print("- Report files: sc93r.txt, sc94r.txt, sc101r.txt")
    print("- Parquet files: smez.parquet, sc93t.parquet, sc94t.parquet, sc101t.parquet")
    print("- SAS7BDAT files: smez.sas7bdat, sc93t.sas7bdat, sc94t.sas7bdat, sc101t.sas7bdat")

if __name__ == "__main__":
    eibrsmez()
