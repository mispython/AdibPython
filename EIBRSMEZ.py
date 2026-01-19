import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
import sys

def eibrsmez():
    base = Path.cwd()
    bnm_path = base / "BNM"
    npgs_path = base / "NPGS"
    npgs_path.mkdir(exist_ok=True)
    
    # Step 1: REPTDATE processing
    reptdate_df = pl.read_parquet(bnm_path / "REPTDATE.parquet")
    reptdate = reptdate_df["REPTDATE"][0]
    
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
    
    # Step 2: SMEZ data
    ln_df = pl.read_parquet(npgs_path / f"LNSMEZ{reptmon}.parquet")
    dp_df = pl.read_parquet(npgs_path / f"DPSMEZ{reptmon}.parquet")
    smez_df = pl.concat([ln_df, dp_df])
    
    # IF CVAR13 NE '         ' (9 spaces)
    smez_df = smez_df.filter(smez_df["CVAR13"].str.strip_chars() != "")
    smez_df = smez_df.with_columns(
        pl.col("CVAR13").alias("NDATE"),
        pl.col("CVAR12").alias("STATUS")
    ).select(["CVAR01", "CVAR06", "STATUS", "NDATE"])
    
    smez_df.write_parquet(npgs_path / "SMEZ.parquet")
    
    # Step 3: NPGS base dataset
    npgs_df = pl.concat([ln_df, dp_df])
    npgs_df = npgs_df.with_columns(
        pl.lit(" " * 10).alias("CVARX1"),  # 10 spaces
        pl.lit(" " * 10).alias("CVARX2"),
        pl.lit(" " * 4).alias("CVARX3"),
        pl.when(pl.col("CVAR12") == "NPL").then("NP").otherwise("AP").alias("CVAR12A")
    )
    
    # Step 4: SC93 processing (CVAR02='93')
    npgs3_df = npgs_df.filter(
        (pl.col("CVAR02") == "93") &
        (pl.col("NATGUAR") == "06") &
        (pl.col("CINSTCL") == "18")
    )
    
    npgs3_df = npgs3_df.with_columns(
        pl.lit(" " * 10).alias("CVARXX"),
        pl.col("ACCRUAL").alias("ACCRUALX")
    )
    
    npgs3_df = npgs3_df.sort(["CVAR01", "CVAR06"])
    
    # Write SC93T file with fixed positions
    with open(base / "SC93T.csv", 'w') as f:
        for row in npgs3_df.iter_rows(named=True):
            # SAS PUT with @001 positions and +(-1) for semicolons
            line = (
                f"{row['CVAR02']};{row['CVAR03']};{row['CVAR04']};"
                f"{row['CVAR06']};{row['CVAR08']:.2f};{row['CVAR09']:.2f};"
                f"{row['CURBAL']:.2f};{row['ACCRUALX']:.2f};{row['CVAR11']};"
                f"{row['CVAR12A']};{row['CVAR13']};{row['CVARX2']};"
                f"{row['CVARX3']};{row['CVAR01']};{row['TRANCHE']};"
            )
            f.write(line + "\n")
    
    # Generate report (simulate SAS PROC PRINTTO)
    print("=" * 60)
    print("PUBLIC BANK BERHAD")
    print(f"DETAIL OF ACCTS (SCH=93) FOR SUBMISSION TO CGC @ {rdate}")
    print("=" * 60)
    
    # Execute NPGS3RPT
    try:
        from npgs3rpt import generate_report as npgs3_report
        npgs3_report(npgs3_df, base / "SC93R.txt")
    except ImportError:
        # Create simple report
        with open(base / "SC93R.txt", 'w') as f:
            f.write(f"SC93 Report - Date: {rdate}\n")
            f.write(f"Total records: {len(npgs3_df)}\n")
    
    # Step 5: SC94 processing (CVAR02='94')
    npgs4_df = npgs_df.filter(
        (pl.col("CVAR02") == "94") &
        (pl.col("NATGUAR") == "06") &
        (pl.col("CINSTCL") == "18")
    )
    
    npgs4_df = npgs4_df.with_columns(
        pl.lit(" " * 10).alias("CVARXX")
    )
    
    npgs4_df = npgs4_df.sort(["CVAR01", "CVAR06"])
    
    # Write SC94T with exact SAS fixed positions
    with open(base / "SC94T.csv", 'w') as f:
        for row in npgs4_df.iter_rows(named=True):
            # Exact SAS @ positions converted to Python string formatting
            line = f"{row['CVAR02']:2s};{row['CVAR03']:15s};{row['CVAR04']:100s};" \
                   f"{'':10s};{row['CVAR06']:10.0f};{row['CVAR08']:10.2f};" \
                   f"{row['CVAR09']:10.2f};{row['CURBAL']:10.2f};{row['ACCRUAL']:10.2f};" \
                   f"{row['CVAR11']:2.0f};{row['CVAR12A']:2s};{row['CVAR13']:10s};" \
                   f"{row['CVARX2']:10s};{row['CVARX3']:4s};{row['CVAR01']:10.0f};" \
                   f"{row['TRANCHE']:8s};"
            f.write(line + "\n")
    
    # SC94 Report
    print("=" * 60)
    print("PUBLIC BANK BERHAD")
    print(f"DETAIL OF ACCTS (SCH=94) FOR SUBMISSION TO CGC @ {rdate}")
    print("=" * 60)
    
    try:
        from npgs4rpt import generate_report as npgs4_report
        npgs4_report(npgs4_df, base / "SC94R.txt")
    except ImportError:
        with open(base / "SC94R.txt", 'w') as f:
            f.write(f"SC94 Report - Date: {rdate}\n")
            f.write(f"Total records: {len(npgs4_df)}\n")
    
    # Step 6: SC101 processing (CVAR02='101')
    npgs5_df = npgs_df.filter(
        (pl.col("CVAR02") == "101") &
        (pl.col("NATGUAR") == "06") &
        (pl.col("CINSTCL") == "18")
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
        pl.when(pl.col("CVAR12") == "NPL")
        .then(pl.col("CVAR13").map_elements(calculate_npl_date, return_dtype=pl.Utf8))
        .otherwise(pl.lit(" " * 10))
        .alias("CVARX2"),
        
        # Handle CVARX3 (NPL reason)
        pl.when(pl.col("CVAR12") == "NPL").then("CFBS").otherwise(" " * 4).alias("CVARX3"),
        
        # Handle CVARX5 (disbursement date)
        pl.when((pl.col("CVAR05") != 0) & (pl.col("CVAR05").is_not_null()))
        .then(pl.col("CVAR05").cast(pl.Int64).cast(pl.Utf8).str.str_pad(10, '0'))
        .otherwise(pl.lit(" " * 10))
        .alias("CVARX5"),
        
        # Handle missing values
        pl.when(pl.col("CVAR17").is_null()).then(0.0).otherwise(pl.col("CVAR17")).alias("CVAR17"),
        pl.when(pl.col("ACCRUAL").is_null()).then(0.0).otherwise(pl.col("ACCRUAL")).alias("ACCRUALX")
    )
    
    npgs5_df = npgs5_df.sort(["CVAR01", "CVAR06"])
    
    # Write SC101T with DLM=';' DSD (comma-separated with semicolons)
    npgs5_df.select([
        "CVAR02", "CVAR03", "CVAR04", "CVAR06", "CVARX5",
        "CVAR16", "CVAR08", "CVAR09", "CVAR17", "ACCRUALX",
        "CVAR10", "CVAR11", "CVAR12A", "CVAR13", "CVARX2", 
        "CVARX3", "CVAR01"
    ]).write_csv(base / "SC101T.csv", separator=";")
    
    # SC101 Report
    print("=" * 60)
    print("PUBLIC BANK BERHAD")
    print(f"DETAIL OF ACCTS (SCH=101) FOR SUBMISSION TO CGC @ {rdate}")
    print("=" * 60)
    
    try:
        from npgs5rpt import generate_report as npgs5_report
        npgs5_report(npgs5_df, base / "SC101R.txt")
    except ImportError:
        with open(base / "SC101R.txt", 'w') as f:
            f.write(f"SC101 Report - Date: {rdate}\n")
            f.write(f"Total records: {len(npgs5_df)}\n")
    
    print("Processing complete. Files created: SC93T, SC94T, SC101T")

if __name__ == "__main__":
    eibrsmez()
