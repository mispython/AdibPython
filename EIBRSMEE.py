import polars as pl
from pathlib import Path
from npgs_reports import generate_report

def eibrsmee():
    base = Path.cwd()
    bnm_path = base / "BNM"
    npgs_path = base / "NPGS"
    
    # REPTDATE processing (same as others)
    reptdate_df = pl.read_parquet(bnm_path / "REPTDATE.parquet")
    reptdate = reptdate_df["REPTDATE"][0]
    
    mm = reptdate.month
    mm1 = mm - 1 if mm > 1 else 12
    
    reptmon = f"{mm:02d}"
    reptmon1 = f"{mm1:02d}"
    reptyear = str(reptdate.year)
    reptday = f"{reptdate.day:02d}"
    rdate = reptdate.strftime("%d%m%y")
    
    print(f"REPTMON: {reptmon}, RDATE: {rdate}")
    
    # Read LNSMEE data
    try:
        npsg_df = pl.read_parquet(npgs_path / f"LNSMEE{reptmon}.parquet")
    except:
        print(f"File not found: NPGS/LNSMEE{reptmon}.parquet")
        return
    
    # Create SMEE dataset
    smee_df = npsg_df.with_columns(
        pl.col("CVAR13").alias("NDATE"),
        pl.col("CVAR12").alias("STATUS")
    ).select(["CVAR01", "CVAR06", "STATUS", "NDATE"])
    
    smee_df.write_parquet(npgs_path / "SMEE.parquet")
    
    # Process NPGS data
    npgs_df = npsg_df.with_columns(
        pl.lit(" " * 10).alias("CVARX1"),
        pl.lit(" " * 10).alias("CVARX2"),
        pl.lit(" " * 4).alias("CVARX3"),
        pl.when(pl.col("CVAR12") == "NPL").then("NP").otherwise("AP").alias("CVAR12A")
    )
    
    # Filter for SCH=E5
    npgs_df = npgs_df.filter(
        (pl.col("CVAR02") == "E5") &
        (pl.col("NATGUAR") == "06") &
        (pl.col("CINSTCL") == "18")
    )
    
    npgs_df = npgs_df.with_columns(pl.lit(" " * 10).alias("CVARXX"))
    npgs_df = npgs_df.sort(["CVAR01", "CVAR06"])
    
    # Write SC5T file with fixed positions
    with open(base / "SC5T.csv", 'w') as f:
        for row in npgs_df.iter_rows(named=True):
            cvar01 = f"{row.get('CVAR01', 0):10.0f}"
            cvar02 = f"{row.get('CVAR02', ''):2s}"
            cvar03 = f"{row.get('CVAR03', ''):15s}"
            cvar04 = f"{row.get('CVAR04', ''):50s}"
            
            # CVAR05 date
            cvar05 = " " * 10
            if 'CVAR05' in row and row['CVAR05']:
                try:
                    if hasattr(row['CVAR05'], 'strftime'):
                        cvar05 = row['CVAR05'].strftime("%d/%m/%Y")
                    else:
                        cvar05 = str(row['CVAR05']).rjust(10)
                except:
                    cvar05 = " " * 10
            
            cvarxx = " " * 10
            cvar06 = f"{row.get('CVAR06', 0):10.0f}"
            cvar07 = f"{row.get('CVAR07', ''):2s}"
            cvar08 = f"{row.get('CVAR08', 0):13.2f}"
            cvar09 = f"{row.get('CVAR09', 0):13.2f}"
            cvar10 = f"{row.get('CVAR10', 0):13.2f}"
            cvar11 = f"{row.get('CVAR11', 0):5.0f}"
            cvar12 = f"{row.get('CVAR12', ''):3s}"
            cvar13 = f"{row.get('CVAR13', ''):10s}"
            cvar14 = f"{row.get('CVAR14', ''):4s}"
            cvar15 = f"{row.get('CVAR15', ''):5s}"
            
            line = f"{cvar01};{cvar02};{cvar03};{cvar04};{cvar05};{cvarxx};" \
                   f"{cvar06};{cvar07};{cvar08};{cvar09};{cvar10};{cvar11};" \
                   f"{cvar12};{cvar13};{cvar14};{cvar15};"
            f.write(line + "\n")
    
    # Generate report using shared module
    print("=" * 60)
    print("PUBLIC BANK BERHAD")
    print(f"DETAIL OF ACCTS (SCH=E5) FOR SUBMISSION TO CGC @ {rdate}")
    print("=" * 60)
    
    # Use the shared report module
    generate_report(npgs_df, "E5", rdate, base / "SC5R.txt")
    
    print(f"Processing complete. Files: SC5T.csv, SC5R.txt")

if __name__ == "__main__":
    eibrsmee()
