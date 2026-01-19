import polars as pl
from pathlib import Path
from datetime import datetime

def eibrp159():
    base = Path.cwd()
    mniln_path = base / "MNILN"
    npgs_path = base / "NPGS"
    
    # Step 1: REPTDATE processing (same as previous)
    reptdate_df = pl.read_parquet(mniln_path / "REPTDATE.parquet")
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
    
    print(f"REPTMON: {reptmon}, RDATE: {rdate}")
    
    # Step 2: NPGS data
    try:
        dp_df = pl.read_parquet(npgs_path / f"DPIPGS{reptmon}.parquet")
    except:
        dp_df = pl.DataFrame()
    
    try:
        ln_df = pl.read_parquet(npgs_path / f"LNIPGS{reptmon}.parquet")
    except:
        ln_df = pl.DataFrame()
    
    npgs_df = pl.concat([dp_df, ln_df])
    npgs_df = npgs_df.with_columns(
        pl.lit(" " * 10).alias("CVARXX")  # 10 spaces
    )
    
    # Step 3: Write MEFT file with exact SAS fixed positions
    with open(base / "MEFT.csv", 'w') as f:
        for row in npgs_df.iter_rows(named=True):
            # Format each field exactly as SAS PUT statements
            cvar01 = f"{row['CVAR01']:10.0f}" if 'CVAR01' in row else " " * 10
            cvar02 = f"{row.get('CVAR02', ''):2s}"
            cvar03 = f"{row.get('CVAR03', ''):15s}"
            cvar04 = f"{row.get('CVAR04', ''):50s}"
            
            # CVAR05: DDMMYY10. format
            cvar05 = " " * 10
            if 'CVAR05' in row and row['CVAR05']:
                try:
                    # Handle date - assuming it's already a date type
                    if isinstance(row['CVAR05'], (datetime, pl.Date)):
                        cvar05 = row['CVAR05'].strftime("%d/%m/%Y")
                    else:
                        # Try to parse if it's string/numeric
                        cvar05 = str(row['CVAR05']).rjust(10)
                except:
                    cvar05 = " " * 10
            
            cvarxx = " " * 10
            cvar06 = f"{row.get('CVAR06', 0):10.0f}"
            cvar07 = f"{row.get('CVAR07', ''):2s}"
            cvar08 = f"{row.get('CVAR08', 0):10.2f}"
            cvar09 = f"{row.get('CVAR09', 0):10.2f}"
            cvar10 = f"{row.get('CVAR10', 0):10.2f}"
            cvar11 = f"{row.get('CVAR11', 0):5.0f}"
            cvar12 = f"{row.get('CVAR12', ''):3s}"
            cvar13 = f"{row.get('CVAR13', ''):10s}"
            cvar14 = f"{row.get('CVAR14', ''):4s}"
            cvar15 = f"{row.get('CVAR15', ''):5s}"
            
            # Write with exact @ positions and semicolons
            line = f"{cvar01};{cvar02};{cvar03};{cvar04};{cvar05};{cvarxx};" \
                   f"{cvar06};{cvar07};{cvar08};{cvar09};{cvar10};{cvar11};" \
                   f"{cvar12};{cvar13};{cvar14};{cvar15};"
            f.write(line + "\n")
    
    # Step 4: Generate report header and call external program
    print("=" * 60)
    print("PUBLIC BANK BERHAD")
    print(f"DETAIL OF ACCTS (MEF PRODUCTS) FOR SUBMISSION TO CGC @ {rdate}")
    print("=" * 60)
    
    # Execute NPGSRPT (external program)
    try:
        from npgsrpt import generate_report
        generate_report(npgs_df, base / "MEFR.txt", rdate)
    except ImportError:
        # Create simple report if module doesn't exist
        with open(base / "MEFR.txt", 'w') as f:
            f.write(f"MEF Report - Date: {rdate}\n")
            f.write("=" * 60 + "\n")
            f.write(f"Total records processed: {len(npgs_df)}\n")
            
            # Simple summary by CVAR02
            if len(npgs_df) > 0 and 'CVAR02' in npgs_df.columns:
                summary = npgs_df.group_by("CVAR02").agg(pl.count().alias("count"))
                for row in summary.iter_rows(named=True):
                    f.write(f"CVAR02={row['CVAR02']}: {row['count']} records\n")
    
    print(f"MEFT file created with {len(npgs_df)} records")
    print(f"Report saved to MEFR.txt")

if __name__ == "__main__":
    eibrp159()
