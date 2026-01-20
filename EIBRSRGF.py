import polars as pl
from pathlib import Path
from npgs_reports import generate_report

def eibrsrgf():
    base = Path.cwd()
    mniln_path = base / "MNILN"
    cgcs_path = base / "CGCS"
    
    # REPTDATE processing (same as others)
    reptdate_df = pl.read_parquet(mniln_path / "REPTDATE.parquet")
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
    
    # Read CGCS data
    try:
        cgcs_df = pl.read_parquet(cgcs_path / f"LNNPGS{reptmon}.parquet")
    except:
        print(f"File not found: CGCS/LNNPGS{reptmon}.parquet")
        return
    
    # Filter for CVAR02 IN ('10','63')
    cgcs_df = cgcs_df.filter(pl.col("CVAR02").is_in(["10", "63"]))
    cgcs_df = cgcs_df.with_columns(pl.lit(" " * 10).alias("CVARXX"))
    cgcs_df = cgcs_df.sort(["CVAR01", "CVAR06"])
    
    # Commented out section in SAS (NPL processing)
    # Skipped as per SAS comments
    
    # Write CGCSF file
    with open(base / "CGCSF.csv", 'w') as f:
        for row in cgcs_df.iter_rows(named=True):
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
            cvar08 = f"{row.get('CVAR08', 0):10.2f}"
            cvar09 = f"{row.get('CVAR09', 0):10.2f}"
            cvar10 = f"{row.get('CVAR10', 0):10.2f}"
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
    print(f"ACCOUNT DETAILS (SCHEME: SRGF) FOR CGC @ {rdate}")
    print("=" * 60)
    
    # Use the shared report module - SRGF uses similar format to MEF
    # Add 'SRGF' to column names mapping for clarity
    report_df = cgcs_df.clone()
    if 'BRANCH' not in report_df.columns:
        # Add dummy BRANCH if missing (SAS report expects it)
        report_df = report_df.with_columns(pl.lit(0).alias("BRANCH"))
    
    # Use shared module with custom title
    generate_report(report_df, "SRGF", rdate, output_file=None)
    
    print(f"Processing complete. File: CGCSF.csv")

if __name__ == "__main__":
    eibrsrgf()
