import polars as pl
from pathlib import Path

def eibrtlio():
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
    
    # Read both datasets
    try:
        dp_df = pl.read_parquet(npgs_path / f"DPNPGS{reptmon}.parquet")
    except:
        dp_df = pl.DataFrame()
    
    try:
        ln_df = pl.read_parquet(npgs_path / f"LNIPGS{reptmon}.parquet")
    except:
        ln_df = pl.DataFrame()
    
    # Combine datasets
    combined_df = pl.concat([dp_df, ln_df])
    
    if combined_df.is_empty():
        print("No data found in DPNPGS or LNIPGS")
        return
    
    # Create TL dataset
    tl_df = combined_df.filter(pl.col("CVAR13").str.strip_chars() != "")
    tl_df = tl_df.with_columns(
        pl.col("CVAR13").alias("NDATE"),
        pl.col("CVAR12").alias("STATUS")
    ).select(["CVAR01", "CVAR06", "STATUS", "NDATE"])
    
    tl_df.write_parquet(npgs_path / "TL.parquet")
    
    # Process NPGS data
    npgs_df = combined_df.with_columns(
        pl.when(pl.col("CVAR12") == "NPL").then("NP").otherwise("AP").alias("CVAR12A")
    )
    
    # Filter for NATGUAR='06' AND CINSTCL='18'
    npgs3_df = npgs_df.filter(
        (pl.col("NATGUAR") == "06") &
        (pl.col("CINSTCL") == "18")
    )
    
    npgs3_df = npgs3_df.with_columns(pl.lit(" " * 10).alias("CVARXX"))
    npgs3_df = npgs3_df.sort(["CVAR01", "CVAR06"])
    
    # Write SC167T file (different positions than other programs)
    with open(base / "SC167T.csv", 'w') as f:
        for row in npgs3_df.iter_rows(named=True):
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
            
            cvar06 = f"{row.get('CVAR06', 0):10.0f}"
            cvar07 = f"{row.get('CVAR07', ''):2s}"
            cvar08 = f"{row.get('CVAR08', 0):10.2f}"
            cvar09 = f"{row.get('CVAR09', 0):10.2f}"
            cvar10 = f"{row.get('CVAR10', 0):10.2f}"
            cvar11 = f"{row.get('CVAR11', 0):5.0f}"
            cvar12a = f"{row.get('CVAR12A', ''):4s}"
            cvar13 = f"{row.get('CVAR13', ''):10s}"
            cvar14 = f"{row.get('CVAR14', ''):4s}"
            cvar15 = f"{row.get('CVAR15', ''):5s}"
            
            # Note: Different @ positions than other programs
            # CVAR07 at @104, CVAR08 at @107 (NETPROC repeated label in comment)
            line = f"{cvar01};{cvar02};{cvar03};{cvar04};{cvar05};{cvar06};" \
                   f"{cvar07};{cvar08};{cvar09};{cvar10};{cvar11};{cvar12a};" \
                   f"{cvar13};{cvar14};{cvar15};"
            f.write(line + "\n")
    
    # Generate report - different bank name
    print("=" * 60)
    print("PUBLIC ISLAMIC BANK BERHAD")  # Different from "PUBLIC BANK BERHAD"
    print(f"DETAIL OF ACCTS FOR SUBMISSION TO CGC @ {rdate}")
    print("=" * 60)
    
    # Use the shared report module with custom bank name
    # We'll create a simple report since CGCRPT is separate
    generate_simple_report(npgs3_df, rdate, base / "SC167R.txt")
    
    print(f"Processing complete. Files: TL.parquet, SC167T.csv, SC167R.txt")

def generate_simple_report(df, rdate, output_file):
    """Simple report for Islamic bank"""
    if df.is_empty():
        return
    
    with open(output_file, 'w') as f:
        f.write("=" * 60 + "\n")
        f.write("PUBLIC ISLAMIC BANK BERHAD\n")
        f.write(f"DETAIL OF ACCTS FOR SUBMISSION TO CGC @ {rdate}\n")
        f.write("=" * 60 + "\n\n")
        
        f.write(f"Total accounts: {len(df)}\n\n")
        
        # Show summary by CVAR02 (scheme)
        if 'CVAR02' in df.columns:
            summary = df.group_by("CVAR02").agg(pl.count().alias("count"))
            f.write("Accounts by scheme:\n")
            for row in summary.iter_rows(named=True):
                f.write(f"  Scheme {row['CVAR02']}: {row['count']} accounts\n")
        
        f.write("\nFirst 10 records:\n")
        
        # Display first few records
        cols_to_show = ['CVAR01', 'CVAR02', 'CVAR03', 'CVAR06', 'CVAR08', 'CVAR09', 'CVAR12A']
        display_cols = [c for c in cols_to_show if c in df.columns]
        
        if display_cols:
            display_df = df.select(display_cols).head(10)
            
            # Rename for readability
            rename_map = {
                'CVAR01': 'REF_NO', 'CVAR02': 'SCH', 'CVAR03': 'IC_NO',
                'CVAR06': 'ACCT_NO', 'CVAR08': 'LOAN_AMT', 'CVAR09': 'OS_BAL',
                'CVAR12A': 'STATUS'
            }
            rename_available = {k:v for k,v in rename_map.items() if k in display_df.columns}
            if rename_available:
                display_df = display_df.rename(rename_available)
            
            f.write(str(display_df))
    
    print(f"Report saved to {output_file}")

# For CGCRPT module (if needed separately)
def cgcrpt(df, rdate, output_file=None):
    """CGCRPT report generator (called via %INC PGM(CGCRPT))"""
    if df.is_empty():
        return df
    
    print("=" * 60)
    print("PUBLIC ISLAMIC BANK BERHAD")
    print(f"DETAIL OF ACCTS FOR SUBMISSION TO CGC @ {rdate}")
    print("=" * 60)
    
    # Simple display
    cols = ['CVAR01','CVAR02','CVAR03','CVAR04','CVAR06',
            'CVAR08','CVAR09','CVAR10','CVAR11','CVAR12A',
            'CVAR13','CVAR14','CVAR15']
    
    display_df = df.select([c for c in cols if c in df.columns])
    rename_map = {
        'CVAR01': 'REF_NO', 'CVAR02': 'SCH', 'CVAR03': 'IC_NO',
        'CVAR04': 'CUSTOMER', 'CVAR06': 'ACCT_NO', 'CVAR08': 'LOAN_AMT',
        'CVAR09': 'OS_BALANCE', 'CVAR10': 'INTEREST', 'CVAR11': 'ARREARS',
        'CVAR12A': 'STATUS', 'CVAR13': 'NPL_DATE', 'CVAR14': 'NPL_NOTIFY',
        'CVAR15': 'NPL_REASON'
    }
    
    rename_available = {k:v for k,v in rename_map.items() if k in display_df.columns}
    if rename_available:
        display_df = display_df.rename(rename_available)
    
    print(display_df)
    print(f"\nTotal: {len(df)} records")
    
    if output_file:
        df.write_csv(output_file)
    
    return df

if __name__ == "__main__":
    eibrtlio()
