import polars as pl
import pyreadstat
from pathlib import Path
from datetime import datetime, timedelta
import saspy

def eibrtlio():
    input_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRTLIO")
    output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRTLIO")
    
    # Create output directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Calculate date using datetime timedelta
    current_date = datetime.now()
    previous_date = current_date - timedelta(days=1)
    
    mm = previous_date.month
    mm1 = mm - 1 if mm > 1 else 12
    
    reptmon = f"{mm:02d}"
    reptmon1 = f"{mm1:02d}"
    reptyear = str(previous_date.year)
    reptday = f"{previous_date.day:02d}"
    rdate = previous_date.strftime("%d%m%y")
    ndate = f"{previous_date.day:02d}{previous_date.month:02d}"
    
    print(f"REPTMON: {reptmon}, RDATE: {rdate}")
    print(f"Input path: {input_path}")
    print(f"Output path: {output_path}")
    
    # Read both datasets from sas7bdat files (all lowercase)
    try:
        dp_df, dp_meta = pyreadstat.read_sas7bdat(
            input_path / f"dpnpgs{reptmon}.sas7bdat"
        )
        dp_df = pl.from_pandas(dp_df)
        # Convert all column names to lowercase
        dp_df = dp_df.rename({col: col.lower() for col in dp_df.columns})
        print(f"DP columns: {dp_df.columns}")
        print(f"DP shape: {dp_df.shape}")
    except FileNotFoundError:
        dp_df = pl.DataFrame()
        print(f"File not found: dpnpgs{reptmon}.sas7bdat")
    except Exception as e:
        dp_df = pl.DataFrame()
        print(f"Error reading dpnpgs{reptmon}.sas7bdat: {e}")
    
    try:
        ln_df, ln_meta = pyreadstat.read_sas7bdat(
            input_path / f"lnipgs{reptmon}.sas7bdat"
        )
        ln_df = pl.from_pandas(ln_df)
        # Convert all column names to lowercase
        ln_df = ln_df.rename({col: col.lower() for col in ln_df.columns})
        print(f"LN columns: {ln_df.columns}")
        print(f"LN shape: {ln_df.shape}")
    except FileNotFoundError:
        ln_df = pl.DataFrame()
        print(f"File not found: lnipgs{reptmon}.sas7bdat")
    except Exception as e:
        ln_df = pl.DataFrame()
        print(f"Error reading lnipgs{reptmon}.sas7bdat: {e}")
    
    # Check if both dataframes are empty
    if dp_df.is_empty() and ln_df.is_empty():
        print("No data found in DPNPGS or LNIPGS")
        return
    
    # If one is empty, use the other
    if dp_df.is_empty():
        print("DP is empty, using only LN data")
        combined_df = ln_df
    elif ln_df.is_empty():
        print("LN is empty, using only DP data")
        combined_df = dp_df
    else:
        # Check if columns match
        dp_cols = set(dp_df.columns)
        ln_cols = set(ln_df.columns)
        
        if dp_cols != ln_cols:
            print(f"Column mismatch detected!")
            print(f"DP only columns: {dp_cols - ln_cols}")
            print(f"LN only columns: {ln_cols - ln_cols}")
            print(f"Common columns: {dp_cols & ln_cols}")
            
            # Option 1: Use only common columns
            common_cols = list(dp_cols & ln_cols)
            if common_cols:
                print(f"Using only common columns: {common_cols}")
                dp_df = dp_df.select(common_cols)
                ln_df = ln_df.select(common_cols)
            else:
                print("No common columns found!")
                return
        else:
            # Ensure same column order
            ln_df = ln_df.select(dp_df.columns)
        
        # Combine datasets
        combined_df = pl.concat([dp_df, ln_df])
    
    print(f"Combined shape: {combined_df.shape}")
    print(f"Combined columns: {combined_df.columns}")
    
    if combined_df.is_empty():
        print("No data after combining")
        return
    
    # Create TL dataset
    if 'cvar13' in combined_df.columns:
        tl_df = combined_df.filter(pl.col("cvar13").str.strip_chars() != "")
        tl_df = tl_df.with_columns(
            pl.col("cvar13").alias("ndate"),
            pl.col("cvar12").alias("status")
        ).select(["cvar01", "cvar06", "status", "ndate"])
    else:
        print("Column 'cvar13' not found in combined data")
        tl_df = pl.DataFrame()
    
    # Write TL as parquet to output directory
    if not tl_df.is_empty():
        tl_df.write_parquet(output_path / "tl.parquet")
        
        # Write TL as sas7bdat using saspy to output directory
        sas = saspy.SASsession()
        tl_pandas = tl_df.to_pandas()
        sas.df2sd(tl_pandas, table='tl', libref='work')
        sas.submit(f"PROC EXPORT DATA=work.tl OUTFILE='{output_path}/tl.sas7bdat' DBMS=SAS7BDAT REPLACE; RUN;")
    
    # Process NPGS data
    npgs_df = combined_df.with_columns(
        pl.when(pl.col("cvar12") == "npl").then(pl.lit("np")).otherwise(pl.lit("ap")).alias("cvar12a")
    )
    
    # Filter for natguar='06' AND cinstcl='18'
    if 'natguar' in npgs_df.columns and 'cinstcl' in npgs_df.columns:
        npgs3_df = npgs_df.filter(
            (pl.col("natguar") == "06") &
            (pl.col("cinstcl") == "18")
        )
    else:
        print("Warning: 'natguar' or 'cinstcl' columns not found")
        print(f"Available columns: {npgs_df.columns}")
        npgs3_df = npgs_df  # Use all data if filter columns not found
    
    npgs3_df = npgs3_df.with_columns(pl.lit(" " * 10).alias("cvarxx"))
    npgs3_df = npgs3_df.sort(["cvar01", "cvar06"])
    
    # Write SC167T text file to output directory
    with open(output_path / "sc167t.txt", 'w') as f:
        for row in npgs3_df.iter_rows(named=True):
            cvar01 = f"{row.get('cvar01', 0):10.0f}"
            cvar02 = f"{row.get('cvar02', ''):2s}"
            cvar03 = f"{row.get('cvar03', ''):15s}"
            cvar04 = f"{row.get('cvar04', ''):50s}"
            
            # cvar05 date handling
            cvar05 = " " * 10
            if 'cvar05' in row and row['cvar05']:
                try:
                    if hasattr(row['cvar05'], 'strftime'):
                        cvar05 = row['cvar05'].strftime("%d/%m/%Y")
                    else:
                        cvar05 = str(row['cvar05']).rjust(10)
                except:
                    cvar05 = " " * 10
            
            cvar06 = f"{row.get('cvar06', 0):10.0f}"
            cvar07 = f"{row.get('cvar07', ''):2s}"
            cvar08 = f"{row.get('cvar08', 0):10.2f}"
            cvar09 = f"{row.get('cvar09', 0):10.2f}"
            cvar10 = f"{row.get('cvar10', 0):10.2f}"
            cvar11 = f"{row.get('cvar11', 0):5.0f}"
            cvar12a = f"{row.get('cvar12a', ''):4s}"
            cvar13 = f"{row.get('cvar13', ''):10s}"
            cvar14 = f"{row.get('cvar14', ''):4s}"
            cvar15 = f"{row.get('cvar15', ''):5s}"
            
            line = f"{cvar01};{cvar02};{cvar03};{cvar04};{cvar05};{cvar06};" \
                   f"{cvar07};{cvar08};{cvar09};{cvar10};{cvar11};{cvar12a};" \
                   f"{cvar13};{cvar14};{cvar15};"
            f.write(line + "\n")
    
    # Write NPGS3 as parquet to output directory
    npgs3_df.write_parquet(output_path / "npgs3.parquet")
    
    # Write NPGS3 as sas7bdat using saspy to output directory
    npgs3_pandas = npgs3_df.to_pandas()
    sas.df2sd(npgs3_pandas, table='npgs3', libref='work')
    sas.submit(f"PROC EXPORT DATA=work.npgs3 OUTFILE='{output_path}/npgs3.sas7bdat' DBMS=SAS7BDAT REPLACE; RUN;")
    
    # Generate report to output directory
    generate_simple_report(npgs3_df, rdate, output_path / "sc167r.txt")
    
    print(f"Processing complete. Output files written to: {output_path}")
    print(f"Files: tl.parquet, tl.sas7bdat, sc167t.txt, npgs3.parquet, npgs3.sas7bdat, sc167r.txt")
    
    # Close SAS session
    sas.endsas()

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
        
        # Show summary by cvar02 (scheme)
        if 'cvar02' in df.columns:
            summary = df.group_by("cvar02").agg(pl.count().alias("count"))
            f.write("Accounts by scheme:\n")
            for row in summary.iter_rows(named=True):
                f.write(f"  Scheme {row['cvar02']}: {row['count']} accounts\n")
        
        f.write("\nFirst 10 records:\n")
        
        # Display first few records
        cols_to_show = ['cvar01', 'cvar02', 'cvar03', 'cvar06', 'cvar08', 'cvar09', 'cvar12a']
        display_cols = [c for c in cols_to_show if c in df.columns]
        
        if display_cols:
            display_df = df.select(display_cols).head(10)
            
            # Rename for readability
            rename_map = {
                'cvar01': 'ref_no', 'cvar02': 'sch', 'cvar03': 'ic_no',
                'cvar06': 'acct_no', 'cvar08': 'loan_amt', 'cvar09': 'os_bal',
                'cvar12a': 'status'
            }
            rename_available = {k:v for k,v in rename_map.items() if k in display_df.columns}
            if rename_available:
                display_df = display_df.rename(rename_available)
            
            f.write(str(display_df))
    
    print(f"Report saved to {output_file}")

# For CGCRPT module (if needed separately)
def cgcrpt(df, rdate, output_file=None):
    """CGCRPT report generator"""
    if df.is_empty():
        return df
    
    print("=" * 60)
    print("PUBLIC ISLAMIC BANK BERHAD")
    print(f"DETAIL OF ACCTS FOR SUBMISSION TO CGC @ {rdate}")
    print("=" * 60)
    
    # Simple display
    cols = ['cvar01','cvar02','cvar03','cvar04','cvar06',
            'cvar08','cvar09','cvar10','cvar11','cvar12a',
            'cvar13','cvar14','cvar15']
    
    display_df = df.select([c for c in cols if c in df.columns])
    rename_map = {
        'cvar01': 'ref_no', 'cvar02': 'sch', 'cvar03': 'ic_no',
        'cvar04': 'customer', 'cvar06': 'acct_no', 'cvar08': 'loan_amt',
        'cvar09': 'os_balance', 'cvar10': 'interest', 'cvar11': 'arrears',
        'cvar12a': 'status', 'cvar13': 'npl_date', 'cvar14': 'npl_notify',
        'cvar15': 'npl_reason'
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
