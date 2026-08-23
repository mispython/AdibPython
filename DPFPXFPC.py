import polars as pl
import pyreadstat
from pathlib import Path
from datetime import datetime, timedelta
import saspy
import sys
import os

# Add the script's directory to Python path to import CGCRPT
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import the existing CGCRPT module
from CGCRPT import cgc_report

def check_sas_log(sas, context=""):
    """Check SAS log for errors and print relevant messages"""
    try:
        log = sas.lastlog()
        if 'ERROR' in log.upper():
            print(f"\nSAS LOG ERROR detected {context}:")
            # Print lines containing ERROR
            for line in log.split('\n'):
                if 'ERROR' in line.upper():
                    print(f"  {line.strip()}")
    except:
        pass

def eibrtlio():
    input_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRTLIO")
    output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRTLIO")
    
    # Create output directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Initialize SAS session at the beginning
    sas = saspy.SASsession()
    
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
    dp_df = pl.DataFrame()
    ln_df = pl.DataFrame()
    
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
        print(f"File not found: dpnpgs{reptmon}.sas7bdat")
    except Exception as e:
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
        print(f"File not found: lnipgs{reptmon}.sas7bdat")
    except Exception as e:
        print(f"Error reading lnipgs{reptmon}.sas7bdat: {e}")
    
    # Check if both dataframes are empty
    if dp_df.is_empty() and ln_df.is_empty():
        print("No data found in DPNPGS or LNIPGS")
        sas.endsas()
        return
    
    # If one is empty, use the other
    if dp_df.is_empty():
        print("DP is empty, using only LN data")
        combined_df = ln_df
    elif ln_df.is_empty():
        print("LN is empty, using only DP data")
        combined_df = dp_df
    else:
        # Check if columns match and combine
        dp_cols = set(dp_df.columns)
        ln_cols = set(ln_df.columns)
        
        if dp_cols != ln_cols:
            print(f"Column mismatch detected!")
            print(f"DP only columns: {dp_cols - ln_cols}")
            print(f"LN only columns: {ln_cols - ln_cols}")
            print(f"Common columns: {dp_cols & ln_cols}")
            
            # Use common columns
            common_cols = list(dp_cols & ln_cols)
            if common_cols:
                print(f"Using only common columns: {common_cols}")
                dp_df = dp_df.select(common_cols)
                ln_df = ln_df.select(common_cols)
            else:
                print("No common columns found!")
                sas.endsas()
                return
        else:
            # Ensure same column order
            ln_df = ln_df.select(dp_df.columns)
        
        # Combine datasets
        combined_df = pl.concat([dp_df, ln_df])
    
    print(f"Combined shape: {combined_df.shape}")
    
    if combined_df.is_empty():
        print("No data after combining")
        sas.endsas()
        return
    
    # Create TL dataset (like DATA NPGS.TL in SAS)
    if 'cvar13' in combined_df.columns:
        tl_df = combined_df.filter(pl.col("cvar13").str.strip_chars() != "")
        tl_df = tl_df.with_columns(
            pl.col("cvar13").alias("ndate"),
            pl.col("cvar12").alias("status")
        ).select(["cvar01", "cvar06", "status", "ndate"])
        
        # Write TL as parquet (always write, even if empty)
        tl_df.write_parquet(output_path / "tl.parquet")
        print(f"TL dataset shape: {tl_df.shape}")
        
        # Write TL as sas7bdat using saspy (only if not empty)
        if not tl_df.is_empty():
            try:
                tl_pandas = tl_df.to_pandas()
                sas.df2sd(tl_pandas, table='tl', libref='work')
                sas.submit(f"PROC EXPORT DATA=work.tl OUTFILE='{output_path}/tl.sas7bdat' DBMS=SAS7BDAT REPLACE; RUN;")
                check_sas_log(sas, "for TL export")
            except Exception as e:
                print(f"Error exporting TL to SAS: {e}")
        else:
            print("TL dataset is empty, skipping SAS export")
    else:
        print("Column 'cvar13' not found, skipping TL dataset")
    
    # Process NPGS data (like DATA NPGS in SAS)
    npgs_df = combined_df.with_columns(
        pl.when(pl.col("cvar12") == "npl").then(pl.lit("np")).otherwise(pl.lit("ap")).alias("cvar12a")
    )
    
    # Filter for natguar='06' AND cinstcl='18' (like DATA NPGS3 in SAS)
    if 'natguar' in npgs_df.columns and 'cinstcl' in npgs_df.columns:
        npgs3_df = npgs_df.filter(
            (pl.col("natguar") == "06") &
            (pl.col("cinstcl") == "18")
        )
        print(f"NPGS3 filtered shape: {npgs3_df.shape}")
    else:
        print("Warning: 'natguar' or 'cinstcl' columns not found")
        print(f"Available columns: {npgs_df.columns}")
        npgs3_df = npgs_df  # Use all data if filter columns not found
    
    # Add CVARXX column (like CVARXX='          ' in SAS)
    npgs3_df = npgs3_df.with_columns(pl.lit(" " * 10).alias("cvarxx"))
    
    # Sort (like PROC SORT; BY CVAR01 CVAR06 in SAS)
    npgs3_df = npgs3_df.sort(["cvar01", "cvar06"])
    
    # Write SC167T text file (like DATA SC93T with FILE SC167T in SAS)
    with open(output_path / "sc167t.txt", 'w') as f:
        for row in npgs3_df.iter_rows(named=True):
            # Format each field according to SAS PUT statements
            cvar01 = f"{row.get('cvar01', 0):10.0f}"
            cvar02 = f"{row.get('cvar02', ''):2s}"
            cvar03 = f"{row.get('cvar03', ''):15s}"
            cvar04 = f"{row.get('cvar04', ''):50s}"
            
            # cvar05 date handling (DDMMYY10 format)
            cvar05 = " " * 10
            if 'cvar05' in row and row['cvar05']:
                try:
                    if hasattr(row['cvar05'], 'strftime'):
                        cvar05 = row['cvar05'].strftime("%d/%m/%Y")
                    else:
                        # Try to parse as date
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
    
    # Write NPGS3 as parquet
    npgs3_df.write_parquet(output_path / "npgs3.parquet")
    
    # Write NPGS3 as sas7bdat using saspy (only if not empty)
    if not npgs3_df.is_empty():
        try:
            npgs3_pandas = npgs3_df.to_pandas()
            sas.df2sd(npgs3_pandas, table='npgs3', libref='work')
            sas.submit(f"PROC EXPORT DATA=work.npgs3 OUTFILE='{output_path}/npgs3.sas7bdat' DBMS=SAS7BDAT REPLACE; RUN;")
            check_sas_log(sas, "for NPGS3 export")
        except Exception as e:
            print(f"Error exporting NPGS3 to SAS: {e}")
    else:
        print("NPGS3 dataset is empty, skipping SAS export")
    
    # Generate report using the existing CGCRPT module
    # Equivalent to: PROC PRINTTO PRINT=SC167R; TITLE1 '...'; TITLE2 '...'; %INC PGM(CGCRPT);
    title1 = 'PUBLIC ISLAMIC BANK BERHAD'
    title2 = f'DETAIL OF ACCTS FOR SUBMISSION TO CGC @ {rdate}'
    report_path = output_path / "sc167r.txt"
    
    print(f"Generating report using CGCRPT module...")
    cgc_report(
        df=npgs3_df,
        report_path=str(report_path),
        title1=title1,
        title2=title2
    )
    
    # List all output files
    print(f"\nOutput files in {output_path}:")
    for file in sorted(output_path.iterdir()):
        if file.is_file():
            size = file.stat().st_size
            print(f"  {file.name} ({size:,} bytes)")
    
    print(f"\nProcessing complete. Output files written to: {output_path}")
    
    # Close SAS session
    sas.endsas()

if __name__ == "__main__":
    eibrtlio()
