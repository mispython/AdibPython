import polars as pl
import pyreadstat
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import saspy

def eibrp159():
    npgs_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRP159")
    
    # Step 1: Get date (using timedelta to get July data)
    current_date = datetime.now()
    prev_date = current_date - timedelta(days=23)  # Changed to get July data
    reptdate = prev_date.date()
    
    mm = reptdate.month
    mm1 = mm - 1 if mm > 1 else 12
    
    # SAS CALL SYMPUT equivalents
    reptmon = f"{mm:02d}"
    reptmon1 = f"{mm1:02d}"
    reptyear = str(reptdate.year)
    reptday = f"{reptdate.day:02d}"
    rdate = reptdate.strftime("%d%m%y")
    ndate = f"{reptdate.day:02d}{reptdate.month:02d}"
    
    print(f"Processing date: {reptdate}")
    print(f"REPTMON: {reptmon}, REPTMON1: {reptmon1}")
    print(f"RDATE: {rdate}, NDATE: {ndate}")
    
    # Step 2: NPGS data - read SAS7BDAT files with pyreadstat
    dp_df = pl.DataFrame()
    ln_df = pl.DataFrame()
    
    dp_file = npgs_path / f"dpipgs{reptmon}.sas7bdat"
    ln_file = npgs_path / f"lnipgs{reptmon}.sas7bdat"
    
    print(f"\nLooking for input files:")
    print(f"DP file: {dp_file}")
    print(f"LN file: {ln_file}")
    print(f"DP file exists: {dp_file.exists()}")
    print(f"LN file exists: {ln_file.exists()}")
    
    # Check if the directory exists
    if not npgs_path.exists():
        print(f"\nERROR: Directory {npgs_path} does not exist!")
        print("Please check the path and permissions.")
        return
    
    if dp_file.exists():
        try:
            print(f"\nReading DP file: {dp_file}")
            dp_df, dp_meta = pyreadstat.read_sas7bdat(str(dp_file))
            print(f"DP file read successfully. Rows: {len(dp_df)}, Columns: {len(dp_df.columns)}")
            dp_df = pl.from_pandas(dp_df)
            # Convert column names to lowercase
            dp_df.columns = [col.lower() for col in dp_df.columns]
        except Exception as e:
            print(f"Error reading DP file: {e}")
    
    if ln_file.exists():
        try:
            print(f"\nReading LN file: {ln_file}")
            ln_df, ln_meta = pyreadstat.read_sas7bdat(str(ln_file))
            print(f"LN file read successfully. Rows: {len(ln_df)}, Columns: {len(ln_df.columns)}")
            ln_df = pl.from_pandas(ln_df)
            # Convert column names to lowercase
            ln_df.columns = [col.lower() for col in ln_df.columns]
        except Exception as e:
            print(f"Error reading LN file: {e}")
    
    # Handle column mismatches between DP and LN files
    if len(dp_df) > 0 and len(ln_df) > 0:
        # Get all unique columns
        all_columns = list(set(dp_df.columns) | set(ln_df.columns))
        
        # Align columns - add missing columns with None values
        for col in all_columns:
            if col not in dp_df.columns:
                dp_df = dp_df.with_columns(pl.lit(None).alias(col))
            if col not in ln_df.columns:
                ln_df = ln_df.with_columns(pl.lit(None).alias(col))
        
        # Ensure same column order
        ln_df = ln_df.select(dp_df.columns)
        
        # Now concatenate
        npgs_df = pl.concat([dp_df, ln_df], how="vertical")
        print(f"\nConcatenated DP and LN: {len(npgs_df)} rows")
    elif len(dp_df) > 0:
        npgs_df = dp_df
        print(f"\nOnly DP data available: {len(npgs_df)} rows")
    elif len(ln_df) > 0:
        npgs_df = ln_df
        print(f"\nOnly LN data available: {len(npgs_df)} rows")
    else:
        npgs_df = pl.DataFrame()
        print(f"\nNo data available from either file")
    
    # Add CVARXX column with 10 spaces if not exists
    if len(npgs_df) > 0 and 'cvarxx' not in npgs_df.columns:
        npgs_df = npgs_df.with_columns(
            pl.lit("          ").alias("cvarxx")  # 10 spaces
        )
    
    # Step 3: Write MEFT.txt file with exact SAS fixed positions
    meft_path = Path("MEFT.txt")
    with open(meft_path, 'w') as f:
        if len(npgs_df) > 0:
            print(f"\nWriting MEFT.txt...")
            for idx, row in enumerate(npgs_df.iter_rows(named=True)):
                # Format each field exactly as SAS PUT statements
                cvar01 = f"{row.get('cvar01', 0):10.0f}" if row.get('cvar01') is not None else " " * 10
                cvar02 = f"{str(row.get('cvar02', '')):2s}" if row.get('cvar02') is not None else "  "
                cvar03 = f"{str(row.get('cvar03', '')):15s}" if row.get('cvar03') is not None else " " * 15
                cvar04 = f"{str(row.get('cvar04', '')):50s}" if row.get('cvar04') is not None else " " * 50
                
                # CVAR05: DDMMYY10. format
                cvar05 = "          "  # 10 spaces
                if 'cvar05' in row and row['cvar05'] is not None:
                    try:
                        # Handle date
                        if isinstance(row['cvar05'], (datetime, pl.Date, pl.Datetime)):
                            cvar05 = row['cvar05'].strftime("%d/%m/%Y")
                        elif isinstance(row['cvar05'], str):
                            # Try to parse string date
                            try:
                                parsed_date = datetime.strptime(row['cvar05'], "%Y-%m-%d")
                                cvar05 = parsed_date.strftime("%d/%m/%Y")
                            except:
                                cvar05 = str(row['cvar05']).rjust(10)
                        else:
                            # Numeric date - assume it's a datetime value
                            base_date = datetime(1960, 1, 1)
                            actual_date = base_date + timedelta(days=int(row['cvar05']))
                            cvar05 = actual_date.strftime("%d/%m/%Y")
                    except:
                        cvar05 = "          "
                
                cvarxx = "          "  # 10 spaces
                cvar06 = f"{row.get('cvar06', 0):10.0f}" if row.get('cvar06') is not None else " " * 10
                cvar07 = f"{str(row.get('cvar07', '')):2s}" if row.get('cvar07') is not None else "  "
                cvar08 = f"{row.get('cvar08', 0):10.2f}" if row.get('cvar08') is not None else " " * 10
                cvar09 = f"{row.get('cvar09', 0):10.2f}" if row.get('cvar09') is not None else " " * 10
                cvar10 = f"{row.get('cvar10', 0):10.2f}" if row.get('cvar10') is not None else " " * 10
                cvar11 = f"{row.get('cvar11', 0):5.0f}" if row.get('cvar11') is not None else " " * 5
                cvar12 = f"{str(row.get('cvar12', '')):3s}" if row.get('cvar12') is not None else " " * 3
                cvar13 = f"{str(row.get('cvar13', '')):10s}" if row.get('cvar13') is not None else " " * 10
                cvar14 = f"{str(row.get('cvar14', '')):4s}" if row.get('cvar14') is not None else " " * 4
                cvar15 = f"{str(row.get('cvar15', '')):5s}" if row.get('cvar15') is not None else " " * 5
                
                # Write with exact @ positions and semicolons
                line = f"{cvar01};{cvar02};{cvar03};{cvar04};{cvar05};{cvarxx};" \
                       f"{cvar06};{cvar07};{cvar08};{cvar09};{cvar10};{cvar11};" \
                       f"{cvar12};{cvar13};{cvar14};{cvar15};"
                f.write(line + "\n")
    
    # Step 4: Generate report header
    print("\n" + "=" * 60)
    print("PUBLIC BANK BERHAD")
    print(f"DETAIL OF ACCTS (MEF PRODUCTS) FOR SUBMISSION TO CGC @ {rdate}")
    print("=" * 60)
    
    # Create MEFR.txt report
    mefr_path = Path("MEFR.txt")
    with open(mefr_path, 'w') as f:
        f.write(f"MEF Report - Date: {rdate}\n")
        f.write("=" * 60 + "\n")
        f.write(f"Total records processed: {len(npgs_df)}\n")
        
        # Simple summary by cvar02 (using pl.len() instead of pl.count())
        if len(npgs_df) > 0 and 'cvar02' in npgs_df.columns:
            summary = npgs_df.group_by("cvar02").agg(pl.len().alias("count"))
            for row in summary.iter_rows(named=True):
                f.write(f"cvar02={row['cvar02']}: {row['count']} records\n")
    
    # Step 5: Output to SAS7BDAT and Parquet files
    if len(npgs_df) > 0:
        output_parquet_path = Path("eibrp159_output.parquet")
        output_sas7bdat_path = Path("eibrp159_output.sas7bdat")
        
        # Save as Parquet
        npgs_df.write_parquet(output_parquet_path)
        print(f"\nParquet output saved to: {output_parquet_path}")
        
        # Save as SAS7BDAT using saspy
        try:
            print("\nCreating SAS7BDAT file using saspy...")
            
            # Initialize SAS session
            sas = saspy.SASsession()
            
            # Convert polars DataFrame to pandas
            pd_df = npgs_df.to_pandas()
            
            # Upload pandas DataFrame to SAS using sd2df method
            # This creates a SAS dataset in the WORK library
            sas.sd2df(pd_df, 'work.npgs_output')
            
            # Verify the dataset was created
            verify_code = """
            proc contents data=work.npgs_output;
            run;
            """
            verify_log = sas.submit(verify_code, results='TEXT')
            print("SAS dataset verification:")
            print(verify_log)
            
            # Save as permanent SAS dataset
            output_dir = str(output_sas7bdat_path.parent.absolute())
            output_name = output_sas7bdat_path.stem
            
            sas_code = f"""
            libname outlib "{output_dir}";
            data outlib.{output_name};
                set work.npgs_output;
            run;
            """
            
            # Submit and capture log
            log = sas.submit(sas_code, results='TEXT')
            
            # Check for errors in log
            if 'ERROR' in str(log):
                print("SAS log contains errors:")
                print(str(log))
            else:
                print(f"SAS7BDAT output saved to: {output_sas7bdat_path}")
            
            # Close SAS session
            sas.endsas()
            
        except Exception as e:
            print(f"Error creating SAS7BDAT file: {e}")
            print("Please check SAS configuration and permissions")
    
    print(f"\nSummary:")
    print(f"MEFT.txt file created with {len(npgs_df)} records")
    print(f"Report saved to MEFR.txt")

if __name__ == "__main__":
    eibrp159()
