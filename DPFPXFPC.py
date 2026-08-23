import polars as pl
import pyreadstat
from pathlib import Path
from datetime import datetime, timedelta
import saspy

def eibrp159():
    npgs_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRP159")
    
    # Step 1: Get previous day's date (replacing REPTDATE)
    current_date = datetime.now()
    prev_date = current_date - timedelta(days=1)
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
    
    print(f"REPTMON: {reptmon}, RDATE: {rdate}")
    
    # Step 2: NPGS data - read SAS7BDAT files with pyreadstat
    dp_df = pl.DataFrame()
    ln_df = pl.DataFrame()
    
    dp_file = npgs_path / f"dpipgs{reptmon}.sas7bdat"
    ln_file = npgs_path / f"lnipgs{reptmon}.sas7bdat"
    
    if dp_file.exists():
        dp_df, dp_meta = pyreadstat.read_sas7bdat(str(dp_file))
        dp_df = pl.from_pandas(dp_df)
        # Convert column names to lowercase
        dp_df.columns = [col.lower() for col in dp_df.columns]
    
    if ln_file.exists():
        ln_df, ln_meta = pyreadstat.read_sas7bdat(str(ln_file))
        ln_df = pl.from_pandas(ln_df)
        # Convert column names to lowercase
        ln_df.columns = [col.lower() for col in ln_df.columns]
    
    # Concatenate dataframes
    if len(dp_df) > 0 or len(ln_df) > 0:
        npgs_df = pl.concat([dp_df, ln_df], how="vertical")
    else:
        npgs_df = pl.DataFrame()
    
    # Add CVARXX column with 10 spaces
    if len(npgs_df) > 0:
        npgs_df = npgs_df.with_columns(
            pl.lit("          ").alias("cvarxx")  # 10 spaces
        )
    
    # Step 3: Write MEFT.txt file with exact SAS fixed positions
    meft_path = Path("MEFT.txt")
    with open(meft_path, 'w') as f:
        if len(npgs_df) > 0:
            for row in npgs_df.iter_rows(named=True):
                # Format each field exactly as SAS PUT statements
                # Using lowercase field names
                cvar01 = f"{row.get('cvar01', 0):10.0f}"
                cvar02 = f"{str(row.get('cvar02', '')):2s}"
                cvar03 = f"{str(row.get('cvar03', '')):15s}"
                cvar04 = f"{str(row.get('cvar04', '')):50s}"
                
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
                cvar06 = f"{row.get('cvar06', 0):10.0f}"
                cvar07 = f"{str(row.get('cvar07', '')):2s}"
                cvar08 = f"{row.get('cvar08', 0):10.2f}"
                cvar09 = f"{row.get('cvar09', 0):10.2f}"
                cvar10 = f"{row.get('cvar10', 0):10.2f}"
                cvar11 = f"{row.get('cvar11', 0):5.0f}"
                cvar12 = f"{str(row.get('cvar12', '')):3s}"
                cvar13 = f"{str(row.get('cvar13', '')):10s}"
                cvar14 = f"{str(row.get('cvar14', '')):4s}"
                cvar15 = f"{str(row.get('cvar15', '')):5s}"
                
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
    
    # Create MEFR.txt report
    mefr_path = Path("MEFR.txt")
    with open(mefr_path, 'w') as f:
        f.write(f"MEF Report - Date: {rdate}\n")
        f.write("=" * 60 + "\n")
        f.write(f"Total records processed: {len(npgs_df)}\n")
        
        # Simple summary by cvar02 (lowercase)
        if len(npgs_df) > 0 and 'cvar02' in npgs_df.columns:
            summary = npgs_df.group_by("cvar02").agg(pl.count().alias("count"))
            for row in summary.iter_rows(named=True):
                f.write(f"cvar02={row['cvar02']}: {row['count']} records\n")
    
    # Step 5: Output to SAS7BDAT and Parquet files
    output_parquet_path = Path("eibrp159_output.parquet")
    output_sas7bdat_path = Path("eibrp159_output.sas7bdat")
    
    if len(npgs_df) > 0:
        # Save as Parquet
        npgs_df.write_parquet(output_parquet_path)
        print(f"Parquet output saved to: {output_parquet_path}")
        
        # Save as SAS7BDAT using saspy
        try:
            # Initialize SAS session
            sas = saspy.SASsession()
            
            # Convert polars DataFrame to pandas for SAS
            pd_df = npgs_df.to_pandas()
            
            # Upload pandas DataFrame to SAS
            sas_df = sas.df2sd(pd_df, 'work.npgs_output')
            
            # Save as SAS7BDAT
            sas_code = f"""
            PROC EXPORT DATA=work.npgs_output 
                OUTFILE="{output_sas7bdat_path}" 
                DBMS=SAS7BDAT REPLACE;
            RUN;
            """
            sas.submit(sas_code)
            
            # Close SAS session
            sas.endsas()
            print(f"SAS7BDAT output saved to: {output_sas7bdat_path}")
            
        except Exception as e:
            print(f"Warning: Could not create SAS7BDAT file with saspy: {e}")
            print("Using alternative method with pyreadstat...")
            
            try:
                # Alternative: use pyreadstat to write SAS7BDAT
                pd_df = npgs_df.to_pandas()
                pyreadstat.write_sas7bdat(pd_df, str(output_sas7bdat_path))
                print(f"SAS7BDAT output saved to: {output_sas7bdat_path}")
            except Exception as e2:
                print(f"Error creating SAS7BDAT: {e2}")
    
    print(f"MEFT.txt file created with {len(npgs_df)} records")
    print(f"Report saved to MEFR.txt")

if __name__ == "__main__":
    eibrp159()
