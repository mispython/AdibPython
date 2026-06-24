import polars as pl
import duckdb
from pathlib import Path
import datetime
import pandas as pd
import pyreadstat

# Configuration
deposit_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/ICL_SASDATA")
output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQPICL")
output_path.mkdir(exist_ok=True)

# DATA REPTDATE (KEEP=REPTDATE);
# REPTDATE=INPUT('01'||PUT(MONTH(TODAY()), Z2.)||PUT(YEAR(TODAY()), 4.), DDMMYY8.)-1;
today = datetime.date.today()
date_string = f"01{today.month:02d}{today.year}"
reptdate = datetime.datetime.strptime(date_string, '%d%m%Y').date() - datetime.timedelta(days=1)

# SELECT(DAY(REPTDATE)); logic
reptday = reptdate.day
if reptday == 8:
    SDD, WK, WK1 = 1, '1', '4'
elif reptday == 15:
    SDD, WK, WK1 = 9, '2', '1'
elif reptday == 22:
    SDD, WK, WK1 = 16, '3', '2'
else:
    SDD, WK, WK1, WK2, WK3 = 23, '4', '3', '2', '1'

MM = reptdate.month

# IF WK = '1' THEN DO;
if WK == '1':
    MM1 = MM - 1
    if MM1 == 0:
        MM1 = 12
else:
    MM1 = MM

# MM2 = MM - 1;
MM2 = MM - 1
if MM2 == 0:
    MM2 = 12

SDATE = datetime.date(reptdate.year, MM, SDD)
SDESC = 'PUBLIC BANK BERHAD'

# CALL SYMPUT equivalent
NOWK = WK
REPTMON = f"{MM:02d}"
REPTYEAR = str(reptdate.year)

print(f"NOWK: {NOWK}, REPTMON: {REPTMON}, REPTYEAR: {REPTYEAR}")
print(f"SDESC: {SDESC}")

# Create REPTDATE DataFrame (KEEP=REPTDATE)
reptdate_df = pl.DataFrame({'REPTDATE': [reptdate]})
reptdate_df.write_parquet(output_path / "REPTDATE.parquet")
reptdate_df.write_csv(output_path / "REPTDATE.csv")

# Convert to pandas for SAS export
reptdate_pd = reptdate_df.to_pandas()
pyreadstat.write_sas7bdat(reptdate_pd, output_path / "REPTDATE.sas7bdat")

# Function to process text files and output to SAS and Parquet
def process_text_file(input_filename, output_basename, deposit_path, output_path, reptmon, repyear):
    """
    Process fixed-width text file and output to SAS and Parquet formats
    """
    filename_parquet = f"{output_basename}{reptmon}{repyear}.parquet"
    filename_sas = f"{output_basename}{reptmon}{repyear}.sas7bdat"
    
    try:
        # Read text file
        with open(input_filename, 'r') as f:
            lines = f.readlines()
        
        # Parse fixed-width format: @001 ACCTNO 10., @012 CURBAL 10.
        data = []
        for line in lines:
            # Remove newline and trailing spaces
            line = line.rstrip('\n\r')
            
            # Extract fields based on fixed positions
            # ACCTNO: positions 0-9 (10 characters)
            # CURBAL: positions 11-20 (10 characters) - note position 11 is index 11 (0-based)
            acctno_str = line[0:10].strip()
            curbal_str = line[11:21].strip() if len(line) > 11 else ''
            
            # Convert to appropriate types
            try:
                acctno = int(acctno_str) if acctno_str else None
            except ValueError:
                acctno = None
                
            try:
                curbal = float(curbal_str) if curbal_str else None
            except ValueError:
                curbal = None
            
            # Only add if ACCTNO is not null
            if acctno is not None:
                data.append({'ACCTNO': acctno, 'CURBAL': curbal})
        
        # Create Polars DataFrame
        df = pl.DataFrame(data)
        
        if df.is_empty():
            print(f"NOTE: No valid data found in {input_filename}, creating empty dataframe")
            df = pl.DataFrame({'ACCTNO': [], 'CURBAL': []})
        else:
            # Remove duplicates and sort
            df = df.unique(subset=['ACCTNO']).sort('ACCTNO')
            
            # Print summary
            print(f"\n{output_basename} DATA SUMMARY:")
            print(df)
            total_curbal = df.select(pl.col('CURBAL').sum()).row(0)[0]
            if total_curbal is not None:
                print(f"TOTAL CURBAL: {total_curbal:.2f}")
            else:
                print(f"TOTAL CURBAL: 0.00")
            print("-" * 50)
        
        # Save to Parquet
        df.write_parquet(deposit_path / filename_parquet)
        print(f"Created {filename_parquet}")
        
        # Convert to pandas and save as SAS
        df_pd = df.to_pandas()
        pyreadstat.write_sas7bdat(df_pd, deposit_path / filename_sas)
        print(f"Created {filename_sas}")
        
        return df
        
    except FileNotFoundError:
        print(f"NOTE: {input_filename} file not found, creating empty dataframe")
        empty_df = pl.DataFrame({'ACCTNO': [], 'CURBAL': []})
        
        # Save empty dataframes
        empty_df.write_parquet(deposit_path / filename_parquet)
        print(f"Created empty {filename_parquet}")
        
        empty_pd = empty_df.to_pandas()
        pyreadstat.write_sas7bdat(empty_pd, deposit_path / filename_sas)
        print(f"Created empty {filename_sas}")
        
        return empty_df
    except Exception as e:
        print(f"Error processing {input_filename}: {e}")
        return pl.DataFrame({'ACCTNO': [], 'CURBAL': []})

# Process PBB file
print("\nProcessing PBB_ICL.txt...")
pbb_df = process_text_file(
    "PBB_ICL.txt", 
    "ICLPBB", 
    deposit_path, 
    output_path, 
    REPTMON, 
    REPTYEAR
)

# Process PIBB file
print("\nProcessing PIBB_ICL.txt...")
pibb_df = process_text_file(
    "PIBB_ICL.txt", 
    "ICLPIBB", 
    deposit_path, 
    output_path, 
    REPTMON, 
    REPTYEAR
)

print("\nPROCESSING COMPLETED SUCCESSFULLY")
print(f"Output files saved to:")
print(f"  - Parquet: {deposit_path}")
print(f"  - SAS: {deposit_path}")
print(f"  - Additional outputs: {output_path}")
