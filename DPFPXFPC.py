import polars as pl
from pathlib import Path
import datetime
import saspy

# Configuration
deposit_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/ICL_SASDATA")
output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQPICL")
output_path.mkdir(exist_ok=True)

# Initialize SAS session
try:
    sas = saspy.SASsession()
    print("SAS session initialized successfully")
except Exception as e:
    print(f"Warning: Could not initialize SAS session: {e}")
    print("Will continue without SAS output")
    sas = None

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
print(f"Report Date: {reptdate}")
print(f"Start Date: {SDATE}")
print("-" * 60)

# Create REPTDATE DataFrame (KEEP=REPTDATE)
reptdate_df = pl.DataFrame({'REPTDATE': [reptdate]})
reptdate_df.write_parquet(output_path / "REPTDATE.parquet")
reptdate_df.write_csv(output_path / "REPTDATE.csv")
print(f"Created REPTDATE.parquet and REPTDATE.csv")

# Function to write SAS dataset using saspy
def write_sas_dataset(df, filename, sas_session, libref='WORK'):
    """
    Write a Polars DataFrame to SAS dataset using saspy
    """
    if sas_session is None:
        print(f"SAS session not available, skipping SAS output for {filename}")
        return False
    
    try:
        # Convert Polars DataFrame to pandas
        df_pd = df.to_pandas()
        
        # Create SAS dataset
        sas_df = sas_session.df2sd(df_pd, table=filename, libref=libref)
        print(f"Created SAS dataset: {libref}.{filename}")
        return True
    except Exception as e:
        print(f"Error creating SAS dataset {filename}: {e}")
        return False

# Function to process text files and output to Parquet and SAS
def process_text_file(input_filename, output_basename, deposit_path, output_path, reptmon, repyear, sas_session):
    """
    Process fixed-width text file and output to Parquet and SAS formats
    """
    filename_parquet = f"{output_basename}{reptmon}{repyear}.parquet"
    filename_sas = f"{output_basename}{reptmon}{repyear}"
    full_input_path = Path(input_filename)
    
    try:
        # Check if file exists
        if not full_input_path.exists():
            raise FileNotFoundError(f"Input file {input_filename} not found")
        
        # Read text file
        with open(full_input_path, 'r') as f:
            lines = f.readlines()
        
        print(f"Read {len(lines)} lines from {input_filename}")
        
        # Parse fixed-width format: @001 ACCTNO 10., @012 CURBAL 10.
        data = []
        skipped_lines = 0
        for line_num, line in enumerate(lines, 1):
            # Remove newline and trailing spaces
            line = line.rstrip('\n\r')
            
            # Skip empty lines
            if not line.strip():
                skipped_lines += 1
                continue
            
            # Extract fields based on fixed positions
            # ACCTNO: positions 0-9 (10 characters)
            # CURBAL: positions 11-20 (10 characters)
            if len(line) < 11:
                print(f"Warning: Line {line_num} is too short: '{line}'")
                skipped_lines += 1
                continue
                
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
            else:
                skipped_lines += 1
        
        print(f"Successfully parsed {len(data)} records, skipped {skipped_lines} lines")
        
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
            print(f"Total records after deduplication: {len(df)}")
            print("\nFirst 10 records:")
            print(df.head(10))
            print("\nLast 10 records:")
            print(df.tail(10))
            
            total_curbal = df.select(pl.col('CURBAL').sum()).row(0)[0]
            if total_curbal is not None:
                print(f"\nTOTAL CURBAL: {total_curbal:,.2f}")
            else:
                print(f"\nTOTAL CURBAL: 0.00")
            
            # Additional statistics
            if len(df) > 0:
                print(f"Minimum CURBAL: {df.select(pl.col('CURBAL').min()).row(0)[0]:,.2f}")
                print(f"Maximum CURBAL: {df.select(pl.col('CURBAL').max()).row(0)[0]:,.2f}")
                print(f"Average CURBAL: {df.select(pl.col('CURBAL').mean()).row(0)[0]:,.2f}")
            print("-" * 60)
        
        # Save to Parquet
        df.write_parquet(deposit_path / filename_parquet)
        print(f"Created {filename_parquet} ({len(df)} records)")
        
        # Save to SAS (using saspy)
        if sas_session is not None and not df.is_empty():
            # Write to SAS dataset in the deposit path
            success = write_sas_dataset(df, filename_sas, sas_session, 'WORK')
            
            # If successful, copy to the deposit path
            if success:
                try:
                    # Copy from WORK to the deposit path
                    sas_code = f"""
                    proc copy in=WORK out={str(deposit_path).replace('/', '.')};
                    select {filename_sas};
                    run;
                    """
                    sas_session.submit(sas_code)
                    print(f"Copied SAS dataset to: {deposit_path}/{filename_sas}.sas7bdat")
                except Exception as e:
                    print(f"Error copying SAS dataset to deposit path: {e}")
                    print(f"SAS dataset remains in WORK library as {filename_sas}")
        elif sas_session is not None and df.is_empty():
            # Create empty SAS dataset
            try:
                # Create an empty dataset with the correct structure
                sas_code = f"""
                data {filename_sas};
                ACCTNO = .;
                CURBAL = .;
                stop;
                run;
                """
                sas_session.submit(sas_code)
                print(f"Created empty SAS dataset: {filename_sas}")
                
                # Copy to deposit path
                sas_code = f"""
                proc copy in=WORK out={str(deposit_path).replace('/', '.')};
                select {filename_sas};
                run;
                """
                sas_session.submit(sas_code)
                print(f"Copied empty SAS dataset to: {deposit_path}/{filename_sas}.sas7bdat")
            except Exception as e:
                print(f"Error creating empty SAS dataset: {e}")
        else:
            print(f"Skipping SAS output for {filename_sas} (SAS session not available)")
        
        return df
        
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        print(f"Creating empty dataframe for {output_basename}")
        empty_df = pl.DataFrame({'ACCTNO': [], 'CURBAL': []})
        
        # Save empty Parquet
        empty_df.write_parquet(deposit_path / filename_parquet)
        print(f"Created empty {filename_parquet}")
        
        # Create empty SAS dataset
        if sas_session is not None:
            try:
                sas_code = f"""
                data {filename_sas};
                ACCTNO = .;
                CURBAL = .;
                stop;
                run;
                proc copy in=WORK out={str(deposit_path).replace('/', '.')};
                select {filename_sas};
                run;
                """
                sas_session.submit(sas_code)
                print(f"Created empty SAS dataset: {deposit_path}/{filename_sas}.sas7bdat")
            except Exception as e:
                print(f"Error creating empty SAS dataset: {e}")
        
        return empty_df
    except Exception as e:
        print(f"Error processing {input_filename}: {e}")
        import traceback
        traceback.print_exc()
        return pl.DataFrame({'ACCTNO': [], 'CURBAL': []})

# Process PBB file (PBB_ICR.txt)
print("\n" + "="*60)
print("Processing PBB_ICR.txt...")
print("="*60)
pbb_df = process_text_file(
    "PBB_ICR.txt", 
    "ICLPBB", 
    deposit_path, 
    output_path, 
    REPTMON, 
    REPTYEAR,
    sas
)

# Process PIBB file (PIBB_ICR.txt)
print("\n" + "="*60)
print("Processing PIBB_ICR.txt...")
print("="*60)
pibb_df = process_text_file(
    "PIBB_ICR.txt", 
    "ICLPIBB", 
    deposit_path, 
    output_path, 
    REPTMON, 
    REPTYEAR,
    sas
)

print("\n" + "="*60)
print("PROCESSING COMPLETED SUCCESSFULLY")
print("="*60)
print(f"\nOutput files created:")
print(f"  Parquet files (deposit path): {deposit_path}")
print(f"    - ICLPBB{REPTMON}{REPTYEAR}.parquet ({len(pbb_df)} records)")
print(f"    - ICLPIBB{REPTMON}{REPTYEAR}.parquet ({len(pibb_df)} records)")
print(f"\n  SAS datasets (deposit path): {deposit_path}")
print(f"    - ICLPBB{REPTMON}{REPTYEAR}.sas7bdat ({len(pbb_df)} records)")
print(f"    - ICLPIBB{REPTMON}{REPTYEAR}.sas7bdat ({len(pibb_df)} records)")
print(f"\n  Additional outputs (output path): {output_path}")
print(f"    - REPTDATE.parquet")
print(f"    - REPTDATE.csv")

# Close SAS session
if sas is not None:
    try:
        sas.endsas()
        print("\nSAS session closed successfully")
    except:
        pass

print("\n" + "="*60)
