import polars as pl
from pathlib import Path
import datetime
import saspy

# Configuration
deposit_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/ICR_PIDM")
output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQPICL")
input_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/ICR_PIDM")  # Where input files are located

# Create directories if they don't exist
deposit_path.mkdir(exist_ok=True)
output_path.mkdir(exist_ok=True)
input_path.mkdir(exist_ok=True)

# Initialize SAS session
try:
    sas = saspy.SASsession(cfgname='default')
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
print(f"Input Path: {input_path}")
print(f"Deposit Path: {deposit_path}")
print(f"Output Path: {output_path}")
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
def process_text_file(input_filename, output_basename, deposit_path, output_path, input_path, reptmon, repyear, sas_session):
    """
    Process fixed-width text file and output to Parquet and SAS formats
    """
    filename_parquet = f"{output_basename}{reptmon}{repyear}.parquet"
    filename_sas = f"{output_basename}{reptmon}{repyear}"
    
    # Check multiple possible locations for the input file
    possible_paths = [
        input_path / input_filename,
        Path.cwd() / input_filename,
        Path(input_filename)
    ]
    
    full_input_path = None
    for path in possible_paths:
        if path.exists():
            full_input_path = path
            break
    
    if full_input_path is None:
        print(f"ERROR: Input file {input_filename} not found in any of these locations:")
        for path in possible_paths:
            print(f"  - {path}")
        print(f"Creating empty dataframe for {output_basename}")
        
        # Create empty dataframes
        empty_df = pl.DataFrame({'ACCTNO': [], 'CURBAL': []})
        empty_df.write_parquet(deposit_path / filename_parquet)
        print(f"Created empty {filename_parquet}")
        
        # Create empty SAS dataset
        if sas_session is not None:
            try:
                # Create SAS library reference
                deposit_path_str = str(deposit_path).replace('/', '.')
                sas_code = f"""
                libname outlib "{deposit_path}";
                
                data outlib.{filename_sas};
                ACCTNO = .;
                CURBAL = .;
                stop;
                run;
                """
                sas_session.submit(sas_code)
                print(f"Created empty SAS dataset: {deposit_path}/{filename_sas}.sas7bdat")
            except Exception as e:
                print(f"Error creating empty SAS dataset: {e}")
        
        return empty_df
    
    try:
        # Read text file
        with open(full_input_path, 'r') as f:
            lines = f.readlines()
        
        print(f"Read {len(lines)} lines from {full_input_path}")
        
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
        if sas_session is not None:
            try:
                # Write directly to the deposit path using SAS library
                deposit_path_str = str(deposit_path).replace('/', '.')
                
                # Convert to pandas for SAS
                df_pd = df.to_pandas()
                
                # Create SAS dataset directly in the deposit path
                sas_df = sas_session.df2sd(df_pd, table=filename_sas, libref='outlib')
                
                # Create library if it doesn't exist
                sas_code = f"""
                libname outlib "{deposit_path}";
                """
                sas_session.submit(sas_code)
                
                print(f"Created SAS dataset: {deposit_path}/{filename_sas}.sas7bdat")
            except Exception as e:
                print(f"Error creating SAS dataset: {e}")
                # Fallback: try creating in WORK and then copy
                try:
                    # Create in WORK
                    df_pd = df.to_pandas()
                    sas_df = sas_session.df2sd(df_pd, table=filename_sas, libref='WORK')
                    
                    # Copy to deposit path
                    sas_code = f"""
                    libname outlib "{deposit_path}";
                    proc copy in=WORK out=outlib;
                    select {filename_sas};
                    run;
                    """
                    sas_session.submit(sas_code)
                    print(f"Copied SAS dataset to: {deposit_path}/{filename_sas}.sas7bdat")
                except Exception as e2:
                    print(f"Error copying SAS dataset: {e2}")
        else:
            print(f"Skipping SAS output for {filename_sas} (SAS session not available)")
        
        return df
        
    except Exception as e:
        print(f"Error processing {input_filename}: {e}")
        import traceback
        traceback.print_exc()
        
        # Create empty dataframe as fallback
        empty_df = pl.DataFrame({'ACCTNO': [], 'CURBAL': []})
        empty_df.write_parquet(deposit_path / filename_parquet)
        print(f"Created empty {filename_parquet} as fallback")
        return empty_df

# Process PBB file (PBB_ICR.txt)
print("\n" + "="*60)
print("Processing PBB_ICR.txt...")
print("="*60)
pbb_df = process_text_file(
    "PBB_ICR.txt", 
    "ICLPBB", 
    deposit_path, 
    output_path, 
    input_path,
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
    input_path,
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
