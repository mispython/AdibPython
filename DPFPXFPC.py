import polars as pl
import duckdb
from pathlib import Path
import datetime
import pyreadstat
import numpy as np
from typing import Iterator, Dict, Any
import gc

# Configuration
loan_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMREPOS")
arrear_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMREPOS")
output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMREPOS")
output_path.mkdir(exist_ok=True)

# Calculate dates using datetime (replacing REPTDATE logic)
today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)

# Determine week parameters based on yesterday's date
day = yesterday.day
month = yesterday.month
year = yesterday.year

if day == 8:
    sdd, wk, wk1 = 1, '1', '4'
elif day == 15:
    sdd, wk, wk1 = 9, '2', '1'
elif day == 22:
    sdd, wk, wk1 = 16, '3', '2'
else:
    sdd, wk, wk1 = 23, '4', '3'

mm = month
mm1 = 12 if (wk == '1' and month == 1) else (month - 1 if wk == '1' else month)
sdate = datetime.date(year, month, sdd)

# Extract parameters
nowk = wk
nowk1 = wk1
reptmon = f"{mm:02d}"
reptmon1 = f"{mm1:02d}"
reptyear = str(year)
reptday = f"{day:02d}"
rdate = yesterday.strftime('%d%m%y')
sdate_str = sdate.strftime('%d%m%y')

print(f"NOWK: {nowk}, NOWK1: {nowk1}, REPTMON: {reptmon}, REPTMON1: {reptmon1}")
print(f"REPTYEAR: {reptyear}, REPTDAY: {reptday}, RDATE: {rdate}, SDATE: {sdate_str}")

# Define chunked reader for large SAS files
def read_sas_chunked(file_path: Path, columns: list = None, chunksize: int = 100000) -> Iterator[pl.DataFrame]:
    """Read SAS file in chunks to handle large files efficiently"""
    reader = pyreadstat.read_file_in_chunks(
        pyreadstat.read_sas7bdat,
        str(file_path),
        chunksize=chunksize,
        usecols=columns
    )
    
    for df_chunk, meta in reader:
        # Convert to Polars DataFrame
        chunk_df = pl.from_pandas(df_chunk)
        yield chunk_df

# Process LNNOTE in chunks (large 20GB file)
print("Processing LNNOTE (large file) in chunks...")
print("NOTE: Checking data types and sample values...")

# Initialize empty list to collect filtered data
lnnote_chunks = []
sample_data = None

# Read and filter in chunks
chunk_count = 0
for chunk in read_sas_chunked(loan_path / "lnnote.sas7bdat", 
                              columns=['ACCTNO', 'LOANTYPE', 'NTBRCH', 'COLLDESC', 'COLLYEAR', 'BALANCE', 'BORSTAT'],
                              chunksize=100000):
    chunk_count += 1
    
    # Print sample data from first chunk for debugging
    if chunk_count == 1:
        print("First chunk schema:")
        print(chunk.schema)
        print("\nFirst 5 rows:")
        print(chunk.head(5))
        print(f"\nUnique LOANTYPE values: {chunk['LOANTYPE'].unique().to_list()[:20]}")
        print(f"Unique BORSTAT values: {chunk['BORSTAT'].unique().to_list()[:20]}")
        print(f"BALANCE stats: min={chunk['BALANCE'].min()}, max={chunk['BALANCE'].max()}")
        sample_data = chunk.head(5)
    
    if chunk_count % 10 == 0:
        print(f"Processed {chunk_count} chunks from LNNOTE...")
        gc.collect()  # Free memory
    
    # Convert columns to appropriate types for filtering
    chunk = chunk.with_columns([
        pl.col('LOANTYPE').cast(pl.Utf8, strict=False),
        pl.col('BORSTAT').cast(pl.Utf8, strict=False),
        pl.col('BALANCE').cast(pl.Float64, strict=False)
    ])
    
    # Filter chunk - try multiple HP variations
    hp_values = ['HP', 'hp', 'Hp', 'hP']
    
    filtered_chunk = chunk.filter(
        (pl.col('LOANTYPE').str.to_uppercase().is_in(['HP'])) &  # Case insensitive
        (pl.col('BALANCE').fill_null(0) > 0) &
        (~pl.col('BORSTAT').str.to_uppercase().is_in(['F', 'I', 'R']))
    ).select([
        'ACCTNO', 'LOANTYPE', 'NTBRCH', 'COLLDESC', 'COLLYEAR'
    ])
    
    if filtered_chunk.height > 0:
        lnnote_chunks.append(filtered_chunk)
        if chunk_count == 1:
            print(f"First chunk filtered rows: {filtered_chunk.height}")
            print("Sample filtered data:")
            print(filtered_chunk.head(3))

# After processing all chunks, check if we found any data
total_chunks = chunk_count
print(f"Total chunks processed: {total_chunks}")

# Combine all filtered chunks
if lnnote_chunks:
    lnnote_df = pl.concat(lnnote_chunks).unique(subset=['ACCTNO']).sort('ACCTNO')
    print(f"\nLNNOTE filtered: {lnnote_df.height} rows")
    
    # Print sample of filtered data
    print("\nSample of filtered LNNOTE data:")
    print(lnnote_df.head(10))
    print(f"\nUnique LOANTYPE in filtered: {lnnote_df['LOANTYPE'].unique().to_list()}")
else:
    print("\nWARNING: No records found matching HP criteria!")
    print("Trying alternative filter without LOANTYPE restriction...")
    
    # Try without LOANTYPE filter to see if data exists
    test_chunks = []
    for chunk in read_sas_chunked(loan_path / "lnnote.sas7bdat", 
                                  columns=['ACCTNO', 'LOANTYPE', 'NTBRCH', 'COLLDESC', 'COLLYEAR', 'BALANCE', 'BORSTAT'],
                                  chunksize=100000):
        chunk = chunk.with_columns([
            pl.col('LOANTYPE').cast(pl.Utf8, strict=False),
            pl.col('BORSTAT').cast(pl.Utf8, strict=False),
            pl.col('BALANCE').cast(pl.Float64, strict=False)
        ])
        
        filtered_chunk = chunk.filter(
            (pl.col('BALANCE').fill_null(0) > 0) &
            (~pl.col('BORSTAT').str.to_uppercase().is_in(['F', 'I', 'R']))
        ).select(['ACCTNO', 'LOANTYPE', 'NTBRCH', 'COLLDESC', 'COLLYEAR'])
        
        if filtered_chunk.height > 0:
            test_chunks.append(filtered_chunk)
            print(f"\nFound data without LOANTYPE filter. Sample LOANTYPE values: {filtered_chunk['LOANTYPE'].unique().to_list()[:10]}")
            break  # Just need one chunk to see what LOANTYPE values exist
    
    # Create empty dataframe with correct schema
    lnnote_df = pl.DataFrame(schema={'ACCTNO': pl.Utf8, 'LOANTYPE': pl.Utf8, 'NTBRCH': pl.Utf8, 
                                     'COLLDESC': pl.Utf8, 'COLLYEAR': pl.Utf8})

del lnnote_chunks
gc.collect()

# Read NAME8 (typically smaller file)
print("\nProcessing NAME8...")
try:
    name8_pandas = pyreadstat.read_sas7bdat(str(loan_path / "name8.sas7bdat"), 
                                           usecols=['ACCTNO', 'LINETHRE', 'LINEFOUR'])[0]
    name8_df = pl.from_pandas(name8_pandas).select([
        'ACCTNO', 'LINETHRE', 'LINEFOUR'
    ]).with_columns([
        pl.col('ACCTNO').cast(pl.Utf8, strict=False),
        pl.col('LINETHRE').cast(pl.Utf8, strict=False),
        pl.col('LINEFOUR').cast(pl.Utf8, strict=False)
    ]).sort('ACCTNO')
    
    print(f"NAME8 records: {name8_df.height}")
    if name8_df.height > 0:
        print("Sample NAME8 data:")
        print(name8_df.head(3))
except Exception as e:
    print(f"Error reading NAME8: {e}")
    name8_df = pl.DataFrame(schema={'ACCTNO': pl.Utf8, 'LINETHRE': pl.Utf8, 'LINEFOUR': pl.Utf8})

# Read LOANTEMP (arrears file)
print("\nProcessing LOANTEMP...")
try:
    arrear_pandas = pyreadstat.read_sas7bdat(str(arrear_path / "loantemp.sas7bdat"), 
                                            usecols=['ACCTNO', 'ARREAR'])[0]
    arrear_df = pl.from_pandas(arrear_pandas).select([
        'ACCTNO', 'ARREAR'
    ]).with_columns([
        pl.col('ACCTNO').cast(pl.Utf8, strict=False),
        pl.col('ARREAR').cast(pl.Float64, strict=False).fill_null(0)
    ]).sort('ACCTNO')
    
    print(f"LOANTEMP records: {arrear_df.height}")
    if arrear_df.height > 0:
        print("Sample LOANTEMP data:")
        print(arrear_df.head(3))
except Exception as e:
    print(f"Warning: LOANTEMP file not found or error: {e}, creating empty dataframe")
    arrear_df = pl.DataFrame(schema={'ACCTNO': pl.Utf8, 'ARREAR': pl.Float64})

# DATA REPO; MERGE LNNOTE(IN=AA) NAME8 ARREAR;
print("\nMerging datasets...")
if lnnote_df.height > 0 and name8_df.height > 0:
    repo_df = lnnote_df.join(
        name8_df.rename({'LINETHRE': 'ENGINE', 'LINEFOUR': 'CHASSIS'}), 
        on='ACCTNO', how='inner'
    ).join(
        arrear_df, on='ACCTNO', how='left'
    ).with_columns([
        # BRABBR and CAC - simplified branch conversion
        pl.col('NTBRCH').cast(pl.Utf8).alias('BRABBR'),
        pl.col('NTBRCH').cast(pl.Utf8).alias('CAC'),
        
        # MAKE, MODEL, REGNO from COLLDESC
        pl.col('COLLDESC').cast(pl.Utf8).str.slice(0, 16).alias('MAKE'),
        pl.col('COLLDESC').cast(pl.Utf8).str.slice(16, 21).alias('MODEL'),
        pl.col('COLLDESC').cast(pl.Utf8).str.slice(40, 13).alias('REGNO'),
        
        # Handle missing values
        pl.col('ARREAR').fill_null(0),
        pl.col('COLLYEAR').cast(pl.Utf8).fill_null('')
    ])
else:
    print("WARNING: No data to merge!")
    repo_df = pl.DataFrame()

print(f"Merged REPO records: {repo_df.height if repo_df.height > 0 else 0}")

# Filter and create REPO datasets
if repo_df.height > 0:
    repo_filtered = repo_df.filter(pl.col('ARREAR') >= 10).sort('REGNO')
    repo1_filtered = repo_filtered.filter(
        pl.col('LOANTYPE').cast(pl.Utf8).is_in(['983', '993'])
    ).sort('REGNO')
else:
    repo_filtered = repo_df
    repo1_filtered = repo_df

print(f"REPO records: {repo_filtered.height}")
print(f"REPO1 records: {repo1_filtered.height}")

# Generate text output file (REPOTXT.txt)
print("\nGenerating REPOTXT.txt...")
with open(output_path / "repotxt.txt", "w") as f:
    # Format date as DD/MM/YY for output (matching production format)
    formatted_date = yesterday.strftime('%d/%m/%y')
    f.write(f"{formatted_date}-REPOSSESSION LISTING\n")
    
    if repo_filtered.height > 0:
        for row in repo_filtered.iter_rows(named=True):
            # Format according to fixed-width specifications
            line = (
                f"{str(row.get('BRABBR', ''))[:3]:<3}"      # @001 BRABBR $3.
                f"{str(row.get('CAC', ''))[:20]:<20}"        # @009 CAC $20.
                f"{str(row.get('REGNO', ''))[:13]:<13}"      # @029 REGNO $13.
                f"{str(row.get('MAKE', ''))[:16]:<16}"       # @043 MAKE $16.
                f"{str(row.get('MODEL', ''))[:21]:<21}"      # @060 MODEL $21.
                f"{str(row.get('ENGINE', ''))[:40]:<40}"     # @082 ENGINE $40.
                f"{str(row.get('CHASSIS', ''))[:40]:<40}"    # @123 CHASSIS $40.
                f"{str(row.get('COLLYEAR', ''))[:4]:<4}\n"    # @164 COLLYEAR $4.
            )
            f.write(line)

print("REPOTXT.txt generated successfully")

# Generate REPOTXT1.txt for REPO1 (983,993)
print("Generating REPOTXT1.txt...")
with open(output_path / "repotxt1.txt", "w") as f:
    formatted_date = yesterday.strftime('%d/%m/%y')
    f.write(f"{formatted_date}-REPOSSESSION LISTING (983,993)\n")
    
    if repo1_filtered.height > 0:
        for row in repo1_filtered.iter_rows(named=True):
            line = (
                f"{str(row.get('BRABBR', ''))[:3]:<3}"
                f"{str(row.get('CAC', ''))[:20]:<20}"
                f"{str(row.get('REGNO', ''))[:13]:<13}"
                f"{str(row.get('MAKE', ''))[:16]:<16}"
                f"{str(row.get('MODEL', ''))[:21]:<21}"
                f"{str(row.get('ENGINE', ''))[:40]:<40}"
                f"{str(row.get('CHASSIS', ''))[:40]:<40}"
                f"{str(row.get('COLLYEAR', ''))[:4]:<4}\n"
            )
            f.write(line)

print("REPOTXT1.txt generated successfully")
print("\nPROCESSING COMPLETED SUCCESSFULLY")
