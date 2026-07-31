import polars as pl
from pathlib import Path
import datetime
import pyreadstat
import gc
from typing import Iterator

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
if wk == '1':
    mm1 = mm - 1
    if mm1 == 0:
        mm1 = 12
else:
    mm1 = mm

sdate = datetime.date(year, mm, sdd)

# Extract parameters (matching SAS CALL SYMPUT)
nowk = wk
nowk1 = wk1
reptmon = f"{mm:02d}"
reptmon1 = f"{mm1:02d}"
reptyear = str(year)
reptday = f"{day:02d}"
rdate = yesterday.strftime('%d%m%y')  # DDMMYY8 format
sdate_str = sdate.strftime('%d%m%y')

print(f"NOWK: {nowk}, NOWK1: {nowk1}, REPTMON: {reptmon}, REPTMON1: {reptmon1}")
print(f"REPTYEAR: {reptyear}, REPTDAY: {reptday}, RDATE: {rdate}, SDATE: {sdate_str}")

# HP loan type codes - You need to set this to match your SAS &HP macro variable
# Based on your data, these are common HP loan type codes
# Check your SAS environment with %PUT &HP; to get the actual values
hp_values = [102.0, 103.0, 104.0, 105.0, 110.0, 111.0, 112.0, 113.0, 114.0, 115.0, 
             116.0, 120.0, 124.0, 127.0, 128.0, 133.0, 134.0, 135.0, 136.0, 138.0,
             422.0, 654.0, 141.0, 413.0, 412.0, 663.0, 184.0]  # Add all HP codes from &HP

print(f"Using HP loan type codes: {hp_values}")

def read_sas_chunked(file_path: Path, columns: list = None, chunksize: int = 100000) -> Iterator[pl.DataFrame]:
    """Read SAS file in chunks to handle large files efficiently"""
    reader = pyreadstat.read_file_in_chunks(
        pyreadstat.read_sas7bdat,
        str(file_path),
        chunksize=chunksize,
        usecols=columns
    )
    
    for df_chunk, meta in reader:
        yield pl.from_pandas(df_chunk)

# PROC SORT DATA=LOAN.LNNOTE OUT=LNNOTE
print("Processing LNNOTE (large file) in chunks...")
lnnote_chunks = []

chunk_count = 0
for chunk in read_sas_chunked(loan_path / "lnnote.sas7bdat", 
                              columns=['ACCTNO', 'LOANTYPE', 'NTBRCH', 'COLLDESC', 'COLLYEAR', 'BALANCE', 'BORSTAT'],
                              chunksize=100000):
    chunk_count += 1
    if chunk_count % 10 == 0:
        print(f"Processed {chunk_count} chunks from LNNOTE...")
        gc.collect()
    
    # WHERE LOANTYPE IN &HP AND BALANCE GT 0 AND BORSTAT NOT IN ('F','I','R')
    filtered_chunk = chunk.filter(
        (pl.col('LOANTYPE').is_in(hp_values)) &
        (pl.col('BALANCE') > 0) &
        (~pl.col('BORSTAT').is_in(['F', 'I', 'R']))
    ).select([
        'ACCTNO', 'LOANTYPE', 'NTBRCH', 'COLLDESC', 'COLLYEAR'
    ])
    
    if filtered_chunk.height > 0:
        lnnote_chunks.append(filtered_chunk)

if lnnote_chunks:
    lnnote_df = pl.concat(lnnote_chunks).sort('ACCTNO')
else:
    lnnote_df = pl.DataFrame(schema={'ACCTNO': pl.Float64, 'LOANTYPE': pl.Float64, 
                                     'NTBRCH': pl.Float64, 'COLLDESC': pl.Utf8, 'COLLYEAR': pl.Float64})

print(f"LNNOTE records: {lnnote_df.height}")
del lnnote_chunks
gc.collect()

# PROC SORT DATA=LOAN.NAME8 OUT=NAME8
print("Processing NAME8...")
name8_df = pl.from_pandas(
    pyreadstat.read_sas7bdat(str(loan_path / "name8.sas7bdat"), 
                            usecols=['ACCTNO', 'LINETHRE', 'LINEFOUR'])[0]
).select(['ACCTNO', 'LINETHRE', 'LINEFOUR']).sort('ACCTNO')

print(f"NAME8 records: {name8_df.height}")

# PROC SORT DATA=ARREAR.LOANTEMP OUT=ARREAR
print("Processing LOANTEMP...")
try:
    arrear_df = pl.from_pandas(
        pyreadstat.read_sas7bdat(str(arrear_path / "loantemp.sas7bdat"), 
                                usecols=['ACCTNO', 'ARREAR'])[0]
    ).select(['ACCTNO', 'ARREAR']).sort('ACCTNO')
except Exception as e:
    print(f"Warning: LOANTEMP not found - {e}")
    arrear_df = pl.DataFrame(schema={'ACCTNO': pl.Float64, 'ARREAR': pl.Float64})

print(f"LOANTEMP records: {arrear_df.height}")

# DATA REPO; MERGE LNNOTE(IN=AA) NAME8 ARREAR; BY ACCTNO; IF AA;
print("Merging datasets...")
repo_df = lnnote_df.join(
    name8_df.rename({'LINETHRE': 'ENGINE', 'LINEFOUR': 'CHASSIS'}), 
    on='ACCTNO', how='inner'  # IN=AA means inner join
).join(
    arrear_df, on='ACCTNO', how='left'
).with_columns([
    # BRABBR = PUT(NTBRCH,BRCHCD.) - Simplified: convert to string
    pl.col('NTBRCH').cast(pl.Int64).cast(pl.Utf8).alias('BRABBR'),
    
    # CAC = PUT(NTBRCH,CACNAME.) - Simplified: convert to string
    # In production, this uses a format catalog. You may need to map branch codes to names
    pl.col('NTBRCH').cast(pl.Int64).cast(pl.Utf8).alias('CAC'),
    
    # MAKE = SUBSTR(COLLDESC,1,16)
    pl.col('COLLDESC').str.slice(0, 16).alias('MAKE'),
    
    # MODEL = SUBSTR(COLLDESC,16,21)
    pl.col('COLLDESC').str.slice(16, 21).alias('MODEL'),
    
    # REGNO = SUBSTR(COLLDESC,40,13)
    pl.col('COLLDESC').str.slice(40, 13).alias('REGNO'),
    
    # Handle missing ARREAR
    pl.col('ARREAR').fill_null(0)
])

print(f"Merged REPO records: {repo_df.height}")

# DATA REPO REPO1; SET REPO; IF ARREAR GE 10;
repo_filtered = repo_df.filter(pl.col('ARREAR') >= 10)

# OUTPUT REPO1 if LOANTYPE IN (983,993)
repo1_filtered = repo_filtered.filter(pl.col('LOANTYPE').is_in([983.0, 993.0]))

print(f"REPO records (ARREAR >= 10): {repo_filtered.height}")
print(f"REPO1 records (983,993): {repo1_filtered.height}")

# PROC SORT DATA=REPO OUT=REPO; BY REGNO;
repo_sorted = repo_filtered.sort('REGNO')

# PROC SORT DATA=REPO1 OUT=REPO1; BY REGNO;
repo1_sorted = repo1_filtered.sort('REGNO')

# DATA _NULL_; SET REPO; FILE REPOTXT NOTITLES;
print("Generating REPOTXT.txt...")
with open(output_path / "repotxt.txt", "w") as f:
    # IF _N_ = 1 THEN PUT @001 "&RDATE" '-REPOSSESSION LISTING'
    f.write(f"{rdate}-REPOSSESSION LISTING\n")
    
    for row in repo_sorted.iter_rows(named=True):
        # PUT @001 BRABBR $3. @009 CAC $20. @029 REGNO $13. @043 MAKE $16. 
        # @060 MODEL $21. @082 ENGINE $40. @123 CHASSIS $40. @164 COLLYEAR $4.
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

print("REPOTXT.txt generated successfully")

# DATA _NULL_; SET REPO1; FILE REPOTXT1 NOTITLES;
print("Generating REPOTXT1.txt...")
with open(output_path / "repotxt1.txt", "w") as f:
    f.write(f"{rdate}-REPOSSESSION LISTING (983,993)\n")
    
    for row in repo1_sorted.iter_rows(named=True):
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
print("PROCESSING COMPLETED SUCCESSFULLY")
