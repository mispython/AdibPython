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

# Calculate dates using datetime
today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)

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

# Extract parameters
nowk = wk
nowk1 = wk1
reptmon = f"{mm:02d}"
reptmon1 = f"{mm1:02d}"
reptyear = str(year)
reptday = f"{day:02d}"
rdate = yesterday.strftime('%d%m%y')

print(f"NOWK: {nowk}, NOWK1: {nowk1}, REPTMON: {reptmon}, REPTMON1: {reptmon1}")
print(f"REPTYEAR: {reptyear}, REPTDAY: {reptday}, RDATE: {rdate}")

# HP loan type codes
hp_values = [102.0, 103.0, 104.0, 105.0, 110.0, 111.0, 112.0, 113.0, 114.0, 115.0, 
             116.0, 120.0, 124.0, 127.0, 128.0, 133.0, 134.0, 135.0, 136.0, 138.0,
             422.0, 654.0, 141.0, 413.0, 412.0, 663.0, 184.0]

print(f"Using HP loan type codes: {hp_values[:5]}... (total: {len(hp_values)})")

def read_sas_chunked(file_path: Path, columns: list = None, chunksize: int = 100000) -> Iterator[pl.DataFrame]:
    """Read SAS file in chunks"""
    reader = pyreadstat.read_file_in_chunks(
        pyreadstat.read_sas7bdat,
        str(file_path),
        chunksize=chunksize,
        usecols=columns
    )
    
    for df_chunk, meta in reader:
        yield pl.from_pandas(df_chunk)

# Branch code mappings (from production output)
branch_abbr = {
    77: 'H08', 811: 'H11', 857: 'JRL', 820: 'H24', 800: 'SBU',
    818: 'H49', 802: 'BWK', 801: 'H05', 112: 'JTZ', 143: 'H20',
    827: 'PLT', 806: 'PIH', 814: 'H12', 824: 'H56', 862: 'H22',
    261: 'H10', 821: 'PBR', 822: 'H14', 825: 'H21', 807: 'JRT',
    856: 'H25', 819: 'H09', 816: 'H07', 826: 'H13', 823: 'H16',
    804: 'H47', 808: 'H04'
}

cac_names = {
    77: 'NON CAC', 811: 'CAC-CITY CENTRE', 857: 'CAC-CITY CENTRE',
    820: 'NON CAC', 800: 'NON CAC', 818: 'NON CAC',
    802: 'CAC-CITY CENTRE', 801: 'CAC-JOHOR BAHRU', 112: 'NON CAC',
    143: 'CAC-KELANG', 827: 'NON CAC', 806: 'CAC-K. LUMPUR',
    814: 'CAC-K. LUMPUR', 824: 'NON CAC', 862: 'CAC-CITY CENTRE',
    261: 'NON CAC', 821: 'NON CAC', 822: 'CAC-CITY CENTRE',
    825: 'CAC-CITY CENTRE', 807: 'CAC-PENANG', 856: 'NON CAC',
    819: 'CAC-PENANG', 816: 'CAC-K. LUMPUR', 826: 'NON CAC',
    823: 'CAC-K. LUMPUR', 804: 'NON CAC', 808: 'NON CAC'
}

# Process LNNOTE
print("Processing LNNOTE in chunks...")
lnnote_chunks = []

chunk_count = 0
for chunk in read_sas_chunked(loan_path / "lnnote.sas7bdat", 
                              columns=['ACCTNO', 'LOANTYPE', 'NTBRCH', 'COLLDESC', 'COLLYEAR', 'BALANCE', 'BORSTAT'],
                              chunksize=100000):
    chunk_count += 1
    if chunk_count % 10 == 0:
        print(f"Processed {chunk_count} chunks...")
        gc.collect()
    
    # Filter according to SAS WHERE clause
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

# Read NAME8
print("Processing NAME8...")
name8_df = pl.from_pandas(
    pyreadstat.read_sas7bdat(str(loan_path / "name8.sas7bdat"), 
                            usecols=['ACCTNO', 'LINETHRE', 'LINEFOUR'])[0]
).select(['ACCTNO', 'LINETHRE', 'LINEFOUR']).sort('ACCTNO')
print(f"NAME8 records: {name8_df.height}")

# Read LOANTEMP
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

# Merge datasets
print("Merging datasets...")
repo_df = lnnote_df.join(
    name8_df.rename({'LINETHRE': 'ENGINE', 'LINEFOUR': 'CHASSIS'}), 
    on='ACCTNO', how='inner'
).join(
    arrear_df, on='ACCTNO', how='left'
)

# Apply formatting
repo_df = repo_df.with_columns([
    pl.col('ARREAR').fill_null(0),
    pl.col('NTBRCH').cast(pl.Int64).alias('NTBRCH_INT')
])

# Apply branch mappings
repo_df = repo_df.with_columns([
    pl.col('NTBRCH_INT').map_elements(
        lambda x: branch_abbr.get(x, str(x)[:3]), 
        return_dtype=pl.Utf8
    ).alias('BRABBR'),
    
    pl.col('NTBRCH_INT').map_elements(
        lambda x: cac_names.get(x, 'NON CAC'), 
        return_dtype=pl.Utf8
    ).alias('CAC'),
    
    # SUBSTR extractions
    pl.col('COLLDESC').str.slice(0, 16).alias('MAKE'),
    pl.col('COLLDESC').str.slice(16, 21).alias('MODEL'),
    pl.col('COLLDESC').str.slice(40, 13).alias('REGNO'),
    
    # COLLYEAR
    pl.col('COLLYEAR').fill_null(0).cast(pl.Int64).cast(pl.Utf8).alias('COLLYEAR_STR')
])

print(f"Merged REPO records: {repo_df.height}")

# Check for LOANTYPE 983 and 993 in the data
print("\nChecking for LOANTYPE 983 and 993...")
loan_types = repo_df['LOANTYPE'].unique().sort().to_list()
print(f"Unique LOANTYPE values in merged data: {loan_types[:20]}...")
print(f"983 in data: {983.0 in loan_types}")
print(f"993 in data: {993.0 in loan_types}")

# Filter for ARREAR >= 10 (this creates both REPO and REPO1)
repo_filtered = repo_df.filter(pl.col('ARREAR') >= 10)

# Split into REPO and REPO1
# REPO: all records with ARREAR >= 10
# REPO1: subset with LOANTYPE IN (983,993)
repo1_filtered = repo_filtered.filter(pl.col('LOANTYPE').is_in([983.0, 993.0]))

print(f"REPO records (ARREAR >= 10): {repo_filtered.height}")
print(f"REPO1 records (983,993 with ARREAR >= 10): {repo1_filtered.height}")

# Sort by REGNO
repo_sorted = repo_filtered.sort('REGNO')
repo1_sorted = repo1_filtered.sort('REGNO')

# Function to write fixed-width output
def write_fixed_width(filename, data, title):
    with open(output_path / filename, "w") as f:
        f.write(f"{rdate}-{title}\n")
        
        for row in data.iter_rows(named=True):
            # Format exactly as SAS PUT statements
            line = (
                f"{str(row.get('BRABBR', ''))[:3]:<3}"
                f"{str(row.get('CAC', ''))[:20]:<20}"
                f"{str(row.get('REGNO', ''))[:13]:<13}"
                f"{str(row.get('MAKE', ''))[:16]:<16}"
                f"{str(row.get('MODEL', ''))[:21]:<21}"
                f"{str(row.get('ENGINE', ''))[:40]:<40}"
                f"{str(row.get('CHASSIS', ''))[:40]:<40}"
                f"{str(row.get('COLLYEAR_STR', ''))[:4]:<4}\n"
            )
            f.write(line)

# Generate REPOTXT
print("\nGenerating REPOTXT.txt...")
write_fixed_width("repotxt.txt", repo_sorted, "REPOSSESSION LISTING")
print(f"REPOTXT.txt generated with {repo_sorted.height} records")

# Generate REPOTXT1
print("Generating REPOTXT1.txt...")
if repo1_sorted.height > 0:
    write_fixed_width("repotxt1.txt", repo1_sorted, "REPOSSESSION LISTING (983,993)")
    print(f"REPOTXT1.txt generated with {repo1_sorted.height} records")
else:
    # Even if empty, create file with header
    with open(output_path / "repotxt1.txt", "w") as f:
        f.write(f"{rdate}-REPOSSESSION LISTING (983,993)\n")
    print("REPOTXT1.txt generated (no records found for LOANTYPE 983,993)")

print("\nPROCESSING COMPLETED SUCCESSFULLY")
