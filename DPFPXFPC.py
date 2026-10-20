import polars as pl
from pathlib import Path
import datetime
import pyreadstat
import gc
from typing import Iterator

# Import format modules
import PBBLNFMT
import PBBELF

# Configuration
loan_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS")
name8_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMREPOI")
loantemp_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMREPOS")
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

# HP loan type codes - from PBBLNFMT.MACROS['HP']
hp_values = [float(v) for v in PBBLNFMT.MACROS['HP']]
print(f"Using HP loan type codes from PBBLNFMT: {hp_values}")
print(f"Total HP codes: {len(hp_values)}")

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

# Process LNNOTE
print("\nProcessing LNNOTE in chunks...")
lnnote_chunks = []

chunk_count = 0
for chunk in read_sas_chunked(
    loan_path / f"enrh_ln_note_m{reptmon}.sas7bdat",
    columns=['ACCTNO', 'LOANTYPE', 'NTBRCH', 'COLLDESC', 'COLLYEAR',
             'BALANCE', 'BORSTAT', 'ENTITY_CD'],
    chunksize=100000
):
    chunk_count += 1
    if chunk_count % 10 == 0:
        print(f"Processed {chunk_count} chunks...")
        gc.collect()

    # WHERE LOANTYPE IN &HP
    #   AND BALANCE GT 0
    #   AND BORSTAT NOT IN ('F','I','R')
    #   AND ENTITY_CD != 'PIBB'      <-- conventional only
    filtered_chunk = chunk.filter(
        (pl.col('LOANTYPE').is_in(hp_values)) &
        (pl.col('BALANCE') > 0) &
        (~pl.col('BORSTAT').is_in(['F', 'I', 'R'])) &
        (pl.col('ENTITY_CD') != 'PIBB')
    ).select([
        'ACCTNO', 'LOANTYPE', 'NTBRCH', 'COLLDESC', 'COLLYEAR'
    ])

    if filtered_chunk.height > 0:
        lnnote_chunks.append(filtered_chunk)

if lnnote_chunks:
    lnnote_df = pl.concat(lnnote_chunks).sort('ACCTNO')
else:
    lnnote_df = pl.DataFrame(schema={'ACCTNO': pl.Float64, 'LOANTYPE': pl.Float64,
                                     'NTBRCH': pl.Float64, 'COLLDESC': pl.Utf8,
                                     'COLLYEAR': pl.Float64})

print(f"LNNOTE records: {lnnote_df.height}")
del lnnote_chunks
gc.collect()

# Read NAME8
print("\nProcessing NAME8...")
name8_df = pl.from_pandas(
    pyreadstat.read_sas7bdat(str(name8_path / "stg_ln_nam8file.sas7bdat"),
                             usecols=['ACCTNO', 'LINETHRE', 'LINEFOUR'])[0]
).select(['ACCTNO', 'LINETHRE', 'LINEFOUR']).sort('ACCTNO')
print(f"NAME8 records: {name8_df.height}")

# Read LOANTEMP
print("\nProcessing LOANTEMP...")
try:
    arrear_df = pl.from_pandas(
        pyreadstat.read_sas7bdat(str(loantemp_path / "loantemp.sas7bdat"),
                                 usecols=['ACCTNO', 'ARREAR'])[0]
    ).select(['ACCTNO', 'ARREAR']).sort('ACCTNO')
    print(f"LOANTEMP records: {arrear_df.height}")
except Exception as e:
    print(f"Warning: LOANTEMP not found - {e}")
    arrear_df = pl.DataFrame(schema={'ACCTNO': pl.Float64, 'ARREAR': pl.Float64})

# DATA REPO; MERGE LNNOTE(IN=AA) NAME8 ARREAR; BY ACCTNO; IF AA;
print("\nMerging datasets...")
repo_df = lnnote_df.join(
    name8_df.rename({'LINETHRE': 'ENGINE', 'LINEFOUR': 'CHASSIS'}),
    on='ACCTNO', how='inner'
).join(
    arrear_df, on='ACCTNO', how='left'
)

repo_df = repo_df.with_columns([
    pl.col('ARREAR').fill_null(0),
    pl.col('NTBRCH').cast(pl.Int64).alias('NTBRCH_INT')
])

# Apply SAS format functions from PBBELF
repo_df = repo_df.with_columns([
    pl.col('NTBRCH_INT').map_elements(
        lambda x: PBBELF.format_brchcd(int(x)) if x is not None else '',
        return_dtype=pl.Utf8
    ).alias('BRABBR'),

    pl.col('NTBRCH_INT').map_elements(
        lambda x: PBBELF.format_cacname(int(x)) if x is not None else 'NON CAC',
        return_dtype=pl.Utf8
    ).alias('CAC'),

    pl.col('COLLDESC').str.slice(0, 16).alias('MAKE'),
    pl.col('COLLDESC').str.slice(16, 21).alias('MODEL'),
    pl.col('COLLDESC').str.slice(40, 13).alias('REGNO'),

    pl.col('COLLYEAR').fill_null(0).cast(pl.Int64).cast(pl.Utf8).alias('COLLYEAR_STR')
])

print(f"Merged REPO records: {repo_df.height}")

# DATA REPO REPO1; SET REPO; IF ARREAR GE 10;
repo_filtered = repo_df.filter(pl.col('ARREAR') >= 10)
repo1_filtered = repo_filtered.filter(pl.col('LOANTYPE').is_in([983.0, 993.0]))

print(f"REPO records (ARREAR >= 10): {repo_filtered.height}")
print(f"REPO1 records (983,993): {repo1_filtered.height}")

# Sort by REGNO
repo_sorted = repo_filtered.sort('REGNO')
repo1_sorted = repo1_filtered.sort('REGNO')

def write_sas_fixed_width(filename, data, title):
    """Write fixed-width text file matching SAS PUT @column format exactly"""
    with open(output_path / filename, "w") as f:
        f.write(f"{rdate}-{title}\n")

        for row in data.iter_rows(named=True):
            line = [' '] * 168

            brabbr = str(row.get('BRABBR') or '').strip()[:3]
            cac = str(row.get('CAC') or '').strip()[:20]
            regno = str(row.get('REGNO') or '').strip()[:13]
            make = str(row.get('MAKE') or '').strip()[:16]
            model = str(row.get('MODEL') or '').strip()[:21]
            engine = str(row.get('ENGINE') or '').strip()[:40]
            chassis = str(row.get('CHASSIS') or '').strip()[:40]
            collyear = str(row.get('COLLYEAR_STR') or '0').strip()[:4]

            for i, char in enumerate(brabbr):
                if i < 3: line[i] = char
            for i, char in enumerate(cac):
                if i < 20: line[8 + i] = char
            for i, char in enumerate(regno):
                if i < 13: line[28 + i] = char
            for i, char in enumerate(make):
                if i < 16: line[42 + i] = char
            for i, char in enumerate(model):
                if i < 21: line[59 + i] = char
            for i, char in enumerate(engine):
                if i < 40: line[81 + i] = char
            for i, char in enumerate(chassis):
                if i < 40: line[122 + i] = char
            for i, char in enumerate(collyear):
                if i < 4: line[163 + i] = char

            f.write(''.join(line) + '\n')

# Generate REPOTXT
print("\nGenerating REPOTXT.txt...")
write_sas_fixed_width("repotxt.txt", repo_sorted, "REPOSSESSION LISTING")
print(f"REPOTXT.txt generated with {repo_sorted.height} records")

# Generate REPOTXT1
print("Generating REPOTXT1.txt...")
write_sas_fixed_width("repotxt1.txt", repo1_sorted, "REPOSSESSION LISTING (983,993)")
print(f"REPOTXT1.txt generated with {repo1_sorted.height} records")

print("\nPROCESSING COMPLETED SUCCESSFULLY")
