import polars as pl
import duckdb
from pathlib import Path
import datetime
import sys
import importlib

# Configuration
bnm_path = Path("BNM")
deposit_path = Path("DEPOSIT")
forate_path = Path("FORATE")
output_path = Path("output")
output_path.mkdir(exist_ok=True)

# DATA BNM.REPTDATE; SET DEPOSIT.REPTDATE;
reptdate_df = pl.read_parquet(deposit_path / "REPTDATE.parquet")

# Process REPTDATE with SELECT/WHEN logic
processed_reptdate = reptdate_df.with_columns([
    pl.col('REPTDATE').dt.day().alias('day')
]).with_columns([
    pl.when(pl.col('day') == 8)
    .then(pl.lit('1'))
    .when(pl.col('day') == 15) 
    .then(pl.lit('2'))
    .when(pl.col('day') == 22)
    .then(pl.lit('3'))
    .otherwise(pl.lit('4'))
    .alias('NOWK_temp')
])

# Extract parameters - CALL SYMPUT equivalent
first_row = processed_reptdate.row(0)
NOWK = first_row['NOWK_temp']
REPTYEAR = str(first_row['REPTDATE'].year)  # YEAR4.
REPTMON = f"{first_row['REPTDATE'].month:02d}"  # Z2.
REPTDAY = f"{first_row['REPTDATE'].day:02d}"  # Z2.
RDATE = first_row['REPTDATE'].strftime('%d%m%y')  # DDMMYY8.
RPTDTE = first_row['REPTDATE']  # Actual date object

print(f"NOWK: {NOWK}, REPTYEAR: {REPTYEAR}, REPTMON: {REPTMON}")
print(f"REPTDAY: {REPTDAY}, RDATE: {RDATE}, RPTDTE: {RPTDTE}")

# Save to BNM.REPTDATE
processed_reptdate.write_parquet(bnm_path / "REPTDATE.parquet")

# PROC SORT DATA=FORATE.FORATEBKP OUT=FXRATE; BY CURCODE DESCENDING REPTDATE;
fxrate_df = pl.read_parquet(forate_path / "FORATEBKP.parquet").filter(
    pl.col('REPTDATE') <= RPTDTE
).sort(['CURCODE', 'REPTDATE'], descending=[False, True])

# PROC SORT DATA=FXRATE(KEEP=SPOTRATE CURCODE) NODUPKEY; BY CURCODE;
fxrate_deduped = fxrate_df.select(['CURCODE', 'SPOTRATE']).unique(subset=['CURCODE'])

# Save FXRATE
fxrate_deduped.write_parquet(output_path / "FXRATE.parquet")
fxrate_deduped.write_csv(output_path / "FXRATE.csv")

# DATA FOFMT;
fofmt_data = []
for row in fxrate_deduped.iter_rows(named=True):
    fofmt_data.append({
        'START': row['CURCODE'],
        'LABEL': str(row['SPOTRATE'])[:13],  # $13.7 format
        'FMTNAME': 'FORATE',
        'TYPE': 'C'
    })

fofmt_df = pl.DataFrame(fofmt_data)
fofmt_df.write_parquet(output_path / "FOFMT.parquet")
fofmt_df.write_csv(output_path / "FOFMT.csv")

print("FOFMT dataset created for format control")

# PROC FORMAT CNTLIN=FOFMT equivalent
# In Python, we create a dictionary mapping for the format
forate_format = {}
for row in fofmt_df.iter_rows(named=True):
    forate_format[row['START']] = row['LABEL']

print(f"Created FORATE format with {len(forate_format)} entries")

# %INC PGM equivalent for external programs
try:
    dalwpbdd = importlib.import_module('DALWPBBD')
    dalwpbdd.process(
        NOWK=NOWK,
        REPTYEAR=REPTYEAR, 
        REPTMON=REPTMON,
        REPTDAY=REPTDAY,
        RDATE=RDATE,
        RPTDTE=RPTDTE
    )
except ImportError:
    print("NOTE: DALWPBBD.py not available")

try:
    eibdrlfm = importlib.import_module('EIBDRLFM') 
    eibdrlfm.process(
        NOWK=NOWK,
        REPTYEAR=REPTYEAR,
        REPTMON=REPTMON, 
        REPTDAY=REPTDAY,
        RDATE=RDATE,
        RPTDTE=RPTDTE
    )
except ImportError:
    print("NOTE: EIBDRLFM.py not available")
