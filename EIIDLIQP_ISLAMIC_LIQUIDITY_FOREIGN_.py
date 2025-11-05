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
processed_reptdate.write_csv(output_path / "REPTDATE.csv")

# PROC SORT DATA=FORATE.FORATEBKP OUT=FXRATE; BY CURCODE DESCENDING REPTDATE;
fxrate_df = pl.read_parquet(forate_path / "FORATEBKP.parquet").filter(
    pl.col('REPTDATE') <= RPTDTE
).sort(['CURCODE', 'REPTDATE'], descending=[False, True])

# Save intermediate FXRATE
fxrate_df.write_parquet(output_path / "FXRATE_full.parquet")

# PROC SORT DATA=FXRATE(KEEP=SPOTRATE CURCODE) NODUPKEY; BY CURCODE;
fxrate_deduped = fxrate_df.select(['CURCODE', 'SPOTRATE']).unique(subset=['CURCODE']).sort('CURCODE')

# Save final FXRATE
fxrate_deduped.write_parquet(output_path / "FXRATE.parquet")
fxrate_deduped.write_csv(output_path / "FXRATE.csv")

print(f"FXRATE dataset created with {len(fxrate_deduped)} unique currency codes")

# DATA FOFMT;
# LENGTH START $3. LABEL $13.7 FMTNAME $6. TYPE $1.;
fofmt_data = []
for row in fxrate_deduped.iter_rows(named=True):
    fofmt_data.append({
        'START': str(row['CURCODE'])[:3],  # $3. format
        'LABEL': f"{row['SPOTRATE']:.7f}"[:13],  # $13.7 format - 13 chars, 7 decimal
        'FMTNAME': 'FORATE',  # $6. format
        'TYPE': 'C'  # $1. format
    })

fofmt_df = pl.DataFrame(fofmt_data)
fofmt_df.write_parquet(output_path / "FOFMT.parquet")
fofmt_df.write_csv(output_path / "FOFMT.csv")

print(f"FOFMT dataset created with {len(fofmt_df)} format entries")

# PROC FORMAT CNTLIN=FOFMT equivalent
# Create a format mapping dictionary similar to SAS format
forate_format = {}
for row in fofmt_df.iter_rows(named=True):
    forate_format[row['START']] = row['LABEL']

print(f"FORATE format created with {len(forate_format)} currency mappings")

# Example of using the format in Python
def forate_format_func(currency_code):
    """Equivalent to PUT(currency_code, FORATE.) in SAS"""
    return forate_format.get(str(currency_code)[:3], '')  # Return empty string if not found

# Test the format function
print("Sample format conversions:")
for i, (curr, rate) in enumerate(list(forate_format.items())[:5]):
    print(f"  {curr} -> {rate}")

# %INC PGM(DALWPBBD);
try:
    print(">>> EXECUTING DALWPBBD")
    dalwpbdd = importlib.import_module('DALWPBBD')
    # Pass all global variables
    dalwpbdd.NOWK = NOWK
    dalwpbdd.REPTYEAR = REPTYEAR
    dalwpbdd.REPTMON = REPTMON
    dalwpbdd.REPTDAY = REPTDAY
    dalwpbdd.RDATE = RDATE
    dalwpbdd.RPTDTE = RPTDTE
    dalwpbdd.FORATE_FORMAT = forate_format_func  # Pass the format function
    dalwpbdd.process()
except ImportError:
    print("ERROR: DALWPBBD.py not found")
except Exception as e:
    print(f"ERROR in DALWPBBD: {e}")

# %INC PGM(EIBDRLFM);
try:
    print(">>> EXECUTING EIBDRLFM")
    eibdrlfm = importlib.import_module('EIBDRLFM')
    # Pass all global variables
    eibdrlfm.NOWK = NOWK
    eibdrlfm.REPTYEAR = REPTYEAR
    eibdrlfm.REPTMON = REPTMON
    eibdrlfm.REPTDAY = REPTDAY
    eibdrlfm.RDATE = RDATE
    eibdrlfm.RPTDTE = RPTDTE
    eibdrlfm.FORATE_FORMAT = forate_format_func  # Pass the format function
    eibdrlfm.process()
except ImportError:
    print("ERROR: EIBDRLFM.py not found")
except Exception as e:
    print(f"ERROR in EIBDRLFM: {e}")

print("PROCESSING COMPLETED SUCCESSFULLY")
