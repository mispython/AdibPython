import polars as pl
import duckdb
from pathlib import Path
import datetime
import sys
import importlib

# Configuration
bnm_path = Path("BNM")
loan_path = Path("LOAN")
output_path = Path("output")
output_path.mkdir(exist_ok=True)

# DATA BNM.REPTDATE;
reptdate_df = pl.read_parquet(loan_path / "REPTDATE.parquet")

# Process REPTDATE with corrected week logic
processed_reptdate = reptdate_df.with_columns([
    pl.col('REPTDATE').dt.day().alias('DD'),
    pl.col('REPTDATE').dt.month().alias('MM'),
    pl.col('REPTDATE').dt.year().alias('YY')
]).with_columns([
    pl.when(pl.col('DD') <= 8).then(pl.struct([
        pl.lit(1).alias('sdd'),
        pl.lit('1').alias('wk'),
        pl.lit('4').alias('wk1')
    ]))
    .when(pl.col('DD') <= 15).then(pl.struct([
        pl.lit(9).alias('sdd'),
        pl.lit('2').alias('wk'),
        pl.lit('1').alias('wk1')
    ]))
    .when(pl.col('DD') <= 22).then(pl.struct([
        pl.lit(16).alias('sdd'),
        pl.lit('3').alias('wk'),
        pl.lit('2').alias('wk1')
    ]))
    .otherwise(pl.struct([
        pl.lit(23).alias('sdd'),
        pl.lit('4').alias('wk'),
        pl.lit('3').alias('wk1')
    ])).alias('week_info')
]).with_columns([
    pl.col('week_info').struct.field('sdd').alias('SDD'),
    pl.col('week_info').struct.field('wk').alias('WK'),
    pl.col('week_info').struct.field('wk1').alias('WK1'),
    pl.col('MM').alias('MM'),
    pl.when(pl.col('WK') == '1').then(
        pl.when(pl.col('MM') == 1).then(12).otherwise(pl.col('MM') - 1)
    ).otherwise(pl.col('MM')).alias('MM1'),
    pl.datetime(pl.col('YY'), pl.col('MM'), pl.col('SDD')).alias('SDATE')
]).drop(['week_info'])

# Extract parameters - CALL SYMPUT equivalent
first_row = processed_reptdate.row(0)
NOWK = first_row['WK']
NOWK1 = first_row['WK1']
REPTMON = f"{first_row['MM']:02d}"
REPTMON1 = f"{first_row['MM1']:02d}"
REPTYEAR = str(first_row['REPTDATE'].year)
REPTDAY = f"{first_row['REPTDATE'].day:02d}"
RDATE = first_row['REPTDATE'].strftime('%d%m%y')
SDATE = first_row['SDATE'].strftime('%d%m%y')
TDATE = f"{first_row['REPTDATE'].day:05d}"  # Z5. format

# QTR logic
if first_row['MM'] in [3, 6, 9, 12] and first_row['DD'] in [30, 31]:
    QTR = '1'
else:
    QTR = '0'

print(f"NOWK: {NOWK}, NOWK1: {NOWK1}, REPTMON: {REPTMON}, REPTMON1: {REPTMON1}")
print(f"REPTYEAR: {REPTYEAR}, REPTDAY: {REPTDAY}, RDATE: {RDATE}, SDATE: {SDATE}")
print(f"TDATE: {TDATE}, QTR: {QTR}")

# Save BNM.REPTDATE
bnm_path.mkdir(exist_ok=True)
processed_reptdate.write_parquet(bnm_path / "REPTDATE.parquet")

# SMR 2021-4114 - CAGAMAS data processing
# DATA CAGAMAS; INFILE CAGA FIRSTOBS=2 MISSOVER; INPUT CAGATAG 9.;
try:
    # Read CAGA file (assuming CSV format)
    cagamas_df = pl.read_csv("CAGA.csv", skip_rows=1, has_header=False, new_columns=['CAGATAG'])
    cagamas_df = cagamas_df.with_columns([
        pl.col('CAGATAG').cast(pl.Int64)
    ])
except FileNotFoundError:
    print("NOTE: CAGA file not found, creating empty dataframe")
    cagamas_df = pl.DataFrame({'CAGATAG': []})

# %LET CAGALIST=999999999; (default value)
CAGALIST = '999999999'

# PROC SQL equivalent to build CAGALIST
if not cagamas_df.is_empty():
    filtered_cagamas = cagamas_df.filter(pl.col('CAGATAG') > 0)
    if not filtered_cagamas.is_empty():
        unique_cagatags = filtered_cagamas.select('CAGATAG').unique()
        CAGALIST = ','.join([str(row[0]) for row in unique_cagatags.rows()])

print(f"CAGATAG=({CAGALIST})")

# DATA _NULL_ for LOAN date
loan_date_df = pl.read_parquet(loan_path / "REPTDATE.parquet").head(1)
LOAN = loan_date_df.row(0)['REPTDATE'].strftime('%d%m%y')

print(f"LOAN date: {LOAN}")
print(f"RDATE: {RDATE}")

# MACRO %PROCESS equivalent
# Note: Condition is commented out in SAS, so always execute the main block

# Since condition is commented out, this block always executes:
try:
    print(">>> EXECUTING LALWPBBC")
    
    # Import the module
    lalwpbbc_module = importlib.import_module('LALWPBBC')
    
    # Pass all global variables to the module
    lalwpbbc_module.NOWK = NOWK
    lalwpbbc_module.NOWK1 = NOWK1
    lalwpbbc_module.REPTMON = REPTMON
    lalwpbbc_module.REPTMON1 = REPTMON1
    lalwpbbc_module.REPTYEAR = REPTYEAR
    lalwpbbc_module.REPTDAY = REPTDAY
    lalwpbbc_module.RDATE = RDATE
    lalwpbbc_module.SDATE = SDATE
    lalwpbbc_module.TDATE = TDATE
    lalwpbbc_module.QTR = QTR
    lalwpbbc_module.LOAN = LOAN
    lalwpbbc_module.CAGALIST = CAGALIST
    
    # Call the process function
    lalwpbbc_module.process()
    
except ImportError:
    print("ERROR: LALWPBBC.py not found")
    print("Please create LALWPBBC.py with the required process() function")
    sys.exit(1)
except Exception as e:
    print(f"ERROR in LALWPBBC: {e}")
    sys.exit(1)

# DATA BNM.NOTE&REPTMON&NOWK;
try:
    note_filename = f"NOTE{REPTMON}{NOWK}.parquet"
    note_df = pl.read_parquet(bnm_path / note_filename)
    
    # IF PZIPCODE IN (&CAGALIST) THEN PRODCD = '54120';
    caga_list_values = [int(x.strip()) for x in CAGALIST.split(',') if x.strip()]
    
    note_updated = note_df.with_columns([
        pl.when(pl.col('PZIPCODE').is_in(caga_list_values))
        .then(pl.lit('54120'))
        .otherwise(pl.col('PRODCD'))
        .alias('PRODCD')
    ])
    
    # Save updated dataset
    note_updated.write_parquet(bnm_path / note_filename)
    print(f"Updated {note_filename} with CAGAMAS logic")
    
except FileNotFoundError:
    print(f"NOTE: {note_filename} not found - skipping CAGAMAS update")

print("PROCESSING COMPLETED SUCCESSFULLY")
