import polars as pl
import duckdb
from pathlib import Path
import datetime
import sys
import importlib

# Configuration
loan_path = Path("LOAN")
arrear_path = Path("ARREAR")
output_path = Path("output")
output_path.mkdir(exist_ok=True)

# Execute external programs - %INC PGM equivalent
try:
    pbbelf = importlib.import_module('PBBELF')
    pbbelf.process()
except ImportError:
    print("NOTE: PBBELF not available")

try:
    pbblnfmt = importlib.import_module('PBBLNFMT') 
    pbblnfmt.process()
except ImportError:
    print("NOTE: PBBLNFMT not available")

# DATA REPTDATE;
reptdate_df = pl.read_parquet(loan_path / "REPTDATE.parquet")

# Process REPTDATE with SELECT/WHEN logic
processed_reptdate = reptdate_df.with_columns([
    pl.col('REPTDATE').dt.day().alias('day'),
    pl.col('REPTDATE').dt.month().alias('month'),
    pl.col('REPTDATE').dt.year().alias('year')
]).with_columns([
    pl.when(pl.col('day') == 8).then(pl.struct([
        pl.lit(1).alias('sdd'),
        pl.lit('1').alias('wk'),
        pl.lit('4').alias('wk1')
    ]))
    .when(pl.col('day') == 15).then(pl.struct([
        pl.lit(9).alias('sdd'),
        pl.lit('2').alias('wk'),
        pl.lit('1').alias('wk1')
    ]))
    .when(pl.col('day') == 22).then(pl.struct([
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
    pl.col('month').alias('MM'),
    pl.when(pl.col('WK') == '1').then(
        pl.when(pl.col('month') == 1).then(12).otherwise(pl.col('month') - 1)
    ).otherwise(pl.col('month')).alias('MM1'),
    pl.datetime(pl.col('year'), pl.col('month'), pl.col('SDD')).alias('SDATE')
]).drop(['day', 'month', 'year', 'week_info'])

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

print(f"NOWK: {NOWK}, NOWK1: {NOWK1}, REPTMON: {REPTMON}, REPTMON1: {REPTMON1}")
print(f"REPTYEAR: {REPTYEAR}, REPTDAY: {REPTDAY}, RDATE: {RDATE}, SDATE: {SDATE}")

# PROC SORT DATA=LOAN.LNNOTE OUT=LNNOTE
hp_values = ['HP']  # Replace with actual &HP values from SAS
lnnote_df = pl.read_parquet(loan_path / "LNNOTE.parquet").filter(
    (pl.col('LOANTYPE').is_in(hp_values)) &
    (pl.col('BALANCE') > 0) &
    (~pl.col('BORSTAT').is_in(['F', 'I', 'R']))
).select([
    'ACCTNO', 'LOANTYPE', 'NTBRCH', 'COLLDESC', 'COLLYEAR'
]).sort('ACCTNO')

# PROC SORT DATA=LOAN.NAME8 OUT=NAME8
name8_df = pl.read_parquet(loan_path / "NAME8.parquet").select([
    'ACCTNO', 'LINETHRE', 'LINEFOUR'
]).sort('ACCTNO')

# PROC SORT DATA=ARREAR.LOANTEMP OUT=ARREAR
arrear_df = pl.read_parquet(arrear_path / "LOANTEMP.parquet").select([
    'ACCTNO', 'ARREAR'
]).sort('ACCTNO')

# DATA REPO; MERGE LNNOTE(IN=AA) NAME8 ARREAR;
repo_df = lnnote_df.join(
    name8_df.rename({'LINETHRE': 'ENGINE', 'LINEFOUR': 'CHASSIS'}), 
    on='ACCTNO', how='inner'
).join(
    arrear_df, on='ACCTNO', how='left'
).with_columns([
    # BRABBR = PUT(NTBRCH,BRCHCD.);
    pl.col('NTBRCH').cast(pl.Utf8).alias('BRABBR'),  # Simplified branch conversion
    
    # CAC = PUT(NTBRCH,CACNAME.);
    pl.col('NTBRCH').cast(pl.Utf8).alias('CAC'),  # Simplified CAC conversion
    
    # MAKE = SUBSTR(COLLDESC,1,16);
    pl.col('COLLDESC').str.slice(0, 16).alias('MAKE'),
    
    # MODEL = SUBSTR(COLLDESC,16,21);
    pl.col('COLLDESC').str.slice(16, 21).alias('MODEL'),
    
    # REGNO = SUBSTR(COLLDESC,40,13);
    pl.col('COLLDESC').str.slice(40, 13).alias('REGNO')
])

# DATA REPO REPO1; SET REPO;
repo_filtered = repo_df.filter(pl.col('ARREAR') >= 10)
repo1_filtered = repo_filtered.filter(pl.col('LOANTYPE').is_in([983, 993]))

# PROC SORT DATA=REPO OUT=REPO; BY REGNO;
repo_sorted = repo_filtered.sort('REGNO')

# PROC SORT DATA=REPO1 OUT=REPO1; BY REGNO;
repo1_sorted = repo1_filtered.sort('REGNO')

# Save datasets
repo_sorted.write_csv(output_path / "REPO.csv")
repo_sorted.write_parquet(output_path / "REPO.parquet")
repo1_sorted.write_csv(output_path / "REPO1.csv")
repo1_sorted.write_parquet(output_path / "REPO1.parquet")

# DATA _NULL_; SET REPO; FILE REPOTXT NOTITLES;
with open(output_path / "REPOTXT.txt", "w") as f:
    for i, row in enumerate(repo_sorted.iter_rows(named=True)):
        if i == 0:
            f.write(f"{RDATE}-REPOSSESSION LISTING\n")
        
        line = (
            f"{str(row.get('BRABBR', '')):>3}"          # @001 BRABBR $3.
            f"{str(row.get('CAC', '')):>20}"            # @009 CAC $20.
            f"{str(row.get('REGNO', '')):>13}"          # @029 REGNO $13.
            f"{str(row.get('MAKE', '')):>16}"           # @043 MAKE $16.
            f"{str(row.get('MODEL', '')):>21}"          # @060 MODEL $21.
            f"{str(row.get('ENGINE', '')):>40}"         # @082 ENGINE $40.
            f"{str(row.get('CHASSIS', '')):>40}"        # @123 CHASSIS $40.
            f"{str(row.get('COLLYEAR', '')):>4}"        # @164 COLLYEAR $4.
        )
        f.write(line + "\n")

print("REPOTXT file generated successfully")

# DATA _NULL_; SET REPO1; FILE REPOTXT1 NOTITLES;
with open(output_path / "REPOTXT1.txt", "w") as f:
    for i, row in enumerate(repo1_sorted.iter_rows(named=True)):
        if i == 0:
            f.write(f"{RDATE}-REPOSSESSION LISTING (983,993)\n")
        
        line = (
            f"{str(row.get('BRABBR', '')):>3}"          # @001 BRABBR $3.
            f"{str(row.get('CAC', '')):>20}"            # @009 CAC $20.
            f"{str(row.get('REGNO', '')):>13}"          # @029 REGNO $13.
            f"{str(row.get('MAKE', '')):>16}"           # @043 MAKE $16.
            f"{str(row.get('MODEL', '')):>21}"          # @060 MODEL $21.
            f"{str(row.get('ENGINE', '')):>40}"         # @082 ENGINE $40.
            f"{str(row.get('CHASSIS', '')):>40}"        # @123 CHASSIS $40.
            f"{str(row.get('COLLYEAR', '')):>4}"        # @164 COLLYEAR $4.
        )
        f.write(line + "\n")

print("REPOTXT1 file generated successfully")
print("PROCESSING COMPLETED SUCCESSFULLY")
