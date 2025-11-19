import polars as pl
import duckdb
from pathlib import Path
import datetime

# Configuration
deposit_path = Path("DEPOSIT")
output_path = Path("output")
output_path.mkdir(exist_ok=True)

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

# Create REPTDATE DataFrame (KEEP=REPTDATE)
reptdate_df = pl.DataFrame({'REPTDATE': [reptdate]})
reptdate_df.write_parquet(output_path / "REPTDATE.parquet")
reptdate_df.write_csv(output_path / "REPTDATE.csv")

# DATA DEPOSIT.ICLPBB&REPTMON&REPTYEAR;
# INFILE PBB;
filename_pbb = f"ICLPBB{REPTMON}{REPTYEAR}.parquet"
try:
    # Read PBB file (assuming fixed-width format)
    pbb_df = pl.read_csv("PBB", 
                        has_header=False,
                        new_columns=['raw_line'])
    
    # Parse fixed-width format: @001 ACCTNO 10., @012 CURBAL 10.
    pbb_parsed = pbb_df.with_columns([
        pl.col('raw_line').str.slice(0, 10).str.strip().cast(pl.Int64).alias('ACCTNO'),
        pl.col('raw_line').str.slice(11, 10).str.strip().cast(pl.Float64).alias('CURBAL')
    ]).drop('raw_line')
    
    # IF ACCTNO = . THEN DELETE;
    pbb_filtered = pbb_parsed.filter(pl.col('ACCTNO').is_not_null())
    
    # Save to DEPOSIT path
    pbb_filtered.write_parquet(deposit_path / filename_pbb)
    print(f"Created {filename_pbb}")
    
except FileNotFoundError:
    print("NOTE: PBB file not found, creating empty dataframe")
    pbb_filtered = pl.DataFrame({'ACCTNO': [], 'CURBAL': []})

# PROC SORT DATA=DEPOSIT.ICLPBB&REPTMON&REPTYEAR NODUPKEYS; BY ACCTNO;
if not pbb_filtered.is_empty():
    pbb_sorted = pbb_filtered.unique(subset=['ACCTNO']).sort('ACCTNO')
    pbb_sorted.write_parquet(deposit_path / filename_pbb)
    
    # PROC PRINT; SUM CURBAL;
    print("PBB DATA SUMMARY:")
    print(pbb_sorted)
    total_curbal = pbb_sorted.select(pl.col('CURBAL').sum()).row(0)[0]
    print(f"TOTAL CURBAL: {total_curbal:.2f}")
    print("-" * 50)

# DATA DEPOSIT.ICLPIBB&REPTMON&REPTYEAR;
# INFILE PIBB;
filename_pibb = f"ICLPIBB{REPTMON}{REPTYEAR}.parquet"
try:
    # Read PIBB file (assuming fixed-width format)
    pibb_df = pl.read_csv("PIBB", 
                         has_header=False,
                         new_columns=['raw_line'])
    
    # Parse fixed-width format: @001 ACCTNO 10., @012 CURBAL 10.
    pibb_parsed = pibb_df.with_columns([
        pl.col('raw_line').str.slice(0, 10).str.strip().cast(pl.Int64).alias('ACCTNO'),
        pl.col('raw_line').str.slice(11, 10).str.strip().cast(pl.Float64).alias('CURBAL')
    ]).drop('raw_line')
    
    # IF ACCTNO = . THEN DELETE;
    pibb_filtered = pibb_parsed.filter(pl.col('ACCTNO').is_not_null())
    
    # Save to DEPOSIT path
    pibb_filtered.write_parquet(deposit_path / filename_pibb)
    print(f"Created {filename_pibb}")
    
except FileNotFoundError:
    print("NOTE: PIBB file not found, creating empty dataframe")
    pibb_filtered = pl.DataFrame({'ACCTNO': [], 'CURBAL': []})

# PROC SORT DATA=DEPOSIT.ICLPIBB&REPTMON&REPTYEAR NODUPKEYS; BY ACCTNO;
if not pibb_filtered.is_empty():
    pibb_sorted = pibb_filtered.unique(subset=['ACCTNO']).sort('ACCTNO')
    pibb_sorted.write_parquet(deposit_path / filename_pibb)
    
    # PROC PRINT; SUM CURBAL;
    print("PIBB DATA SUMMARY:")
    print(pibb_sorted)
    total_curbal = pibb_sorted.select(pl.col('CURBAL').sum()).row(0)[0]
    print(f"TOTAL CURBAL: {total_curbal:.2f}")
    print("-" * 50)

print("PROCESSING COMPLETED SUCCESSFULLY")
