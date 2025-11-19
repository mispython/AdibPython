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

# DATA DEPOSIT.UNCLAIM&REPTYEAR DEPOSIT.NOTUNCLAIM&REPTYEAR;
# INFILE UNCLAIM;
filename_unclaim = f"UNCLAIM{REPTYEAR}.parquet"
filename_notunclaim = f"NOTUNCLAIM{REPTYEAR}.parquet"

try:
    # Read UNCLAIM file (assuming fixed-width format)
    unclaim_df = pl.read_csv("UNCLAIM", 
                           has_header=False,
                           new_columns=['raw_line'])
    
    # Parse fixed-width format:
    # @003 ACCTNO PD6., @040 LEDGBAL PD7.2, @047 STATUS $1., @055 PAYMODE $10., @075 NAME $40.
    unclaim_parsed = unclaim_df.with_columns([
        pl.col('raw_line').str.slice(2, 6).str.strip().cast(pl.Int64).alias('ACCTNO'),      # @003 ACCTNO PD6.
        pl.col('raw_line').str.slice(39, 7).str.strip().cast(pl.Float64).alias('LEDGBAL'),  # @040 LEDGBAL PD7.2
        pl.col('raw_line').str.slice(46, 1).str.strip().alias('STATUS'),                    # @047 STATUS $1.
        pl.col('raw_line').str.slice(54, 10).str.strip().alias('PAYMODE'),                  # @055 PAYMODE $10.
        pl.col('raw_line').str.slice(74, 40).str.strip().alias('NAME')                      # @075 NAME $40.
    ]).drop('raw_line')
    
    # IF STATUS = 'U';
    unclaim_filtered = unclaim_parsed.filter(pl.col('STATUS') == 'U')
    
    # CATEGORY assignments
    unclaim_with_category = unclaim_filtered.with_columns([
        pl.when(pl.col('PAYMODE').str.slice(0, 1).is_in(['4', '6']))
        .then(pl.lit('SA'))
        .when(pl.col('PAYMODE').str.slice(0, 1).is_in(['3']))
        .then(pl.lit('CA'))
        .when(pl.col('PAYMODE').str.slice(0, 1).is_in(['1', '7']))
        .then(pl.lit('FD'))
        .otherwise(pl.lit('OTHER'))
        .alias('CATEGORY')
    ])
    
    # Split into UNCLAIM and NOTUNCLAIM based on PAYMODE
    valid_paymodes = ['1', '2', '3', '4', '5', '6', '7', '8', '9']
    
    unclaim_valid = unclaim_with_category.filter(
        pl.col('PAYMODE').str.slice(0, 1).is_in(valid_paymodes)
    )
    
    notunclaim_invalid = unclaim_with_category.filter(
        ~pl.col('PAYMODE').str.slice(0, 1).is_in(valid_paymodes)
    )
    
    # Save datasets
    unclaim_valid.write_parquet(deposit_path / filename_unclaim)
    notunclaim_invalid.write_parquet(deposit_path / filename_notunclaim)
    
    print(f"Created {filename_unclaim} and {filename_notunclaim}")
    
except FileNotFoundError:
    print("NOTE: UNCLAIM file not found")
    unclaim_valid = pl.DataFrame({
        'ACCTNO': [], 'LEDGBAL': [], 'STATUS': [], 'PAYMODE': [], 
        'NAME': [], 'CATEGORY': []
    })
    notunclaim_invalid = pl.DataFrame({
        'ACCTNO': [], 'LEDGBAL': [], 'STATUS': [], 'PAYMODE': [], 
        'NAME': [], 'CATEGORY': []
    })

# PROC SORT DATA=DEPOSIT.UNCLAIM&REPTYEAR; BY PAYMODE;
if not unclaim_valid.is_empty():
    unclaim_sorted = unclaim_valid.sort('PAYMODE')
    unclaim_sorted.write_parquet(deposit_path / filename_unclaim)

# PROC SORT DATA=DEPOSIT.NOTUNCLAIM&REPTYEAR; BY PAYMODE NAME;
if not notunclaim_invalid.is_empty():
    notunclaim_sorted = notunclaim_invalid.sort(['PAYMODE', 'NAME'])
    notunclaim_sorted.write_parquet(deposit_path / filename_notunclaim)
    
    # PROC PRINT; SUM LEDGBAL;
    print("NOTUNCLAIM DATA SUMMARY:")
    print(notunclaim_sorted)
    total_ledgbal = notunclaim_sorted.select(pl.col('LEDGBAL').sum()).row(0)[0]
    print(f"TOTAL LEDGBAL: {total_ledgbal:.2f}")
    print("-" * 50)

# PROC SUMMARY DATA=DEPOSIT.UNCLAIM&REPTYEAR; BY PAYMODE; VAR LEDGBAL;
if not unclaim_valid.is_empty():
    unclaim_summary = unclaim_sorted.group_by('PAYMODE').agg([
        pl.col('LEDGBAL').sum().alias('LEDGBAL_sum')
    ])
    
    # OUTPUT OUT=UNCLAIM(DROP=_FREQ_ _TYPE_) SUM=;
    unclaim_summary_clean = unclaim_summary.rename({'LEDGBAL_sum': 'LEDGBAL'})
    
    # PROC SORT DATA=DEPOSIT.UNCLAIM&REPTYEAR NODUPKEYS; BY PAYMODE;
    unclaim_deduped = unclaim_sorted.unique(subset=['PAYMODE']).drop('LEDGBAL')
    
    # DATA DEPOSIT.UNCLAIM&REPTYEAR; MERGE UNCLAIM(IN=A) DEPOSIT.UNCLAIM&REPTYEAR (IN=B DROP=LEDGBAL);
    unclaim_merged = unclaim_deduped.join(
        unclaim_summary_clean, on='PAYMODE', how='inner'
    )
    
    # Save final merged dataset
    unclaim_merged.write_parquet(deposit_path / filename_unclaim)
    
    # PROC PRINT; SUM LEDGBAL;
    print("UNCLAIM DATA SUMMARY (AFTER MERGE):")
    print(unclaim_merged)
    total_ledgbal_final = unclaim_merged.select(pl.col('LEDGBAL').sum()).row(0)[0]
    print(f"TOTAL LEDGBAL: {total_ledgbal_final:.2f}")
    print("-" * 50)

print("PROCESSING COMPLETED SUCCESSFULLY")
