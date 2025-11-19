import polars as pl
import duckdb
from pathlib import Path
import datetime

# Configuration
deposit_path = Path("DEPOSIT")
mni_path = Path("MNI")
imni_path = Path("IMNI")
output_path = Path("output")
output_path.mkdir(exist_ok=True)

# DATA REPTDATE (KEEP=REPTDATE);
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

# Create REPTDATE DataFrame
reptdate_df = pl.DataFrame({'REPTDATE': [reptdate]})
reptdate_df.write_parquet(output_path / "REPTDATE.parquet")
reptdate_df.write_csv(output_path / "REPTDATE.csv")

# DATA DEPOSIT.REMIT(DROP=ISSMM ISSDD ISSYY) NONDEBIT;
# INFILE REMIT;
try:
    # Read REMIT file (fixed-width format)
    remit_df = pl.read_csv("REMIT", 
                          has_header=False,
                          new_columns=['raw_line'])
    
    # Parse fixed-width format:
    remit_parsed = remit_df.with_columns([
        pl.col('raw_line').str.slice(2, 6).str.strip().cast(pl.Int64).alias('ACCTNO'),        # @003 ACCTNO PD6.
        pl.col('raw_line').str.slice(8, 6).str.strip().cast(pl.Int64).alias('CHEQNO'),        # @009 CHEQNO 6.
        pl.col('raw_line').str.slice(26, 4).str.strip().alias('ISSYY'),                       # @027 ISSYY $4.
        pl.col('raw_line').str.slice(31, 2).str.strip().alias('ISSMM'),                       # @032 ISSMM $2.
        pl.col('raw_line').str.slice(34, 2).str.strip().alias('ISSDD'),                       # @035 ISSDD $2.
        pl.col('raw_line').str.slice(36, 3).str.strip().cast(pl.Int64).alias('ISSBRANCH'),    # @037 ISSBRANCH PD3.
        pl.col('raw_line').str.slice(39, 7).str.strip().cast(pl.Float64).alias('LEDGBAL'),    # @040 LEDGBAL PD7.2
        pl.col('raw_line').str.slice(46, 2).str.strip().alias('STATUS'),                      # @047 STATUS $2.
        pl.col('raw_line').str.slice(54, 10).str.strip().alias('PAYMODE'),                    # @055 PAYMODE $10.
        pl.col('raw_line').str.slice(74, 40).str.strip().alias('NAME')                        # @075 NAME $40.
    ]).drop('raw_line')
    
    # ISSDTE = MDY(ISSMM,ISSDD,ISSYY);
    remit_with_dates = remit_parsed.with_columns([
        pl.concat_str([pl.col('ISSMM'), pl.col('ISSDD'), pl.col('ISSYY')], separator='')
        .str.strptime(pl.Date, '%m%d%Y')
        .alias('ISSDTE')
    ])
    
    # CATEGORY assignments
    remit_with_category = remit_with_dates.with_columns([
        pl.when(pl.col('PAYMODE').str.slice(0, 1).is_in(['4', '5', '6']))
        .then(pl.lit('SA'))
        .when(pl.col('PAYMODE').str.slice(0, 1).is_in(['3']))
        .then(pl.lit('CA'))
        .when(pl.col('PAYMODE').str.slice(0, 1).is_in(['1', '7']))
        .then(pl.lit('FD'))
        .otherwise(pl.lit('OTHER'))
        .alias('CATEGORY')
    ])
    
    # Split into REMIT and NONDEBIT based on PAYMODE
    valid_paymodes = ['1', '2', '3', '4', '5', '6', '7', '8', '9']
    
    remit_valid = remit_with_category.filter(
        pl.col('PAYMODE').str.slice(0, 1).is_in(valid_paymodes)
    ).drop(['ISSMM', 'ISSDD', 'ISSYY'])  # DROP=ISSMM ISSDD ISSYY
    
    nondebit_invalid = remit_with_category.filter(
        ~pl.col('PAYMODE').str.slice(0, 1).is_in(valid_paymodes)
    )
    
    # Save datasets
    remit_valid.write_parquet(deposit_path / "REMIT.parquet")
    nondebit_invalid.write_parquet(output_path / "NONDEBIT.parquet")
    
    print("Created REMIT and NONDEBIT datasets")
    
except FileNotFoundError:
    print("NOTE: REMIT file not found")
    remit_valid = pl.DataFrame()
    nondebit_invalid = pl.DataFrame()

# PROC SORT DATA=DEPOSIT.REMIT; BY PAYMODE;
if not remit_valid.is_empty():
    remit_sorted = remit_valid.sort('PAYMODE')
    remit_sorted.write_parquet(deposit_path / "REMIT.parquet")

# PROC SORT DATA=NONDEBIT; BY PAYMODE NAME;
if not nondebit_invalid.is_empty():
    nondebit_sorted = nondebit_invalid.sort(['PAYMODE', 'NAME'])
    nondebit_sorted.write_parquet(output_path / "NONDEBIT.parquet")

# PROC SUMMARY DATA=DEPOSIT.REMIT; BY PAYMODE; VAR LEDGBAL;
if not remit_sorted.is_empty():
    remit_summary = remit_sorted.group_by('PAYMODE').agg([
        pl.col('LEDGBAL').sum().alias('LEDGBAL_sum')
    ])
    
    # OUTPUT OUT=REMIT(DROP=_FREQ_ _TYPE_) SUM=;
    remit_summary_clean = remit_summary.rename({'LEDGBAL_sum': 'LEDGBAL'})
    
    # DATA REMIT; MERGE REMIT(IN=A) DEPOSIT.REMIT (IN=B DROP=LEDGBAL);
    remit_deduped = remit_sorted.unique(subset=['PAYMODE']).drop('LEDGBAL')
    remit_merged = remit_deduped.join(remit_summary_clean, on='PAYMODE', how='inner')
    
    # PROC SORT DATA=REMIT NODUPKEYS; BY PAYMODE;
    remit_final = remit_merged.unique(subset=['PAYMODE'])
    remit_final.write_parquet(output_path / "REMIT_FINAL.parquet")

# Load additional datasets
savg_filename = f"SAVG{REPTMON}{NOWK}.parquet"
curn_filename = f"CURN{REPTMON}{NOWK}.parquet"
isavg_filename = f"ISAVG{REPTMON}{NOWK}.parquet"
icurn_filename = f"ICURN{REPTMON}{NOWK}.parquet"

datasets = []
try:
    savg_df = pl.read_parquet(mni_path / savg_filename).select(['ACCTNO', 'PRODCD', 'COSTCTR'])
    datasets.append(savg_df)
except FileNotFoundError:
    print(f"NOTE: {savg_filename} not found")

try:
    curn_df = pl.read_parquet(mni_path / curn_filename).select(['ACCTNO', 'PRODCD', 'COSTCTR'])
    datasets.append(curn_df)
except FileNotFoundError:
    print(f"NOTE: {curn_filename} not found")

try:
    isavg_df = pl.read_parquet(imni_path / isavg_filename).select(['ACCTNO', 'PRODCD', 'COSTCTR'])
    datasets.append(isavg_df)
except FileNotFoundError:
    print(f"NOTE: {isavg_filename} not found")

try:
    icurn_df = pl.read_parquet(imni_path / icurn_filename).select(['ACCTNO', 'PRODCD', 'COSTCTR'])
    datasets.append(icurn_df)
except FileNotFoundError:
    print(f"NOTE: {icurn_filename} not found")

# DATA DEP; SET all datasets;
if datasets:
    dep_df = pl.concat(datasets, how="diagonal")
    
    # IF PRODCD IN specified values;
    valid_prodcd = ['42110', '42310', '42120', '42320', '42130', '42132', '42180', '42199', '42699']
    dep_filtered = dep_df.filter(pl.col('PRODCD').is_in(valid_prodcd))
    
    # PROC SORT DATA=DEP NODUPKEYS; BY ACCTNO;
    dep_deduped = dep_filtered.unique(subset=['ACCTNO'])
else:
    dep_deduped = pl.DataFrame()

# DATA REMIT; SET REMIT; FORMAT ACCTNO 10.; ACCTNO = PAYMODE;
if not remit_final.is_empty():
    remit_for_merge = remit_final.with_columns([
        pl.col('PAYMODE').cast(pl.Int64).alias('ACCTNO')
    ])
    
    # DATA DEP; MERGE DEP(IN=A) REMIT(IN=B);
    if not dep_deduped.is_empty():
        dep_merged = dep_deduped.join(remit_for_merge, on='ACCTNO', how='right', suffix='_remit')
        
        # BC assignment logic
        dep_with_bc = dep_merged.with_columns([
            pl.when(pl.col('PRODCD').is_not_null() & pl.col('LEDGBAL').is_not_null())
            .then(pl.lit('DEBITTED'))
            .otherwise(pl.lit('NOT_FOUND'))
            .alias('BC')
        ]).filter(pl.col('LEDGBAL').is_not_null())  # IF B THEN OUTPUT;
    else:
        dep_with_bc = remit_for_merge.with_columns([
            pl.lit('NOT_FOUND').alias('BC'),
            pl.lit(None).cast(pl.Utf8).alias('PRODCD'),
            pl.lit(None).cast(pl.Int64).alias('COSTCTR')
        ])
    
    # PROC SORT DATA=DEP; BY CATEGORY;
    dep_sorted = dep_with_bc.sort('CATEGORY')
    
    print("BANKERS CHEQUE WITH DEBITTED A/C (CONVENTIONAL)")
    # WHERE BC = 'DEBITTED' AND (COSTCTR < 3000 OR COSTCTR>3999);
    debitted_filtered = dep_sorted.filter(
        (pl.col('BC') == 'DEBITTED') & 
        ((pl.col('COSTCTR') < 3000) | (pl.col('COSTCTR') > 3999))
    )
    
    if not debitted_filtered.is_empty():
        debitted_summary = debitted_filtered.group_by('CATEGORY').agg([
            pl.col('LEDGBAL').sum().alias('LEDGBAL')
        ])
        print(debitted_summary)
        total = debitted_summary.select(pl.col('LEDGBAL').sum()).row(0)[0]
        print(f"TOTAL BC/DD AMOUNT: {total:.2f}")
    
    print("\nBANKERS CHEQUE WITH DEBITTED A/C NOT FOUND IN FISS (CONV&ISLM)")
    # Complex WHERE condition for NOT_FOUND
    notfound_filtered = dep_sorted.filter(
        (pl.col('BC') == 'NOT_FOUND') &
        ~(
            ((pl.col('ACCTNO') > 3700000000) & (pl.col('ACCTNO') < 3999999999)) |
            ((pl.col('ACCTNO') > 4700000000) & (pl.col('ACCTNO') < 4999999999)) |
            ((pl.col('ACCTNO') > 6700000000) & (pl.col('ACCTNO') < 6999999999)) |
            ((pl.col('ACCTNO') > 1700000000) & (pl.col('ACCTNO') < 1999999999)) |
            ((pl.col('ACCTNO') > 7700000000) & (pl.col('ACCTNO') < 7999999999))
        )
    )
    
    if not notfound_filtered.is_empty():
        notfound_summary = notfound_filtered.group_by('CATEGORY').agg([
            pl.col('LEDGBAL').sum().alias('LEDGBAL')
        ])
        print(notfound_summary)
        total = notfound_summary.select(pl.col('LEDGBAL').sum()).row(0)[0]
        print(f"TOTAL BC/DD AMOUNT: {total:.2f}")

# DATA NONDEBIT; SET NONDEBIT; BC = 'NON_DEBIT'; ACCTNO = PAYMODE;
if not nondebit_sorted.is_empty():
    nondebit_processed = nondebit_sorted.with_columns([
        pl.lit('NON_DEBIT').alias('BC'),
        pl.col('PAYMODE').cast(pl.Int64).alias('ACCTNO')
    ])
    
    print("\nBANKERS CHEQUE WITH NON-DEBITTED A/C")
    nondebit_summary = nondebit_processed.group_by('CATEGORY').agg([
        pl.col('LEDGBAL').sum().alias('LEDGBAL')
    ])
    print(nondebit_summary)
    total = nondebit_summary.select(pl.col('LEDGBAL').sum()).row(0)[0]
    print(f"TOTAL BC/DD AMOUNT: {total:.2f}")

print("PROCESSING COMPLETED SUCCESSFULLY")
