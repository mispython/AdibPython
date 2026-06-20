import polars as pl
import duckdb
from pathlib import Path
import datetime
import pyreadstat

# Configuration
deposit_path = Path("DEPOSIT")
mni_path = Path("MNI")
imni_path = Path("IMNI")
output_path = Path("output")
output_path.mkdir(exist_ok=True)
deposit_path.mkdir(exist_ok=True)

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
    # Assuming REMIT is a text file without header
    remit_df = pl.read_csv("REMIT", 
                          has_header=False,
                          new_columns=['raw_line'],
                          separator='\n')  # Read entire line
    
    # Parse fixed-width format based on SAS layout:
    # @003 ACCTNO PD6. -> positions 3-9 (0-indexed: 2-8)
    # @009 CHEQNO 6. -> positions 9-15 (0-indexed: 8-14)
    # @027 ISSYY $4. -> positions 27-31 (0-indexed: 26-30)
    # @032 ISSMM $2. -> positions 32-34 (0-indexed: 31-33)
    # @035 ISSDD $2. -> positions 35-37 (0-indexed: 34-36)
    # @037 ISSBRANCH PD3. -> positions 37-40 (0-indexed: 36-39)
    # @040 LEDGBAL PD7.2 -> positions 40-49 (0-indexed: 39-48)
    # @047 STATUS $2. -> positions 47-49 (0-indexed: 46-48)
    # @055 PAYMODE $10. -> positions 55-65 (0-indexed: 54-64)
    # @075 NAME $40. -> positions 75-115 (0-indexed: 74-114)
    
    remit_parsed = remit_df.with_columns([
        pl.col('raw_line').str.slice(2, 7).str.strip().cast(pl.Int64, strict=False).alias('ACCTNO'),     # @003 ACCTNO PD6. (positions 3-9)
        pl.col('raw_line').str.slice(8, 6).str.strip().cast(pl.Int64, strict=False).alias('CHEQNO'),    # @009 CHEQNO 6. (positions 9-15)
        pl.col('raw_line').str.slice(26, 4).str.strip().alias('ISSYY'),                                 # @027 ISSYY $4.
        pl.col('raw_line').str.slice(31, 2).str.strip().alias('ISSMM'),                                 # @032 ISSMM $2.
        pl.col('raw_line').str.slice(34, 2).str.strip().alias('ISSDD'),                                 # @035 ISSDD $2.
        pl.col('raw_line').str.slice(36, 3).str.strip().cast(pl.Int64, strict=False).alias('ISSBRANCH'), # @037 ISSBRANCH PD3.
        pl.col('raw_line').str.slice(39, 8).str.strip().cast(pl.Float64, strict=False).alias('LEDGBAL'), # @040 LEDGBAL PD7.2 (including decimal)
        pl.col('raw_line').str.slice(46, 2).str.strip().alias('STATUS'),                                # @047 STATUS $2.
        pl.col('raw_line').str.slice(54, 10).str.strip().alias('PAYMODE'),                              # @055 PAYMODE $10.
        pl.col('raw_line').str.slice(74, 40).str.strip().alias('NAME')                                  # @075 NAME $40.
    ]).drop('raw_line')
    
    # Remove rows with null ACCTNO or LEDGBAL
    remit_parsed = remit_parsed.filter(
        pl.col('ACCTNO').is_not_null() & pl.col('LEDGBAL').is_not_null()
    )
    
    # ISSDTE = MDY(ISSMM,ISSDD,ISSYY);
    remit_with_dates = remit_parsed.with_columns([
        pl.when(
            pl.col('ISSMM').is_not_null() & 
            pl.col('ISSDD').is_not_null() & 
            pl.col('ISSYY').is_not_null()
        )
        .then(
            pl.concat_str([pl.col('ISSMM'), pl.col('ISSDD'), pl.col('ISSYY')], separator='')
            .str.strptime(pl.Date, '%m%d%Y', strict=False)
        )
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
    remit_valid.write_csv(deposit_path / "REMIT.csv")
    nondebit_invalid.write_parquet(output_path / "NONDEBIT.parquet")
    nondebit_invalid.write_csv(output_path / "NONDEBIT.csv")
    
    print(f"Created REMIT with {remit_valid.height} records and NONDEBIT with {nondebit_invalid.height} records")
    
except FileNotFoundError:
    print("NOTE: REMIT file not found")
    remit_valid = pl.DataFrame()
    nondebit_invalid = pl.DataFrame()

# PROC SORT DATA=DEPOSIT.REMIT; BY PAYMODE;
if not remit_valid.is_empty():
    remit_sorted = remit_valid.sort('PAYMODE')
    remit_sorted.write_parquet(deposit_path / "REMIT.parquet")
    remit_sorted.write_csv(deposit_path / "REMIT_sorted.csv")

# PROC SORT DATA=NONDEBIT; BY PAYMODE NAME;
if not nondebit_invalid.is_empty():
    nondebit_sorted = nondebit_invalid.sort(['PAYMODE', 'NAME'])
    nondebit_sorted.write_parquet(output_path / "NONDEBIT.parquet")
    nondebit_sorted.write_csv(output_path / "NONDEBIT_sorted.csv")

# PROC SUMMARY DATA=DEPOSIT.REMIT; BY PAYMODE; VAR LEDGBAL;
if not remit_valid.is_empty():
    remit_summary = remit_valid.group_by('PAYMODE').agg([
        pl.col('LEDGBAL').sum().alias('LEDGBAL_sum')
    ])
    
    # OUTPUT OUT=REMIT(DROP=_FREQ_ _TYPE_) SUM=;
    remit_summary_clean = remit_summary.rename({'LEDGBAL_sum': 'LEDGBAL'})
    
    # DATA REMIT; MERGE REMIT(IN=A) DEPOSIT.REMIT (IN=B DROP=LEDGBAL);
    remit_deduped = remit_valid.unique(subset=['PAYMODE']).drop('LEDGBAL')
    remit_merged = remit_deduped.join(remit_summary_clean, on='PAYMODE', how='inner')
    
    # PROC SORT DATA=REMIT NODUPKEYS; BY PAYMODE;
    remit_final = remit_merged.unique(subset=['PAYMODE'])
    remit_final.write_parquet(output_path / "REMIT_FINAL.parquet")
    remit_final.write_csv(output_path / "REMIT_FINAL.csv")
else:
    remit_final = pl.DataFrame()

# Load additional datasets from SAS files
savg_filename = f"SAVG{REPTMON}{NOWK}.sas7bdat"
curn_filename = f"CURN{REPTMON}{NOWK}.sas7bdat"
isavg_filename = f"ISAVG{REPTMON}{NOWK}.sas7bdat"
icurn_filename = f"ICURN{REPTMON}{NOWK}.sas7bdat"

datasets = []

def read_sas_file(filepath, columns):
    """Read SAS file and select specific columns"""
    try:
        df, meta = pyreadstat.read_sas7bdat(filepath)
        pl_df = pl.DataFrame(df)
        # Select only needed columns if they exist
        existing_cols = [col for col in columns if col in pl_df.columns]
        if existing_cols:
            return pl_df.select(existing_cols)
        else:
            print(f"Warning: None of the expected columns found in {filepath}")
            return None
    except FileNotFoundError:
        print(f"NOTE: {filepath} not found")
        return None
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None

# Read SAS files
savg_df = read_sas_file(mni_path / savg_filename, ['ACCTNO', 'PRODCD', 'COSTCTR'])
if savg_df is not None:
    datasets.append(savg_df)
    # Also save as parquet for faster subsequent access
    savg_df.write_parquet(mni_path / f"SAVG{REPTMON}{NOWK}.parquet")

curn_df = read_sas_file(mni_path / curn_filename, ['ACCTNO', 'PRODCD', 'COSTCTR'])
if curn_df is not None:
    datasets.append(curn_df)
    curn_df.write_parquet(mni_path / f"CURN{REPTMON}{NOWK}.parquet")

isavg_df = read_sas_file(imni_path / isavg_filename, ['ACCTNO', 'PRODCD', 'COSTCTR'])
if isavg_df is not None:
    datasets.append(isavg_df)
    isavg_df.write_parquet(imni_path / f"ISAVG{REPTMON}{NOWK}.parquet")

icurn_df = read_sas_file(imni_path / icurn_filename, ['ACCTNO', 'PRODCD', 'COSTCTR'])
if icurn_df is not None:
    datasets.append(icurn_df)
    icurn_df.write_parquet(imni_path / f"ICURN{REPTMON}{NOWK}.parquet")

# DATA DEP; SET all datasets;
if datasets:
    dep_df = pl.concat(datasets, how="diagonal")
    print(f"Combined DEP dataset with {dep_df.height} records")
    
    # IF PRODCD IN specified values;
    valid_prodcd = ['42110', '42310', '42120', '42320', '42130', '42132', '42180', '42199', '42699']
    dep_filtered = dep_df.filter(pl.col('PRODCD').is_in(valid_prodcd))
    print(f"Filtered DEP with valid PRODCD: {dep_filtered.height} records")
    
    # PROC SORT DATA=DEP NODUPKEYS; BY ACCTNO;
    dep_deduped = dep_filtered.unique(subset=['ACCTNO'])
    print(f"Unique DEP records by ACCTNO: {dep_deduped.height} records")
else:
    dep_deduped = pl.DataFrame()
    print("No DEP datasets found")

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
        
        # Fill null values for missing DEP data
        dep_with_bc = dep_with_bc.with_columns([
            pl.col('PRODCD').fill_null('UNKNOWN'),
            pl.col('COSTCTR').fill_null(0)
        ])
    else:
        dep_with_bc = remit_for_merge.with_columns([
            pl.lit('NOT_FOUND').alias('BC'),
            pl.lit('UNKNOWN').alias('PRODCD'),
            pl.lit(0).cast(pl.Int64).alias('COSTCTR')
        ])
    
    # PROC SORT DATA=DEP; BY CATEGORY;
    dep_sorted = dep_with_bc.sort('CATEGORY')
    
    print("\n" + "="*80)
    print("BANKERS CHEQUE WITH DEBITTED A/C (CONVENTIONAL)")
    print("="*80)
    # WHERE BC = 'DEBITTED' AND (COSTCTR < 3000 OR COSTCTR>3999);
    debitted_filtered = dep_sorted.filter(
        (pl.col('BC') == 'DEBITTED') & 
        ((pl.col('COSTCTR') < 3000) | (pl.col('COSTCTR') > 3999))
    )
    
    if not debitted_filtered.is_empty():
        debitted_summary = debitted_filtered.group_by('CATEGORY').agg([
            pl.col('LEDGBAL').sum().alias('LEDGBAL')
        ])
        print("\nSummary by Category:")
        print(debitted_summary)
        
        # Show detailed records
        print("\nDetailed Records:")
        print(debitted_filtered.select(['ACCTNO', 'PAYMODE', 'CATEGORY', 'LEDGBAL', 'NAME', 'COSTCTR']))
        
        total = debitted_summary.select(pl.col('LEDGBAL').sum()).row(0)[0]
        print(f"\nTOTAL BC/DD AMOUNT: {total:,.2f}")
    else:
        print("No DEBITTED records found")
    
    print("\n" + "="*80)
    print("BANKERS CHEQUE WITH DEBITTED A/C NOT FOUND IN FISS (CONV&ISLM)")
    print("="*80)
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
        print("\nSummary by Category:")
        print(notfound_summary)
        
        # Show detailed records
        print("\nDetailed Records:")
        print(notfound_filtered.select(['ACCTNO', 'PAYMODE', 'CATEGORY', 'LEDGBAL', 'NAME']))
        
        total = notfound_summary.select(pl.col('LEDGBAL').sum()).row(0)[0]
        print(f"\nTOTAL BC/DD AMOUNT: {total:,.2f}")
    else:
        print("No NOT_FOUND records found")
    
    # Save results
    dep_sorted.write_parquet(output_path / "DEP_SORTED.parquet")
    dep_sorted.write_csv(output_path / "DEP_SORTED.csv")
    
    if not debitted_filtered.is_empty():
        debitted_filtered.write_parquet(output_path / "DEBITTED_FILTERED.parquet")
        debitted_filtered.write_csv(output_path / "DEBITTED_FILTERED.csv")
    
    if not notfound_filtered.is_empty():
        notfound_filtered.write_parquet(output_path / "NOTFOUND_FILTERED.parquet")
        notfound_filtered.write_csv(output_path / "NOTFOUND_FILTERED.csv")

# DATA NONDEBIT; SET NONDEBIT; BC = 'NON_DEBIT'; ACCTNO = PAYMODE;
if not nondebit_invalid.is_empty():
    nondebit_processed = nondebit_invalid.with_columns([
        pl.lit('NON_DEBIT').alias('BC'),
        pl.col('PAYMODE').cast(pl.Int64).alias('ACCTNO')
    ])
    
    print("\n" + "="*80)
    print("BANKERS CHEQUE WITH NON-DEBITTED A/C")
    print("="*80)
    nondebit_summary = nondebit_processed.group_by('CATEGORY').agg([
        pl.col('LEDGBAL').sum().alias('LEDGBAL')
    ])
    print("\nSummary by Category:")
    print(nondebit_summary)
    
    # Show detailed records
    print("\nDetailed Records:")
    print(nondebit_processed.select(['ACCTNO', 'PAYMODE', 'CATEGORY', 'LEDGBAL', 'NAME']))
    
    total = nondebit_summary.select(pl.col('LEDGBAL').sum()).row(0)[0]
    print(f"\nTOTAL BC/DD AMOUNT: {total:,.2f}")
    
    nondebit_processed.write_parquet(output_path / "NONDEBIT_PROCESSED.parquet")
    nondebit_processed.write_csv(output_path / "NONDEBIT_PROCESSED.csv")

print("\n" + "="*80)
print("PROCESSING COMPLETED SUCCESSFULLY")
print("="*80)
