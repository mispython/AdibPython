import polars as pl
import duckdb
from pathlib import Path
import datetime

# Configuration
unclaim_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQUCLM")
mni_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/MNI")
imni_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/IMNI")
output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQUCLM")
output_path.mkdir(exist_ok=True)

# DATA REPTDATE (KEEP=REPTDATE);
# REPTDATE=INPUT('01'||'01'||PUT(YEAR(TODAY()), 4.), DDMMYY8.)-1;
today = datetime.date.today()
date_string = f"0101{today.year}"  # Fixed '0101' + current year
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

# Initialize variables to avoid NameError
unclaim_sorted = pl.DataFrame()
nondebit_sorted = pl.DataFrame()
unclaim_final = pl.DataFrame()
dep_deduped = pl.DataFrame()

# DATA UNCLAIM NONDEBIT;
# SET UNCLAIM.UNCLAIM&REPTYEAR UNCLAIM.NOTUNCLAIM&REPTYEAR;
filename_unclaim = f"UNCLAIM{REPTYEAR}.parquet"
filename_notunclaim = f"NOTUNCLAIM{REPTYEAR}.parquet"

try:
    unclaim_df1 = pl.read_parquet(unclaim_path / filename_unclaim)
    print(f"Loaded {filename_unclaim} with {len(unclaim_df1)} records")
except FileNotFoundError:
    unclaim_df1 = pl.DataFrame()
    print(f"NOTE: {filename_unclaim} not found")

try:
    unclaim_df2 = pl.read_parquet(unclaim_path / filename_notunclaim)
    print(f"Loaded {filename_notunclaim} with {len(unclaim_df2)} records")
except FileNotFoundError:
    unclaim_df2 = pl.DataFrame()
    print(f"NOTE: {filename_notunclaim} not found")

# Combine both datasets
if not unclaim_df1.is_empty() or not unclaim_df2.is_empty():
    combined_unclaim = pl.concat([unclaim_df1, unclaim_df2], how="diagonal")
    print(f"Combined dataset has {len(combined_unclaim)} records")
    
    # CATEGORY assignments
    unclaim_with_category = combined_unclaim.with_columns([
        pl.when(pl.col('PAYMODE').str.slice(0, 1).is_in(['4', '6']))
        .then(pl.lit('SA'))
        .when(pl.col('PAYMODE').str.slice(0, 1).is_in(['3']))
        .then(pl.lit('CA'))
        .when(pl.col('PAYMODE').str.slice(0, 1).is_in(['1', '7']))
        .then(pl.lit('FD'))
        .otherwise(pl.lit('OTHER'))
        .alias('CATEGORY')
    ])
    
    # Split into UNCLAIM and NONDEBIT based on PAYMODE
    valid_paymodes = ['1', '2', '3', '4', '5', '6', '7', '8', '9']
    
    unclaim_valid = unclaim_with_category.filter(
        pl.col('PAYMODE').str.slice(0, 1).is_in(valid_paymodes)
    )
    
    nondebit_invalid = unclaim_with_category.filter(
        ~pl.col('PAYMODE').str.slice(0, 1).is_in(valid_paymodes)
    )
    
    print(f"UNCLAIM records: {len(unclaim_valid)}")
    print(f"NONDEBIT records: {len(nondebit_invalid)}")
    
    # PROC SORT DATA=UNCLAIM; BY PAYMODE;
    if not unclaim_valid.is_empty():
        unclaim_sorted = unclaim_valid.sort('PAYMODE')
        unclaim_sorted.write_parquet(output_path / "UNCLAIM.parquet")
        unclaim_sorted.write_csv(output_path / "UNCLAIM.csv")
        print(f"Saved UNCLAIM with {len(unclaim_sorted)} records")
    
    # PROC SORT DATA=NONDEBIT; BY PAYMODE;
    if not nondebit_invalid.is_empty():
        nondebit_sorted = nondebit_invalid.sort('PAYMODE')
        nondebit_sorted.write_parquet(output_path / "NONDEBIT.parquet")
        nondebit_sorted.write_csv(output_path / "NONDEBIT.csv")
        print(f"Saved NONDEBIT with {len(nondebit_sorted)} records")
    
    # PROC SUMMARY DATA=UNCLAIM; BY PAYMODE; VAR LEDGBAL;
    if not unclaim_sorted.is_empty():
        unclaim_summary = unclaim_sorted.group_by('PAYMODE').agg([
            pl.col('LEDGBAL').sum().alias('LEDGBAL_sum')
        ])
        
        # OUTPUT OUT=UNCLAIMX(DROP=_FREQ_ _TYPE_) SUM=;
        unclaim_summary_clean = unclaim_summary.rename({'LEDGBAL_sum': 'LEDGBAL'})
        
        # DATA UNCLAIM; MERGE UNCLAIMX(IN=A) UNCLAIM (IN=B DROP=LEDGBAL);
        unclaim_deduped = unclaim_sorted.unique(subset=['PAYMODE']).drop('LEDGBAL')
        unclaim_merged = unclaim_deduped.join(unclaim_summary_clean, on='PAYMODE', how='inner')
        
        # PROC SORT DATA=UNCLAIM NODUPKEYS; BY PAYMODE;
        unclaim_final = unclaim_merged.unique(subset=['PAYMODE'])
        unclaim_final.write_parquet(output_path / "UNCLAIM_FINAL.parquet")
        unclaim_final.write_csv(output_path / "UNCLAIM_FINAL.csv")
        print(f"Saved UNCLAIM_FINAL with {len(unclaim_final)} records")
else:
    print("No UNCLAIM or NOTUNCLAIM data found")

# Load additional datasets
savg_filename = f"SAVG{REPTMON}{NOWK}.parquet"
curn_filename = f"CURN{REPTMON}{NOWK}.parquet"
isavg_filename = f"ISAVG{REPTMON}{NOWK}.parquet"
icurn_filename = f"ICURN{REPTMON}{NOWK}.parquet"

print(f"\nLooking for files:")
print(f"  SAVG: {savg_filename}")
print(f"  CURN: {curn_filename}")
print(f"  ISAVG: {isavg_filename}")
print(f"  ICURN: {icurn_filename}")

datasets = []
try:
    savg_df = pl.read_parquet(mni_path / savg_filename).select(['ACCTNO', 'PRODCD', 'COSTCTR'])
    datasets.append(savg_df)
    print(f"Loaded {savg_filename} with {len(savg_df)} records")
except FileNotFoundError:
    print(f"NOTE: {savg_filename} not found")
except Exception as e:
    print(f"Error loading {savg_filename}: {e}")

try:
    curn_df = pl.read_parquet(mni_path / curn_filename).select(['ACCTNO', 'PRODCD', 'COSTCTR'])
    datasets.append(curn_df)
    print(f"Loaded {curn_filename} with {len(curn_df)} records")
except FileNotFoundError:
    print(f"NOTE: {curn_filename} not found")
except Exception as e:
    print(f"Error loading {curn_filename}: {e}")

try:
    isavg_df = pl.read_parquet(imni_path / isavg_filename).select(['ACCTNO', 'PRODCD', 'COSTCTR'])
    datasets.append(isavg_df)
    print(f"Loaded {isavg_filename} with {len(isavg_df)} records")
except FileNotFoundError:
    print(f"NOTE: {isavg_filename} not found")
except Exception as e:
    print(f"Error loading {isavg_filename}: {e}")

try:
    icurn_df = pl.read_parquet(imni_path / icurn_filename).select(['ACCTNO', 'PRODCD', 'COSTCTR'])
    datasets.append(icurn_df)
    print(f"Loaded {icurn_filename} with {len(icurn_df)} records")
except FileNotFoundError:
    print(f"NOTE: {icurn_filename} not found")
except Exception as e:
    print(f"Error loading {icurn_filename}: {e}")

# DATA DEP; SET all datasets;
if datasets:
    dep_df = pl.concat(datasets, how="diagonal")
    print(f"Combined DEP dataset has {len(dep_df)} records")
    
    # IF PRODCD IN specified values;
    valid_prodcd = ['42110', '42310', '42120', '42320', '42130', '42132', '42180', '42199', '42699']
    dep_filtered = dep_df.filter(pl.col('PRODCD').is_in(valid_prodcd))
    print(f"After filtering PRODCD: {len(dep_filtered)} records")
    
    # PROC SORT DATA=DEP NODUPKEYS; BY ACCTNO;
    dep_deduped = dep_filtered.unique(subset=['ACCTNO'])
    dep_deduped.write_parquet(output_path / "DEP.parquet")
    dep_deduped.write_csv(output_path / "DEP.csv")
    print(f"Saved DEP with {len(dep_deduped)} records")
else:
    dep_deduped = pl.DataFrame()
    print("No DEP datasets loaded")

# DATA UNCLAIM; SET UNCLAIM(DROP=ACCTNO); FORMAT ACCTNO 10.; ACCTNO = PAYMODE;
if not unclaim_final.is_empty():
    unclaim_for_merge = unclaim_final.drop('ACCTNO').with_columns([
        pl.col('PAYMODE').cast(pl.Int64).alias('ACCTNO')
    ])
    print(f"UNCLAIM for merge has {len(unclaim_for_merge)} records")
    
    # DATA DEP; MERGE DEP(IN=A) UNCLAIM(IN=B);
    if not dep_deduped.is_empty():
        dep_merged = dep_deduped.join(unclaim_for_merge, on='ACCTNO', how='right', suffix='_unclaim')
        print(f"Merged dataset has {len(dep_merged)} records")
        
        # BC assignment logic
        dep_with_bc = dep_merged.with_columns([
            pl.when(pl.col('PRODCD').is_not_null() & pl.col('LEDGBAL').is_not_null())
            .then(pl.lit('DEBITTED'))
            .otherwise(pl.lit('NOT_FOUND'))
            .alias('BC')
        ]).filter(pl.col('LEDGBAL').is_not_null())  # IF B THEN OUTPUT;
        print(f"After BC assignment: {len(dep_with_bc)} records")
    else:
        dep_with_bc = unclaim_for_merge.with_columns([
            pl.lit('NOT_FOUND').alias('BC'),
            pl.lit(None).cast(pl.Utf8).alias('PRODCD'),
            pl.lit(None).cast(pl.Int64).alias('COSTCTR')
        ])
        print(f"Using UNCLAIM only: {len(dep_with_bc)} records")
    
    # PROC SORT DATA=DEP; BY CATEGORY;
    if not dep_with_bc.is_empty():
        dep_sorted = dep_with_bc.sort('CATEGORY')
        dep_sorted.write_parquet(output_path / "DEP_FINAL.parquet")
        dep_sorted.write_csv(output_path / "DEP_FINAL.csv")
        print(f"Saved DEP_FINAL with {len(dep_sorted)} records")
        
        # TITLE1 'BANKERS CHEQUE WITH DEBITTED A/C (CONVENTIONAL)';
        print("\n" + "="*80)
        print("BANKERS CHEQUE WITH DEBITTED A/C (CONVENTIONAL)")
        print("="*80)
        
        # WHERE BC = 'DEBITTED';
        debitted_filtered = dep_sorted.filter(pl.col('BC') == 'DEBITTED')
        
        if not debitted_filtered.is_empty():
            # PROC SUMMARY DATA=DEP NWAY MISSING; CLASS CATEGORY; VAR LEDGBAL;
            debitted_summary = debitted_filtered.group_by('CATEGORY').agg([
                pl.col('LEDGBAL').sum().alias('LEDGBAL')
            ])
            
            # PROC PRINT DATA=XXX LABEL; LABEL LEDGBAL = 'BC/DD AMOUNT'; SUM LEDGBAL;
            print("\nBC/DD AMOUNT by Category (DEBITTED):")
            print(debitted_summary)
            total = debitted_summary.select(pl.col('LEDGBAL').sum()).row(0)[0]
            print(f"\nTOTAL BC/DD AMOUNT: {total:,.2f}")
        else:
            print("No debitted records found")
        
        print("\n" + "="*80)
        
        # TITLE1 'BANKERS CHEQUE WITH DEBITTED A/C NOT FOUND IN FISS (CONV&ISLM)';
        print("BANKERS CHEQUE WITH DEBITTED A/C NOT FOUND IN FISS (CONV&ISLM)")
        print("="*80)
        
        # WHERE BC = 'NOT_FOUND';
        notfound_filtered = dep_sorted.filter(pl.col('BC') == 'NOT_FOUND')
        
        if not notfound_filtered.is_empty():
            # PROC SUMMARY DATA=DEP NWAY MISSING; CLASS CATEGORY; VAR LEDGBAL;
            notfound_summary = notfound_filtered.group_by('CATEGORY').agg([
                pl.col('LEDGBAL').sum().alias('LEDGBAL')
            ])
            
            # PROC PRINT DATA=XXX LABEL; LABEL LEDGBAL = 'BC/DD AMOUNT'; SUM LEDGBAL;
            print("\nBC/DD AMOUNT by Category (NOT FOUND):")
            print(notfound_summary)
            total = notfound_summary.select(pl.col('LEDGBAL').sum()).row(0)[0]
            print(f"\nTOTAL BC/DD AMOUNT: {total:,.2f}")
        else:
            print("No not-found records found")
    else:
        print("No data in dep_sorted")
else:
    print("No UNCLAIM_FINAL data available for processing")

# DATA NONDEBIT; SET NONDEBIT; BC = 'NON_DEBIT'; ACCTNO = PAYMODE;
if not nondebit_sorted.is_empty():
    nondebit_processed = nondebit_sorted.with_columns([
        pl.lit('NON_DEBIT').alias('BC'),
        pl.col('PAYMODE').cast(pl.Int64).alias('ACCTNO')
    ])
    nondebit_processed.write_parquet(output_path / "NONDEBIT_PROCESSED.parquet")
    nondebit_processed.write_csv(output_path / "NONDEBIT_PROCESSED.csv")
    print(f"Saved NONDEBIT_PROCESSED with {len(nondebit_processed)} records")
    
    print("\n" + "="*80)
    # TITLE1 'BANKERS CHEQUE WITH NON-DEBITTED A/C';
    print("BANKERS CHEQUE WITH NON-DEBITTED A/C")
    print("="*80)
    
    # PROC SUMMARY DATA=NONDEBIT NWAY MISSING; CLASS CATEGORY; VAR LEDGBAL;
    nondebit_summary = nondebit_processed.group_by('CATEGORY').agg([
        pl.col('LEDGBAL').sum().alias('LEDGBAL')
    ])
    
    # PROC PRINT DATA=XXX LABEL; LABEL LEDGBAL = 'BC/DD AMOUNT'; SUM LEDGBAL;
    print("\nBC/DD AMOUNT by Category (NON-DEBITTED):")
    print(nondebit_summary)
    total = nondebit_summary.select(pl.col('LEDGBAL').sum()).row(0)[0]
    print(f"\nTOTAL BC/DD AMOUNT: {total:,.2f}")
else:
    print("No NONDEBIT data available for processing")

print("\n" + "="*80)
print(f"All output files saved to: {output_path}")
print("PROCESSING COMPLETED SUCCESSFULLY")
print("="*80)
