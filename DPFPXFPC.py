import polars as pl
import duckdb
from pathlib import Path
import datetime
import pyreadstat 

# Configuration
mni_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/MNI")
imni_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/IMNI")
output_path = Path("/host/mis/output")
output_path.mkdir(exist_ok=True)

# ============================================
# PRODUCTION DATE LOGIC
# ============================================

# DATA REPTDATE (KEEP=REPTDATE);
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

print(f"REPTDATE: {reptdate}")
print(f"NOWK: {NOWK}, REPTMON: {REPTMON}, REPTYEAR: {REPTYEAR}")
print(f"SDESC: {SDESC}")
print(f"SDATE: {SDATE}")

# Create REPTDATE DataFrame
reptdate_df = pl.DataFrame({'REPTDATE': [reptdate]})
reptdate_df.write_parquet(output_path / "REPTDATE.parquet")

# ============================================
# READ IBG_YEAREND.txt FROM MNI PATH
# ============================================
try:
    # Read IBGPIDM file from MNI path with correct filename
    ibg_file_path = mni_path / "IBG_YEAREND.txt"
    
    if not ibg_file_path.exists():
        print(f"ERROR: IBG_YEAREND.txt not found at {ibg_file_path}")
        ibg_valid = pl.DataFrame()
        nondebit_invalid = pl.DataFrame()
    else:
        print(f"Reading IBG file from: {ibg_file_path}")
        
        # Read IBGPIDM file (fixed-width format)
        ibg_df = pl.read_csv(ibg_file_path, 
                            has_header=False,
                            new_columns=['raw_line'])
        
        # Parse fixed-width format:
        # @01 PAYMODE $10., @12 IBGAMT 16.2
        ibg_parsed = ibg_df.with_columns([
            pl.col('raw_line').str.slice(0, 10).str.replace_all(' ', '').alias('PAYMODE'),
            pl.col('raw_line').str.slice(11, 16).str.replace_all(' ', '').cast(pl.Float64).alias('IBGAMT')
        ]).drop('raw_line')
        
        # CATEGORY assignments
        ibg_with_category = ibg_parsed.with_columns([
            pl.when(pl.col('PAYMODE').str.slice(0, 1).is_in(['4', '5', '6']))
            .then(pl.lit('SA'))
            .when(pl.col('PAYMODE').str.slice(0, 1).is_in(['3']))
            .then(pl.lit('CA'))
            .when(pl.col('PAYMODE').str.slice(0, 1).is_in(['1', '7']))
            .then(pl.lit('FD'))
            .otherwise(pl.lit('OTHER'))
            .alias('CATEGORY')
        ])
        
        # Split into IBG and NONDEBIT based on PAYMODE
        valid_paymodes = ['1', '2', '3', '4', '5', '6', '7', '8', '9']
        
        ibg_valid = ibg_with_category.filter(
            pl.col('PAYMODE').str.slice(0, 1).is_in(valid_paymodes)
        )
        
        nondebit_invalid = ibg_with_category.filter(
            ~pl.col('PAYMODE').str.slice(0, 1).is_in(valid_paymodes)
        )
        
        print(f"IBG records: {ibg_valid.height}")
        print(f"NONDEBIT records: {nondebit_invalid.height}")
        
except Exception as e:
    print(f"Error reading IBG_YEAREND.txt: {e}")
    import traceback
    traceback.print_exc()
    ibg_valid = pl.DataFrame()
    nondebit_invalid = pl.DataFrame()

# PROC SORT DATA=IBG; BY PAYMODE;
if not ibg_valid.is_empty():
    ibg_sorted = ibg_valid.sort('PAYMODE')
    ibg_sorted.write_parquet(output_path / "IBG.parquet")
else:
    ibg_sorted = pl.DataFrame()

# PROC SORT DATA=NONDEBIT; BY PAYMODE;
if not nondebit_invalid.is_empty():
    nondebit_sorted = nondebit_invalid.sort('PAYMODE')
    nondebit_sorted.write_parquet(output_path / "NONDEBIT.parquet")
else:
    nondebit_sorted = pl.DataFrame()

# PROC SUMMARY DATA=IBG; BY PAYMODE; VAR IBGAMT;
if not ibg_sorted.is_empty():
    ibg_summary = ibg_sorted.group_by('PAYMODE').agg([
        pl.col('IBGAMT').sum().alias('IBGAMT_sum')
    ])
    
    # OUTPUT OUT=IBGX(DROP=_FREQ_ _TYPE_) SUM=;
    ibg_summary_clean = ibg_summary.rename({'IBGAMT_sum': 'IBGAMT'})
    
    # DATA IBG; MERGE IBGX(IN=A) IBG (IN=B DROP=IBGAMT);
    ibg_deduped = ibg_sorted.unique(subset=['PAYMODE']).drop('IBGAMT')
    ibg_merged = ibg_deduped.join(ibg_summary_clean, on='PAYMODE', how='inner')
    
    # PROC SORT DATA=IBG NODUPKEYS; BY PAYMODE;
    ibg_final = ibg_merged.unique(subset=['PAYMODE'])
    ibg_final.write_parquet(output_path / "IBG_FINAL.parquet")
else:
    ibg_final = pl.DataFrame()

# ============================================
# READ SAS FILES
# ============================================
def read_sas_file(filepath, columns=None):
    """Read SAS file and return Polars DataFrame with selected columns"""
    try:
        if columns:
            # Try different parameter names for different pyreadstat versions
            try:
                # Try with column_names parameter (newer versions)
                df, meta = pyreadstat.read_sas7bdat(filepath, column_names=columns)
            except TypeError:
                # Try with columns parameter (older versions)
                try:
                    df, meta = pyreadstat.read_sas7bdat(filepath, columns=columns)
                except TypeError:
                    # Read all columns then select
                    df, meta = pyreadstat.read_sas7bdat(filepath)
                    # Filter to only columns that exist
                    existing_cols = [col for col in columns if col in df.columns]
                    if existing_cols:
                        df = df[existing_cols]
        else:
            df, meta = pyreadstat.read_sas7bdat(filepath)
        
        # Convert to Polars DataFrame
        return pl.DataFrame(df)
        
    except FileNotFoundError:
        print(f"NOTE: {filepath.name} not found")
        return None
    except Exception as e:
        print(f"Error reading {filepath.name}: {e}")
        return None

# Load additional datasets from SAS files
savg_filename = f"savg{REPTMON}{NOWK}.sas7bdat"
curn_filename = f"curn{REPTMON}{NOWK}.sas7bdat"
isavg_filename = f"savg{REPTMON}{NOWK}.sas7bdat"
icurn_filename = f"curn{REPTMON}{NOWK}.sas7bdat"

print(f"\nLooking for SAS files with pattern: {REPTMON}{NOWK}")
print(f"Expected files: savg{REPTMON}{NOWK}.sas7bdat, curn{REPTMON}{NOWK}.sas7bdat")

datasets = []

# Try reading SAVG
savg_df = read_sas_file(mni_path / savg_filename, ['ACCTNO', 'PRODCD', 'COSTCTR'])
if savg_df is not None and not savg_df.is_empty():
    # Ensure ACCTNO is integer type for consistency
    savg_df = savg_df.with_columns([
        pl.col('ACCTNO').cast(pl.Int64)
    ])
    datasets.append(savg_df)
    print(f"SAVG records: {savg_df.height}")

# Try reading CURN
curn_df = read_sas_file(mni_path / curn_filename, ['ACCTNO', 'PRODCD', 'COSTCTR'])
if curn_df is not None and not curn_df.is_empty():
    # Ensure ACCTNO is integer type for consistency
    curn_df = curn_df.with_columns([
        pl.col('ACCTNO').cast(pl.Int64)
    ])
    datasets.append(curn_df)
    print(f"CURN records: {curn_df.height}")

# Try reading ISAVG
isavg_df = read_sas_file(imni_path / isavg_filename, ['ACCTNO', 'PRODCD', 'COSTCTR'])
if isavg_df is not None and not isavg_df.is_empty():
    # Ensure ACCTNO is integer type for consistency
    isavg_df = isavg_df.with_columns([
        pl.col('ACCTNO').cast(pl.Int64)
    ])
    datasets.append(isavg_df)
    print(f"ISAVG records: {isavg_df.height}")

# Try reading ICURN
icurn_df = read_sas_file(imni_path / icurn_filename, ['ACCTNO', 'PRODCD', 'COSTCTR'])
if icurn_df is not None and not icurn_df.is_empty():
    # Ensure ACCTNO is integer type for consistency
    icurn_df = icurn_df.with_columns([
        pl.col('ACCTNO').cast(pl.Int64)
    ])
    datasets.append(icurn_df)
    print(f"ICURN records: {icurn_df.height}")

# DATA DEP; SET all datasets;
if datasets:
    dep_df = pl.concat(datasets, how="diagonal")
    
    # IF PRODCD IN specified values;
    valid_prodcd = ['42110', '42310', '42120', '42320', '42130', '42132', '42180', '42199', '42699']
    dep_filtered = dep_df.filter(pl.col('PRODCD').is_in(valid_prodcd))
    
    # PROC SORT DATA=DEP NODUPKEYS; BY ACCTNO;
    dep_deduped = dep_filtered.unique(subset=['ACCTNO'])
    dep_deduped.write_parquet(output_path / "DEP.parquet")
    print(f"DEP records: {dep_deduped.height}")
else:
    dep_deduped = pl.DataFrame()
    print("No DEP data created")

# DATA IBG; SET IBG; FORMAT ACCTNO 10.; ACCTNO = PAYMODE;
if not ibg_final.is_empty():
    # Ensure ACCTNO is integer type for consistency with DEP
    ibg_for_merge = ibg_final.with_columns([
        pl.col('PAYMODE').cast(pl.Int64).alias('ACCTNO')
    ])
    
    # DATA DEP; MERGE DEP(IN=A) IBG(IN=B);
    if not dep_deduped.is_empty():
        # Cast ACCTNO in dep_deduped to Int64 if it's not already
        if dep_deduped['ACCTNO'].dtype != pl.Int64:
            dep_deduped = dep_deduped.with_columns([
                pl.col('ACCTNO').cast(pl.Int64)
            ])
        
        # Perform the join - both sides should now be Int64
        dep_merged = dep_deduped.join(ibg_for_merge, on='ACCTNO', how='right', suffix='_ibg')
        
        # BC assignment logic
        dep_with_bc = dep_merged.with_columns([
            pl.when(pl.col('PRODCD').is_not_null() & pl.col('IBGAMT').is_not_null())
            .then(pl.lit('DEBITTED'))
            .otherwise(pl.lit('NOT_FOUND'))
            .alias('BC')
        ]).filter(pl.col('IBGAMT').is_not_null())  # IF B THEN OUTPUT;
    else:
        dep_with_bc = ibg_for_merge.with_columns([
            pl.lit('NOT_FOUND').alias('BC'),
            pl.lit(None).cast(pl.Utf8).alias('PRODCD'),
            pl.lit(None).cast(pl.Int64).alias('COSTCTR')
        ])
    
    # PROC SORT DATA=DEP; BY CATEGORY;
    dep_sorted = dep_with_bc.sort('CATEGORY')
    dep_sorted.write_parquet(output_path / "DEP_FINAL.parquet")
    
    # TITLE1 'DEBITTED A/C';
    print("\n" + "="*50)
    print("DEBITTED A/C")
    print("="*50)
    
    # PROC SUMMARY DATA=DEP NWAY MISSING; WHERE BC = 'DEBITTED';
    debitted_filtered = dep_sorted.filter(pl.col('BC') == 'DEBITTED')
    
    if not debitted_filtered.is_empty():
        debitted_summary = debitted_filtered.group_by('CATEGORY').agg([
            pl.col('IBGAMT').sum().alias('IBGAMT'),
            pl.len().alias('_FREQ_')
        ]).with_columns([
            pl.lit(1).alias('_TYPE_')
        ])
        
        # PROC PRINT DATA=XXX LABEL; SUM IBGAMT;
        print("IBG AMOUNT by Category (Debitted):")
        print(debitted_summary)
        total = debitted_summary.select(pl.col('IBGAMT').sum()).row(0)[0]
        print(f"TOTAL IBG AMOUNT: {total:,.2f}")
        
        # Save summary
        debitted_summary.write_parquet(output_path / "DEBITTED_SUMMARY.parquet")
    else:
        print("No debitted records found")
    
    print("\n" + "="*50)
    # TITLE1 'DEBITTED A/C NOT FOUND IN FISS';
    print("DEBITTED A/C NOT FOUND IN FISS")
    print("="*50)
    
    # PROC SUMMARY DATA=DEP NWAY MISSING; WHERE BC = 'NOT_FOUND';
    notfound_filtered = dep_sorted.filter(pl.col('BC') == 'NOT_FOUND')
    
    if not notfound_filtered.is_empty():
        notfound_summary = notfound_filtered.group_by('CATEGORY').agg([
            pl.col('IBGAMT').sum().alias('IBGAMT'),
            pl.len().alias('_FREQ_')
        ]).with_columns([
            pl.lit(1).alias('_TYPE_')
        ])
        
        # PROC PRINT DATA=XXX LABEL; SUM IBGAMT;
        print("IBG AMOUNT by Category (Not Found in FISS):")
        print(notfound_summary)
        total = notfound_summary.select(pl.col('IBGAMT').sum()).row(0)[0]
        print(f"TOTAL IBG AMOUNT: {total:,.2f}")
        
        # Save summary
        notfound_summary.write_parquet(output_path / "NOTFOUND_SUMMARY.parquet")
    else:
        print("No not-found records found")

# DATA NONDEBIT; SET NONDEBIT; BC = 'NON_DEBIT'; ACCTNO = PAYMODE;
if not nondebit_sorted.is_empty():
    nondebit_processed = nondebit_sorted.with_columns([
        pl.lit('NON_DEBIT').alias('BC'),
        pl.col('PAYMODE').cast(pl.Int64).alias('ACCTNO')
    ])
    nondebit_processed.write_parquet(output_path / "NONDEBIT_PROCESSED.parquet")
    
    print("\n" + "="*50)
    # TITLE1 'NON-DEBITTED A/C';
    print("NON-DEBITTED A/C")
    print("="*50)
    
    # PROC SUMMARY DATA=NONDEBIT NWAY MISSING; CLASS CATEGORY; VAR IBGAMT;
    nondebit_summary = nondebit_processed.group_by('CATEGORY').agg([
        pl.col('IBGAMT').sum().alias('BC/DD AMOUNT'),
        pl.len().alias('_FREQ_')
    ]).with_columns([
        pl.lit(1).alias('_TYPE_')
    ])
    
    # PROC PRINT DATA=XXX LABEL; LABEL IBGAMT = 'BC/DD AMOUNT'; SUM IBGAMT;
    print("IBG AMOUNT by Category (Non-Debitted):")
    print(nondebit_summary)
    total = nondebit_summary.select(pl.col('BC/DD AMOUNT').sum()).row(0)[0]
    print(f"TOTAL IBG AMOUNT: {total:,.2f}")
    
    # Save summary
    nondebit_summary.write_parquet(output_path / "NONDEBIT_SUMMARY.parquet")

# Generate text report matching production output format exactly
print("\n" + "="*50)
print("GENERATING TEXT REPORT")
print("="*50)

# Get current timestamp for report header
current_time = datetime.datetime.now().strftime("%H:%M %A, %B %d, %Y")

with open(output_path / "report.txt", 'w') as f:
    # Write header for first section
    f.write(f"DEBITTED A/C                                                                                       {current_time}   1\n")
    f.write(" \n")
    f.write(" \n")
    f.write("Obs    CATEGORY    _TYPE_    _FREQ_       IBGAMT\n")
    f.write(" \n")
    
    # Write debitted summary
    if Path(output_path / "DEBITTED_SUMMARY.parquet").exists():
        deb_summary = pl.read_parquet(output_path / "DEBITTED_SUMMARY.parquet")
        # Sort by CATEGORY for consistent output
        deb_summary = deb_summary.sort('CATEGORY')
        
        obs_num = 1
        for row in deb_summary.iter_rows():
            f.write(f"  {obs_num:<4} {row[0]:<8} {row[2]:<7} {row[3]:<8} {row[1]:>15,.2f}\n")
            obs_num += 1
        
        total = deb_summary.select(pl.col('IBGAMT').sum()).row(0)[0]
        total_str = f"{total:,.2f}"
        f.write(f"                                     {'=' * 11}\n")
        f.write(f"                                 {total_str:>15}\n")
    
    # Write second section header
    f.write(f"DEBITTED A/C NOT FOUND IN FISS                                                                     {current_time}   2\n")
    f.write(" \n")
    f.write(" \n")
    f.write("Obs    CATEGORY    _TYPE_    _FREQ_      IBGAMT\n")
    f.write(" \n")
    
    # Write not found summary
    if Path(output_path / "NOTFOUND_SUMMARY.parquet").exists():
        nf_summary = pl.read_parquet(output_path / "NOTFOUND_SUMMARY.parquet")
        # Sort by CATEGORY for consistent output
        nf_summary = nf_summary.sort('CATEGORY')
        
        obs_num = 1
        for row in nf_summary.iter_rows():
            if row[0] == '':  # Blank category
                f.write(f"  {obs_num:<4} {row[0]:<9} {row[2]:<7} {row[3]:<8} {row[1]:>15,.2f}\n")
            else:
                f.write(f"  {obs_num:<4} {row[0]:<8} {row[2]:<7} {row[3]:<8} {row[1]:>15,.2f}\n")
            obs_num += 1
        
        total = nf_summary.select(pl.col('IBGAMT').sum()).row(0)[0]
        total_str = f"{total:,.2f}"
        f.write(f"                                     {'=' * 11}\n")
        f.write(f"                                 {total_str:>15}\n")
    
    # Write third section header
    f.write(f"NON-DEBITTED A/C                                                                                   {current_time}   3\n")
    f.write(" \n")
    f.write("                                         BC/DD\n")
    f.write("Obs    CATEGORY    _TYPE_    _FREQ_      AMOUNT\n")
    f.write(" \n")
    
    # Write non-debitted summary
    if Path(output_path / "NONDEBIT_SUMMARY.parquet").exists():
        nd_summary = pl.read_parquet(output_path / "NONDEBIT_SUMMARY.parquet")
        # Sort by CATEGORY for consistent output
        nd_summary = nd_summary.sort('CATEGORY')
        
        obs_num = 1
        for row in nd_summary.iter_rows():
            if row[0] == '':  # Blank category
                f.write(f"  {obs_num:<4} {row[0]:<9} {row[2]:<7} {row[3]:<8} {row[1]:>15,.2f}\n")
            else:
                f.write(f"  {obs_num:<4} {row[0]:<8} {row[2]:<7} {row[3]:<8} {row[1]:>15,.2f}\n")
            obs_num += 1
        
        total = nd_summary.select(pl.col('BC/DD AMOUNT').sum()).row(0)[0]
        total_str = f"{total:,.2f}"
        f.write(f"                                     {'=' * 11}\n")
        f.write(f"                                 {total_str:>15}\n")

print(f"Report generated: {output_path / 'report.txt'}")
print("\nPROCESSING COMPLETED SUCCESSFULLY")
