import polars as pl
import duckdb
from pathlib import Path
import datetime
import pyreadstat
import re

# Configuration
deposit_path = Path("DEPOSIT")
mni_path = Path("MNI")
imni_path = Path("IMNI")
output_path = Path("output")
output_path.mkdir(exist_ok=True)
deposit_path.mkdir(exist_ok=True)

# DATA REPTDATE (KEEP=REPTDATE);
# PRODUCTION DATE - COMMENTED OUT FOR TESTING
# today = datetime.date.today()
# date_string = f"01{today.month:02d}{today.year}"
# reptdate = datetime.datetime.strptime(date_string, '%d%m%Y').date() - datetime.timedelta(days=1)

# TEST DATE - December Week 4 (December 23, 2026)
# For Week 4, the logic sets SDD=23, WK='4', WK1='3', WK2='2', WK3='1'
reptdate = datetime.date(2026, 12, 23)  # December 23, 2026 (Week 4)
print(f"*** TEST MODE - Using date: {reptdate} (December Week 4) ***")

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
print(f"SDATE: {SDATE}")

# Create REPTDATE DataFrame
reptdate_df = pl.DataFrame({'REPTDATE': [reptdate]})
reptdate_df.write_parquet(output_path / "REPTDATE.parquet")
reptdate_df.write_csv(output_path / "REPTDATE.csv")

def clean_text(text):
    """Remove non-printable characters except spaces and newlines"""
    if text is None:
        return ""
    # Keep only printable characters (ASCII 32-126) and newlines
    cleaned = ''.join(ch for ch in text if 31 < ord(ch) < 127 or ch == '\n' or ch == '\r')
    return cleaned.strip()

def parse_remit_file(filepath):
    """Parse fixed-width REMIT file"""
    records = []
    
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                # Clean the line but keep it as is for parsing
                raw_line = line.rstrip('\n\r')
                
                # Skip empty lines
                if not raw_line.strip():
                    continue
                
                # Parse fixed-width fields based on SAS layout
                try:
                    # Extract fields (0-indexed positions)
                    raw = raw_line
                    line_len = len(raw)
                    
                    # Extract fields based on SAS positions
                    acctno_raw = raw[2:9] if line_len > 9 else ""
                    cheqno_raw = raw[8:14] if line_len > 14 else ""
                    issyy_raw = raw[26:30] if line_len > 30 else ""
                    issmm_raw = raw[31:33] if line_len > 33 else ""
                    issdd_raw = raw[34:36] if line_len > 36 else ""
                    issbranch_raw = raw[36:39] if line_len > 39 else ""
                    ledgbal_raw = raw[39:48] if line_len > 48 else ""
                    status_raw = raw[46:48] if line_len > 48 else ""
                    paymode_raw = raw[54:64] if line_len > 64 else ""
                    name_raw = raw[74:114] if line_len > 114 else ""
                    
                    # Clean each field
                    acctno = clean_text(acctno_raw)
                    cheqno = clean_text(cheqno_raw)
                    issyy = clean_text(issyy_raw)
                    issmm = clean_text(issmm_raw)
                    issdd = clean_text(issdd_raw)
                    issbranch = clean_text(issbranch_raw)
                    ledgbal = clean_text(ledgbal_raw)
                    status = clean_text(status_raw)
                    paymode = clean_text(paymode_raw)
                    name = clean_text(name_raw)
                    
                    # Clean the full line for debugging
                    clean_line = clean_text(raw_line)
                    
                    # Try to extract ACCTNO from visible pattern if parsing failed
                    if not acctno or len(acctno) < 6:
                        # Try to find pattern like "N075422" or "075422"
                        match = re.search(r'([0-9]{6,7})', raw)
                        if match:
                            acctno = match.group(1)
                    
                    # Only add if we have at least some data
                    if acctno or name:
                        records.append({
                            'ACCTNO': acctno,
                            'CHEQNO': cheqno,
                            'ISSYY': issyy,
                            'ISSMM': issmm,
                            'ISSDD': issdd,
                            'ISSBRANCH': issbranch,
                            'LEDGBAL': ledgbal,
                            'STATUS': status,
                            'PAYMODE': paymode,
                            'NAME': name,
                            'RAW_LINE': clean_line[:100]  # Store first 100 chars for debugging
                        })
                        
                except Exception as e:
                    print(f"Error parsing line: {e}")
                    continue
    
    except FileNotFoundError:
        print("REMIT file not found")
        return pl.DataFrame()
    except Exception as e:
        print(f"Error reading REMIT file: {e}")
        return pl.DataFrame()
    
    if not records:
        print("No records parsed from REMIT file")
        return pl.DataFrame()
    
    # Create DataFrame
    df = pl.DataFrame(records)
    
    # Convert numeric fields
    df = df.with_columns([
        pl.col('ACCTNO').cast(pl.Int64, strict=False).alias('ACCTNO'),
        pl.col('CHEQNO').cast(pl.Int64, strict=False).alias('CHEQNO'),
        pl.col('ISSBRANCH').cast(pl.Int64, strict=False).alias('ISSBRANCH'),
        pl.col('LEDGBAL').cast(pl.Float64, strict=False).alias('LEDGBAL'),
    ])
    
    # Remove rows with null ACCTNO
    df = df.filter(pl.col('ACCTNO').is_not_null())
    
    print(f"Parsed {df.height} records from REMIT file")
    return df

# DATA DEPOSIT.REMIT(DROP=ISSMM ISSDD ISSYY) NONDEBIT;
# INFILE REMIT;
try:
    # Parse the REMIT fixed-width file
    remit_df = parse_remit_file("REMIT")
    
    if not remit_df.is_empty():
        # Create ISSDTE = MDY(ISSMM,ISSDD,ISSYY)
        remit_with_dates = remit_df.with_columns([
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
    else:
        print("No valid records in REMIT file")
        remit_valid = pl.DataFrame()
        nondebit_invalid = pl.DataFrame()
    
except Exception as e:
    print(f"Error processing REMIT file: {e}")
    remit_valid = pl.DataFrame()
    nondebit_invalid = pl.DataFrame()

# PROC SORT DATA=DEPOSIT.REMIT; BY PAYMODE;
if not remit_valid.is_empty():
    remit_sorted = remit_valid.sort('PAYMODE')
    remit_sorted.write_parquet(deposit_path / "REMIT_sorted.parquet")
    remit_sorted.write_csv(deposit_path / "REMIT_sorted.csv")
else:
    remit_sorted = pl.DataFrame()

# PROC SORT DATA=NONDEBIT; BY PAYMODE NAME;
if not nondebit_invalid.is_empty():
    nondebit_sorted = nondebit_invalid.sort(['PAYMODE', 'NAME'])
    nondebit_sorted.write_parquet(output_path / "NONDEBIT_sorted.parquet")
    nondebit_sorted.write_csv(output_path / "NONDEBIT_sorted.csv")
else:
    nondebit_sorted = pl.DataFrame()

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
# For December Week 4 testing:
# WK='4', REPTMON='12'
savg_filename = f"SAVG{REPTMON}{NOWK}.sas7bdat"  # SAVG124
curn_filename = f"CURN{REPTMON}{NOWK}.sas7bdat"  # CURN124
isavg_filename = f"ISAVG{REPTMON}{NOWK}.sas7bdat"  # ISAVG124
icurn_filename = f"ICURN{REPTMON}{NOWK}.sas7bdat"  # ICURN124

print(f"\nLooking for SAS files with pattern: *{REPTMON}{NOWK}*")
print(f"SAVG: {savg_filename}")
print(f"CURN: {curn_filename}")
print(f"ISAVG: {isavg_filename}")
print(f"ICURN: {icurn_filename}")

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
print("\nReading SAS files...")
savg_df = read_sas_file(mni_path / savg_filename, ['ACCTNO', 'PRODCD', 'COSTCTR'])
if savg_df is not None:
    datasets.append(savg_df)
    savg_df.write_parquet(mni_path / f"SAVG{REPTMON}{NOWK}.parquet")
    savg_df.write_csv(mni_path / f"SAVG{REPTMON}{NOWK}.csv")
    print(f"Loaded SAVG with {savg_df.height} records")

curn_df = read_sas_file(mni_path / curn_filename, ['ACCTNO', 'PRODCD', 'COSTCTR'])
if curn_df is not None:
    datasets.append(curn_df)
    curn_df.write_parquet(mni_path / f"CURN{REPTMON}{NOWK}.parquet")
    curn_df.write_csv(mni_path / f"CURN{REPTMON}{NOWK}.csv")
    print(f"Loaded CURN with {curn_df.height} records")

isavg_df = read_sas_file(imni_path / isavg_filename, ['ACCTNO', 'PRODCD', 'COSTCTR'])
if isavg_df is not None:
    datasets.append(isavg_df)
    isavg_df.write_parquet(imni_path / f"ISAVG{REPTMON}{NOWK}.parquet")
    isavg_df.write_csv(imni_path / f"ISAVG{REPTMON}{NOWK}.csv")
    print(f"Loaded ISAVG with {isavg_df.height} records")

icurn_df = read_sas_file(imni_path / icurn_filename, ['ACCTNO', 'PRODCD', 'COSTCTR'])
if icurn_df is not None:
    datasets.append(icurn_df)
    icurn_df.write_parquet(imni_path / f"ICURN{REPTMON}{NOWK}.parquet")
    icurn_df.write_csv(imni_path / f"ICURN{REPTMON}{NOWK}.csv")
    print(f"Loaded ICURN with {icurn_df.height} records")

# DATA DEP; SET all datasets;
if datasets:
    dep_df = pl.concat(datasets, how="diagonal")
    print(f"\nCombined DEP dataset with {dep_df.height} records")
    
    # IF PRODCD IN specified values;
    valid_prodcd = ['42110', '42310', '42120', '42320', '42130', '42132', '42180', '42199', '42699']
    dep_filtered = dep_df.filter(pl.col('PRODCD').is_in(valid_prodcd))
    print(f"Filtered DEP with valid PRODCD: {dep_filtered.height} records")
    
    # PROC SORT DATA=DEP NODUPKEYS; BY ACCTNO;
    dep_deduped = dep_filtered.unique(subset=['ACCTNO'])
    dep_deduped.write_parquet(output_path / "DEP_deduped.parquet")
    dep_deduped.write_csv(output_path / "DEP_deduped.csv")
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
        
        # BC assignment logic (IF A & B THEN BC='DEBITTED' ELSE BC='NOT_FOUND')
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
    dep_sorted.write_parquet(output_path / "DEP_SORTED.parquet")
    dep_sorted.write_csv(output_path / "DEP_SORTED.csv")
    
    # ============================================================
    # REPORT 1: BANKERS CHEQUE WITH DEBITTED A/C (CONVENTIONAL)
    # ============================================================
    print("\n" + "="*80)
    print("REPORT 1: BANKERS CHEQUE WITH DEBITTED A/C (CONVENTIONAL)")
    print("="*80)
    
    # WHERE BC = 'DEBITTED' AND (COSTCTR < 3000 OR COSTCTR>3999)
    debitted_filtered = dep_sorted.filter(
        (pl.col('BC') == 'DEBITTED') & 
        ((pl.col('COSTCTR') < 3000) | (pl.col('COSTCTR') > 3999))
    )
    
    if not debitted_filtered.is_empty():
        # Summary by CATEGORY
        debitted_summary = debitted_filtered.group_by('CATEGORY').agg([
            pl.col('LEDGBAL').sum().alias('LEDGBAL')
        ]).sort('CATEGORY')
        
        print("\nSummary by Category:")
        print(debitted_summary)
        
        # Calculate grand total
        total_debitted = debitted_summary.select(pl.col('LEDGBAL').sum()).row(0)[0]
        print(f"\nTOTAL BC/DD AMOUNT: {total_debitted:,.2f}")
        
        # Save detailed records
        debitted_filtered.write_parquet(output_path / "DEBITTED_FILTERED.parquet")
        debitted_filtered.write_csv(output_path / "DEBITTED_FILTERED.csv")
        
        # Save summary
        debitted_summary.write_parquet(output_path / "DEBITTED_SUMMARY.parquet")
        debitted_summary.write_csv(output_path / "DEBITTED_SUMMARY.csv")
        
        # Show sample records
        print("\nSample Detailed Records (first 10):")
        sample_records = debitted_filtered.select(['ACCTNO', 'PAYMODE', 'CATEGORY', 'LEDGBAL', 'NAME', 'COSTCTR']).head(10)
        print(sample_records)
    else:
        print("No DEBITTED records found")
    
    # ============================================================
    # REPORT 2: BANKERS CHEQUE WITH DEBITTED A/C NOT FOUND IN FISS (CONV&ISLM)
    # ============================================================
    print("\n" + "="*80)
    print("REPORT 2: BANKERS CHEQUE WITH DEBITTED A/C NOT FOUND IN FISS (CONV&ISLM)")
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
        # Summary by CATEGORY
        notfound_summary = notfound_filtered.group_by('CATEGORY').agg([
            pl.col('LEDGBAL').sum().alias('LEDGBAL')
        ]).sort('CATEGORY')
        
        print("\nSummary by Category:")
        print(notfound_summary)
        
        # Calculate grand total
        total_notfound = notfound_summary.select(pl.col('LEDGBAL').sum()).row(0)[0]
        print(f"\nTOTAL BC/DD AMOUNT: {total_notfound:,.2f}")
        
        # Save detailed records
        notfound_filtered.write_parquet(output_path / "NOTFOUND_FILTERED.parquet")
        notfound_filtered.write_csv(output_path / "NOTFOUND_FILTERED.csv")
        
        # Save summary
        notfound_summary.write_parquet(output_path / "NOTFOUND_SUMMARY.parquet")
        notfound_summary.write_csv(output_path / "NOTFOUND_SUMMARY.csv")
        
        # Show sample records
        print("\nSample Detailed Records (first 10):")
        sample_records = notfound_filtered.select(['ACCTNO', 'PAYMODE', 'CATEGORY', 'LEDGBAL', 'NAME']).head(10)
        print(sample_records)
    else:
        print("No NOT_FOUND records found")

# ============================================================
# REPORT 3: BANKERS CHEQUE WITH NON-DEBITTED A/C
# ============================================================
if not nondebit_invalid.is_empty():
    print("\n" + "="*80)
    print("REPORT 3: BANKERS CHEQUE WITH NON-DEBITTED A/C")
    print("="*80)
    
    # DATA NONDEBIT; SET NONDEBIT; BC = 'NON_DEBIT'; ACCTNO = PAYMODE;
    nondebit_processed = nondebit_invalid.with_columns([
        pl.lit('NON_DEBIT').alias('BC'),
        pl.col('PAYMODE').cast(pl.Int64).alias('ACCTNO')
    ])
    
    # Summary by CATEGORY
    nondebit_summary = nondebit_processed.group_by('CATEGORY').agg([
        pl.col('LEDGBAL').sum().alias('LEDGBAL')
    ]).sort('CATEGORY')
    
    print("\nSummary by Category:")
    print(nondebit_summary)
    
    # Calculate grand total
    total_non_debit = nondebit_summary.select(pl.col('LEDGBAL').sum()).row(0)[0]
    print(f"\nTOTAL BC/DD AMOUNT: {total_non_debit:,.2f}")
    
    # Show sample records
    print("\nSample Detailed Records (first 10):")
    sample_records = nondebit_processed.select(['ACCTNO', 'PAYMODE', 'CATEGORY', 'LEDGBAL', 'NAME']).head(10)
    print(sample_records)
    
    # Save datasets
    nondebit_processed.write_parquet(output_path / "NONDEBIT_PROCESSED.parquet")
    nondebit_processed.write_csv(output_path / "NONDEBIT_PROCESSED.csv")
    nondebit_summary.write_parquet(output_path / "NONDEBIT_SUMMARY.parquet")
    nondebit_summary.write_csv(output_path / "NONDEBIT_SUMMARY.csv")

# ============================================================
# FINAL SUMMARY REPORT
# ============================================================
print("\n" + "="*80)
print("FINAL SUMMARY - ALL REPORTS")
print("="*80)

# Create a consolidated summary
summary_data = []

# Report 1: Debitted
if 'debitted_summary' in locals() and not debitted_summary.is_empty():
    for row in debitted_summary.rows():
        summary_data.append({
            'REPORT': 'DEBITTED A/C (CONVENTIONAL)',
            'CATEGORY': row[0],
            'LEDGBAL': row[1]
        })

# Report 2: Not Found
if 'notfound_summary' in locals() and not notfound_summary.is_empty():
    for row in notfound_summary.rows():
        summary_data.append({
            'REPORT': 'NOT FOUND IN FISS (CONV&ISLM)',
            'CATEGORY': row[0],
            'LEDGBAL': row[1]
        })

# Report 3: Non-Debit
if 'nondebit_summary' in locals() and not nondebit_summary.is_empty():
    for row in nondebit_summary.rows():
        summary_data.append({
            'REPORT': 'NON-DEBITTED A/C',
            'CATEGORY': row[0],
            'LEDGBAL': row[1]
        })

if summary_data:
    final_summary = pl.DataFrame(summary_data)
    print("\nConsolidated Summary:")
    print(final_summary)
    
    # Save consolidated summary
    final_summary.write_parquet(output_path / "FINAL_SUMMARY.parquet")
    final_summary.write_csv(output_path / "FINAL_SUMMARY.csv")
    
    # Grand total across all reports
    grand_total = final_summary.select(pl.col('LEDGBAL').sum()).row(0)[0]
    print(f"\nGRAND TOTAL (ALL REPORTS): {grand_total:,.2f}")

print("\n" + "="*80)
print("PROCESSING COMPLETED SUCCESSFULLY")
print("="*80)

# Print summary of all files created
print("\nOUTPUT FILES CREATED:")
print("\nIn DEPOSIT directory:")
for file in deposit_path.glob("*"):
    print(f"  - {file.name}")

print("\nIn output directory:")
for file in output_path.glob("*"):
    print(f"  - {file.name}")

print("\nIn MNI directory (converted from SAS):")
for file in mni_path.glob("*.parquet"):
    print(f"  - {file.name}")
for file in mni_path.glob("*.csv"):
    print(f"  - {file.name}")

print("\nIn IMNI directory (converted from SAS):")
for file in imni_path.glob("*.parquet"):
    print(f"  - {file.name}")
for file in imni_path.glob("*.csv"):
    print(f"  - {file.name}")

# Print test configuration summary
print("\n" + "="*80)
print("TEST CONFIGURATION SUMMARY")
print("="*80)
print(f"Test Date: {reptdate}")
print(f"Week (WK): {NOWK}")
print(f"Month (MM): {REPTMON}")
print(f"Year: {REPTYEAR}")
print(f"SDD: {SDD}")
print(f"File patterns expected:")
print(f"  - MNI: SAVG{REPTMON}{NOWK}.sas7bdat, CURN{REPTMON}{NOWK}.sas7bdat")
print(f"  - IMNI: ISAVG{REPTMON}{NOWK}.sas7bdat, ICURN{REPTMON}{NOWK}.sas7bdat")
print("="*80)
