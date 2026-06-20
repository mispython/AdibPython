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
                # Based on the sample, fields appear to be:
                # Pos 0-1: Control characters (skip)
                # Pos 2: Maybe 'N' or other identifier
                # Pos 3-8: ACCTNO? (6 digits)
                # Pos 9-20: Other fields
                # Let's try to parse based on visible patterns
                
                try:
                    # Extract based on visible patterns in your sample
                    # The sample shows: "N075422          A 2025-07-22"
                    # ACCTNO appears to be 6 digits starting after 'N'
                    
                    # First, clean the line for parsing
                    clean_line = clean_text(raw_line)
                    
                    # Extract fields based on fixed positions from your SAS code
                    # @003 ACCTNO PD6. (positions 3-9 in SAS 1-indexed)
                    # @009 CHEQNO 6. (positions 9-15)
                    # @027 ISSYY $4. (positions 27-31)
                    # @032 ISSMM $2. (positions 32-34)
                    # @035 ISSDD $2. (positions 35-37)
                    # @037 ISSBRANCH PD3. (positions 37-40)
                    # @040 LEDGBAL PD7.2 (positions 40-49)
                    # @047 STATUS $2. (positions 47-49)
                    # @055 PAYMODE $10. (positions 55-65)
                    # @075 NAME $40. (positions 75-115)
                    
                    # But since there are control characters, we need to handle carefully
                    # Let's use the raw_line with control characters preserved for position
                    # and then clean each field
                    
                    # Get the raw line with control characters
                    raw = raw_line
                    line_len = len(raw)
                    
                    # Extract fields (0-indexed positions)
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
                    
                    # Try to extract ACCTNO from visible pattern if parsing failed
                    if not acctno or len(acctno) < 6:
                        # Try to find pattern like "N075422" or "075422"
                        import re
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
    remit_sorted.write_parquet(deposit_path / "REMIT.parquet")
    remit_sorted.write_csv(deposit_path / "REMIT_sorted.csv")
else:
    remit_sorted = pl.DataFrame()

# PROC SORT DATA=NONDEBIT; BY PAYMODE NAME;
if not nondebit_invalid.is_empty():
    nondebit_sorted = nondebit_invalid.sort(['PAYMODE', 'NAME'])
    nondebit_sorted.write_parquet(output_path / "NONDEBIT.parquet")
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
print("\nReading SAS files...")
savg_df = read_sas_file(mni_path / savg_filename, ['ACCTNO', 'PRODCD', 'COSTCTR'])
if savg_df is not None:
    datasets.append(savg_df)
    savg_df.write_parquet(mni_path / f"SAVG{REPTMON}{NOWK}.parquet")
    print(f"Loaded SAVG with {savg_df.height} records")

curn_df = read_sas_file(mni_path / curn_filename, ['ACCTNO', 'PRODCD', 'COSTCTR'])
if curn_df is not None:
    datasets.append(curn_df)
    curn_df.write_parquet(mni_path / f"CURN{REPTMON}{NOWK}.parquet")
    print(f"Loaded CURN with {curn_df.height} records")

isavg_df = read_sas_file(imni_path / isavg_filename, ['ACCTNO', 'PRODCD', 'COSTCTR'])
if isavg_df is not None:
    datasets.append(isavg_df)
    isavg_df.write_parquet(imni_path / f"ISAVG{REPTMON}{NOWK}.parquet")
    print(f"Loaded ISAVG with {isavg_df.height} records")

icurn_df = read_sas_file(imni_path / icurn_filename, ['ACCTNO', 'PRODCD', 'COSTCTR'])
if icurn_df is not None:
    datasets.append(icurn_df)
    icurn_df.write_parquet(imni_path / f"ICURN{REPTMON}{NOWK}.parquet")
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
        print("\nSample Detailed Records (first 10):")
        sample_records = debitted_filtered.select(['ACCTNO', 'PAYMODE', 'CATEGORY', 'LEDGBAL', 'NAME', 'COSTCTR']).head(10)
        print(sample_records)
        
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
        print("\nSample Detailed Records (first 10):")
        sample_records = notfound_filtered.select(['ACCTNO', 'PAYMODE', 'CATEGORY', 'LEDGBAL', 'NAME']).head(10)
        print(sample_records)
        
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
    print("\nSample Detailed Records (first 10):")
    sample_records = nondebit_processed.select(['ACCTNO', 'PAYMODE', 'CATEGORY', 'LEDGBAL', 'NAME']).head(10)
    print(sample_records)
    
    total = nondebit_summary.select(pl.col('LEDGBAL').sum()).row(0)[0]
    print(f"\nTOTAL BC/DD AMOUNT: {total:,.2f}")
    
    nondebit_processed.write_parquet(output_path / "NONDEBIT_PROCESSED.parquet")
    nondebit_processed.write_csv(output_path / "NONDEBIT_PROCESSED.csv")

print("\n" + "="*80)
print("PROCESSING COMPLETED SUCCESSFULLY")
print("="*80)

# Print summary of all files created
print("\nOutput files created:")
for file in output_path.glob("*"):
    print(f"  - {file.name}")
for file in deposit_path.glob("*"):
    print(f"  - {file.name}")
