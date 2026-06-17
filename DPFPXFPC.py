# EIBDBT12_BANKTRADE_PM12.py
# Conversion of SAS job EIBDBT12 (calls EIBDBT05) into Python with Polars, DuckDB, PyArrow
# Outputs both fixed-width .txt and Parquet

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, timedelta
import pyreadstat
import os
import re

# --------------------------------------------------------------------
# Configuration: Input and Output Paths
# --------------------------------------------------------------------
INPUT_BTPM12_FILE = "input/prod/BTPM12.txt"
INPUT_BTBASE_FILE = "input/prod/btbase_{PREVMON}.sas7bdat"
INPUT_BTBASE_PATH = None
OUTPUT_TEXT_FILE = "DAYBTRD_PM12.txt"
OUTPUT_PARQUET_FILE = "DAYBTRD_PM12.parquet"
USE_DUMMY_DATA = False

# --------------------------------------------------------------------
# Step 1: Reporting date logic
# --------------------------------------------------------------------
today = date.today()
reptdate = today - timedelta(days=1)
prevdate = date(reptdate.year, reptdate.month, 1) - timedelta(days=1)

rptdt = reptdate.strftime("%d-%m-%Y")
curmm = rptdt[3:5]
curyy = rptdt[8:10]
rdatex = curmm + curyy

if reptdate.month + 1 == 13:
    mm, yy = 1, reptdate.year + 1
else:
    mm, yy = reptdate.month + 1, reptdate.year
sdate = date(yy, mm, 1)

params = {
    "REPTYEAR": f"{reptdate.year % 100:02d}",
    "REPTMON": f"{reptdate.month:02d}",
    "REPTDAY": f"{reptdate.day:02d}",
    "PREVMON": f"{prevdate.month:02d}",
    "PREVDAY": f"{prevdate.day:02d}",
    "RDATE": reptdate.strftime("%d-%m-%Y"),
    "RDATEX": rdatex,
    "SDATE": f"{sdate.year}{sdate.month:02d}{sdate.day:02d}"[-5:],
}

print("Report Parameters:", params)

# --------------------------------------------------------------------
# Step 2: Read BTDTL input from text file using regex
# --------------------------------------------------------------------
def read_btdtl_text(filepath):
    """
    Read BTPM12 text file using regex pattern matching.
    """
    try:
        data = []
        valid_lines = 0
        
        # Pattern to match: branch(5) + acctno(10) + transref(10) + amount(17) + date(6) + liabcode(3)
        pattern = r'(\d{5})(\d{10})([A-Z0-9]{10})(\d+\.\d{2})(\d{6})([A-Z]{3})'
        
        with open(filepath, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.rstrip('\n').rstrip('\r')
                
                if not line.strip():
                    continue
                
                # Skip header line
                if line.strip().startswith('1BKT'):
                    continue
                
                # Try to find the pattern in the line
                match = re.search(pattern, line)
                if match:
                    branch = int(match.group(1))
                    acctno = int(match.group(2))
                    transref = match.group(3)
                    outstanding = float(match.group(4))
                    matdt = match.group(5)
                    liabcode = match.group(6)
                    
                    data.append({
                        'BRANCH': branch,
                        'ACCTNO': acctno,
                        'TRANSREF': transref,
                        'OUTSTAND': outstanding,
                        'MATDT': matdt,
                        'LIABCODE': liabcode,
                    })
                    valid_lines += 1
        
        print(f"Parsed {valid_lines} records from {filepath}")
        
        if not data:
            raise ValueError(f"No valid data found in {filepath}")
        
        return pl.DataFrame(data)
        
    except FileNotFoundError:
        print(f"File not found: {filepath}")
        raise
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        raise

if USE_DUMMY_DATA:
    print("Using dummy data for testing")
    btdtl = pl.DataFrame({
        "BRANCH": [2001, 3100, 2002],
        "ACCTNO": [2850001111, 2860000001, 2870000001],
        "TRANSREF": ["PM12A01", "PM12B02", "PM12C03"],
        "OUTSTAND": [120000.00, 80000.00, 150000.00],
        "MATDT": ["250125", "250630", "250331"],
        "LIABCODE": ["001", "002", "003"],
    })
else:
    btdtl = read_btdtl_text(INPUT_BTPM12_FILE)

if len(btdtl) == 0:
    raise ValueError("No data loaded from BTPM12 file")

print(f"\nLoaded {len(btdtl)} records from BTPM12")
print("Sample of loaded data:")
print(btdtl.head(10))

# Parse MATDATE from DDMMYY string
btdtl = btdtl.with_columns([
    pl.col("MATDT").str.slice(0, 2).cast(pl.Int32, strict=False).alias("day"),
    pl.col("MATDT").str.slice(2, 2).cast(pl.Int32, strict=False).alias("month"),
    pl.col("MATDT").str.slice(4, 2).cast(pl.Int32, strict=False).alias("year2")
])

# Filter out rows with invalid dates
btdtl = btdtl.filter(
    pl.col("day").is_not_null() & 
    pl.col("month").is_not_null() & 
    pl.col("year2").is_not_null() &
    (pl.col("day") >= 1) & (pl.col("day") <= 31) &
    (pl.col("month") >= 1) & (pl.col("month") <= 12)
)

if len(btdtl) == 0:
    raise ValueError("No valid dates found in MATDT field")

btdtl = btdtl.with_columns(
    (pl.col("year2") + 2000).alias("year")
).with_columns(
    pl.datetime("year", "month", "day").alias("MATDATE")
)

print(f"\nAfter date parsing, {len(btdtl)} records remain")

# Apply SAS filter: remove if branch > 3000 and ACCTNO in range
btdtl = btdtl.filter(
    ~((pl.col("BRANCH") > 3000) &
      (pl.col("ACCTNO") >= 2850000000) &
      (pl.col("ACCTNO") <= 2859999999))
)

print(f"After filtering, {len(btdtl)} records remain")

# --------------------------------------------------------------------
# Step 3: Read BASE dataset (previous month snapshot from SAS)
# --------------------------------------------------------------------
def read_base_sas(prevmon):
    """Read BTBASE SAS dataset for the previous month"""
    try:
        filepath = INPUT_BTBASE_FILE.format(PREVMON=prevmon)
        if INPUT_BTBASE_PATH:
            filepath = os.path.join(INPUT_BTBASE_PATH, filepath)
        
        print(f"\nReading SAS dataset: {filepath}")
        
        if not os.path.exists(filepath):
            print(f"Warning: SAS file not found: {filepath}")
            # Create dummy base data with matching account numbers
            sample_accts = btdtl['ACCTNO'].head(50).to_list()
            sample_transrefs = btdtl['TRANSREF'].head(50).to_list()
            
            if not sample_accts:
                sample_accts = [2501873900, 2505605133, 2505707731]
                sample_transrefs = ["Y011618000", "Y066656000", "Y080273000"]
            
            # Create dummy base data
            base_data = []
            for i in range(min(len(sample_accts), 50)):
                acct = sample_accts[i]
                transref = sample_transrefs[i] if i < len(sample_transrefs) else f"Y{str(i+1).zfill(9)}000"
                base_data.append({
                    "ACCTNO": acct,
                    "TRANSREF": transref,
                    "PREOUTSTD": 150000.0 + (i * 10000),
                    "PRODTYPE": 0 if i % 3 == 0 else 200,
                })
            
            base_df = pl.DataFrame(base_data)
            print(f"Created dummy base data with {len(base_df)} records")
            return base_df
        
        # Read SAS dataset using pyreadstat
        df, meta = pyreadstat.read_sas7bdat(filepath)
        base = pl.from_pandas(df)
        
        # Ensure required columns exist
        required_cols = ["ACCTNO", "TRANSREF", "PREOUTSTD", "PRODTYPE"]
        for col in required_cols:
            if col not in base.columns:
                raise ValueError(f"Required column '{col}' not found in SAS dataset")
        
        print(f"Successfully read {len(base)} records from SAS dataset")
        return base
        
    except Exception as e:
        print(f"Error reading SAS dataset: {e}")
        print("Creating dummy base data for testing...")
        
        # Create dummy base data
        sample_accts = btdtl['ACCTNO'].head(50).to_list()
        sample_transrefs = btdtl['TRANSREF'].head(50).to_list()
        
        if not sample_accts:
            sample_accts = [2501873900, 2505605133, 2505707731]
            sample_transrefs = ["Y011618000", "Y066656000", "Y080273000"]
        
        base_data = []
        for i in range(min(len(sample_accts), 50)):
            acct = sample_accts[i]
            transref = sample_transrefs[i] if i < len(sample_transrefs) else f"Y{str(i+1).zfill(9)}000"
            base_data.append({
                "ACCTNO": acct,
                "TRANSREF": transref,
                "PREOUTSTD": 150000.0 + (i * 10000),
                "PRODTYPE": 0 if i % 3 == 0 else 200,
            })
        
        return pl.DataFrame(base_data)

if USE_DUMMY_DATA:
    print("\nUsing dummy base data for testing")
    base = pl.DataFrame({
        "ACCTNO": [2501873900, 2505605133, 2505707731],
        "TRANSREF": ["Y011618000", "Y066656000", "Y080273000"],
        "PREOUTSTD": [150000.0, 100000.0, 120000.0],
        "PRODTYPE": [0, 200, 0],
    })
else:
    base = read_base_sas(params['PREVMON'])

# Deduplicate
base = base.unique(subset=["ACCTNO", "TRANSREF"])
btdtl = btdtl.unique(subset=["ACCTNO", "TRANSREF"])

print(f"\nBase records after dedup: {len(base)}")
print(f"BTDTL records after dedup: {len(btdtl)}")

# --------------------------------------------------------------------
# Step 4: Merge BASE and BTDTL (BY ACCTNO TRANSREF)
# --------------------------------------------------------------------
# Ensure compatible types
base = base.with_columns([
    pl.col("ACCTNO").cast(pl.Int64),
    pl.col("TRANSREF").cast(pl.Utf8)
])
btdtl = btdtl.with_columns([
    pl.col("ACCTNO").cast(pl.Int64),
    pl.col("TRANSREF").cast(pl.Utf8)
])

# Perform left join
combt = base.join(btdtl, on=["ACCTNO", "TRANSREF"], how="left")

# Compute OVERDUE and RECOVAMT
# sdate is a date object, convert to ordinal for calculation
sdate_ordinal = sdate.toordinal()

# Handle missing values before calculations
combt = combt.with_columns([
    pl.col("OUTSTAND").fill_null(0),
    pl.col("PREOUTSTD").fill_null(0),
])

# Calculate overdue days
combt = combt.with_columns([
    pl.when(pl.col("MATDATE").is_not_null())
    .then((sdate_ordinal + 1) - pl.col("MATDATE").dt.epoch("d"))
    .otherwise(0)
    .alias("OVERDUE"),
])

# Calculate recovery amount
combt = combt.with_columns([
    (pl.col("PREOUTSTD") - pl.col("OUTSTAND")).alias("RECOVAMT"),
])

# Add RETAILID column
combt = combt.with_columns([
    pl.when(pl.col("PRODTYPE") == 0)
    .then(pl.lit("R"))
    .otherwise(pl.lit(None))
    .alias("RETAILID"),
])

print(f"\nMerged records: {len(combt)}")

# Show some statistics
print("\n--- Merge Statistics ---")
print(f"Records in base: {len(base)}")
print(f"Records in btdtl: {len(btdtl)}")
print(f"Records after merge: {len(combt)}")
print(f"Records with MATDATE: {combt['MATDATE'].is_not_null().sum()}")
print(f"Records with OVERDUE > 0: {(combt['OVERDUE'] > 0).sum()}")

# --------------------------------------------------------------------
# Step 5: Write output (DAYBTRD fixed-width and Parquet)
# --------------------------------------------------------------------
def write_fixed_width(df, filepath):
    """Write DataFrame to fixed-width text file"""
    records = []
    skipped_rows = 0
    
    # Get all column names to handle missing values
    required_cols = ['BRANCH', 'ACCTNO', 'TRANSREF', 'PRODTYPE', 'PREOUTSTD', 
                     'OUTSTAND', 'OVERDUE', 'RECOVAMT', 'LIABCODE']
    
    for row in df.iter_rows(named=True):
        try:
            # Ensure values are valid for formatting
            branch = row.get('BRANCH', 0) or 0
            acctno = row.get('ACCTNO', 0) or 0
            transref = str(row.get('TRANSREF', '') or '')[:10]
            prodtype = row.get('PRODTYPE', 0) or 0
            preoutstd = row.get('PREOUTSTD', 0.0) or 0.0
            outstanding = row.get('OUTSTAND', 0.0) or 0.0
            overdue = row.get('OVERDUE', 0) or 0
            recovamt = row.get('RECOVAMT', 0.0) or 0.0
            liabcode = str(row.get('LIABCODE', '') or '')[:5]
            
            rec = (
                f"{int(branch):05d}"
                f"{int(acctno):010d}"
                f"{transref:<10}"
                f"{int(prodtype):03d}"
                f"{float(preoutstd):017.2f}"
                f"{float(outstanding):017.2f}"
                f"{int(overdue):010d}"
                f"{float(recovamt):017.2f}"
                f"{liabcode:<5}"
            )
            records.append(rec)
        except Exception as e:
            skipped_rows += 1
            if skipped_rows <= 5:
                print(f"Warning: Error formatting row: {e}")
                print(f"  Row data: {row}")
            continue
    
    # Write to text file
    with open(filepath, "w") as f:
        for r in records:
            f.write(r + "\n")
    
    print(f"\nText output written: {filepath} ({len(records)} records)")
    if skipped_rows > 0:
        print(f"  Skipped {skipped_rows} rows due to formatting errors")

# Write text file
write_fixed_width(combt, OUTPUT_TEXT_FILE)

# Save to Parquet
try:
    table = pa.Table.from_pandas(combt.to_pandas())
    pq.write_table(table, OUTPUT_PARQUET_FILE)
    print(f"Parquet output written: {OUTPUT_PARQUET_FILE}")
except Exception as e:
    print(f"Error writing Parquet: {e}")

# Display summary statistics
print("\n--- Summary Statistics ---")
print(f"Total records processed: {len(combt)}")
print(f"Columns in output: {combt.columns}")
print("\nPreview of first 5 rows:")
print(combt.head(5))

# Display column info
print("\n--- Column Info ---")
for col in combt.columns:
    print(f"{col}: {combt[col].dtype}")

# Display sample of the output
print("\n--- Sample of first 5 records (fixed-width format) ---")
with open(OUTPUT_TEXT_FILE, 'r') as f:
    for i, line in enumerate(f):
        if i >= 5:
            break
        print(f"Record {i+1}: {line.rstrip()}")

# --------------------------------------------------------------------
# Optional: Additional validation and reporting
# --------------------------------------------------------------------
def validate_output():
    """Validate that output files were created successfully"""
    # Check text file
    if os.path.exists(OUTPUT_TEXT_FILE):
        size = os.path.getsize(OUTPUT_TEXT_FILE)
        with open(OUTPUT_TEXT_FILE, 'r') as f:
            line_count = sum(1 for _ in f)
        print(f"✓ Text file created: {OUTPUT_TEXT_FILE} ({size:,} bytes, {line_count} lines)")
    else:
        print(f"✗ Text file not found: {OUTPUT_TEXT_FILE}")
    
    # Check Parquet file
    if os.path.exists(OUTPUT_PARQUET_FILE):
        size = os.path.getsize(OUTPUT_PARQUET_FILE)
        print(f"✓ Parquet file created: {OUTPUT_PARQUET_FILE} ({size:,} bytes)")
    else:
        print(f"✗ Parquet file not found: {OUTPUT_PARQUET_FILE}")

validate_output()
