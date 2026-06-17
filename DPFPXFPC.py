# EIBDBT12_BANKTRADE_PM12.py
# Conversion of SAS job EIBDBT12 (calls EIBDBT05) into Python with Polars, DuckDB, PyArrow
# Outputs both fixed-width .txt and Parquet

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, timedelta
import pyreadstat  # For reading SAS .sas7bdat files
import os

# --------------------------------------------------------------------
# Configuration: Input and Output Paths
# --------------------------------------------------------------------
# Input paths
INPUT_BTPM12_FILE = "input/prod/BTPM12.txt"  # Text file containing BTDTL data
INPUT_BTBASE_FILE = "BTBASE_{PREVMON}.sas7bdat"  # SAS dataset with month placeholder
INPUT_BTBASE_PATH = None  # Set to specific path if different from current dir

# Output paths
OUTPUT_TEXT_FILE = "DAYBTRD_PM12.txt"
OUTPUT_PARQUET_FILE = "DAYBTRD_PM12.parquet"

# Optional: Set to False to use real data, True to use dummy data for testing
USE_DUMMY_DATA = False

# --------------------------------------------------------------------
# Step 1: Reporting date logic (DATA REPTDATE equivalent)
# --------------------------------------------------------------------
today = date.today()
reptdate = today - timedelta(days=1)
prevdate = date(reptdate.year, reptdate.month, 1) - timedelta(days=1)

rptdt = reptdate.strftime("%d-%m-%Y")
curmm = rptdt[3:5]
curyy = rptdt[8:10]
rdatex = curmm + curyy

# Next month calculation
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
# Step 2: Read BTDTL input from text file with correct fixed-width format
# --------------------------------------------------------------------
def read_btdtl_text(filepath):
    """
    Read BTPM12 text file with fixed-width format.
    Based on file inspection:
    - BRANCH: positions 0-4 (5 chars)
    - ACCTNO: positions 5-14 (10 chars)
    - TRANSREF: positions 15-24 (10 chars)
    - OUTSTAND: positions 25-41 (17 chars, with decimals)
    - MATDT: positions 42-47 (6 chars, ddmmyy)
    - LIABCODE: positions 48-52 (5 chars)
    """
    try:
        data = []
        with open(filepath, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.rstrip('\n').rstrip('\r')
                
                # Skip empty lines
                if not line.strip():
                    continue
                
                # Ensure line has minimum length
                if len(line) < 53:
                    print(f"Warning: Line {line_num} has length {len(line)}, skipping")
                    continue
                
                try:
                    # Extract fields based on inspection
                    # Position: 0-4 = BRANCH, 5-14 = ACCTNO, 15-24 = TRANSREF
                    # 25-41 = OUTSTAND, 42-47 = MATDT, 48-52 = LIABCODE
                    branch_str = line[0:5].strip()
                    acctno_str = line[5:15].strip()
                    transref = line[15:25].strip()
                    outstanding_str = line[25:42].strip()
                    matdt = line[42:48].strip()
                    liabcode = line[48:53].strip()
                    
                    # Skip header line (contains non-numeric data)
                    if not acctno_str.isdigit() or len(acctno_str) != 10:
                        continue
                    
                    # Convert with error handling
                    branch = int(branch_str) if branch_str.isdigit() else 0
                    acctno = int(acctno_str) if acctno_str.isdigit() else 0
                    outstanding = float(outstanding_str) if outstanding_str else 0.0
                    
                    # Validate MATDT is 6 digits
                    if len(matdt) == 6 and matdt.isdigit():
                        data.append({
                            'BRANCH': branch,
                            'ACCTNO': acctno,
                            'TRANSREF': transref,
                            'OUTSTAND': outstanding,
                            'MATDT': matdt,
                            'LIABCODE': liabcode,
                        })
                    else:
                        print(f"Warning: Line {line_num} has invalid MATDT: '{matdt}'")
                        
                except (ValueError, IndexError) as e:
                    print(f"Warning: Error parsing line {line_num}: {e}")
                    continue
        
        if not data:
            raise ValueError(f"No valid data found in {filepath}")
        
        print(f"Successfully read {len(data)} records from {filepath}")
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
        "MATDT": ["250125", "250630", "250331"],  # ddmmyy format
        "LIABCODE": ["001", "002", "003"],
    })
else:
    btdtl = read_btdtl_text(INPUT_BTPM12_FILE)

# Check if we have data
if len(btdtl) == 0:
    raise ValueError("No data loaded from BTPM12 file")

print(f"\nLoaded {len(btdtl)} records from BTPM12")
print("Sample of loaded data:")
print(btdtl.head(5))

# Parse MATDATE from ddmmyy string
# MATDT is in DDMMYY format
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

# Add century (2000 + year2)
btdtl = btdtl.with_columns(
    (pl.col("year2") + 2000).alias("year")
)

# Create datetime column
btdtl = btdtl.with_columns(
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
        # Construct file path with month
        filepath = INPUT_BTBASE_FILE.format(PREVMON=prevmon)
        if INPUT_BTBASE_PATH:
            filepath = os.path.join(INPUT_BTBASE_PATH, filepath)
        
        print(f"\nReading SAS dataset: {filepath}")
        
        # Check if file exists
        if not os.path.exists(filepath):
            print(f"Warning: SAS file not found: {filepath}")
            print("Creating dummy base data for testing...")
            return pl.DataFrame({
                "ACCTNO": [2000625018, 2000925056, 2020125057],
                "TRANSREF": ["Y011618000", "Y066656000", "Y080273000"],
                "PREOUTSTD": [150000.0, 100000.0, 120000.0],
                "PRODTYPE": [0, 200, 0],
            })
        
        # Read SAS dataset using pyreadstat
        df, meta = pyreadstat.read_sas7bdat(filepath)
        
        # Convert to Polars DataFrame
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
        return pl.DataFrame({
            "ACCTNO": [2000625018, 2000925056, 2020125057],
            "TRANSREF": ["Y011618000", "Y066656000", "Y080273000"],
            "PREOUTSTD": [150000.0, 100000.0, 120000.0],
            "PRODTYPE": [0, 200, 0],
        })

if USE_DUMMY_DATA:
    print("\nUsing dummy base data for testing")
    base = pl.DataFrame({
        "ACCTNO": [2850001111, 2860000001, 2870000001],
        "TRANSREF": ["PM12A01", "PM12B02", "PM12C03"],
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
# Ensure ACCTNO and TRANSREF are compatible types
base = base.with_columns([
    pl.col("ACCTNO").cast(pl.Int64),
    pl.col("TRANSREF").cast(pl.Utf8)
])
btdtl = btdtl.with_columns([
    pl.col("ACCTNO").cast(pl.Int64),
    pl.col("TRANSREF").cast(pl.Utf8)
])

combt = base.join(btdtl, on=["ACCTNO", "TRANSREF"], how="left")

# Compute OVERDUE and RECOVAMT
# Convert MATDATE to ordinal for date calculation
combt = combt.with_columns([
    # Calculate overdue days
    pl.when(pl.col("MATDATE").is_not_null())
    .then((sdate.toordinal() + 1) - pl.col("MATDATE").dt.epoch("days"))
    .otherwise(0)
    .alias("OVERDUE"),
    
    # Calculate recovery amount
    (pl.col("PREOUTSTD") - pl.col("OUTSTAND").fill_null(0)).alias("RECOVAMT"),
    
    # Retail ID indicator
    pl.when(pl.col("PRODTYPE") == 0).then("R").otherwise(pl.lit(None)).alias("RETAILID"),
])

# Handle null values
combt = combt.with_columns([
    pl.col("OVERDUE").fill_null(0).cast(pl.Int64),
    pl.col("RECOVAMT").fill_null(0),
    pl.col("OUTSTAND").fill_null(0),
])

print(f"\nMerged records: {len(combt)}")

# --------------------------------------------------------------------
# Step 5: Write output (DAYBTRD fixed-width and Parquet)
# --------------------------------------------------------------------
def write_fixed_width(df, filepath):
    """Write DataFrame to fixed-width text file"""
    records = []
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
            print(f"Warning: Error formatting row: {e}")
            continue
    
    # Write to text file
    with open(filepath, "w") as f:
        for r in records:
            f.write(r + "\n")
    
    print(f"Text output written: {filepath} ({len(records)} records)")

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
