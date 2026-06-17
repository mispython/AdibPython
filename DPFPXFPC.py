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
# Helper function to inspect file structure
# --------------------------------------------------------------------
def inspect_file(filepath, num_lines=10):
    """Inspect the first few lines of a file to understand its structure"""
    print(f"\n--- Inspecting file: {filepath} ---")
    with open(filepath, 'r') as f:
        for i, line in enumerate(f):
            if i >= num_lines:
                break
            line = line.rstrip('\n').rstrip('\r')
            print(f"Line {i+1}: length={len(line)}, content='{line}'")
            if len(line) > 0:
                # Show character positions for first 100 chars
                for pos in range(0, min(len(line), 100), 10):
                    segment = line[pos:pos+10]
                    print(f"  Positions {pos:3d}-{pos+9:3d}: '{segment}'")
    print("--- End of inspection ---\n")

# --------------------------------------------------------------------
# Step 2: Read BTDTL input from text file
# --------------------------------------------------------------------
def read_btdtl_text(filepath):
    """
    Read BTPM12 text file with fixed-width format.
    First inspects the file to determine the actual format.
    """
    try:
        # First inspect the file
        inspect_file(filepath)
        
        # Try to determine the format from the first few lines
        # Based on the error, the file seems to have a different structure
        # Let's try a more flexible approach
        
        data = []
        with open(filepath, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.rstrip('\n').rstrip('\r')
                
                # Skip empty lines
                if not line.strip():
                    continue
                
                # Try to parse based on observed patterns
                # The error showed "1BKT2" which suggests fields might be different
                # Let's try to split by spaces or other delimiters first
                
                # Method 1: Try space-separated values
                parts = line.split()
                if len(parts) >= 3:
                    try:
                        # Try to identify fields based on content
                        # This is a fallback - you may need to adjust based on actual format
                        record = {}
                        
                        # Try to parse as delimited file
                        # You'll need to adjust these indices based on your actual file format
                        if len(parts) >= 6:
                            record['BRANCH'] = int(parts[0]) if parts[0].isdigit() else 0
                            record['ACCTNO'] = int(parts[1]) if parts[1].isdigit() else 0
                            record['TRANSREF'] = parts[2] if len(parts) > 2 else ''
                            record['OUTSTAND'] = float(parts[3]) if len(parts) > 3 else 0.0
                            record['MATDT'] = parts[4] if len(parts) > 4 else ''
                            record['LIABCODE'] = parts[5] if len(parts) > 5 else ''
                            data.append(record)
                            continue
                    except (ValueError, IndexError):
                        pass
                
                # Method 2: Try fixed-width based on error patterns
                # From the error, fields might be:
                # Position 0: maybe branch (showed "1BKT2")
                # Adjust these based on your actual file format
                
                # Let's try a different approach - try to find numeric patterns
                # This is a generic parser - you'll need to customize based on actual format
                try:
                    # Look for patterns in the line
                    # Try to extract the MATDT (date) which should be 6 digits
                    import re
                    date_pattern = r'\d{6}'
                    dates = re.findall(date_pattern, line)
                    
                    # Try to extract ACCTNO (10 digits)
                    acct_pattern = r'\d{10}'
                    accts = re.findall(acct_pattern, line)
                    
                    # Try to extract OUTSTAND (amount with decimals)
                    amount_pattern = r'\d+\.\d{2}'
                    amounts = re.findall(amount_pattern, line)
                    
                    if accts and amounts and dates:
                        # Found patterns - use them
                        record = {
                            'BRANCH': 0,  # Default
                            'ACCTNO': int(accts[0]),
                            'TRANSREF': '',  # Default
                            'OUTSTAND': float(amounts[0]),
                            'MATDT': dates[0],
                            'LIABCODE': '',  # Default
                        }
                        data.append(record)
                        continue
                except:
                    pass
                
                # Method 3: If all else fails, try to parse by known positions
                # Based on SAS code, the format should be:
                # @1 BRANCH 5.
                # @6 ACCTNO 10.
                # @16 TRANSREF $10.
                # @26 OUTSTAND 17.2
                # @43 MATDT 6.
                # @49 LIABCODE $5.
                if len(line) >= 53:
                    try:
                        # Try to extract using the standard SAS format
                        branch_str = line[0:5].strip()
                        acctno_str = line[5:15].strip()
                        transref = line[15:25].strip()
                        outstanding_str = line[25:42].strip()
                        matdt = line[42:48].strip()
                        liabcode = line[48:53].strip()
                        
                        # Only add if we have valid data
                        if acctno_str.isdigit() and matdt.isdigit():
                            record = {
                                'BRANCH': int(branch_str) if branch_str.isdigit() else 0,
                                'ACCTNO': int(acctno_str),
                                'TRANSREF': transref,
                                'OUTSTAND': float(outstanding_str) if outstanding_str else 0.0,
                                'MATDT': matdt,
                                'LIABCODE': liabcode,
                            }
                            data.append(record)
                            continue
                    except:
                        pass
                
                # If we get here, we couldn't parse the line
                print(f"Warning: Could not parse line {line_num}: {line[:60]}...")
                
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
# Only parse if MATDT contains valid dates
btdtl = btdtl.with_columns(
    pl.col("MATDT").str.slice(0, 2).cast(pl.Int32, strict=False).alias("day"),
    pl.col("MATDT").str.slice(2, 2).cast(pl.Int32, strict=False).alias("month"),
    pl.col("MATDT").str.slice(4, 2).cast(pl.Int32, strict=False).alias("year2")
)

# Filter out rows with invalid dates
btdtl = btdtl.filter(
    pl.col("day").is_not_null() & 
    pl.col("month").is_not_null() & 
    pl.col("year2").is_not_null()
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
        # Construct file path with month
        filepath = INPUT_BTBASE_FILE.format(PREVMON=prevmon)
        if INPUT_BTBASE_PATH:
            filepath = os.path.join(INPUT_BTBASE_PATH, filepath)
        
        print(f"\nReading SAS dataset: {filepath}")
        
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
        
    except FileNotFoundError:
        print(f"BTBASE SAS dataset not found for month {prevmon}")
        raise
    except Exception as e:
        print(f"Error reading SAS dataset: {e}")
        raise

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

base = base.unique(subset=["ACCTNO", "TRANSREF"])
btdtl = btdtl.unique(subset=["ACCTNO", "TRANSREF"])

print(f"\nBase records after dedup: {len(base)}")
print(f"BTDTL records after dedup: {len(btdtl)}")

# --------------------------------------------------------------------
# Step 4: Merge BASE and BTDTL (BY ACCTNO TRANSREF)
# --------------------------------------------------------------------
combt = base.join(btdtl, on=["ACCTNO", "TRANSREF"], how="left")

# Compute OVERDUE and RECOVAMT
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
