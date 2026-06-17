# EIBDBT12_BANKTRADE_PM12.py
# Conversion of SAS job EIBDBT12 (calls EIBDBT05) into Python with Polars, DuckDB, PyArrow
# Outputs both fixed-width .txt and Parquet

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, timedelta
import pyreadstat  # For reading SAS .sas7bdat files

# --------------------------------------------------------------------
# Configuration: Input and Output Paths
# --------------------------------------------------------------------
# Input paths
INPUT_BTPM12_FILE = "BTPM12.txt"  # Text file containing BTDTL data
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
# Step 2: Read BTDTL input from text file
# --------------------------------------------------------------------
def read_btdtl_text(filepath):
    """Read BTPM12 text file with fixed-width format"""
    try:
        # Define column widths based on SAS INFILE specification
        # Assuming format: BRANCH(5), ACCTNO(10), TRANSREF(10), OUTSTAND(17), MATDT(6), LIABCODE(5)
        # Adjust widths as needed based on actual file format
        col_specs = [
            (0, 5),    # BRANCH
            (5, 15),   # ACCTNO
            (15, 25),  # TRANSREF
            (25, 42),  # OUTSTAND
            (42, 48),  # MATDT
            (48, 53),  # LIABCODE
        ]
        
        # Read the text file as fixed-width
        df = pl.read_csv(
            filepath,
            has_header=False,
            dtypes=[pl.Utf8, pl.Utf8, pl.Utf8, pl.Utf8, pl.Utf8, pl.Utf8],
            newline_character='\n'
        )
        
        # Parse the fixed-width columns
        # This is a simplified approach - may need adjustment based on actual format
        data = []
        with open(filepath, 'r') as f:
            for line in f:
                line = line.rstrip('\n')
                if len(line) >= 53:  # Ensure minimum length
                    data.append({
                        'BRANCH': int(line[0:5].strip()),
                        'ACCTNO': int(line[5:15].strip()),
                        'TRANSREF': line[15:25].strip(),
                        'OUTSTAND': float(line[25:42].strip()) if line[25:42].strip() else 0.0,
                        'MATDT': line[42:48].strip(),
                        'LIABCODE': line[48:53].strip(),
                    })
        
        return pl.DataFrame(data)
        
    except FileNotFoundError:
        print(f"{filepath} not found")
        raise

if USE_DUMMY_DATA:
    print("Using dummy data for testing")
    btdtl = pl.DataFrame({
        "BRANCH": [2001, 3100],
        "ACCTNO": [2850001111, 2860000001],
        "TRANSREF": ["PM12A01", "PM12B02"],
        "OUTSTAND": [120000.00, 80000.00],
        "MATDT": ["250125", "250630"],  # ddmmyy format
        "LIABCODE": ["001", "002"],
    })
else:
    btdtl = read_btdtl_text(INPUT_BTPM12_FILE)

# Parse MATDATE from ddmmyy string
btdtl = btdtl.with_columns(
    pl.col("MATDT").str.slice(0, 2).cast(pl.Int32).alias("day"),
    pl.col("MATDT").str.slice(2, 2).cast(pl.Int32).alias("month"),
    pl.col("MATDT").str.slice(4, 2).cast(pl.Int32).alias("year2")
).with_columns(
    (pl.col("year2") + 2000).alias("year")
).with_columns(
    pl.datetime("year", "month", "day").alias("MATDATE")
)

# Apply SAS filter: remove if branch > 3000 and ACCTNO in range
btdtl = btdtl.filter(
    ~((pl.col("BRANCH") > 3000) &
      (pl.col("ACCTNO") >= 2850000000) &
      (pl.col("ACCTNO") <= 2859999999))
)

# --------------------------------------------------------------------
# Step 3: Read BASE dataset (previous month snapshot from SAS)
# --------------------------------------------------------------------
def read_base_sas(prevmon):
    """Read BTBASE SAS dataset for the previous month"""
    try:
        # Construct file path with month
        filepath = INPUT_BTBASE_FILE.format(PREVMON=prevmon)
        if INPUT_BTBASE_PATH:
            import os
            filepath = os.path.join(INPUT_BTBASE_PATH, filepath)
        
        # Read SAS dataset using pyreadstat
        df, meta = pyreadstat.read_sas7bdat(filepath)
        
        # Convert to Polars DataFrame
        base = pl.from_pandas(df)
        
        # Ensure required columns exist
        required_cols = ["ACCTNO", "TRANSREF", "PREOUTSTD", "PRODTYPE"]
        for col in required_cols:
            if col not in base.columns:
                raise ValueError(f"Required column '{col}' not found in SAS dataset")
        
        return base
        
    except FileNotFoundError:
        print(f"BTBASE SAS dataset not found for month {prevmon}")
        raise

if USE_DUMMY_DATA:
    print("Using dummy base data for testing")
    base = pl.DataFrame({
        "ACCTNO": [2850001111, 2860000001],
        "TRANSREF": ["PM12A01", "PM12B02"],
        "PREOUTSTD": [150000.0, 100000.0],
        "PRODTYPE": [0, 200],
    })
else:
    base = read_base_sas(params['PREVMON'])

base = base.unique(subset=["ACCTNO", "TRANSREF"])
btdtl = btdtl.unique(subset=["ACCTNO", "TRANSREF"])

# --------------------------------------------------------------------
# Step 4: Merge BASE and BTDTL (BY ACCTNO TRANSREF)
# --------------------------------------------------------------------
combt = base.join(btdtl, on=["ACCTNO", "TRANSREF"], how="left")

# Compute OVERDUE and RECOVAMT
sdate_num = sdate.toordinal()
combt = combt.with_columns([
    ((sdate_num + 1) - pl.col("MATDATE").dt.to_python_datetime().dt.toordinal()).alias("OVERDUE"),
    (pl.col("PREOUTSTD") - pl.col("OUTSTAND")).alias("RECOVAMT"),
    pl.when(pl.col("PRODTYPE") == 0).then("R").otherwise(pl.lit(None)).alias("RETAILID"),
])

# Handle null values in RECOVAMT and OVERDUE
combt = combt.with_columns([
    pl.col("OVERDUE").fill_null(0),
    pl.col("RECOVAMT").fill_null(0),
])

# --------------------------------------------------------------------
# Step 5: Write output (DAYBTRD fixed-width and Parquet)
# --------------------------------------------------------------------
records = []
for row in combt.iter_rows(named=True):
    # Ensure values are valid for formatting
    branch = row.get('BRANCH', 0) or 0
    acctno = row.get('ACCTNO', 0) or 0
    transref = row.get('TRANSREF', '') or ''
    prodtype = row.get('PRODTYPE', 0) or 0
    preoutstd = row.get('PREOUTSTD', 0.0) or 0.0
    outstanding = row.get('OUTSTAND', 0.0) or 0.0
    overdue = row.get('OVERDUE', 0) or 0
    recovamt = row.get('RECOVAMT', 0.0) or 0.0
    liabcode = row.get('LIABCODE', '') or ''
    
    rec = (
        f"{int(branch):05d}"
        f"{int(acctno):010d}"
        f"{str(transref):<10}"
        f"{int(prodtype):03d}"
        f"{float(preoutstd):017.2f}"
        f"{float(outstanding):017.2f}"
        f"{int(overdue):010d}"
        f"{float(recovamt):017.2f}"
        f"{str(liabcode):<5}"
    )
    records.append(rec)

# Write to text file
with open(OUTPUT_TEXT_FILE, "w") as f:
    for r in records:
        f.write(r + "\n")

print(f"Text output written: {OUTPUT_TEXT_FILE}")

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
print(f"Preview of first 5 rows:")
print(combt.head(5))

# --------------------------------------------------------------------
# Optional: Additional validation and reporting
# --------------------------------------------------------------------
def validate_output():
    """Validate that output files were created successfully"""
    import os
    
    # Check text file
    if os.path.exists(OUTPUT_TEXT_FILE):
        size = os.path.getsize(OUTPUT_TEXT_FILE)
        print(f"✓ Text file created: {OUTPUT_TEXT_FILE} ({size:,} bytes)")
    else:
        print(f"✗ Text file not found: {OUTPUT_TEXT_FILE}")
    
    # Check Parquet file
    if os.path.exists(OUTPUT_PARQUET_FILE):
        size = os.path.getsize(OUTPUT_PARQUET_FILE)
        print(f"✓ Parquet file created: {OUTPUT_PARQUET_FILE} ({size:,} bytes)")
    else:
        print(f"✗ Parquet file not found: {OUTPUT_PARQUET_FILE}")

validate_output()
