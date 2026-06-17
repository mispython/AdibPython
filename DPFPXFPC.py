# EIBDBT12_BANKTRADE_PM12_DEBUG.py
# Debug version to compare with production output

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, timedelta
import pyreadstat
import os
import re

# --------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------
INPUT_BTPM12_FILE = "input/prod/BTPM12.txt"
INPUT_BTBASE_FILE = "input/prod/btbase{PREVMON}.sas7bdat"  # Note: no underscore
OUTPUT_TEXT_FILE = "DAYBTRD_PM12_DEBUG.txt"
DEBUG = True

# --------------------------------------------------------------------
# Step 1: Reporting date logic
# --------------------------------------------------------------------
today = date.today()
reptdate = today - timedelta(days=1)
prevdate = date(reptdate.year, reptdate.month, 1) - timedelta(days=1)

# Calculate SDATE (first day of next month)
if reptdate.month + 1 == 13:
    mm, yy = 1, reptdate.year + 1
else:
    mm, yy = reptdate.month + 1, reptdate.year
sdate = date(yy, mm, 1)

params = {
    "REPTDATE": reptdate.strftime("%Y-%m-%d"),
    "PREVDATE": prevdate.strftime("%Y-%m-%d"),
    "PREVMON": f"{prevdate.month:02d}",
    "SDATE": sdate.strftime("%Y-%m-%d"),
    "SDATE_ORDINAL": sdate.toordinal(),
}

print("=" * 80)
print("DEBUG: Report Parameters")
print("=" * 80)
for key, value in params.items():
    print(f"  {key}: {value}")
print("=" * 80)

# --------------------------------------------------------------------
# Step 2: Read and debug BTDTL input
# --------------------------------------------------------------------
def read_and_debug_btdtl(filepath):
    """Read BTPM12 with extensive debugging"""
    print("\n" + "=" * 80)
    print("DEBUG: Reading BTPM12.txt")
    print("=" * 80)
    
    # First, let's look at the raw file structure
    with open(filepath, 'r') as f:
        lines = f.readlines()
    
    print(f"Total lines in file: {len(lines)}")
    print("\nFirst 5 data lines (showing first 80 chars):")
    for i, line in enumerate(lines[1:6], 1):  # Skip header
        line = line.rstrip('\n')
        print(f"  Line {i}: '{line[:80]}...' (length: {len(line)})")
        # Show character positions
        if len(line) >= 50:
            print(f"    Positions 0-4 (BRANCH): '{line[0:5]}'")
            print(f"    Positions 5-14 (ACCTNO): '{line[5:15]}'")
            print(f"    Positions 15-21 (TRANSREF): '{line[15:22]}'")
            print(f"    Positions 22-36 (OUTSTAND): '{line[22:37]}'")
            print(f"    Positions 37-42 (MATDT): '{line[37:43]}'")
            print(f"    Positions 42-44 (LIABCODE): '{line[42:45]}'")
            print()
    
    # Now parse all records
    data = []
    for line_num, line in enumerate(lines, 1):
        line = line.rstrip('\n').rstrip('\r')
        
        if not line.strip():
            continue
        if line.strip().startswith('1BKT'):
            continue
        
        try:
            # Extract using positions from SAS
            branch_str = line[1:5].strip() if len(line) > 5 else ''
            acctno_str = line[5:15].strip() if len(line) > 15 else ''
            transref = line[15:22].strip() if len(line) > 22 else ''
            outstanding_str = line[22:37].strip() if len(line) > 37 else ''
            matdt = line[37:43].strip() if len(line) > 43 else ''
            liabcode = line[42:45].strip() if len(line) > 45 else ''
            
            # Skip invalid records
            if not acctno_str.isdigit() or len(acctno_str) != 10:
                continue
            if not matdt.isdigit() or len(matdt) != 6:
                continue
            
            branch = int(branch_str) if branch_str.isdigit() else 0
            acctno = int(acctno_str)
            outstanding = float(outstanding_str) if outstanding_str else 0.0
            
            data.append({
                'BRANCH': branch,
                'ACCTNO': acctno,
                'TRANSREF': transref,
                'OUTSTAND': outstanding,
                'MATDT': matdt,
                'LIABCODE': liabcode,
            })
        except Exception as e:
            if DEBUG and line_num <= 10:
                print(f"Warning: Error on line {line_num}: {e}")
    
    print(f"\nParsed {len(data)} records from BTPM12")
    return pl.DataFrame(data)

btdtl = read_and_debug_btdtl(INPUT_BTPM12_FILE)

if len(btdtl) == 0:
    raise ValueError("No data loaded from BTPM12 file")

print("\nBTDTL Sample (first 5 records):")
print(btdtl.head(5))

# --------------------------------------------------------------------
# Step 3: Parse MATDATE (DDMMYY format)
# --------------------------------------------------------------------
print("\n" + "=" * 80)
print("DEBUG: Parsing MATDATE")
print("=" * 80)

# MATDT is DDMMYY
btdtl = btdtl.with_columns([
    pl.col("MATDT").str.slice(0, 2).cast(pl.Int32).alias("day"),
    pl.col("MATDT").str.slice(2, 2).cast(pl.Int32).alias("month"),
    pl.col("MATDT").str.slice(4, 2).cast(pl.Int32).alias("year2")
])

# Add century
btdtl = btdtl.with_columns(
    (pl.col("year2") + 2000).alias("year")
).with_columns(
    pl.date("year", "month", "day").alias("MATDATE")
)

print("BTDTL with MATDATE (first 5):")
print(btdtl.select(["MATDT", "day", "month", "year", "MATDATE"]).head(5))

# Apply filter
btdtl = btdtl.filter(
    ~((pl.col("BRANCH") > 3000) &
      (pl.col("ACCTNO") >= 2850000000) &
      (pl.col("ACCTNO") <= 2859999999))
)

print(f"\nAfter filter: {len(btdtl)} records")
print("BTDTL unique keys:", btdtl.select(["ACCTNO", "TRANSREF"]).n_unique())

# --------------------------------------------------------------------
# Step 4: Read BASE dataset
# --------------------------------------------------------------------
print("\n" + "=" * 80)
print("DEBUG: Reading BASE dataset")
print("=" * 80)

def read_and_debug_base(prevmon):
    """Read BASE dataset with debugging"""
    # Try both naming conventions
    filepaths = [
        INPUT_BTBASE_FILE.format(PREVMON=prevmon),
        f"input/prod/btbase_{prevmon}.sas7bdat",
        f"input/prod/BTBASE{prevmon}.sas7bdat",
        f"input/prod/BTBASE_{prevmon}.sas7bdat",
    ]
    
    base_df = None
    for filepath in filepaths:
        if os.path.exists(filepath):
            print(f"Found BASE file: {filepath}")
            try:
                df, meta = pyreadstat.read_sas7bdat(filepath)
                base_df = pl.from_pandas(df)
                print(f"Loaded {len(base_df)} records")
                print(f"Columns: {base_df.columns}")
                print("\nFirst 5 records from BASE:")
                print(base_df.head(5))
                return base_df
            except Exception as e:
                print(f"Error reading {filepath}: {e}")
                continue
    
    if base_df is None:
        print("ERROR: Could not find BASE dataset")
        print("Tried these paths:")
        for filepath in filepaths:
            print(f"  - {filepath} (exists: {os.path.exists(filepath)})")
        raise FileNotFoundError("BASE dataset not found")
    
    return base_df

base = read_and_debug_base(params['PREVMON'])

# Keep only required columns
required_cols = ["ACCTNO", "TRANSREF", "OUTSTAND", "PRODTYPE"]
available_cols = [col for col in required_cols if col in base.columns]
if "OUTSTAND" not in available_cols and "PREOUTSTD" in base.columns:
    base = base.rename({"PREOUTSTD": "OUTSTAND"})
    available_cols = ["ACCTNO", "TRANSREF", "OUTSTAND", "PRODTYPE"]

base = base.select(available_cols).rename({"OUTSTAND": "PREOUTSTD"})

print(f"\nBASE after selection: {len(base)} records")
print("BASE sample:")
print(base.head(5))

# Deduplicate
base = base.unique(subset=["ACCTNO", "TRANSREF"])
btdtl = btdtl.unique(subset=["ACCTNO", "TRANSREF"])

print(f"\nBASE after dedup: {len(base)}")
print(f"BTDTL after dedup: {len(btdtl)}")

# --------------------------------------------------------------------
# Step 5: Merge and calculate
# --------------------------------------------------------------------
print("\n" + "=" * 80)
print("DEBUG: Merging and calculating")
print("=" * 80)

# Ensure types match
base = base.with_columns([
    pl.col("ACCTNO").cast(pl.Int64),
    pl.col("TRANSREF").cast(pl.Utf8)
])
btdtl = btdtl.with_columns([
    pl.col("ACCTNO").cast(pl.Int64),
    pl.col("TRANSREF").cast(pl.Utf8)
])

# Perform merge
combt = base.join(btdtl, on=["ACCTNO", "TRANSREF"], how="left")

print(f"Merged records: {len(combt)}")

# Check merge results
matched = combt.filter(pl.col("MATDATE").is_not_null())
unmatched = combt.filter(pl.col("MATDATE").is_null())

print(f"Matched records: {len(matched)}")
print(f"Unmatched records: {len(unmatched)}")

if len(unmatched) > 0:
    print("\nSample of unmatched records (BASE only):")
    print(unmatched.select(["ACCTNO", "TRANSREF", "PREOUTSTD", "PRODTYPE"]).head(10))

# Calculate OVERDUE and RECOVAMT
sdate_ordinal = params["SDATE_ORDINAL"]
print(f"\nSDATE ordinal: {sdate_ordinal}")

combt = combt.with_columns([
    # OVERDUE = (SDATE+1) - MATDATE
    pl.when(pl.col("MATDATE").is_not_null())
    .then((sdate_ordinal + 1) - pl.col("MATDATE").cast(pl.Int32).dt.epoch("d"))
    .otherwise(pl.lit(None))
    .alias("OVERDUE"),
    
    # RECOVAMT = PREOUTSTD - OUTSTAND
    (pl.col("PREOUTSTD") - pl.col("OUTSTAND").fill_null(0)).alias("RECOVAMT"),
])

# Fill nulls
combt = combt.with_columns([
    pl.col("OVERDUE").fill_null(0).cast(pl.Int64),
    pl.col("RECOVAMT").fill_null(0),
    pl.col("OUTSTAND").fill_null(0),
])

print("\nSample of merged data with calculations:")
print(combt.select(["ACCTNO", "TRANSREF", "PREOUTSTD", "OUTSTAND", "OVERDUE", "RECOVAMT", "PRODTYPE"]).head(10))

# --------------------------------------------------------------------
# Step 6: Compare with known production values
# --------------------------------------------------------------------
print("\n" + "=" * 80)
print("DEBUG: Comparison with Production Data")
print("=" * 80)

# Known production values for specific accounts from your sample
production_samples = [
    {"BRANCH": 6, "ACCTNO": 2501873900, "TRANSREF": "Y011618", "PRODTYPE": 0, 
     "PREOUTSTD": 33531.01, "OUTSTAND": 33531.01, "OVERDUE": 7074, "RECOVAMT": 0.00},
    {"BRANCH": 2, "ACCTNO": 2501466705, "TRANSREF": "Y081465", "PRODTYPE": 0,
     "PREOUTSTD": 72866.61, "OUTSTAND": 73463.61, "OVERDUE": 1048, "RECOVAMT": -597.00},
    {"BRANCH": 201, "ACCTNO": 2505707731, "TRANSREF": "Y080273", "PRODTYPE": 0,
     "PREOUTSTD": 68696.13, "OUTSTAND": 69258.93, "OVERDUE": 1180, "RECOVAMT": -562.80},
]

print("Comparing specific records:")
for prod in production_samples:
    acctno = prod["ACCTNO"]
    transref = prod["TRANSREF"]
    
    # Find in our data
    match = combt.filter(
        (pl.col("ACCTNO") == acctno) & 
        (pl.col("TRANSREF") == transref)
    )
    
    if len(match) > 0:
        row = match.row(0, named=True)
        print(f"\nACCTNO: {acctno}, TRANSREF: {transref}")
        print(f"  Production: PREOUTSTD={prod['PREOUTSTD']:>12.2f}, OUTSTAND={prod['OUTSTAND']:>12.2f}, OVERDUE={prod['OVERDUE']:>6d}, RECOVAMT={prod['RECOVAMT']:>12.2f}")
        print(f"  Python:     PREOUTSTD={row['PREOUTSTD']:>12.2f}, OUTSTAND={row['OUTSTAND']:>12.2f}, OVERDUE={row['OVERDUE']:>6d}, RECOVAMT={row['RECOVAMT']:>12.2f}")
        
        # Check for differences
        if row['PREOUTSTD'] != prod['PREOUTSTD']:
            print(f"  DIFFERENCE: PREOUTSTD differs by {row['PREOUTSTD'] - prod['PREOUTSTD']:.2f}")
        if row['OUTSTAND'] != prod['OUTSTAND']:
            print(f"  DIFFERENCE: OUTSTAND differs by {row['OUTSTAND'] - prod['OUTSTAND']:.2f}")
        if row['OVERDUE'] != prod['OVERDUE']:
            print(f"  DIFFERENCE: OVERDUE differs by {row['OVERDUE'] - prod['OVERDUE']}")
        if row['RECOVAMT'] != prod['RECOVAMT']:
            print(f"  DIFFERENCE: RECOVAMT differs by {row['RECOVAMT'] - prod['RECOVAMT']:.2f}")
    else:
        print(f"\nACCTNO: {acctno}, TRANSREF: {transref} - NOT FOUND in merged data")

# --------------------------------------------------------------------
# Step 7: Write output
# --------------------------------------------------------------------
def write_fixed_width(df, filepath):
    """Write matching SAS PUT statement"""
    records = []
    
    for row in df.iter_rows(named=True):
        try:
            branch = row.get('BRANCH', 0) or 0
            acctno = row.get('ACCTNO', 0) or 0
            transref = str(row.get('TRANSREF', '') or '')[:10]
            prodtype = row.get('PRODTYPE', 0) or 0
            preoutstd = row.get('PREOUTSTD', 0.0) or 0.0
            outstanding = row.get('OUTSTAND', 0.0) or 0.0
            overdue = row.get('OVERDUE', 0) or 0
            recovamt = row.get('RECOVAMT', 0.0) or 0.0
            facility = '99999'  # Default, should come from format
            
            record = [' '] * 103
            
            record[0:5] = f"{int(branch):5d}"
            record[6:16] = f"{int(acctno):10d}"
            record[17:27] = f"{transref:<10}"
            record[28:31] = f"{int(prodtype):03d}"
            record[32:49] = f"{float(preoutstd):17.2f}"
            record[50:67] = f"{float(outstanding):17.2f}"
            record[68:78] = f"{int(overdue):10d}"
            record[79:96] = f"{float(recovamt):17.2f}"
            record[97:102] = f"{facility:<5}"
            
            records.append(''.join(record))
        except Exception as e:
            continue
    
    with open(filepath, "w") as f:
        for r in records:
            f.write(r + "\n")
    
    print(f"\nOutput written: {filepath} ({len(records)} records)")

write_fixed_width(combt, OUTPUT_TEXT_FILE)

print("\n" + "=" * 80)
print("DEBUG COMPLETE")
print("=" * 80)
