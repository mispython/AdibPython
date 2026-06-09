from __future__ import annotations
from pathlib import Path
from datetime import date, timedelta
import polars as pl
import duckdb  # as requested
import pyarrow.parquet as pq  # as requested

# ---------- SAS-like libs (adjust paths only) ----------
DPAA_TXT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/uat/RBP2.B033.ODPA.EXT.FILE.MIS.txt")  # Input text file

# Output directory
OUTPUT_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------- 1) DATA LMTDET.LMTDET ----------
# Read directly from the txt file
req = [
 "AANO","APRVDT","APRVAMT","ACCTNO","TOTLMTAMT","LASTMNTDT","LMTID","LMTAMT",
 "LMTSTARTDT","LMTENDDT","LMTTERM","LMTTERMID","LMTPAIDIND",
 "COLL1","COLL2","COLL3","COLL4","COLL5","COLL6","COLL7","COLL8","COLL9","COLL10"
]

# Check if text file exists
if not DPAA_TXT.exists():
    raise FileNotFoundError(f"Text file not found: {DPAA_TXT}")

print(f"Reading from text file: {DPAA_TXT}")

# First, let's examine the raw file to understand its structure
print("\n--- Examining first few lines of the file ---")
with open(DPAA_TXT, 'r') as f:
    for i, line in enumerate(f):
        if i < 5:  # Show first 5 lines
            print(f"Line {i+1}: {line[:200]}...")  # First 200 chars
            print(f"  Length: {len(line)}")
            # Check for special characters without using backslash in f-string
            has_tab = '\t' in line
            has_pipe = '|' in line
            has_comma = ',' in line
            has_semicolon = ';' in line
            print(f"  Contains tab: {has_tab}")
            print(f"  Contains pipe: {has_pipe}")
            print(f"  Contains comma: {has_comma}")
            print(f"  Contains semicolon: {has_semicolon}")
        else:
            break

print("\n--- Trying to read the file ---")

# Try reading with different approaches
df = None

# Approach 1: Let Polars auto-detect everything
print("\nApproach 1: Auto-detection")
try:
    df = pl.read_csv(
        DPAA_TXT,
        has_header=True,  # Assume first row is header
        try_parse_dates=True
    )
    print(f"  Auto-detection: {df.height} rows, {df.width} columns")
    if df.height > 0:
        print(f"  First row sample: {df.row(0)[:5]}")
except Exception as e:
    print(f"  Auto-detection failed: {e}")

# Approach 2: Read as raw text to see structure
print("\nApproach 2: Reading as raw text to understand structure")
try:
    # Read the file as lines
    with open(DPAA_TXT, 'r') as f:
        lines = f.readlines()
    
    print(f"  Total lines in file: {len(lines)}")
    if len(lines) > 1:
        # Check the header line
        header = lines[0].strip()
        print(f"  Header line: {header[:200]}")
        
        # Try to detect delimiter by counting common delimiters in header
        delimiters = {'tab': '\t', 'pipe': '|', 'comma': ',', 'semicolon': ';', 'space': ' '}
        delimiter_counts = {}
        
        for name, delim in delimiters.items():
            count = header.count(delim)
            delimiter_counts[name] = count
            if count > 0:
                print(f"    {name} count: {count}")
        
        # Use the most common delimiter
        best_delimiter = max(delimiter_counts.items(), key=lambda x: x[1])
        if best_delimiter[1] > 0:
            delim_char = delimiters[best_delimiter[0]]
            print(f"\n  Using delimiter: {best_delimiter[0]} ('{delim_char}')")
            df = pl.read_csv(
                DPAA_TXT,
                separator=delim_char,
                has_header=True,
                try_parse_dates=True
            )
            print(f"  Result: {df.height} rows, {df.width} columns")
            if df.height > 0:
                print(f"  First row sample: {df.row(0)[:5]}")
except Exception as e:
    print(f"  Raw read approach failed: {e}")

# Approach 3: Read without header first, then assign
if df is None or df.height == 0:
    print("\nApproach 3: Read without header")
    try:
        # Read all lines as data
        df_no_header = pl.read_csv(
            DPAA_TXT,
            has_header=False,
            try_parse_dates=True
        )
        print(f"  No header: {df_no_header.height} rows, {df_no_header.width} columns")
        
        if df_no_header.height > 1:
            # First row might be header
            df = df_no_header.slice(1)  # Skip first row
            # Use first row as column names
            new_columns = [str(col) for col in df_no_header.row(0)]
            df = df.rename({old: new for old, new in zip(df.columns, new_columns)})
            print(f"  After assigning header: {df.height} rows, {df.width} columns")
    except Exception as e:
        print(f"  No-header approach failed: {e}")

if df is None or df.height == 0:
    raise ValueError("Could not read any data from the text file")

print(f"\n--- Successfully read {df.height} rows with {df.width} columns ---")
print(f"Column names: {df.columns[:10]}...")

# Check for missing required columns and add them as nulls
miss = [c for c in req if c not in df.columns]
if miss:
    print(f"\nAdding missing columns: {miss}")
    df = df.with_columns([pl.lit(None).alias(c) for c in miss])

# Select only required columns that exist
available_req = [c for c in req if c in df.columns]
LMTDET_LMTDET = df.select(available_req)

print(f"\nSelected {len(available_req)} columns for output")
print(f"Output shape: {LMTDET_LMTDET.height} rows × {LMTDET_LMTDET.width} columns")

# Verify data exists before writing
if LMTDET_LMTDET.height == 0:
    print("\nWARNING: No data rows found! The CSV file will only have headers.")
    print("First few rows of raw data:")
    with open(DPAA_TXT, 'r') as f:
        for i, line in enumerate(f):
            if i < 10:
                print(f"  Row {i}: {line[:100]}")
            else:
                break

# Write to both Parquet and CSV in output directory
parquet_path = OUTPUT_DIR / "LMTDET.parquet"
csv_path = OUTPUT_DIR / "LMTDET.csv"

LMTDET_LMTDET.write_parquet(parquet_path)
LMTDET_LMTDET.write_csv(csv_path)

print(f"\nWritten to Parquet: {parquet_path} ({LMTDET_LMTDET.height} rows)")
print(f"Written to CSV: {csv_path} ({LMTDET_LMTDET.height} rows)")

# Show a sample of the output data
if LMTDET_LMTDET.height > 0:
    print("\n--- Sample of output data (first 3 rows) ---")
    print(LMTDET_LMTDET.head(3))

# ---------- 2) DATA LMTDET.REPTDATE ----------
today = date.today()
REPTDATE = today - timedelta(days=1)
YYYY = f"{REPTDATE.year:04d}"
MM   = f"{REPTDATE.month:02d}"
DD   = f"{REPTDATE.day:02d}"
DAY1 = date(REPTDATE.year, 1, 1)
DAYS = (today - DAY1).days
TEMPDATE = f"{MM}{DD}{YYYY}{DAYS}"
EXTDATE = int(TEMPDATE)

reptdate_df = pl.DataFrame({"EXTDATE":[EXTDATE], "REPTDATE":[REPTDATE]})

# Write REPTDATE to both Parquet and CSV
reptdate_parquet_path = OUTPUT_DIR / "REPTDATE.parquet"
reptdate_csv_path = OUTPUT_DIR / "REPTDATE.csv"

reptdate_df.write_parquet(reptdate_parquet_path)
reptdate_df.write_csv(reptdate_csv_path)

print(f"\nWritten REPTDATE to Parquet: {reptdate_parquet_path}")
print(f"Written REPTDATE to CSV: {reptdate_csv_path}")

print("\n" + "="*50)
print("DONE - All outputs generated:")
print("="*50)
print(f"\nOutput directory: {OUTPUT_DIR}")
print(f"\nFiles created:")
print(f"  • {parquet_path}")
print(f"  • {csv_path}")
print(f"  • {reptdate_parquet_path}")
print(f"  • {reptdate_csv_path}")
print(f"\nREPTDATE: {REPTDATE}")
print(f"EXTDATE: {EXTDATE}")
