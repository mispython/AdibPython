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
# Define column specifications based on SAS INPUT statement
# Format: (start_position, length, name, dtype)
# Note: SAS positions start at 1, Python starts at 0
columns = [
    (1, 13, "AANO", pl.Utf8),           # @001 AANO $13.
    (21, 11, "APRVDT", pl.Int64),       # @021 APRVDT 11.
    (32, 11, "APRVAMT", pl.Int64),      # @032 APRVAMT 11.
    (43, 11, "ACCTNO", pl.Int64),       # @043 ACCTNO 11.
    (54, 11, "TOTLMTAMT", pl.Int64),    # @054 TOTLMTAMT 11.
    (65, 11, "LASTMNTDT", pl.Int64),    # @065 LASTMNTDT 11.
    (76, 3, "LMTID", pl.Int64),         # @076 LMTID 3.
    (79, 11, "LMTAMT", pl.Int64),       # @079 LMTAMT 11.
    (90, 11, "LMTSTARTDT", pl.Int64),   # @090 LMTSTARTDT 11.
    (101, 11, "LMTENDDT", pl.Int64),    # @101 LMTENDDT 11.
    (112, 3, "LMTTERM", pl.Int64),      # @112 LMTTERM 3.
    (115, 1, "LMTTERMID", pl.Utf8),     # @115 LMTTERMID $1.
    (116, 1, "LMTPAIDIND", pl.Utf8),    # @116 LMTPAIDIND $1.
    (117, 11, "COLL1", pl.Int64),       # @117 COLL1 11.
    (128, 11, "COLL2", pl.Int64),       # @128 COLL2 11.
    (139, 11, "COLL3", pl.Int64),       # @139 COLL3 11.
    (150, 11, "COLL4", pl.Int64),       # @150 COLL4 11.
    (161, 11, "COLL5", pl.Int64),       # @161 COLL5 11.
    (172, 11, "COLL6", pl.Int64),       # @172 COLL6 11.
    (183, 11, "COLL7", pl.Int64),       # @183 COLL7 11.
    (194, 11, "COLL8", pl.Int64),       # @194 COLL8 11.
    (205, 11, "COLL9", pl.Int64),       # @205 COLL9 11.
    (216, 11, "COLL10", pl.Int64),      # @216 COLL10 11.
]

# Check if text file exists
if not DPAA_TXT.exists():
    raise FileNotFoundError(f"Text file not found: {DPAA_TXT}")

print(f"Reading from text file: {DPAA_TXT}")

# Read the file as fixed-width format
print("\n--- Reading fixed-width file based on SAS INPUT statement ---")

# Read all lines from the file
with open(DPAA_TXT, 'r') as f:
    lines = f.readlines()

print(f"Total lines in file: {len(lines)}")

# Parse each line according to the fixed-width specifications
parsed_data = []
error_count = 0

for line_num, line in enumerate(lines, 1):
    line = line.rstrip('\n\r')
    
    if not line.strip():  # Skip empty lines
        continue
    
    # Ensure line is long enough (maximum position is 216+11-1 = 226)
    if len(line) < 226:
        print(f"Warning: Line {line_num} is too short ({len(line)} chars), padding with spaces")
        line = line.ljust(226, ' ')
    
    row = {}
    valid_row = True
    
    for start_pos, length, col_name, dtype in columns:
        # Convert from SAS 1-based position to Python 0-based index
        start_idx = start_pos - 1
        end_idx = start_idx + length
        
        # Extract the field
        value = line[start_idx:end_idx].strip()
        
        # Convert to appropriate type
        if value == '' or value == ' ':
            row[col_name] = None
        else:
            try:
                if dtype == pl.Int64:
                    row[col_name] = int(value)
                else:  # Utf8
                    row[col_name] = value
            except ValueError as e:
                print(f"Error converting line {line_num}, column {col_name}, value '{value}': {e}")
                row[col_name] = None
                valid_row = False
    
    if valid_row:
        parsed_data.append(row)
    else:
        error_count += 1

print(f"\nSuccessfully parsed {len(parsed_data)} rows")
if error_count > 0:
    print(f"Errors encountered: {error_count}")

if parsed_data:
    # Create DataFrame
    df = pl.DataFrame(parsed_data)
    
    print(f"\nDataFrame shape: {df.height} rows × {df.width} columns")
    print("\nColumn names:")
    for col in df.columns:
        print(f"  - {col}")
    
    print("\nFirst 3 rows of parsed data:")
    print(df.head(3))
    
    # Show data types
    print("\nData types:")
    for col, dtype in zip(df.columns, df.dtypes):
        print(f"  {col}: {dtype}")
    
    # Write to both Parquet and CSV
    parquet_path = OUTPUT_DIR / "LMTDET.parquet"
    csv_path = OUTPUT_DIR / "LMTDET.csv"
    
    df.write_parquet(parquet_path)
    df.write_csv(csv_path)
    
    print(f"\nWritten to Parquet: {parquet_path}")
    print(f"Written to CSV: {csv_path}")
    
    # Display some statistics
    print("\n--- Data Statistics ---")
    for col in df.columns:
        non_null = df[col].null_count()
        print(f"{col}: {df.height - non_null}/{df.height} non-null values")
    
    # Show sample of first row to verify parsing
    if df.height > 0:
        print("\n--- First row sample (verification) ---")
        first_row = df.row(0)
        for i, col in enumerate(df.columns):
            if first_row[i] is not None:
                print(f"{col}: {first_row[i]}")
    
else:
    print("No data could be parsed successfully")
    # Create empty DataFrame with correct columns
    empty_data = {col_name: [] for _, _, col_name, _ in columns}
    df = pl.DataFrame(empty_data)
    
    parquet_path = OUTPUT_DIR / "LMTDET.parquet"
    csv_path = OUTPUT_DIR / "LMTDET.csv"
    
    df.write_parquet(parquet_path)
    df.write_csv(csv_path)
    
    print(f"Created empty DataFrame with correct schema")
    print(f"Written to Parquet: {parquet_path}")
    print(f"Written to CSV: {csv_path}")

# ---------- 2) DATA LMTDET.REPTDATE (KEEP=EXTDATE REPTDATE) ----------
# Calculate REPTDATE and EXTDATE exactly like SAS
today = date.today()
REPTDATE = today - timedelta(days=1)  # TODAY() - 1

# Get date components
YYYY = REPTDATE.strftime("%Y")  # 4-digit year
MM = REPTDATE.strftime("%m")    # 2-digit month
DD = REPTDATE.strftime("%d")    # 2-digit day

# Calculate DAY1 = first day of the year of REPTDATE
DAY1 = date(REPTDATE.year, 1, 1)

# Calculate DAYS = TODAY() - DAY1 (using actual today, not REPTDATE)
DAYS = (today - DAY1).days

# Create TEMPDATE = COMPRESS(MM||DD||YYYY||DAYS, ' ')
TEMPDATE = f"{MM}{DD}{YYYY}{DAYS}"

# Convert to numeric (EXTDATE = TEMPDATE * 1)
EXTDATE = int(TEMPDATE)

# Create REPTDATE DataFrame
reptdate_df = pl.DataFrame({
    "EXTDATE": [EXTDATE],
    "REPTDATE": [REPTDATE]
})

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
print(f"  (Format: MMDDYYYY + days since Jan 1)")

# Verify EXTDATE calculation matches SAS logic
print(f"\n--- EXTDATE breakdown ---")
print(f"MM: {MM}")
print(f"DD: {DD}")
print(f"YYYY: {YYYY}")
print(f"DAYS (today - Jan 1): {DAYS}")
print(f"TEMPDATE string: {TEMPDATE}")
print(f"EXTDATE numeric: {EXTDATE}")
