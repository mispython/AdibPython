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
# Define column names based on the SAS input format
# Based on the example data, these appear to be the actual columns
req = [
    "AANO", "APRVDT", "APRVAMT", "ACCTNO", "TOTLMTAMT", "LASTMNTDT", "LMTID", "LMTAMT",
    "LMTSTARTDT", "LMTENDDT", "LMTTERM", "LMTTERMID", "LMTPAIDIND",
    "COLL1", "COLL2", "COLL3", "COLL4", "COLL5", "COLL6", "COLL7", "COLL8", "COLL9", "COLL10"
]

# Check if text file exists
if not DPAA_TXT.exists():
    raise FileNotFoundError(f"Text file not found: {DPAA_TXT}")

print(f"Reading from text file: {DPAA_TXT}")

# Read the file as fixed-width format
# Based on the example data, let's analyze the structure
print("\n--- Analyzing file structure ---")

# Read a few lines to determine column positions
with open(DPAA_TXT, 'r') as f:
    lines = f.readlines()

print(f"Total lines in file: {len(lines)}")
print(f"\nFirst 2 rows of raw data:")
for i in range(min(2, len(lines))):
    print(f"Row {i+1}: {lines[i].rstrip()}")
    print(f"  Length: {len(lines[i].rstrip())} characters")

# Based on the example, the data appears to have the following structure:
# Column 1: AANO (e.g., "AKH/000004/19") - variable length
# Then spaces, then a long string of concatenated fields

print("\n--- Parsing as space-delimited to see columns ---")

# First, try splitting by whitespace to see how many columns we get
sample_line = lines[0].strip()
split_by_space = sample_line.split()
print(f"\nSplitting first row by whitespace gives {len(split_by_space)} columns:")
for i, col in enumerate(split_by_space[:10]):  # Show first 10 columns
    print(f"  Col {i+1}: '{col}'")

# It looks like the data is in a fixed format where:
# - First field is AANO (account number)
# - Then a long string containing multiple concatenated fields
# This suggests the original SAS INPUT statement uses formatted input

print("\n--- Based on SAS pattern, parsing as fixed-width fields ---")

# Define column positions based on typical SAS layout
# From the example, we need to parse the long string into specific positions
# Let me parse the second field which contains multiple concatenated values

def parse_sas_line(line):
    """Parse a line from the SAS fixed-width format"""
    line = line.rstrip('\n\r')
    
    # The first field (AANO) is separated by space
    parts = line.split()
    
    if len(parts) < 2:
        return None
    
    aano = parts[0]  # First field
    
    # The rest is a concatenated string of fixed-width fields
    data_str = parts[1] if len(parts) > 1 else ""
    
    # Define field widths based on the example data
    # This needs to be adjusted based on your actual SAS INPUT statement
    field_widths = {
        'APRVDT': 9,      # Date format: DDMMMYYYY? Actually from example: 040520190950...
        'APRVAMT': 11,    # Amount field
        'ACCTNO': 11,     # Account number
        'TOTLMTAMT': 11,  # Total limit amount
        'LASTMNTDT': 9,   # Last maintain date
        # Continue based on your SAS INPUT statement
    }
    
    result = {'AANO': aano}
    
    # Parse the concatenated string based on positions
    # This is a simplification - you'll need the exact SAS INPUT format
    pos = 0
    for field, width in field_widths.items():
        if pos + width <= len(data_str):
            result[field] = data_str[pos:pos+width].strip()
        else:
            result[field] = None
        pos += width
    
    return result

# For now, let's just read the entire file and show what we have
print("\n--- Reading all data as space-separated to see actual columns ---")

# Read all lines and split by whitespace
data_rows = []
with open(DPAA_TXT, 'r') as f:
    for line in f:
        if line.strip():  # Skip empty lines
            # Split by whitespace (handles multiple spaces)
            cols = line.strip().split()
            data_rows.append(cols)

print(f"Found {len(data_rows)} rows of data")

if data_rows:
    # Find maximum number of columns
    max_cols = max(len(row) for row in data_rows)
    print(f"Maximum columns in any row: {max_cols}")
    
    # Create column names dynamically
    column_names = ['COL_' + str(i+1) for i in range(max_cols)]
    
    # Create DataFrame
    df = pl.DataFrame(data_rows, schema=column_names, orient='row')
    
    print(f"\nCreated DataFrame with {df.height} rows and {df.width} columns")
    print("\nFirst 5 columns of first 3 rows:")
    print(df.select(['COL_1', 'COL_2', 'COL_3', 'COL_4', 'COL_5']).head(3))
    
    # Rename columns based on expected names
    # COL_1 appears to be AANO
    # COL_2 contains all the other concatenated data
    
    # Let's create a proper DataFrame with the actual structure
    print("\n--- Creating structured DataFrame ---")
    
    structured_data = []
    for row in data_rows:
        if len(row) >= 2:
            # First column is AANO
            aano = row[0]
            # Second column contains concatenated fields
            concat_fields = row[1]
            
            # For now, let's just keep them as separate columns
            # You'll need to parse concat_fields based on your SAS INPUT statement
            structured_data.append({
                'AANO': aano,
                'RAW_DATA': concat_fields,
                'EXTRA_COLUMNS': ' '.join(row[2:]) if len(row) > 2 else None
            })
    
    if structured_data:
        df_structured = pl.DataFrame(structured_data)
        print(f"\nStructured DataFrame with {df_structured.height} rows")
        print("\nSample of parsed data:")
        print(df_structured.head(5))
        
        # Write the structured data
        parquet_path = OUTPUT_DIR / "LMTDET.parquet"
        csv_path = OUTPUT_DIR / "LMTDET.csv"
        
        df_structured.write_parquet(parquet_path)
        df_structured.write_csv(csv_path)
        
        print(f"\nWritten to Parquet: {parquet_path}")
        print(f"Written to CSV: {csv_path}")
        
        # Also write the raw split data for debugging
        df_raw = pl.DataFrame(data_rows, schema=column_names, orient='row')
        raw_parquet = OUTPUT_DIR / "LMTDET_RAW.parquet"
        raw_csv = OUTPUT_DIR / "LMTDET_RAW.csv"
        df_raw.write_parquet(raw_parquet)
        df_raw.write_csv(raw_csv)
        print(f"Written raw data to: {raw_parquet} and {raw_csv}")

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
print("DONE - Files created:")
print("="*50)
print(f"\nOutput directory: {OUTPUT_DIR}")
print(f"\nREPTDATE: {REPTDATE}")
print(f"EXTDATE: {EXTDATE}")
