import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
import polars as pl
import datetime
import duckdb
import re

# -----------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------
DPADDR_FILE = "DPADDR.txt"              # Input fixed-width file (ASCII)
OUTPUT_PARQUET = "ADDR_SAVINGS.parquet" # Final output parquet
OUTPUT_CSV = "ADDR_SAVINGS.csv"         # Final output CSV
REPTDATE_PARQUET = "ADDR_REPTDATE.parquet" # REPTDATE dataset

REPTDATE = datetime.date.today() - datetime.timedelta(days=1)

# -----------------------------------------------------------
# STEP 1: Read fixed-width ASCII file
# -----------------------------------------------------------
print("=" * 60)
print("STEP 1: Reading fixed-width file...")
print("=" * 60)

# Define column specifications
column_specs = [
    ("BANKNO", 0, 2, "pd"),
    ("APPCODE", 2, 1, "char"),
    ("ACCTNO", 3, 6, "pd"),
    ("BRANCH", 9, 4, "pd"),
    ("NAME", 13, 24, "char"),
    ("OLDIC", 37, 11, "char"),
    ("OPENDATE", 48, 6, "pd"),
    ("PRODUCT", 54, 2, "pd"),
    ("OPENIND", 56, 1, "char"),
    ("PURPOSE", 57, 1, "char"),
    ("RACE", 58, 1, "char"),
    ("USER3", 59, 1, "char"),
    ("DORMANT", 60, 1, "char"),
    ("DEPTYPE", 61, 1, "char"),
    ("BDATE", 62, 6, "pd"),
    ("DEPTNO", 68, 3, "pd"),
    ("NEWIC", 71, 12, "char"),
    ("LEDGBAL", 83, 7, "pd2"),
    ("CURBAL", 90, 7, "pd2"),
    ("YTDBAL", 97, 8, "pd2"),
    ("YTDDAYS", 105, 2, "pd"),
    ("NAMETYPE", 107, 1, "char"),
    ("NAMELN1", 108, 40, "char"),
    ("NAMELN2", 148, 40, "char"),
    ("NAMELN3", 188, 40, "char"),
    ("NAMELN4", 228, 40, "char"),
    ("NAMELN5", 268, 40, "char"),
    ("NAMELN6", 308, 40, "char"),
    ("NAMELN7", 348, 40, "char"),
    ("NAMELN8", 388, 40, "char"),
]

with open(DPADDR_FILE, "rb") as f:
    raw_data = f.read()

record_length = 428
num_records = len(raw_data) // record_length
print(f"File size: {len(raw_data):,} bytes")
print(f"Records: {num_records:,}")

# Parse all records
print("Parsing records...")
data_dict = {spec[0]: [] for spec in column_specs}

for i in range(num_records):
    start_pos = i * record_length
    record = raw_data[start_pos:start_pos + record_length]
    
    for col_name, start, length, dtype in column_specs:
        raw_bytes = record[start:start+length]
        data_dict[col_name].append(raw_bytes)

print("Parsing complete!\n")

# -----------------------------------------------------------
# STEP 2: Convert data types
# -----------------------------------------------------------
print("=" * 60)
print("STEP 2: Converting data types...")
print("=" * 60)

# Create DataFrame
df = pl.DataFrame(data_dict)

# Helper function for packed decimal
def unpack_packed_decimal(byte_array, decimals=0):
    """Convert IBM packed decimal bytes to integer/float"""
    if byte_array is None or len(byte_array) == 0:
        return 0.0 if decimals > 0 else 0
    
    hex_str = byte_array.hex().upper()
    
    # Check if all zeros
    if all(b == 0 for b in byte_array):
        return 0.0 if decimals > 0 else 0
    
    sign = hex_str[-1]
    digits = hex_str[:-1]
    
    # Valid packed decimal signs
    if sign not in ('C', 'D', 'F', 'A', 'B', 'E') or not digits:
        # Might not be packed decimal - try to interpret as plain bytes
        try:
            return int(byte_array.hex(), 16)
        except:
            return 0.0 if decimals > 0 else 0
    
    try:
        value = int(digits)
        if sign in ('D', 'B'):  # D and B are negative signs
            value = -value
        if decimals > 0:
            value = value / (10 ** decimals)
        return value
    except:
        return 0.0 if decimals > 0 else 0

# Helper function for cleaning strings
def clean_string(byte_array):
    """Clean and decode ASCII string, removing control characters"""
    if byte_array is None or len(byte_array) == 0:
        return ""
    
    # Remove null bytes and control characters (except newlines which we'll handle)
    cleaned = bytes(b for b in byte_array if b >= 32 or b in (10, 13))
    
    # Decode as ASCII
    try:
        text = cleaned.decode("ascii").strip()
    except:
        try:
            text = cleaned.decode("latin-1").strip()
        except:
            return ""
    
    # Replace \r\n with space
    text = text.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
    
    # Remove multiple spaces
    text = ' '.join(text.split())
    
    return text

# Convert string columns
print("Converting string columns...")
char_cols = [spec[0] for spec in column_specs if spec[3] == "char"]

for col in char_cols:
    df = df.with_columns(
        pl.col(col).map_elements(clean_string, return_dtype=pl.Utf8)
    )

# Convert packed decimal columns (integers)
print("Converting numeric columns...")
pd_cols = [spec[0] for spec in column_specs if spec[3] == "pd"]

for col in pd_cols:
    df = df.with_columns(
        pl.col(col).map_elements(lambda x: unpack_packed_decimal(x, 0), return_dtype=pl.Int64)
    )

# Convert packed decimal columns (2 decimal places)
pd2_cols = [spec[0] for spec in column_specs if spec[3] == "pd2"]

for col in pd2_cols:
    df = df.with_columns(
        pl.col(col).map_elements(lambda x: unpack_packed_decimal(x, 2), return_dtype=pl.Float64)
    )

print("Conversion complete!\n")

# -----------------------------------------------------------
# STEP 3: Write output files
# -----------------------------------------------------------
print("=" * 60)
print("STEP 3: Writing output files...")
print("=" * 60)

# Create ADDR.SAVINGS (main dataset)
arrow_table = df.to_arrow()
pq.write_table(arrow_table, OUTPUT_PARQUET)
print(f"✓ ADDR.SAVINGS Parquet: {OUTPUT_PARQUET}")

csv.write_csv(arrow_table, OUTPUT_CSV)
print(f"✓ ADDR.SAVINGS CSV: {OUTPUT_CSV}")

# Create ADDR.REPTDATE (equivalent to DATA ADDR.REPTDATE)
reptdate_df = pl.DataFrame({"REPTDATE": [REPTDATE.isoformat()]})
reptdate_table = reptdate_df.to_arrow()
pq.write_table(reptdate_table, REPTDATE_PARQUET)
print(f"✓ ADDR.REPTDATE Parquet: {REPTDATE_PARQUET}")
print(f"  REPTDATE = {REPTDATE}\n")

# -----------------------------------------------------------
# STEP 4: Validation
# -----------------------------------------------------------
print("=" * 60)
print("STEP 4: Validation...")
print("=" * 60)

con = duckdb.connect(database=":memory:")
con.register("savings", arrow_table)

row_count = con.execute("SELECT COUNT(*) FROM savings").fetchone()[0]
print(f"Total rows: {row_count:,}")
print(f"REPTDATE: {REPTDATE}")

# Show clean samples
print("\n" + "=" * 60)
print("SAMPLE - Records with valid ACCTNO (> 0):")
print("=" * 60)
sample_df = con.execute("""
    SELECT 
        BANKNO, APPCODE, ACCTNO, BRANCH,
        NAME, OLDIC, NEWIC,
        LEDGBAL, CURBAL,
        OPENDATE, PRODUCT
    FROM savings 
    WHERE ACCTNO > 0
    LIMIT 10
""").fetch_df()
print(sample_df)

# Statistics
print("\n" + "=" * 60)
print("STATISTICS:")
print("=" * 60)
stats = con.execute("""
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN ACCTNO > 0 THEN 1 END) as valid_accounts,
        COUNT(CASE WHEN NAME != '' THEN 1 END) as with_names,
        COUNT(DISTINCT ACCTNO) as unique_accounts,
        AVG(CASE WHEN LEDGBAL > 0 THEN LEDGBAL END) as avg_positive_balance,
        SUM(CASE WHEN CURBAL > 0 THEN 1 ELSE 0 END) as positive_balances
    FROM savings
""").fetch_df()
print(stats)

print("\n" + "=" * 60)
print("PROGRAM COMPLETED SUCCESSFULLY!")
print("=" * 60)
