import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
import polars as pl
import datetime
import duckdb

# -----------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------
DPADDR_FILE = "DPADDR.txt"              # Input EBCDIC fixed-width file
TEMP_PARQUET = "DPADDR_TEMP.parquet"    # Temporary parquet with raw bytes
OUTPUT_PARQUET = "ADDR_SAVINGS.parquet" # Final output parquet
OUTPUT_CSV = "ADDR_SAVINGS.csv"         # Final output CSV

REPTDATE = datetime.date.today() - datetime.timedelta(days=1)

# -----------------------------------------------------------
# STEP 1: Read EBCDIC fixed-width file and store as raw bytes in Parquet
# -----------------------------------------------------------
print("=" * 60)
print("STEP 1: Reading EBCDIC fixed-width file...")
print("=" * 60)

# Define the exact byte positions and lengths (0-indexed)
column_specs = [
    ("BANKNO", 0, 2),
    ("APPCODE", 2, 1),
    ("ACCTNO", 3, 6),
    ("BRANCH", 9, 4),
    ("NAME", 13, 24),
    ("OLDIC", 37, 11),
    ("OPENDATE", 48, 6),
    ("PRODUCT", 54, 2),
    ("OPENIND", 56, 1),
    ("PURPOSE", 57, 1),
    ("RACE", 58, 1),
    ("USER3", 59, 1),
    ("DORMANT", 60, 1),
    ("DEPTYPE", 61, 1),
    ("BDATE", 62, 6),
    ("DEPTNO", 68, 3),
    ("NEWIC", 71, 12),
    ("LEDGBAL", 83, 7),
    ("CURBAL", 90, 7),
    ("YTDBAL", 97, 8),
    ("YTDDAYS", 105, 2),
    ("NAMETYPE", 107, 1),
    ("NAMELN1", 108, 40),
    ("NAMELN2", 148, 40),
    ("NAMELN3", 188, 40),
    ("NAMELN4", 228, 40),
    ("NAMELN5", 268, 40),
    ("NAMELN6", 308, 40),
    ("NAMELN7", 348, 40),
    ("NAMELN8", 388, 40),
]

# Read binary file
with open(DPADDR_FILE, "rb") as f:
    raw_data = f.read()

# Calculate record length (last column: 388 + 40 = 428)
record_length = 428
num_records = len(raw_data) // record_length
print(f"File size: {len(raw_data):,} bytes")
print(f"Record length: {record_length} bytes")
print(f"Number of records: {num_records:,}")

# Parse fixed-width data into raw bytes
print("Parsing fixed-width records...")
data_dict = {}
for col_name, start, length in column_specs:
    data_dict[col_name] = []

for i in range(num_records):
    start_pos = i * record_length
    record = raw_data[start_pos:start_pos + record_length]
    
    for col_name, start, length in column_specs:
        data_dict[col_name].append(record[start:start+length])

# Create Polars DataFrame with binary columns
df_raw = pl.DataFrame(data_dict)

# Save raw bytes to temporary parquet
print(f"Saving raw bytes to {TEMP_PARQUET}...")
df_raw.write_parquet(TEMP_PARQUET)
print(f"Step 1 complete! Raw data saved to {TEMP_PARQUET}\n")

# -----------------------------------------------------------
# STEP 2: Convert EBCDIC and Packed Decimal to readable format
# -----------------------------------------------------------
print("=" * 60)
print("STEP 2: Converting EBCDIC and Packed Decimal...")
print("=" * 60)

# Read the raw parquet file
df = pl.read_parquet(TEMP_PARQUET)

# Helper function for packed decimal conversion
def unpack_packed_decimal(byte_array, decimals=0):
    """Convert IBM packed decimal bytes to integer/float"""
    if byte_array is None or len(byte_array) == 0:
        return 0.0 if decimals > 0 else 0
    
    # Convert to hex
    hex_str = byte_array.hex().upper()
    
    # Get sign from last nibble (C=positive, D=negative, F=unsigned)
    sign = hex_str[-1]
    digits = hex_str[:-1]
    
    if sign not in ('C', 'D', 'F') or not digits:
        return 0.0 if decimals > 0 else 0
    
    try:
        value = int(digits)
        if sign == 'D':
            value = -value
        if decimals > 0:
            value = value / (10 ** decimals)
        return value
    except:
        return 0.0 if decimals > 0 else 0

# Helper function for EBCDIC to ASCII conversion
def ebcdic_to_ascii(byte_array):
    """Convert EBCDIC bytes to ASCII string"""
    if byte_array is None or len(byte_array) == 0:
        return ""
    try:
        return byte_array.decode("cp037").strip()
    except:
        return byte_array.decode("latin-1", errors="ignore").strip()

# Process string columns (EBCDIC to ASCII)
print("Converting EBCDIC string columns...")
string_cols = ["APPCODE", "NAME", "OLDIC", "OPENIND", "PURPOSE", "RACE", 
               "USER3", "DORMANT", "DEPTYPE", "NEWIC", "NAMETYPE",
               "NAMELN1", "NAMELN2", "NAMELN3", "NAMELN4", 
               "NAMELN5", "NAMELN6", "NAMELN7", "NAMELN8"]

for col in string_cols:
    df = df.with_columns(
        pl.col(col).map_elements(ebcdic_to_ascii, return_dtype=pl.Utf8)
    )

# Process packed decimal columns (no decimals)
print("Converting packed decimal columns (integers)...")
pd_cols = ["BANKNO", "ACCTNO", "BRANCH", "OPENDATE", "PRODUCT", 
           "BDATE", "DEPTNO", "YTDDAYS"]

for col in pd_cols:
    df = df.with_columns(
        pl.col(col).map_elements(lambda x: unpack_packed_decimal(x, 0), return_dtype=pl.Int64)
    )

# Process packed decimal columns (2 decimal places)
print("Converting packed decimal columns (decimals)...")
pd2_cols = ["LEDGBAL", "CURBAL", "YTDBAL"]

for col in pd2_cols:
    df = df.with_columns(
        pl.col(col).map_elements(lambda x: unpack_packed_decimal(x, 2), return_dtype=pl.Float64)
    )

print("Conversion complete!\n")

# -----------------------------------------------------------
# STEP 3: Write final output files
# -----------------------------------------------------------
print("=" * 60)
print("STEP 3: Writing output files...")
print("=" * 60)

# Convert to Arrow table
arrow_table = df.to_arrow()

# Write to Parquet
pq.write_table(arrow_table, OUTPUT_PARQUET)
print(f"✓ Parquet file saved: {OUTPUT_PARQUET}")

# Write to CSV
csv.write_csv(arrow_table, OUTPUT_CSV)
print(f"✓ CSV file saved: {OUTPUT_CSV}")

# -----------------------------------------------------------
# STEP 4: Validation with DuckDB
# -----------------------------------------------------------
print("\n" + "=" * 60)
print("STEP 4: Validating output...")
print("=" * 60)

con = duckdb.connect(database=":memory:")
con.register("savings", arrow_table)

row_count = con.execute("SELECT COUNT(*) FROM savings").fetchone()[0]
print(f"Total rows: {row_count:,}")
print(f"REPTDATE (yesterday): {REPTDATE}")
print("\nSample data (first 5 rows):")
print(con.execute("""
    SELECT 
        BANKNO, APPCODE, ACCTNO, BRANCH, 
        LEFT(NAME, 20) as NAME_PART,
        LEDGBAL, CURBAL, YTDBAL
    FROM savings 
    LIMIT 5
""").fetch_df())

print("\nColumn data types:")
print(con.execute("DESCRIBE savings").fetch_df())

print("\n" + "=" * 60)
print("PROGRAM COMPLETED SUCCESSFULLY!")
print("=" * 60)
