import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
import polars as pl
import datetime
import duckdb

# -----------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------
DPADDR_FILE = "DPADDR.txt"              # Input fixed-width file (ASCII, not EBCDIC!)
TEMP_PARQUET = "DPADDR_TEMP.parquet"    # Temporary parquet with raw bytes
OUTPUT_PARQUET = "ADDR_SAVINGS.parquet" # Final output parquet
OUTPUT_CSV = "ADDR_SAVINGS.csv"         # Final output CSV

REPTDATE = datetime.date.today() - datetime.timedelta(days=1)

# -----------------------------------------------------------
# STEP 1: Read and store raw bytes
# -----------------------------------------------------------
print("=" * 60)
print("STEP 1: Reading fixed-width file (ASCII format)...")
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

with open(DPADDR_FILE, "rb") as f:
    raw_data = f.read()

record_length = 428
num_records = len(raw_data) // record_length
print(f"File size: {len(raw_data):,} bytes")
print(f"Record length: {record_length} bytes")
print(f"Number of records: {num_records:,}")

# Verify it's ASCII by checking first record's name field
first_name_hex = raw_data[13:37].hex()
print(f"\nFirst record NAME hex: {first_name_hex}")
print(f"First record NAME ASCII: {raw_data[13:37].decode('ascii', errors='ignore').strip()}")
print("File is in ASCII format, not EBCDIC!\n")

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
print(f"Step 1 complete!\n")

# -----------------------------------------------------------
# STEP 2: Convert Packed Decimal and decode ASCII strings
# -----------------------------------------------------------
print("=" * 60)
print("STEP 2: Converting data types...")
print("=" * 60)

# Read the raw parquet file
df = pl.read_parquet(TEMP_PARQUET)

# Helper function for packed decimal conversion
def unpack_packed_decimal(byte_array, decimals=0):
    """Convert IBM packed decimal bytes to integer/float"""
    if byte_array is None or len(byte_array) == 0:
        return 0.0 if decimals > 0 else 0
    
    hex_str = byte_array.hex().upper()
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

# Helper function for ASCII string conversion
def ascii_decode(byte_array):
    """Convert bytes to ASCII string"""
    if byte_array is None or len(byte_array) == 0:
        return ""
    try:
        return byte_array.decode("ascii").strip()
    except:
        return byte_array.decode("latin-1", errors="ignore").strip()

# Process string columns (ASCII)
print("Converting string columns (ASCII)...")
string_cols = ["APPCODE", "NAME", "OLDIC", "OPENIND", "PURPOSE", "RACE", 
               "USER3", "DORMANT", "DEPTYPE", "NEWIC", "NAMETYPE",
               "NAMELN1", "NAMELN2", "NAMELN3", "NAMELN4", 
               "NAMELN5", "NAMELN6", "NAMELN7", "NAMELN8"]

for col in string_cols:
    df = df.with_columns(
        pl.col(col).map_elements(ascii_decode, return_dtype=pl.Utf8)
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

arrow_table = df.to_arrow()
pq.write_table(arrow_table, OUTPUT_PARQUET)
print(f"✓ Parquet file saved: {OUTPUT_PARQUET}")

csv.write_csv(arrow_table, OUTPUT_CSV)
print(f"✓ CSV file saved: {OUTPUT_CSV}")

# -----------------------------------------------------------
# STEP 4: Validation
# -----------------------------------------------------------
print("\n" + "=" * 60)
print("STEP 4: Validating output...")
print("=" * 60)

con = duckdb.connect(database=":memory:")
con.register("savings", arrow_table)

row_count = con.execute("SELECT COUNT(*) FROM savings").fetchone()[0]
print(f"Total rows: {row_count:,}")
print(f"REPTDATE (yesterday): {REPTDATE}")

# Show better sample
print("\n" + "=" * 60)
print("SAMPLE DATA (first 10 rows with non-empty NAME):")
print("=" * 60)
sample_df = con.execute("""
    SELECT 
        BANKNO, 
        APPCODE, 
        ACCTNO, 
        BRANCH, 
        NAME,
        OLDIC,
        NEWIC,
        LEDGBAL, 
        CURBAL,
        YTDBAL,
        OPENDATE,
        PRODUCT,
        NAMETYPE
    FROM savings 
    WHERE NAME != ''
    LIMIT 10
""").fetch_df()
print(sample_df)

# Show non-empty names
non_empty = con.execute("SELECT COUNT(*) FROM savings WHERE NAME != '' AND NAME IS NOT NULL").fetchone()[0]
print(f"\nRecords with non-empty NAME: {non_empty:,}")

print("\nSample names (first 30 non-empty):")
names_df = con.execute("""
    SELECT DISTINCT NAME 
    FROM savings 
    WHERE NAME != '' 
    ORDER BY NAME
    LIMIT 30
""").fetch_df()
print(names_df)

# Show some statistics
print("\n" + "=" * 60)
print("DATA STATISTICS:")
print("=" * 60)
stats = con.execute("""
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT NAME) as unique_names,
        COUNT(DISTINCT ACCTNO) as unique_accounts,
        COUNT(DISTINCT NEWIC) as unique_newic,
        AVG(LEDGBAL) as avg_ledger_balance,
        SUM(CASE WHEN LEDGBAL > 0 THEN 1 ELSE 0 END) as accounts_with_balance
    FROM savings
""").fetch_df()
print(stats)

print("\n" + "=" * 60)
print("PROGRAM COMPLETED SUCCESSFULLY!")
print("=" * 60)
