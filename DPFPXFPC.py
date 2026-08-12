import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
import polars as pl
import datetime
import duckdb

# -----------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------
DPADDR_FILE = "DPADDR.txt"              # Input fixed-width file (ASCII)
OUTPUT_PARQUET = "ADDR_SAVINGS.parquet" # Final output parquet
OUTPUT_CSV = "ADDR_SAVINGS.csv"         # Final output CSV

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

# Helper functions
def unpack_packed_decimal_int(byte_array):
    """Convert IBM packed decimal bytes to integer - always returns int"""
    if byte_array is None or len(byte_array) == 0:
        return 0
    
    hex_str = byte_array.hex().upper()
    
    if all(b == 0 for b in byte_array):
        return 0
    
    sign = hex_str[-1]
    digits = hex_str[:-1]
    
    if sign not in ('C', 'D', 'F', 'A', 'B', 'E') or not digits:
        try:
            return int(byte_array.hex(), 16)
        except:
            return 0
    
    try:
        value = int(digits)
        if sign in ('D', 'B'):
            value = -value
        return value
    except:
        return 0

def unpack_packed_decimal_float(byte_array, decimals=2):
    """Convert IBM packed decimal bytes to float - always returns float"""
    if byte_array is None or len(byte_array) == 0:
        return 0.0
    
    hex_str = byte_array.hex().upper()
    
    if all(b == 0 for b in byte_array):
        return 0.0
    
    sign = hex_str[-1]
    digits = hex_str[:-1]
    
    if sign not in ('C', 'D', 'F', 'A', 'B', 'E') or not digits:
        try:
            return float(int(byte_array.hex(), 16))
        except:
            return 0.0
    
    try:
        value = int(digits)
        if sign in ('D', 'B'):
            value = -value
        return float(value) / (10 ** decimals)
    except:
        return 0.0

def clean_string(byte_array):
    """Clean and decode ASCII string, removing control characters"""
    if byte_array is None or len(byte_array) == 0:
        return ""
    
    cleaned = bytes(b for b in byte_array if b >= 32 or b in (10, 13))
    
    try:
        text = cleaned.decode("ascii").strip()
    except:
        try:
            text = cleaned.decode("latin-1").strip()
        except:
            return ""
    
    text = text.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
    text = ' '.join(text.split())
    
    return text

# Convert string columns first
print("Converting string columns...")
char_cols = [spec[0] for spec in column_specs if spec[3] == "char"]

# Process strings in small batches to avoid memory issues
batch_size = 50000
total_batches = (num_records + batch_size - 1) // batch_size

# Create empty DataFrame with correct schema
df_processed = None

for batch_idx in range(total_batches):
    start_idx = batch_idx * batch_size
    end_idx = min(start_idx + batch_size, num_records)
    
    batch_dict = {}
    for col_name in data_dict:
        batch_dict[col_name] = data_dict[col_name][start_idx:end_idx]
    
    batch_df = pl.DataFrame(batch_dict)
    
    # Convert string columns
    for col in char_cols:
        batch_df = batch_df.with_columns(
            pl.col(col).map_elements(clean_string, return_dtype=pl.Utf8)
        )
    
    if df_processed is None:
        df_processed = batch_df
    else:
        df_processed = pl.concat([df_processed, batch_df])
    
    if (batch_idx + 1) % 10 == 0:
        print(f"  Processed {end_idx:,}/{num_records:,} records")

print("String conversion complete!")

# Convert numeric columns
print("Converting integer columns...")
pd_cols = [spec[0] for spec in column_specs if spec[3] == "pd"]

for col in pd_cols:
    df_processed = df_processed.with_columns(
        pl.col(col).map_elements(unpack_packed_decimal_int, return_dtype=pl.Int64)
    )

print("Converting decimal columns...")
pd2_cols = [spec[0] for spec in column_specs if spec[3] == "pd2"]

for col in pd2_cols:
    df_processed = df_processed.with_columns(
        pl.col(col).map_elements(unpack_packed_decimal_float, return_dtype=pl.Float64)
    )

print("Conversion complete!\n")

# -----------------------------------------------------------
# STEP 3: Write output files
# -----------------------------------------------------------
print("=" * 60)
print("STEP 3: Writing output files...")
print("=" * 60)

arrow_table = df_processed.to_arrow()

pq.write_table(arrow_table, OUTPUT_PARQUET)
print(f"✓ Parquet saved: {OUTPUT_PARQUET}")

csv.write_csv(arrow_table, OUTPUT_CSV)
print(f"✓ CSV saved: {OUTPUT_CSV}")

print(f"\nREPTDATE (yesterday): {REPTDATE}\n")

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

print("\nSample records with valid ACCTNO:")
sample_df = con.execute("""
    SELECT 
        BANKNO, APPCODE, ACCTNO, BRANCH,
        NAME, OLDIC, NEWIC,
        LEDGBAL, CURBAL,
        OPENDATE, PRODUCT
    FROM savings 
    WHERE ACCTNO > 0
    LIMIT 5
""").fetch_df()
print(sample_df)

print("\nStatistics:")
stats = con.execute("""
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN ACCTNO > 0 THEN 1 END) as valid_accounts,
        COUNT(CASE WHEN NAME != '' THEN 1 END) as with_names,
        AVG(CASE WHEN LEDGBAL > 0 THEN LEDGBAL END) as avg_positive_balance
    FROM savings
""").fetch_df()
print(stats)

print("\n" + "=" * 60)
print("PROGRAM COMPLETED SUCCESSFULLY!")
print("=" * 60)
