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

# Try different EBCDIC code pages (common ones for Asian/Malaysian systems)
EBCDIC_ENCODINGS = [
    "cp037",   # US/Canada
    "cp1140",  # US/Canada with Euro
    "cp273",   # Germany/Austria
    "cp277",   # Denmark/Norway
    "cp278",   # Finland/Sweden
    "cp280",   # Italy
    "cp284",   # Spain/Latin America
    "cp285",   # UK
    "cp297",   # France
    "cp500",   # International
    "cp871",   # Iceland
    "cp1047",  # Latin-1/Open Systems
    "cp1148",  # International with Euro
    "cp424",   # Hebrew
    "cp875",   # Greek
    "cp1026",  # Turkish
    "cp870",   # Eastern Europe (Latin-2)
    "cp1097",  # Farsi
    "cp01140", # Another US variant
    "cp01141", # German variant
    "cp01148", # International variant
]

# -----------------------------------------------------------
# STEP 1: Detect encoding by reading first record
# -----------------------------------------------------------
print("=" * 60)
print("STEP 0: Detecting EBCDIC encoding...")
print("=" * 60)

with open(DPADDR_FILE, "rb") as f:
    raw_data = f.read()

record_length = 428
# Get first record for encoding detection
sample_record = raw_data[:record_length]

# Extract name field (bytes 13-37)
name_bytes = sample_record[13:37]

print("Raw hex of NAME field:")
print(name_bytes.hex())
print()

# Try different encodings and show results
valid_encodings = {}
for encoding in EBCDIC_ENCODINGS:
    try:
        decoded = name_bytes.decode(encoding)
        # Count printable characters
        printable = sum(1 for c in decoded if c.isprintable() or c.isspace())
        ratio = printable / len(decoded) if decoded else 0
        if ratio > 0.3:  # At least 30% printable
            valid_encodings[encoding] = decoded.strip()
            print(f"{encoding}: '{decoded.strip()}' (printable ratio: {ratio:.2%})")
    except:
        pass

print()

# Let user choose or auto-select best encoding
if not valid_encodings:
    print("WARNING: No good encoding found. Will try common ones with fallback.")
    selected_encoding = "cp037"  # Default fallback
else:
    # Find encoding with highest printable ratio
    # Try Malaysian/Asian specific encodings first
    preferred = ["cp037", "cp1140", "cp1047", "cp500", "cp1148"]
    for enc in preferred:
        if enc in valid_encodings:
            selected_encoding = enc
            break
    else:
        selected_encoding = list(valid_encodings.keys())[0]
    
    print(f"✓ Selected encoding: {selected_encoding}")
    print()

# -----------------------------------------------------------
# STEP 2: Read EBCDIC fixed-width file and store as raw bytes in Parquet
# -----------------------------------------------------------
print("=" * 60)
print("STEP 2: Reading EBCDIC fixed-width file...")
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
print(f"Step 2 complete!\n")

# -----------------------------------------------------------
# STEP 3: Convert EBCDIC and Packed Decimal to readable format
# -----------------------------------------------------------
print("=" * 60)
print(f"STEP 3: Converting using encoding '{selected_encoding}'...")
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

# Helper function for EBCDIC to ASCII
def ebcdic_to_ascii(byte_array):
    """Convert EBCDIC bytes to ASCII string"""
    if byte_array is None or len(byte_array) == 0:
        return ""
    try:
        return byte_array.decode(selected_encoding).strip()
    except:
        # Fallback: try cp037, then latin-1
        try:
            return byte_array.decode("cp037").strip()
        except:
            return byte_array.decode("latin-1", errors="ignore").strip()

# Process string columns
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
# STEP 4: Write final output files
# -----------------------------------------------------------
print("=" * 60)
print("STEP 4: Writing output files...")
print("=" * 60)

arrow_table = df.to_arrow()
pq.write_table(arrow_table, OUTPUT_PARQUET)
print(f"✓ Parquet file saved: {OUTPUT_PARQUET}")

csv.write_csv(arrow_table, OUTPUT_CSV)
print(f"✓ CSV file saved: {OUTPUT_CSV}")

# -----------------------------------------------------------
# STEP 5: Validation
# -----------------------------------------------------------
print("\n" + "=" * 60)
print("STEP 5: Validating output...")
print("=" * 60)

con = duckdb.connect(database=":memory:")
con.register("savings", arrow_table)

row_count = con.execute("SELECT COUNT(*) FROM savings").fetchone()[0]
print(f"Total rows: {row_count:,}")
print(f"REPTDATE (yesterday): {REPTDATE}")
print(f"Using encoding: {selected_encoding}")

# Show better sample with proper column names
print("\nSample data (first 10 rows, key fields):")
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
        OPENDATE,
        NAMETYPE
    FROM savings 
    LIMIT 10
""").fetch_df()
print(sample_df)

# Check for non-null records with actual names
non_empty = con.execute("""
    SELECT COUNT(*) 
    FROM savings 
    WHERE NAME != '' AND NAME IS NOT NULL
""").fetchone()[0]
print(f"\nRecords with non-empty NAME: {non_empty:,}")

# Show some actual names
print("\nSample names (first 20 non-empty):")
names_df = con.execute("""
    SELECT DISTINCT NAME 
    FROM savings 
    WHERE NAME != '' 
    LIMIT 20
""").fetch_df()
print(names_df)

print("\n" + "=" * 60)
print("PROGRAM COMPLETED SUCCESSFULLY!")
print("=" * 60)
