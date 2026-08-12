import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
import polars as pl
import datetime
import duckdb

# -----------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------
DPADDR_FILE = "DPADDR.txt"
OUTPUT_PARQUET = "ADDR_SAVINGS.parquet"
OUTPUT_CSV = "ADDR_SAVINGS.csv"

REPTDATE = datetime.date.today() - datetime.timedelta(days=1)

print("=" * 60)
print("PROGRAM START")
print(f"REPTDATE: {REPTDATE}")
print("=" * 60)

# -----------------------------------------------------------
# STEP 1: Read and parse fixed-width file
# -----------------------------------------------------------
with open(DPADDR_FILE, "rb") as f:
    raw_data = f.read()

record_length = 428
num_records = len(raw_data) // record_length
print(f"\nRecords: {num_records:,}")

# Column positions (0-indexed)
columns = {
    "BANKNO":   (0, 2),
    "APPCODE":  (2, 3),
    "ACCTNO":   (3, 9),
    "BRANCH":   (9, 13),
    "NAME":     (13, 37),
    "OLDIC":    (37, 48),
    "OPENDATE": (48, 54),
    "PRODUCT":  (54, 56),
    "OPENIND":  (56, 57),
    "PURPOSE":  (57, 58),
    "RACE":     (58, 59),
    "USER3":    (59, 60),
    "DORMANT":  (60, 61),
    "DEPTYPE":  (61, 62),
    "BDATE":    (62, 68),
    "DEPTNO":   (68, 71),
    "NEWIC":    (71, 83),
    "LEDGBAL":  (83, 90),
    "CURBAL":   (90, 97),
    "YTDBAL":   (97, 105),
    "YTDDAYS":  (105, 107),
    "NAMETYPE": (107, 108),
    "NAMELN1":  (108, 148),
    "NAMELN2":  (148, 188),
    "NAMELN3":  (188, 228),
    "NAMELN4":  (228, 268),
    "NAMELN5":  (268, 308),
    "NAMELN6":  (308, 348),
    "NAMELN7":  (348, 388),
    "NAMELN8":  (388, 428),
}

# Parse records
print("Parsing records...")
data = {col: [] for col in columns}

for i in range(num_records):
    start = i * record_length
    record = raw_data[start:start + record_length]
    for col, (s, e) in columns.items():
        data[col].append(record[s:e])

print("Parsing complete!")

# -----------------------------------------------------------
# STEP 2: Convert data types
# -----------------------------------------------------------
print("\nConverting data types...")

# Packed decimal to integer
def pd_to_int(b):
    """Convert packed decimal bytes to integer"""
    if not b or len(b) == 0:
        return 0
    if all(x == 0 for x in b):
        return 0
    
    hex_str = b.hex().upper()
    sign_nibble = hex_str[-1]
    digits = hex_str[:-1]
    
    # Check if it looks like valid packed decimal
    if sign_nibble in ('A', 'B', 'C', 'D', 'E', 'F') and digits.isdigit():
        val = int(digits)
        if sign_nibble in ('B', 'D'):
            val = -val
        return val
    
    # Not packed decimal - try as plain bytes
    try:
        return int.from_bytes(b, byteorder='big', signed=False)
    except:
        return 0

# Packed decimal to float (with decimals)
def pd_to_float(b, decimals=2):
    """Convert packed decimal bytes to float"""
    if not b or len(b) == 0:
        return 0.0
    if all(x == 0 for x in b):
        return 0.0
    
    hex_str = b.hex().upper()
    sign_nibble = hex_str[-1]
    digits = hex_str[:-1]
    
    if sign_nibble in ('A', 'B', 'C', 'D', 'E', 'F') and digits.isdigit():
        val = int(digits)
        if sign_nibble in ('B', 'D'):
            val = -val
        return val / (10 ** decimals)
    
    return 0.0

# String cleanup
def clean_str(b):
    """Clean ASCII string"""
    if not b or len(b) == 0:
        return ""
    # Filter printable ASCII and common whitespace
    cleaned = bytes(x for x in b if 32 <= x <= 126 or x in (10, 13))
    try:
        text = cleaned.decode('ascii', errors='ignore').strip()
    except:
        return ""
    # Replace line breaks
    text = text.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
    return ' '.join(text.split())

# Integer fields (packed decimal)
int_fields = ["BANKNO", "ACCTNO", "BRANCH", "OPENDATE", "PRODUCT", 
              "BDATE", "DEPTNO", "YTDDAYS"]

# Float fields (packed decimal with 2 decimals)
float_fields = ["LEDGBAL", "CURBAL", "YTDBAL"]

# String fields
str_fields = ["APPCODE", "NAME", "OLDIC", "OPENIND", "PURPOSE", "RACE",
              "USER3", "DORMANT", "DEPTYPE", "NEWIC", "NAMETYPE",
              "NAMELN1", "NAMELN2", "NAMELN3", "NAMELN4",
              "NAMELN5", "NAMELN6", "NAMELN7", "NAMELN8"]

# Convert
for field in int_fields:
    print(f"  Converting {field}...")
    data[field] = [pd_to_int(b) for b in data[field]]

for field in float_fields:
    print(f"  Converting {field}...")
    data[field] = [pd_to_float(b) for b in data[field]]

for field in str_fields:
    print(f"  Converting {field}...")
    data[field] = [clean_str(b) for b in data[field]]

print("Conversion complete!")

# -----------------------------------------------------------
# STEP 3: Create DataFrame and save
# -----------------------------------------------------------
print("\nCreating DataFrame...")
df = pl.DataFrame(data)

# Cast to proper types
df = df.with_columns([
    pl.col("BANKNO").cast(pl.Int64),
    pl.col("ACCTNO").cast(pl.Int64),
    pl.col("BRANCH").cast(pl.Int64),
    pl.col("OPENDATE").cast(pl.Int64),
    pl.col("PRODUCT").cast(pl.Int64),
    pl.col("BDATE").cast(pl.Int64),
    pl.col("DEPTNO").cast(pl.Int64),
    pl.col("YTDDAYS").cast(pl.Int64),
    pl.col("LEDGBAL").cast(pl.Float64),
    pl.col("CURBAL").cast(pl.Float64),
    pl.col("YTDBAL").cast(pl.Float64),
])

print(f"DataFrame shape: {df.shape}")

# Save
print("\nSaving files...")
arrow_table = df.to_arrow()
pq.write_table(arrow_table, OUTPUT_PARQUET)
csv.write_csv(arrow_table, OUTPUT_CSV)
print(f"✓ {OUTPUT_PARQUET}")
print(f"✓ {OUTPUT_CSV}")

# -----------------------------------------------------------
# STEP 4: Validation
# -----------------------------------------------------------
print("\n" + "=" * 60)
print("VALIDATION")
print("=" * 60)

con = duckdb.connect(":memory:")
con.register("savings", arrow_table)

print(f"Total rows: {con.execute('SELECT COUNT(*) FROM savings').fetchone()[0]:,}")

print("\nRecords with valid ACCTNO (> 0):")
result = con.execute("""
    SELECT BANKNO, APPCODE, ACCTNO, BRANCH, NAME, 
           OLDIC, NEWIC, LEDGBAL, CURBAL
    FROM savings 
    WHERE ACCTNO > 0
    LIMIT 10
""").fetch_df()
print(result)

print("\nNumeric summary:")
stats = con.execute("""
    SELECT 
        COUNT(*) as total,
        COUNT(CASE WHEN ACCTNO > 0 THEN 1 END) as valid_acctno,
        COUNT(CASE WHEN NAME != '' THEN 1 END) as has_name,
        AVG(CASE WHEN LEDGBAL != 0 THEN LEDGBAL END) as avg_balance
    FROM savings
""").fetch_df()
print(stats)

print("\n" + "=" * 60)
print("PROGRAM COMPLETED SUCCESSFULLY!")
print(f"REPTDATE: {REPTDATE}")
print("=" * 60)
