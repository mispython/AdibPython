import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
import datetime
import polars as pl

# -----------------------------------------------------------
# Step 2: Process DPADDR.parquet to ADDR_SAVINGS
# -----------------------------------------------------------

REPTDATE = datetime.date.today() - datetime.timedelta(days=1)

TEMP_PARQUET = "DPADDR.parquet"
OUTPUT_PARQUET = "ADDR_SAVINGS.parquet"
OUTPUT_CSV = "ADDR_SAVINGS.csv"

# Read the raw parquet file
df = pl.read_parquet(TEMP_PARQUET)

# Helper function for packed decimal
def unpack_packed_decimal(byte_array, decimals=0):
    """Convert IBM packed decimal bytes to integer/float"""
    if byte_array is None or len(byte_array) == 0:
        return 0.0 if decimals > 0 else 0
    
    # Convert to hex
    hex_str = byte_array.hex().upper()
    
    # Get sign from last nibble
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
        return byte_array.decode("cp037").strip()
    except:
        return byte_array.decode("latin-1", errors="ignore").strip()

# Process each column
print("Converting columns...")

# String columns (EBCDIC)
string_cols = ["APPCODE", "NAME", "OLDIC", "OPENIND", "PURPOSE", "RACE", 
               "USER3", "DORMANT", "DEPTYPE", "NEWIC", "NAMETYPE",
               "NAMELN1", "NAMELN2", "NAMELN3", "NAMELN4", 
               "NAMELN5", "NAMELN6", "NAMELN7", "NAMELN8"]

for col in string_cols:
    df = df.with_columns(
        pl.col(col).map_elements(ebcdic_to_ascii, return_dtype=pl.Utf8)
    )

# Packed decimal columns (no decimals)
pd_cols = ["BANKNO", "ACCTNO", "BRANCH", "OPENDATE", "PRODUCT", 
           "BDATE", "DEPTNO", "YTDDAYS"]

for col in pd_cols:
    df = df.with_columns(
        pl.col(col).map_elements(lambda x: unpack_packed_decimal(x, 0), return_dtype=pl.Int64)
    )

# Packed decimal columns (2 decimal places)
pd2_cols = ["LEDGBAL", "CURBAL", "YTDBAL"]

for col in pd2_cols:
    df = df.with_columns(
        pl.col(col).map_elements(lambda x: unpack_packed_decimal(x, 2), return_dtype=pl.Float64)
    )

print("Writing output files...")
# Convert to Arrow and save
arrow_table = df.to_arrow()
pq.write_table(arrow_table, OUTPUT_PARQUET)
csv.write_csv(arrow_table, OUTPUT_CSV)

# Validate
con = duckdb.connect(database=":memory:")
con.register("savings", arrow_table)
print(f"Row count: {con.execute('SELECT COUNT(*) FROM savings').fetchone()[0]}")
print("Sample rows:")
print(con.execute("SELECT * FROM savings LIMIT 5").fetch_df())
print("Done!")
