import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
import datetime
import polars as pl
import struct

# -----------------------------------------------------------
# 1. Use datetime timedelta - 1 (yesterday's date)
# -----------------------------------------------------------
REPTDATE = datetime.date.today() - datetime.timedelta(days=1)

# -----------------------------------------------------------
# 2. INPUT fixed-width DPADDR file (text file)
# -----------------------------------------------------------

DPADDR_FILE = "DPADDR.txt"   # input text file
OUTPUT_PARQUET = "ADDR_SAVINGS.parquet"
OUTPUT_CSV = "ADDR_SAVINGS.csv"

# Define fixed-width column positions
fwf_layout = [
    ("BANKNO", 1, 2, "pd"),
    ("APPCODE", 3, 1, "string"),
    ("ACCTNO", 4, 6, "pd"),
    ("BRANCH", 10, 4, "pd"),
    ("NAME", 14, 24, "string"),
    ("OLDIC", 38, 11, "string"),
    ("OPENDATE", 49, 6, "pd"),
    ("PRODUCT", 55, 2, "pd"),
    ("OPENIND", 57, 1, "string"),
    ("PURPOSE", 58, 1, "string"),
    ("RACE", 59, 1, "string"),
    ("USER3", 60, 1, "string"),
    ("DORMANT", 61, 1, "string"),
    ("DEPTYPE", 62, 1, "string"),
    ("BDATE", 63, 6, "pd"),
    ("DEPTNO", 69, 3, "pd"),
    ("NEWIC", 72, 12, "string"),
    ("LEDGBAL", 84, 7, "pd2"),  # PD7.2
    ("CURBAL", 91, 7, "pd2"),   # PD7.2
    ("YTDBAL", 98, 8, "pd2"),   # PD8.2
    ("YTDDAYS", 106, 2, "pd"),
    ("NAMETYPE", 108, 1, "string"),
    ("NAMELN1", 109, 40, "string"),
    ("NAMELN2", 149, 40, "string"),
    ("NAMELN3", 189, 40, "string"),
    ("NAMELN4", 229, 40, "string"),
    ("NAMELN5", 269, 40, "string"),
    ("NAMELN6", 309, 40, "string"),
    ("NAMELN7", 349, 40, "string"),
    ("NAMELN8", 389, 40, "string"),
]

# -----------------------------------------------------------
# 3. Helper function to decode packed decimal
# -----------------------------------------------------------
def unpack_packed_decimal(bytes_data, decimals=0):
    """Convert IBM packed decimal bytes to integer/float"""
    if not bytes_data or all(b == 0 for b in bytes_data):
        return 0 if decimals == 0 else 0.0
    
    # Convert bytes to hex string
    hex_str = bytes_data.hex().upper()
    
    # Packed decimal format: last nibble is sign (C=positive, D=negative, F=unsigned)
    sign_nibble = hex_str[-1]
    digits = hex_str[:-1]
    
    # Check for valid packed decimal
    if sign_nibble not in ('C', 'D', 'F'):
        return 0 if decimals == 0 else 0.0
    
    # Convert digits to integer
    try:
        value = int(digits)
    except ValueError:
        return 0 if decimals == 0 else 0.0
    
    # Apply sign
    if sign_nibble == 'D':
        value = -value
    
    # Apply decimal places
    if decimals > 0:
        value = value / (10 ** decimals)
    
    return value

# -----------------------------------------------------------
# 4. Read Fixed Width File with proper EBCDIC and PD handling
# -----------------------------------------------------------
def read_fixed_width(file_path, layout):
    data = []
    with open(file_path, "rb") as f:
        for line_num, line in enumerate(f, 1):
            try:
                row = {}
                for name, start, length, dtype in layout:
                    # Extract raw bytes (1-indexed positions)
                    raw_bytes = line[start-1:start-1+length]
                    
                    if dtype == "pd":
                        # Packed decimal without decimals
                        row[name] = unpack_packed_decimal(raw_bytes)
                    elif dtype == "pd2":
                        # Packed decimal with 2 decimal places
                        row[name] = unpack_packed_decimal(raw_bytes, decimals=2)
                    elif dtype == "string":
                        # EBCDIC string conversion
                        try:
                            text = raw_bytes.decode("cp037")
                        except:
                            text = raw_bytes.decode("latin-1", errors="ignore")
                        row[name] = text.strip()
                    else:
                        row[name] = raw_bytes.hex()  # fallback
                
                data.append(row)
            except Exception as e:
                print(f"Error processing line {line_num}: {e}")
                continue
    
    return data

records = read_fixed_width(DPADDR_FILE, fwf_layout)

# -----------------------------------------------------------
# 5. Convert to Arrow Table
# -----------------------------------------------------------
if records:
    pl_df = pl.DataFrame(records)
    arrow_table = pl_df.to_arrow()
    
    # -----------------------------------------------------------
    # 6. Write to Parquet and CSV (ADDR.SAVINGS)
    # -----------------------------------------------------------
    pq.write_table(arrow_table, OUTPUT_PARQUET)
    csv.write_csv(arrow_table, OUTPUT_CSV)
    
    # -----------------------------------------------------------
    # 7. Optional validation with DuckDB
    # -----------------------------------------------------------
    con = duckdb.connect(database=":memory:")
    con.register("savings", arrow_table)
    print("Row count:", con.execute("SELECT COUNT(*) FROM savings").fetchone()[0])
    print("Sample rows:")
    print(con.execute("SELECT * FROM savings LIMIT 5").fetch_df())
else:
    print("No records were read from the file.")
