import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
import datetime
import polars as pl

# -----------------------------------------------------------
# 1. Equivalent of: DATA ADDR.REPTDATE;
# -----------------------------------------------------------
REPTDATE = datetime.date.today()

# -----------------------------------------------------------
# 2. INPUT fixed-width DPADDR file (convert EBCDIC to ASCII if needed)
# -----------------------------------------------------------

DPADDR_FILE = "DPADDR.dat"   # input file
OUTPUT_PARQUET = "ADDR_SAVINGS.parquet"
OUTPUT_CSV = "ADDR_SAVINGS.csv"

# Define fixed-width column positions
fwf_layout = [
    ("BANKNO", 1, 2, "int32"),
    ("APPCODE", 3, 1, "string"),
    ("ACCTNO", 4, 6, "int64"),
    ("BRANCH", 10, 4, "int32"),
    ("NAME", 14, 24, "string"),
    ("OLDIC", 38, 11, "string"),
    ("OPENDATE", 49, 6, "int32"),
    ("PRODUCT", 55, 2, "int32"),
    ("OPENIND", 57, 1, "string"),
    ("PURPOSE", 58, 1, "string"),
    ("RACE", 59, 1, "string"),
    ("USER3", 60, 1, "string"),
    ("DORMANT", 61, 1, "string"),
    ("DEPTYPE", 62, 1, "string"),
    ("BDATE", 63, 6, "int32"),
    ("DEPTNO", 69, 3, "int32"),
    ("NEWIC", 72, 12, "string"),
    ("LEDGBAL", 84, 7, "float64"),
    ("CURBAL", 91, 7, "float64"),
    ("YTDBAL", 98, 8, "float64"),
    ("YTDDAYS", 106, 2, "int32"),
    ("NAMETYPE", 108, 1, "int32"),
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
# 3. Read Fixed Width File (ASCII assumed; decode if EBCDIC)
# -----------------------------------------------------------
def read_fixed_width(file_path, layout):
    data = []
    with open(file_path, "rb") as f:
        for line in f:
            try:
                text = line.decode("cp037")  # convert EBCDIC to ASCII
            except UnicodeDecodeError:
                text = line.decode("latin-1")  # fallback
            row = {}
            for name, start, length, dtype in layout:
                raw = text[start - 1 : start - 1 + length].strip()
                if dtype.startswith("int"):
                    row[name] = int(raw or 0)
                elif dtype.startswith("float"):
                    row[name] = float(raw or 0.0)
                else:
                    row[name] = raw
            data.append(row)
    return data


records = read_fixed_width(DPADDR_FILE, fwf_layout)

# -----------------------------------------------------------
# 4. Convert to Arrow Table (via Polars if available)
# -----------------------------------------------------------
pl_df = pl.DataFrame(records)
arrow_table = pl_df.to_arrow()

# -----------------------------------------------------------
# 5. Store REPTDATE (like DATA ADDR.REPTDATE)
# -----------------------------------------------------------
reptdate_table = pa.table({"REPTDATE": [REPTDATE.isoformat()]})

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
