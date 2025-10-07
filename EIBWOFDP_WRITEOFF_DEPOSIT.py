import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
import polars as pl

# -----------------------------------------------------------
# 1. Input and output setup
# -----------------------------------------------------------
INPUT_FILE = "OD.dat"  # Input fixed-width file
OUTPUT_ODWOF_PARQUET = "ODWOF_ODWOF.parquet"
OUTPUT_ODWOF_CSV = "ODWOF_ODWOF.csv"
OUTPUT_IODWOF_PARQUET = "IODWOF_IODWOF.parquet"
OUTPUT_IODWOF_CSV = "IODWOF_IODWOF.csv"

# -----------------------------------------------------------
# 2. Define fixed-width column layout (SAS INPUT structure)
# -----------------------------------------------------------
fwf_layout = [
    ("ACCTNO", 3, 6, "int64"),
    ("WODT10CM", 9, 5, "int32"),
    ("WOTYPE", 14, 1, "string"),
    ("WOSTAT", 15, 1, "string"),
    ("ACSTAT", 16, 1, "string"),
    ("STATCDATE", 17, 8, "int64"),
    ("LEDGBAL", 25, 7, "float64"),
    ("CUMWOSP", 32, 7, "float64"),
    ("CUMWOIS", 39, 7, "float64"),
    ("ORIWDB", 46, 7, "float64"),
    ("WRITE_DOWN_BAL", 53, 7, "float64"),
    ("NAI", 60, 10, "float64"),
    ("CUMNAI", 70, 10, "float64"),
    ("BDR", 80, 7, "float64"),
    ("CUMRC", 87, 7, "float64"),
    ("CUMSC", 94, 7, "float64"),
    ("IISR", 101, 7, "float64"),
    ("ORIPRODUCT", 108, 3, "int32"),
    ("ACCBRCH", 111, 3, "int32"),
    ("COSTCTR", 114, 4, "int32"),
    ("LASTMDATE", 118, 8, "int64"),
    ("PRODUCT", 198, 3, "int32"),
]

# -----------------------------------------------------------
# 3. Read the fixed-width file (EBCDIC → ASCII)
# -----------------------------------------------------------
def read_fixed_width(file_path, layout):
    records = []
    with open(file_path, "rb") as f:
        for line in f:
            try:
                text = line.decode("cp037")  # decode EBCDIC to ASCII
            except UnicodeDecodeError:
                text = line.decode("latin-1")  # fallback if not EBCDIC
            row = {}
            for name, start, length, dtype in layout:
                raw = text[start - 1 : start - 1 + length].strip()
                if dtype.startswith("int"):
                    row[name] = int(raw or 0)
                elif dtype.startswith("float"):
                    row[name] = float(raw or 0.0)
                else:
                    row[name] = raw
            # Derived field: ORIWODATE = 100000000 - WODT10CM
            row["ORIWODATE"] = 100000000 - row["WODT10CM"]
            records.append(row)
    return records


records = read_fixed_width(INPUT_FILE, fwf_layout)

# -----------------------------------------------------------
# 4. Convert to Arrow table (via Polars)
# -----------------------------------------------------------
pl_df = pl.DataFrame(records)
arrow_table = pl_df.to_arrow()

# -----------------------------------------------------------
# 5. Use DuckDB to split data (IODWOF vs ODWOF)
# -----------------------------------------------------------
con = duckdb.connect(database=":memory:")
con.register("wof", arrow_table)

# Split using SAS condition
iodwof_arrow = con.execute("""
    SELECT * FROM wof WHERE COSTCTR BETWEEN 3000 AND 4999
""").arrow()

odwof_arrow = con.execute("""
    SELECT * FROM wof WHERE COSTCTR < 3000 OR COSTCTR > 4999
""").arrow()

# -----------------------------------------------------------
# 6. Write to Parquet and CSV
# -----------------------------------------------------------
pq.write_table(odwof_arrow, OUTPUT_ODWOF_PARQUET)
csv.write_csv(odwof_arrow, OUTPUT_ODWOF_CSV)

pq.write_table(iodwi_arrow := iodwof_arrow, OUTPUT_IODWOF_PARQUET)
csv.write_csv(iodwi_arrow, OUTPUT_IODWOF_CSV)

# -----------------------------------------------------------
# 7. Print summary (like SAS log)
# -----------------------------------------------------------
count_odwof = con.execute("SELECT COUNT(*) FROM wof WHERE COSTCTR < 3000 OR COSTCTR > 4999").fetchone()[0]
count_iodwof = con.execute("SELECT COUNT(*) FROM wof WHERE COSTCTR BETWEEN 3000 AND 4999").fetchone()[0]

print(f"ODWOF.ODWOF rows : {count_odwof}")
print(f"IODWOF.IODWOF rows: {count_iodwof}")
print("Output files created:")
print(f" - {OUTPUT_ODWOF_PARQUET}")
print(f" - {OUTPUT_ODWOF_CSV}")
print(f" - {OUTPUT_IODWOF_PARQUET}")
print(f" - {OUTPUT_IODWOF_CSV}")
