import duckdb
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.compute as pc
from datetime import datetime

# ==========================
# Step 1: Define schema + read fixed-width RATEFILE
# ==========================
colspecs = [
    (0, 2),    # RINDEX
    (13, 19),  # EFFDATE
    (40, 46)   # CURRATE
]

# Generate CSV-like reader options for fixed-width
read_opts = pv.ReadOptions(column_names=["RINDEX", "EFFDATE", "CURRATE"])
parse_opts = pv.ParseOptions(delimiter=" ")  # dummy delimiter (fwf override later)

# FWF reading via pyarrow: load as raw bytes then slice
with open("RATEFILE.txt", "r") as f:
    lines = f.readlines()

records = []
for line in lines:
    rindex = line[colspecs[0][0]:colspecs[0][1]].strip()
    effdate = line[colspecs[1][0]:colspecs[1][1]].strip()
    currate = line[colspecs[2][0]:colspecs[2][1]].strip()
    records.append((rindex, effdate, currate))

table = pa.table(
    {
        "RINDEX": pa.array([r[0] for r in records], pa.string()),
        "EFFDATE": pa.array([r[1] for r in records], pa.string()),
        "CURRATE": pa.array([r[2] for r in records], pa.string())
    }
)

# ==========================
# Step 2: Convert types
# ==========================
# RINDEX → int
table = table.set_column(
    table.schema.get_field_index("RINDEX"),
    "RINDEX",
    pc.cast(table["RINDEX"], pa.int32())
)

# CURRATE → float
table = table.set_column(
    table.schema.get_field_index("CURRATE"),
    "CURRATE",
    pc.cast(table["CURRATE"], pa.float64())
)

# EFFDATE → proper date
def parse_effdate(arr):
    parsed = []
    for val in arr.to_pylist():
        if val and val.isdigit() and int(val) > 0:
            s = str(val).zfill(8)[:8]   # ensure MMDDYYYY
            try:
                parsed.append(datetime.strptime(s, "%m%d%Y").date())
            except ValueError:
                parsed.append(None)
        else:
            parsed.append(None)
    return pa.array(parsed, pa.date32())

table = table.set_column(
    table.schema.get_field_index("EFFDATE"),
    "EFFDATE",
    parse_effdate(table["EFFDATE"])
)

# ==========================
# Step 3: Use DuckDB SQL for filter + sort
# ==========================
con = duckdb.connect()
con.register("lnrate", table)

lnrate_sorted = con.execute("""
    SELECT RINDEX, EFFDATE, CURRATE
    FROM lnrate
    WHERE RINDEX = 1
    ORDER BY RINDEX ASC, EFFDATE DESC
""").arrow()

# ==========================
# Step 4: Export (equivalent of PROC CPORT)
# ==========================
pa.parquet.write_table(lnrate_sorted, "SAP_CRMS_RATE_INDEXFTP.parquet")

print("✅ Exported to SAP_CRMS_RATE_INDEXFTP.parquet")
