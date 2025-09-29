# ============================================================
# EICRRATE_LNRATE
# Translation of SAS DATA step + PROC SORT + PROC CPORT
# ============================================================

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from datetime import datetime

# --- Step 1: Define fixed-width positions from SAS INPUT
colspecs = [
    (0, 2),    # RINDEX PD2.
    (13, 19),  # EFFDATE PD6.
    (40, 46)   # CURRATE PD5.6
]

# --- Step 2: Read RATEFILE into Arrow Table
records = []
with open("RATEFILE.txt", "r") as f:
    for line in f:
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

# --- Step 3: Type conversions
# RINDEX → integer
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

# EFFDATE → proper date (MMDDYY8. in SAS)
def parse_effdate(arr):
    parsed = []
    for val in arr.to_pylist():
        if val and val.isdigit() and int(val) > 0:
            s = str(val).zfill(8)[:8]
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

# --- Step 4: DuckDB SQL (Filter RINDEX=1, Sort by EFFDATE desc)
con = duckdb.connect()
con.register("lnrate", table)

lnrate_sorted = con.execute("""
    SELECT RINDEX, EFFDATE, CURRATE
    FROM lnrate
    WHERE RINDEX = 1
    ORDER BY RINDEX ASC, EFFDATE DESC
""").arrow()

# --- Step 5: Export (equivalent of PROC CPORT)
output_name = "EICRRATE_LNRATE.parquet"
pq.write_table(lnrate_sorted, output_name)
