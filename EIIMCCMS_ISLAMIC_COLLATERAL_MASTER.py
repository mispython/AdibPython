# EIIMCCMS.py
# Conversion of SAS job EIIMCCMS using Polars, DuckDB, PyArrow

import polars as pl
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date

# --------------------------------------------------------------------
# Step 1: Reporting date logic
# --------------------------------------------------------------------
reptdate = date.today()
wkday = reptdate.day
if wkday == 8:
    sdd, wk, wk1 = 1, "1", "4"
elif wkday == 15:
    sdd, wk, wk1 = 9, "2", "1"
elif wkday == 22:
    sdd, wk, wk1 = 16, "3", "2"
else:
    sdd, wk, wk1 = 23, "4", "3"

mm = reptdate.month
if wk == "1":
    mm1 = mm - 1 if mm > 1 else 12
else:
    mm1 = mm
if wk == "4":
    mm2 = mm
else:
    mm2 = mm - 1 if mm > 1 else 12

params = {
    "NOWK": wk,
    "NOWK1": wk1,
    "REPTMON": f"{mm:02d}",
    "REPTMON1": f"{mm1:02d}",
    "REPTMON2": f"{mm2:02d}",
    "REPTYEAR": f"{reptdate.year % 100:02d}",
    "RDATE": reptdate.strftime("%d-%m-%Y"),
    "REPTDAY": f"{reptdate.day:02d}",
    "MDATE": reptdate.strftime("%Y%m%d"),
}

print("Report Parameters:", params)

# --------------------------------------------------------------------
# Step 2: Read SUBACRED file
# --------------------------------------------------------------------
try:
    subacred = pl.read_parquet("SUBACRED.parquet")
except FileNotFoundError:
    subacred = pl.DataFrame({
        "BRANCH": [101, 102],
        "ACCTNO": ["1234567890", "9876543210"],
        "NOTENO": ["11111", "22222"],
        "AANO": ["AANO001", "AANO002"],
    })

# Deduplicate by AANO + ACCTNO
subacred = subacred.unique(subset=["AANO", "ACCTNO"])

# --------------------------------------------------------------------
# Step 3: Read ELNA7 (collateral master)
# --------------------------------------------------------------------
try:
    elna7 = pl.read_parquet("ELNA7.parquet")
except FileNotFoundError:
    elna7 = pl.DataFrame({
        "AANO": ["AANO001", "AANO002"],
        "CCOLLNO": ["1001", "1002"],
        "CPRPROPD": ["RESIDENTIAL", "COMMERCIAL"],
        "CPRVALDT": ["01-01-2025", "15-02-2025"],
        "ADDRESS1": ["LOT 1", "LOT 88"],
        "ADDRESS2": ["JALAN TEST", "JALAN SAMPLE"],
        "ADDRESS3": ["TMN ABC", "TMN XYZ"],
    })

# Build concatenated address
elna7 = elna7.with_columns([
    (pl.col("ADDRESS1") + ", " + pl.col("ADDRESS2")).alias("CPRPARC1"),
    pl.col("ADDRESS3").alias("CPRPARC2"),
    (pl.col("ADDRESS1") + ", " + pl.col("ADDRESS2") + ", " + pl.col("ADDRESS3")).alias("ADDRESS"),
    pl.lit(1).alias("CPRRANKC"),
])

# Deduplicate
elna7 = elna7.unique(subset=["AANO", "CCOLLNO"])

# --------------------------------------------------------------------
# Step 4: Merge SUBACRED + ELNA7
# --------------------------------------------------------------------
combine = subacred.join(elna7, on="AANO", how="left")

# --------------------------------------------------------------------
# Step 5: Output fixed-width file
# --------------------------------------------------------------------
records = []
for row in combine.iter_rows(named=True):
    rec = (
        f"{row['BRANCH']:03d}"
        f"{row['ACCTNO']:<10}"
        f"{row['AANO']:<13}"
        f"{row.get('CCOLLNO',''):<11}"
        f"{row.get('CPRPROPD',''):<45}"
        f"{row.get('ADDRESS',''):<100}"
        f"{row.get('CPRRANKC',1):1d}"
    )
    records.append(rec)

with open(f"ICCMS_{params['REPTMON']}{params['REPTYEAR']}.txt", "w") as f:
    for r in records:
        f.write(r + "\n")

# Save as Parquet for analytics
table = pa.Table.from_pandas(combine.to_pandas())
pq.write_table(table, f"ICCMS_{params['REPTMON']}{params['REPTYEAR']}.parquet")

# Register in DuckDB
duckdb.sql("INSTALL parquet; LOAD parquet;")
duckdb.sql(f"CREATE OR REPLACE TABLE iccms AS SELECT * FROM read_parquet('ICCMS_{params['REPTMON']}{params['REPTYEAR']}.parquet')")

print("Output written:", f"ICCMS_{params['REPTMON']}{params['REPTYEAR']}.txt")
