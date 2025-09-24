# EIBDBT05_DAILY_TRADE_BILLS.py
# Conversion of SAS program EIBDBT05 to Python
# Uses Polars, PyArrow, DuckDB (no Pandas)

import polars as pl
import pyarrow.parquet as pq
from datetime import date, timedelta

# --------------------------------------------------------------------
# Step 1: Reporting Date Logic (DATA REPTDATE equivalent)
# --------------------------------------------------------------------
today = date.today()
reptdate = today - timedelta(days=1)   # REPTDATE = TODAY() - 1
prevdate = date(reptdate.year, reptdate.month, 1) - timedelta(days=1)

rptdt = reptdate.strftime("%d-%m-%Y")
curmm = rptdt[3:5]   # substring month
curyy = rptdt[8:10]  # substring year
rdatex = curmm + curyy

# Next month calculation
if reptdate.month + 1 == 13:
    mm, yy = 1, reptdate.year + 1
else:
    mm, yy = reptdate.month + 1, reptdate.year
sdate = date(yy, mm, 1)

# Equivalent SYMPUT variables
params = {
    "REPTYEAR": f"{reptdate.year % 100:02d}",
    "REPTMON": f"{reptdate.month:02d}",
    "REPTDAY": f"{reptdate.day:02d}",
    "PREVMON": f"{prevdate.month:02d}",
    "PREVDAY": f"{prevdate.day:02d}",
    "RDATE": reptdate.strftime("%d-%m-%Y"),
    "RDATEX": rdatex,
    "SDATE": f"{sdate.year}{sdate.month:02d}{sdate.day:02d}"[-5:],  # Z5. format
}

print("=== Report Parameters ===")
for k, v in params.items():
    print(f"{k} = {v}")

# --------------------------------------------------------------------
# Step 2: Read BTDTL input (INFILE BTFILE equivalent)
# --------------------------------------------------------------------
try:
    btdtl = pl.read_parquet("BTFILE.parquet")
except FileNotFoundError:
    print("BTFILE.parquet not found, creating dummy input")
    btdtl = pl.DataFrame({
        "BRANCH": [3001, 1002],
        "ACCTNO": [2850001000, 2851000001],
        "TRANSREF": ["TREF001", "TREF002"],
        "OUTSTAND": [50000.0, 120000.0],
        "MATDT": ["250125", "250630"],  # ddmmyy
        "LIABCODE": ["001", "002"],
    })

# Decode MATDATE from string ddmmyy
btdtl = btdtl.with_columns(
    pl.col("MATDT").str.slice(0, 2).cast(pl.Int32).alias("day"),
    pl.col("MATDT").str.slice(2, 2).cast(pl.Int32).alias("month"),
    pl.col("MATDT").str.slice(4, 2).cast(pl.Int32).alias("year2"),
)

# Convert YY to 20YY
btdtl = btdtl.with_columns(
    ((pl.col("year2") + 2000).cast(pl.Int32)).alias("year")
)

# Construct MATDATE
btdtl = btdtl.with_columns(
    pl.datetime(pl.col("year"), pl.col("month"), pl.col("day")).alias("MATDATE")
)

# Apply branch/account filter
btdtl = btdtl.filter(
    ~((pl.col("BRANCH") > 3000) &
      (pl.col("ACCTNO") >= 2850000000) &
      (pl.col("ACCTNO") <= 2859999999))
)

# --------------------------------------------------------------------
# Step 3: Read BASE dataset (previous month snapshot)
# --------------------------------------------------------------------
try:
    base = pl.read_parquet(f"BTBASE_{params['PREVMON']}.parquet")
except FileNotFoundError:
    print("BTBASE parquet not found, using dummy")
    base = pl.DataFrame({
        "ACCTNO": [2851000001, 2852000002],
        "TRANSREF": ["TREF002", "TREF003"],
        "PREOUTSTD": [150000.0, 90000.0],
        "PRODTYPE": [0, 100],
    })

# Deduplicate
base = base.unique(subset=["ACCTNO", "TRANSREF"])
btdtl = btdtl.unique(subset=["ACCTNO", "TRANSREF"])

# --------------------------------------------------------------------
# Step 4: Merge BASE and BTDTL (BY ACCTNO TRANSREF)
# --------------------------------------------------------------------
combt = base.join(btdtl, on=["ACCTNO", "TRANSREF"], how="left")

# Compute OVERDUE and RECOVAMT
sdate_num = sdate.toordinal()
combt = combt.with_columns([
    ((sdate_num + 1) - pl.col("MATDATE").dt.to_python_datetime().dt.toordinal()).alias("OVERDUE"),
    (pl.col("PREOUTSTD") - pl.col("OUTSTAND")).alias("RECOVAMT"),
    pl.when(pl.col("PRODTYPE") == 0).then("R").otherwise(pl.lit(None)).alias("RETAILID"),
])

# --------------------------------------------------------------------
# Step 5: Write output (FILE DAYBTRD equivalent)
# --------------------------------------------------------------------
# Format like SAS PUT statement
output_records = []
for row in combt.iter_rows(named=True):
    record = (
        f"{row['BRANCH']:05d}"
        f"{row['ACCTNO']:010d}"
        f"{row['TRANSREF']:<10}"
        f"{row['PRODTYPE']:03d}"
        f"{row['PREOUTSTD']:017.2f}"
        f"{row['OUTSTAND']:017.2f}"
        f"{row['OVERDUE']:010d}"
        f"{row['RECOVAMT']:017.2f}"
        f"{row['LIABCODE']:<5}"
    )
    output_records.append(record)

with open("DAYBTRD.txt", "w") as f:
    for rec in output_records:
        f.write(rec + "\n")

print("Output written to DAYBTRD.txt")
