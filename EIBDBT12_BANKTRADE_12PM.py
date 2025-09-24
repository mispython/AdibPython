# EIBDBT12_BANKTRADE_PM12.py
# Conversion of SAS job EIBDBT12 (calls EIBDBT05) into Python with Polars, DuckDB, PyArrow
# Outputs both fixed-width .txt and Parquet

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, timedelta

# --------------------------------------------------------------------
# Step 1: Reporting date logic (DATA REPTDATE equivalent)
# --------------------------------------------------------------------
today = date.today()
reptdate = today - timedelta(days=1)
prevdate = date(reptdate.year, reptdate.month, 1) - timedelta(days=1)

rptdt = reptdate.strftime("%d-%m-%Y")
curmm = rptdt[3:5]
curyy = rptdt[8:10]
rdatex = curmm + curyy

# Next month calculation
if reptdate.month + 1 == 13:
    mm, yy = 1, reptdate.year + 1
else:
    mm, yy = reptdate.month + 1, reptdate.year
sdate = date(yy, mm, 1)

params = {
    "REPTYEAR": f"{reptdate.year % 100:02d}",
    "REPTMON": f"{reptdate.month:02d}",
    "REPTDAY": f"{reptdate.day:02d}",
    "PREVMON": f"{prevdate.month:02d}",
    "PREVDAY": f"{prevdate.day:02d}",
    "RDATE": reptdate.strftime("%d-%m-%Y"),
    "RDATEX": rdatex,
    "SDATE": f"{sdate.year}{sdate.month:02d}{sdate.day:02d}"[-5:],
}

print("Report Parameters:", params)

# --------------------------------------------------------------------
# Step 2: Read BTDTL input (INFILE BTFILE equivalent)
# --------------------------------------------------------------------
try:
    btdtl = pl.read_parquet("BTPM12.parquet")
except FileNotFoundError:
    print("BTPM12.parquet not found, using dummy sample")
    btdtl = pl.DataFrame({
        "BRANCH": [2001, 3100],
        "ACCTNO": [2850001111, 2860000001],
        "TRANSREF": ["PM12A01", "PM12B02"],
        "OUTSTAND": [120000.00, 80000.00],
        "MATDT": ["250125", "250630"],  # ddmmyy format
        "LIABCODE": ["001", "002"],
    })

# Parse MATDATE from ddmmyy string
btdtl = btdtl.with_columns(
    pl.col("MATDT").str.slice(0, 2).cast(pl.Int32).alias("day"),
    pl.col("MATDT").str.slice(2, 2).cast(pl.Int32).alias("month"),
    pl.col("MATDT").str.slice(4, 2).cast(pl.Int32).alias("year2")
).with_columns(
    (pl.col("year2") + 2000).alias("year")
).with_columns(
    pl.datetime("year", "month", "day").alias("MATDATE")
)

# Apply SAS filter: remove if branch > 3000 and ACCTNO in range
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
        "ACCTNO": [2850001111, 2860000001],
        "TRANSREF": ["PM12A01", "PM12B02"],
        "PREOUTSTD": [150000.0, 100000.0],
        "PRODTYPE": [0, 200],
    })

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
# Step 5: Write output (DAYBTRD fixed-width and Parquet)
# --------------------------------------------------------------------
records = []
for row in combt.iter_rows(named=True):
    rec = (
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
    records.append(rec)

with open("DAYBTRD_PM12.txt", "w") as f:
    for r in records:
        f.write(r + "\n")

# Save to Parquet
table = pa.Table.from_pandas(combt.to_pandas())
pq.write_table(table, "DAYBTRD_PM12.parquet")

print("Output written: DAYBTRD_PM12.txt and DAYBTRD_PM12.parquet")
