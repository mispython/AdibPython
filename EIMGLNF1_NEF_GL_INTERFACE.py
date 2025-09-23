# EIMGLNF1_NEF_GL_INTERFACE.py
# Conversion of JCL/SAS job EIMGLNF1 to Python using Polars, DuckDB, PyArrow

import polars as pl
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, datetime, timedelta

# -------------------------------------------------
# Step 1: Reporting date logic (SAS DATA _NULL_ equivalent)
# -------------------------------------------------
today = date.today()
reptdate = today
mm1 = reptdate.month - 1 if reptdate.month > 1 else 12

datenow = reptdate.strftime("%Y%m%d")
timenow = datetime.now().strftime("%H%M%S")

params = {
    "REPTMON": f"{reptdate.month:02d}",
    "PREVMON": f"{mm1:02d}",
    "RDATE": reptdate.strftime("%d-%m-%Y"),
    "REPTDAT": reptdate,
    "NOWK": "4",
    "RPTDTE": datenow,
    "RPTTIME": timenow,
    "CURDAY": f"{reptdate.day:02d}",
    "CURMTH": f"{reptdate.month:02d}",
    "CURYR": f"{reptdate.year}",
    "RPTYR": f"{reptdate.year}",
    "RPTDY": f"{reptdate.day:02d}",
}

print("=== Report Parameters ===")
for k, v in params.items():
    print(f"{k} = {v}")

# -------------------------------------------------
# Step 2: Load input datasets
# (placeholders – replace with actual parquet/CSV paths)
# -------------------------------------------------
try:
    fund = pl.read_parquet("BNM_NEF_FUND.parquet")     # FUND DSN
    mast = pl.read_parquet("MASTER_NEF.parquet")       # MAST DSN
except FileNotFoundError:
    print("⚠️ Input parquet files not found, using dummy data...")
    fund = pl.DataFrame({
        "BRHCODE": [101, 102],
        "BRHNAME": ["B01", "B02"],
        "BNMREF": ["REF001", "REF002"],
        "CUSTNAME": ["Alice", "Bob"],
        "ACCTNO": ["12345", "67890"],
        "NOTENO": [1, 2],
        "MATUREDT": ["20251231", "20251231"],
        "BNMBAL": [100000.0, 200000.0]
    })

    mast = pl.DataFrame({
        "BRHCODE": [101, 102],
        "REPAY15": [50000.0, None]
    })

# -------------------------------------------------
# Step 3: Merge datasets (DRAW + REPAY)
# -------------------------------------------------
draw = fund.filter(pl.col("BNMREF") != "")

repay = mast.select(["BRHCODE", "REPAY15"])

glfund = draw.join(repay, on="BRHCODE", how="left")

# Apply SAS logic for ARID, JDESC, AVALUE
glfund = glfund.with_columns([
    pl.lit("PNEF").alias("ARID"),
    pl.when(pl.col("REPAY15").is_not_null())
      .then(pl.lit("BG REPAYMENT OF NEF"))
      .otherwise(pl.lit("BG DRAWDOWN OF NEF"))
      .alias("JDESC"),
    pl.when(pl.col("REPAY15").is_not_null())
      .then(pl.col("REPAY15"))
      .otherwise(pl.col("BNMBAL"))
      .alias("AVALUE")
])

# -------------------------------------------------
# Step 4: Aggregate (PROC SUMMARY equivalent)
# -------------------------------------------------
glfund_summary = (
    glfund.group_by(["BRHCODE", "ARID", "JDESC"])
          .agg([
              pl.sum("AVALUE").alias("AVALUE"),
              pl.sum("REPAY15", ignore_nulls=True).alias("REPAY15")
          ])
)

print("=== GLFUND Summary ===")
print(glfund_summary)

# -------------------------------------------------
# Step 5: Write output (as Parquet instead of flat file GLFILE)
# -------------------------------------------------
table = pa.Table.from_pandas(glfund_summary.to_pandas())
pq.write_table(table, "EIMGLNF1_GLFUND_OUTPUT.parquet")

print("Output written: EIMGLNF1_GLFUND_OUTPUT.parquet")

# -------------------------------------------------
# Step 6: Load into DuckDB for reporting
# -------------------------------------------------
duckdb.sql("INSTALL parquet; LOAD parquet;")
duckdb.sql("CREATE OR REPLACE TABLE glfund AS SELECT * FROM 'EIMGLNF1_GLFUND_OUTPUT.parquet'")
print("DuckDB table created: glfund")

# Example query: total by branch
q = duckdb.sql("SELECT BRHCODE, SUM(AVALUE) AS TOTAL_VALUE FROM glfund GROUP BY BRHCODE")
print("=== Totals by Branch ===")
print(q.df())
