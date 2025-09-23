# EIMGLNF2_NEF_GL_INTERFACE.py
# Conversion of JCL/SAS job EIMGLNF2 to Python using Polars, DuckDB, PyArrow

import polars as pl
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, datetime

# -------------------------------------------------
# Step 1: Reporting date logic (SAS DATA _NULL_ equivalent)
# -------------------------------------------------
today = date.today()
reptdate = today
mm1 = reptdate.month - 1 if reptdate.month > 1 else 12

rday = 25  # fixed day in SAS job
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
    "RPTDY": f"{rday:02d}",
}

print("=== Report Parameters ===")
for k, v in params.items():
    print(f"{k} = {v}")

# -------------------------------------------------
# Step 2: Load MASTER dataset
# (placeholder – replace with actual parquet/CSV path)
# -------------------------------------------------
try:
    mast = pl.read_parquet("MASTER_NEF.parquet")  # MAST.MASTER DSN
except FileNotFoundError:
    print("⚠️ Input parquet not found, using dummy MASTER data...")
    mast = pl.DataFrame({
        "BRHCODE": [101, 102, 103],
        "REPAY25": [120000.0, 250000.0, 80000.0]
    })

# -------------------------------------------------
# Step 3: Build GLFUND table (PROC SORT + DATA step logic)
# -------------------------------------------------
glfund = mast.select([
    pl.col("BRHCODE"),
    pl.lit("PNEF").alias("ARID"),
    pl.lit("BG REPAYMENT OF NEF").alias("JDESC"),
    pl.col("REPAY25")
])

# -------------------------------------------------
# Step 4: Aggregate (PROC SUMMARY equivalent)
# -------------------------------------------------
glfund_summary = (
    glfund.group_by(["BRHCODE", "ARID", "JDESC"])
          .agg([
              pl.sum("REPAY25").alias("REPAY25")
          ])
)

print("=== GLFUND Summary ===")
print(glfund_summary)

# -------------------------------------------------
# Step 5: Write output as Parquet (instead of GLFILE flat-file)
# -------------------------------------------------
table = pa.Table.from_pandas(glfund_summary.to_pandas())
pq.write_table(table, "EIMGLNF2_GLFUND_OUTPUT.parquet")

print("Output written: EIMGLNF2_GLFUND_OUTPUT.parquet")

# -------------------------------------------------
# Step 6: Register in DuckDB for SQL-style analysis
# -------------------------------------------------
duckdb.sql("INSTALL parquet; LOAD parquet;")
duckdb.sql("CREATE OR REPLACE TABLE glfund2 AS SELECT * FROM 'EIMGLNF2_GLFUND_OUTPUT.parquet'")
print("DuckDB table created: glfund2")

# Example query: totals by branch
q = duckdb.sql("SELECT BRHCODE, SUM(REPAY25) AS TOTAL_REPAY FROM glfund2 GROUP BY BRHCODE")
print("=== Totals by Branch ===")
print(q.df())
