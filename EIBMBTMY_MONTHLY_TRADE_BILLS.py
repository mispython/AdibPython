# EIBMBTMY_MONTHLY_TRADE_BILLS.py
# Conversion of JCL/SAS job EIBMBTMY to Python using Polars, DuckDB, PyArrow
# No Pandas used

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
# Step 2: Load APPROVE dataset (BNM.BTMAST&REPTMON&NOWK)
# -------------------------------------------------
try:
    approve = pl.read_parquet("APPROVE_DATA.parquet")
except FileNotFoundError:
    print("⚠️ Input parquet not found, using dummy APPROVE data...")
    approve = pl.DataFrame({
        "BRANCH": [101, 102, 103],
        "RETAILID": ['C', 'R', 'C'],
        "APPRLIMT": [1200000.0, 2500000.0, 800000.0],
    })

# -------------------------------------------------
# Step 3: Apply WHERE condition (SAS WHERE)
# -------------------------------------------------
approve_filtered = approve.filter(
    (pl.col("RETAILID") == "C") | (pl.col("RETAILID") == "R")
)

# -------------------------------------------------
# Step 4: Summary statistics (PROC SUMMARY equivalent)
# -------------------------------------------------
approve_summary = (
    approve_filtered.group_by(["BRANCH", "RETAILID"])
                    .agg(pl.sum("APPRLIMT").alias("APPRLIMT"))
)

print("=== APPROVE Summary ===")
print(approve_summary)

# -------------------------------------------------
# Step 5: Write output as Parquet
# -------------------------------------------------
pq.write_table(approve_summary.to_arrow(), "EIBMBTMY_APPROVE_SUMMARY.parquet")
print("Output written: EIBMBTMY_APPROVE_SUMMARY.parquet")

# -------------------------------------------------
# Step 6: Register in DuckDB for SQL-style analysis
# -------------------------------------------------
duckdb.sql("INSTALL parquet; LOAD parquet;")
duckdb.sql("CREATE OR REPLACE TABLE approve_summary AS SELECT * FROM 'EIBMBTMY_APPROVE_SUMMARY.parquet'")
print("DuckDB table created: approve_summary")

# Example query: total limits by branch
q = duckdb.sql("SELECT BRANCH, SUM(APPRLIMT) AS TOTAL_APPRLIMT FROM approve_summary GROUP BY BRANCH")
print("=== Total Approve Limits by Branch ===")
print(q.pl())   # returns Polars dataframe

# -------------------------------------------------
# Step 7: Report-like Output (PROC TABULATE equivalent in Polars)
# -------------------------------------------------
# Add BRANCHX column (branch code + retail type)
approve_summary = approve_summary.with_columns(
    (pl.col("BRANCH").cast(pl.Utf8) + "-" + pl.col("RETAILID")).alias("BRANCHX")
)

# Create pivot (branch x category, sum of APPRLIMT)
pivot_report = approve_summary.pivot(
    values="APPRLIMT",
    index="BRANCHX",
    columns="RETAILID",
    aggregate_fn="sum"
)

print("=== Monthly Trade Bills by Approved Limits (Polars Pivot) ===")
print(pivot_report)

# Save pivot report as parquet
pq.write_table(pivot_report.to_arrow(), "EIBMBTMY_MONTHLY_TRADE_BILLS.parquet")
print("Report saved: EIBMBTMY_MONTHLY_TRADE_BILLS.parquet")
