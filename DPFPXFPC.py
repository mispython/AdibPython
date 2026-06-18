# eibaegld.py
import polars as pl
import pyarrow.parquet as pq
import duckdb
from datetime import date

# -----------------------------
# Step 1: Define report date (like SAS SET DEPOSIT.REPTDATE)
# -----------------------------
today = date.today()
REPTMON = f"{today.month:02d}"
REPTDAY = f"{today.day:02d}"
REPTDT = today.strftime("%Y-%m-%d")

# Weekly bucket (NOWK)
day = today.day
if 1 <= day <= 8:
    NOWK = "1"
elif 9 <= day <= 15:
    NOWK = "2"
elif 16 <= day <= 22:
    NOWK = "3"
else:
    NOWK = "4"

print(f"Running EIBAEGLD for {REPTDT}, MON={REPTMON}, DAY={REPTDAY}, NOWK={NOWK}")

# -----------------------------
# Step 2: Load MIS gold transaction dataset
# -----------------------------
# SAS: MIS.GOLDTRAN&REPTMON&NOWK
# Here: assume Parquet partitioned storage
mis_file = f"MIS_GOLDTRAN_{REPTMON}_{NOWK}.parquet"

try:
    mis_goldtran = pl.read_parquet(mis_file)
except FileNotFoundError:
    raise FileNotFoundError(f"Missing input file: {mis_file}")

# -----------------------------
# Step 3: Filter by REPTDATE
# -----------------------------
temp_goldtran = mis_goldtran.filter(pl.col("REPTDATE") == REPTDT)

print("Filtered MIS gold transactions:")
print(temp_goldtran)

# -----------------------------
# Step 4: Write to TEMP dataset
# -----------------------------
# Equivalent to: DATA TEMP.GOLDTRAND&REPTDAY
temp_file = f"TEMP_GOLDTRAND_{REPTDAY}.parquet"
temp_goldtran.write_parquet(temp_file)
temp_goldtran.write_csv(f"TEMP_GOLDTRAND_{REPTDAY}.csv")

print(f"TEMP dataset written to {temp_file}")

# -----------------------------
# Step 5: Export for FTP (SAS CPORT replacement)
# -----------------------------
# SAS used PROC CPORT (binary SAS library dump). Here, we simply export Parquet.
export_file = "SAP_PBB_GOLD_GOLDFTPD.parquet"
temp_goldtran.write_parquet(export_file)
print(f"Export file ready for FTP: {export_file}")

# -----------------------------
# Step 6: Optional DuckDB for appending to MIS
# -----------------------------
duckdb.sql("INSTALL parquet; LOAD parquet;")
duckdb.sql(f"""
    CREATE TABLE goldtran AS SELECT * FROM read_parquet('{export_file}')
""")

print("DuckDB table 'goldtran' available for queries, e.g.:")
print(duckdb.sql("SELECT COUNT(*) FROM goldtran").fetchall())
