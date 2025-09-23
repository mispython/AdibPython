# EIBWFISS_WEEKLY_FISS_REPORT.py
import polars as pl
import duckdb
from datetime import date, timedelta

# -------------------------------------------------
# Step 1: Reporting date logic (simulate BNM.REPTDATE dataset)
# -------------------------------------------------
today = date.today()
reptdate = today  # In SAS, it came from BNM.REPTDATE dataset

day = reptdate.day
if day == 8:
    sdd, wk, wk1 = 1, "1", "4"
elif day == 15:
    sdd, wk, wk1 = 9, "2", "1"
elif day == 22:
    sdd, wk, wk1 = 16, "3", "2"
else:
    sdd, wk, wk1 = 23, "4", "3"

mm = reptdate.month
if wk == "1":
    mm1 = mm - 1 if mm > 1 else 12
else:
    mm1 = mm

sdate = date(reptdate.year, mm, sdd)

# Parameters (equivalent to SYMPUT in SAS)
params = {
    "NOWK": wk,
    "NOWK1": wk1,
    "REPTMON": f"{mm:02d}",
    "REPTMON1": f"{mm1:02d}",
    "REPTYEAR": reptdate.year,
    "REPTDAY": f"{reptdate.day:02d}",
    "RDATE": reptdate.strftime("%d-%m-%Y"),
    "SDATE": sdate.strftime("%d-%m-%Y"),
}

print("=== Report Parameters ===")
for k, v in params.items():
    print(f"{k} = {v}")

# -------------------------------------------------
# Step 2: Load datasets (for both PBB and PIBB)
# -------------------------------------------------
# (In mainframe, these were SAS datasets. Here assume converted to Parquet.)

pbb_file = "BNM_SASDATA_PBB_REPTDATE.parquet"
pibb_file = "BNM_SASDATA_PIBB_REPTDATE.parquet"

try:
    pbb_rept = pl.read_parquet(pbb_file)
    pibb_rept = pl.read_parquet(pibb_file)
except FileNotFoundError:
    print("Missing input dataset(s), using reptdate from system date.")
    pbb_rept, pibb_rept = None, None

# -------------------------------------------------
# Step 3: Create report dataframes
# -------------------------------------------------
# In SAS: %INC PGM(EIBWFI01) – an include program (not given).
# Here we just build a placeholder output.

pbb_report = pl.DataFrame({
    "BANK": ["PUBLIC BANK BERHAD"],
    "REPORT_DATE": [params["RDATE"]],
    "START_DATE": [params["SDATE"]],
    "WEEK": [params["NOWK"]],
})

pibb_report = pl.DataFrame({
    "BANK": ["PUBLIC ISLAMIC BANK BERHAD"],
    "REPORT_DATE": [params["RDATE"]],
    "START_DATE": [params["SDATE"]],
    "WEEK": [params["NOWK"]],
})

# -------------------------------------------------
# Step 4: Save outputs (Parquet instead of SASLIST text)
# -------------------------------------------------
pbb_report.write_parquet("EIBWFISS_PBB_WEEKLY.parquet")
pibb_report.write_parquet("EIBWFISS_PIBB_WEEKLY.parquet")

print("Reports generated:")
print("   EIBWFISS_PBB_WEEKLY.parquet")
print("   EIBWFISS_PIBB_WEEKLY.parquet")

# -------------------------------------------------
# Step 5: Register in DuckDB for analysis
# -------------------------------------------------
duckdb.sql("INSTALL parquet; LOAD parquet;")

duckdb.sql("""
    CREATE OR REPLACE TABLE fiss_pbb AS
    SELECT * FROM read_parquet('EIBWFISS_PBB_WEEKLY.parquet')
""")

duckdb.sql("""
    CREATE OR REPLACE TABLE fiss_pibb AS
    SELECT * FROM read_parquet('EIBWFISS_PIBB_WEEKLY.parquet')
""")

print("DuckDB tables created: fiss_pbb, fiss_pibb")
