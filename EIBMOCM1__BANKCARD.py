# ===============================================================
#  Program Name : EIBMOCM1_PY
#  Description  : Monthly OCM Bankcard Merge and Update
#  Tools Used   : DuckDB, PyArrow, Polars
# ===============================================================

import duckdb
import polars as pl
from datetime import date, timedelta

# ---------------------------------------------------------------
# STEP 1.  Get Current and Previous Report Dates
# ---------------------------------------------------------------
today = date.today()
rept_date = today.replace(day=1) - timedelta(days=1)  # previous month-end
srept_date = rept_date.replace(day=1)
prev_rept_date = srept_date - timedelta(days=1)

REPTMON = f"{rept_date.month:02d}"
REPTYEAR = f"{str(rept_date.year)[2:]}"
PREPTMON = f"{prev_rept_date.month:02d}"
PREPTYEAR = f"{str(prev_rept_date.year)[2:]}"
RDATE = rept_date.strftime("%d%m%Y")

print(f"Processing report month {REPTMON}-{REPTYEAR}, previous {PREPTMON}-{PREPTYEAR}")

# ---------------------------------------------------------------
# STEP 2.  Load Data
# ---------------------------------------------------------------
con = duckdb.connect()

# Example: read parquet or CSV versions of the input files
# (Replace with actual paths)
OCM_FILE = "data/OCMFILE.parquet"
DEP1_FILE = "data/CISSAFD_DEPOSIT.parquet"
DEP2_FILE = "data/CISCADP_DEPOSIT.parquet"
PREV_FILE = f"data/OCMSET_BANKCARD{PREPTMON}{PREPTYEAR}.parquet"

# Load into Polars for flexible transformations
ocm = pl.read_parquet(OCM_FILE).select(
    "CARDNO", "CUSTNAME", "BRANCH", "STATUS", "ISSUEDT",
    "ACCTNO1", "ACCTNO2", "ACCTNO3", "ACCTNO4", "ACCTNO5"
)

dep1 = pl.read_parquet(DEP1_FILE)
dep2 = pl.read_parquet(DEP2_FILE)

# Combine deposit sources
dep = (
    pl.concat([dep1, dep2])
    .filter((pl.col("NEWIC") != " ") & (pl.col("SECCUST") == 901))
    .select("NEWIC", pl.col("ACCTNO").alias("ACCTNO1"), "BRANCH")
    .unique(subset=["ACCTNO1"])
)

# ---------------------------------------------------------------
# STEP 3.  Join OCM Data with Deposit Data
# ---------------------------------------------------------------
ocm_dep = (
    ocm.join(dep, on="ACCTNO1", how="left")
    .unique(subset=["CARDNO"])
)

# ---------------------------------------------------------------
# STEP 4.  Merge with Previous Month’s Data
# ---------------------------------------------------------------
prev = pl.read_parquet(PREV_FILE).unique(subset=["CARDNO"])

merged = prev.join(ocm_dep, on="CARDNO", how="outer").unique(subset=["CARDNO"])

# ---------------------------------------------------------------
# STEP 5.  Save Output to OCMSET and TEMP folders
# ---------------------------------------------------------------
OUTPUT_OCM = f"output/OCMSET_BANKCARD{REPTMON}{REPTYEAR}.parquet"
OUTPUT_TEMP = f"output/TEMP_BANKCARD{REPTMON}{REPTYEAR}.parquet"

merged.write_parquet(OUTPUT_OCM)
merged.write_csv(OUTPUT_OCM.replace(".parquet", ".csv"))

merged.write_parquet(OUTPUT_TEMP)
merged.write_csv(OUTPUT_TEMP.replace(".parquet", ".csv"))

print(f" OCMSET_BANKCARD{REPTMON}{REPTYEAR} generated successfully.")
