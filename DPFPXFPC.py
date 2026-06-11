# ============================================================
# JOB NAME : EIBMBTAT (Python version)
# DESC     : Average Original Tenure for BA Primary Rediscounted
# INPUT    : Binary (pickle)
# OUTPUT   : Parquet
# ============================================================

import pandas as pd
import pickle
from datetime import datetime

# ============================================================
# 1. LOAD REPORT DATE (FROM LOAN.REPTDATE)
# ============================================================

def load_binary(path):
    with open(path, "rb") as f:
        return pickle.load(f)

# Binary input (replace with real paths)
LOAN_REPTDATE_BIN = "LOAN_REPTDATE.bin"
BTRWH_BASE_PATH   = "BTRWH_BTRAD"   # base name only

rept_df = load_binary(LOAN_REPTDATE_BIN)

# Assume single-row dataset like SAS
REPTDATE = pd.to_datetime(rept_df.iloc[0]["REPTDATE"])

day = REPTDATE.day
if day == 8:
    NOWK = "1"
elif day == 15:
    NOWK = "2"
elif day == 22:
    NOWK = "3"
else:
    NOWK = "4"

REPTYEAR = REPTDATE.strftime("%y")
REPTMON  = REPTDATE.strftime("%m")
RDATE    = REPTDATE.strftime("%d/%m/%Y")

# ============================================================
# 2. LOAD BTRAD DATASET (DYNAMIC NAME)
# SAS: BTRWH.BTRAD&REPTMON&NOWK&REPTYEAR
# ============================================================

btrad_bin = f"{BTRWH_BASE_PATH}{REPTMON}{NOWK}{REPTYEAR}.bin"
btrad = load_binary(btrad_bin)

# ============================================================
# 3. FILTER DATA (SAS WHERE CLAUSE)
# ============================================================

FACILITY_LIST = [
    "34411", "34412", "34421", "34422",
    "34440", "34470", "34480", "34490"
]

LIABCODE_LIST = ["BAE", "BAI", "BAP", "BAS"]

btrad = btrad[
    (btrad["FACILITY"].isin(FACILITY_LIST)) &
    (btrad["LIABCODE"].isin(LIABCODE_LIST)) &
    (btrad["UTRDF"] == "D") &
    (btrad["BALANCE"] > 0)
].copy()

# ============================================================
# 4. CALCULATE TENURE & TOTAMT
# ============================================================

btrad["ISSDTE"]  = pd.to_datetime(btrad["ISSDTE"])
btrad["MATDATE"] = pd.to_datetime(btrad["MATDATE"])

btrad["TENURE"] = (btrad["MATDATE"] - btrad["ISSDTE"]).dt.days + 1
btrad["TOTAMT"] = btrad["FCVALUE"] * btrad["TENURE"]

btrad = btrad[
    ["BRANCH", "ACCTNOX", "TRANSREF", "FCVALUE",
     "MATDATE", "ISSDTE", "TENURE", "TOTAMT"]
]

# ============================================================
# 5. AGGREGATE (PROC SUMMARY)
# ============================================================

avgt = (
    btrad
    .groupby("BRANCH", as_index=False)
    .agg({
        "FCVALUE": "sum",
        "TOTAMT": "sum"
    })
)

# ============================================================
# 6. CALCULATE AVERAGE TENURE
# ============================================================

avgt["TENURE"] = avgt["TOTAMT"] / avgt["FCVALUE"]

# ============================================================
# 7. ADD REPORT METADATA (OPTIONAL BUT USEFUL)
# ============================================================

avgt["REPORT_ID"]   = "EIBMBTAT"
avgt["REPORT_DATE"] = RDATE
avgt["REPTMON"]     = REPTMON
avgt["NOWK"]        = NOWK
avgt["REPTYEAR"]    = REPTYEAR

# ============================================================
# 8. WRITE OUTPUT AS PARQUET
# ============================================================

OUTPUT_PARQUET = "SAP.EIBMBTAT.AVGTENURE.parquet"

avgt.to_parquet(
    OUTPUT_PARQUET,
    engine="pyarrow",
    compression="snappy",
    index=False
)

# ============================================================
# END OF JOB
# ============================================================

print("EIBMBTAT job completed successfully (Binary → Parquet).")
