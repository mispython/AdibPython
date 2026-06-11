# ============================================================
# JOB NAME : EIBMBTAT (Python version)
# DESC     : Average Original Tenure for BA Primary Rediscounted
# INPUT    : SAS7BDAT (SAS dataset)
# OUTPUT   : Parquet + CSV
# ============================================================

import pandas as pd
import sas7bdat  # Requires: pip install sas7bdat
from datetime import datetime
import glob
import os

# ============================================================
# 1. LOAD REPORT DATE (FROM LOAN.REPTDATE)
# ============================================================

def load_sas(path):
    """Load SAS .sas7bdat file into pandas DataFrame"""
    with sas7bdat.SAS7BDAT(path) as f:
        df = f.to_data_frame()
    return df

# SAS input paths (replace with real paths)
LOAN_REPTDATE_SAS = "LOAN_REPTDATE.sas7bdat"
BTRWH_BASE_PATH   = "BTRWH_BTRAD"   # base name only

rept_df = load_sas(LOAN_REPTDATE_SAS)

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

btrad_sas = f"{BTRWH_BASE_PATH}{REPTMON}{NOWK}{REPTYEAR}.sas7bdat"
print(f"Loading: {btrad_sas}")

if not os.path.exists(btrad_sas):
    raise FileNotFoundError(f"Input file not found: {btrad_sas}")

btrad = load_sas(btrad_sas)

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

print(f"Records after filtering: {len(btrad):,}")

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

# Round to 2 decimal places (optional, like SAS)
avgt["TENURE"] = avgt["TENURE"].round(2)

# ============================================================
# 7. ADD REPORT METADATA (OPTIONAL BUT USEFUL)
# ============================================================

avgt["REPORT_ID"]   = "EIBMBTAT"
avgt["REPORT_DATE"] = RDATE
avgt["REPTMON"]     = REPTMON
avgt["NOWK"]        = NOWK
avgt["REPTYEAR"]    = REPTYEAR

# Reorder columns for better readability
avgt = avgt[[
    "BRANCH", "FCVALUE", "TOTAMT", "TENURE",
    "REPORT_ID", "REPORT_DATE", "REPTMON", "NOWK", "REPTYEAR"
]]

# ============================================================
# 8. WRITE OUTPUT AS PARQUET AND CSV
# ============================================================

# Create output directory if it doesn't exist
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Output filenames with timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
base_filename = f"EIBMBTAT_AVGTENURE_{REPTMON}{NOWK}{REPTYEAR}"

OUTPUT_PARQUET = os.path.join(OUTPUT_DIR, f"{base_filename}.parquet")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, f"{base_filename}.csv")

# Save as Parquet (recommended)
avgt.to_parquet(
    OUTPUT_PARQUET,
    engine="pyarrow",
    compression="snappy",
    index=False
)
print(f"Parquet output saved to: {OUTPUT_PARQUET}")

# Save as CSV (optional, for compatibility)
avgt.to_csv(
    OUTPUT_CSV,
    index=False,
    encoding='utf-8'
)
print(f"CSV output saved to: {OUTPUT_CSV}")

# ============================================================
# 9. DISPLAY SUMMARY STATISTICS
# ============================================================

print("\n" + "="*60)
print("JOB SUMMARY")
print("="*60)
print(f"Report Date: {RDATE}")
print(f"Period: {REPTMON}/{NOWK}/{REPTYEAR}")
print(f"Total Branches: {len(avgt):,}")
print(f"Total FCVALUE: {avgt['FCVALUE'].sum():,.2f}")
print(f"Overall Average Tenure: {(avgt['TOTAMT'].sum() / avgt['FCVALUE'].sum()):.2f} days")
print("\nTop 10 Branches by Average Tenure:")
print(avgt.nlargest(10, "TENURE")[["BRANCH", "TENURE", "FCVALUE"]].to_string(index=False))
print("="*60)

# ============================================================
# END OF JOB
# ============================================================
print("\nEIBMBTAT job completed successfully (SAS7BDAT → Parquet + CSV).")
