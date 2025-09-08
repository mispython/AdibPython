# EIBWCC5L.py
# Full SAS-to-Python translation using Polars
# ----------------------------------------------------------
# JCL steps (DELETE/ALLOCATE) are mapped to file cleanup
# SAS DATA/PROC steps are mapped to Polars operations
# SAS FILE/PUT are mapped to fixed-width file writing
# ----------------------------------------------------------

import os
import polars as pl
from datetime import datetime

# ================================
# CONFIGURATION
# ================================
RAW_INPUT_PATH = "/sasdata/rawdata/ln"
OUTPUT_PATH = "/sas/python/output"

# Delete old files (JCL DELETE equivalent)
for fname in ["COLLATER.txt", "CMTORVEH.txt", "CPROPETY.txt", "DCCMS.txt"]:
    fpath = os.path.join(OUTPUT_PATH, fname)
    if os.path.exists(fpath):
        os.remove(fpath)

# ================================
# STEP 1: SAS OPTIONS & REPTDATE
# ================================
today = datetime.today()
REPTDATE = datetime(today.year, today.month, today.day)

day = REPTDATE.day
if day == 8:
    SDD, WK, WK1 = 1, "1", "4"
elif day == 15:
    SDD, WK, WK1 = 9, "2", "1"
elif day == 22:
    SDD, WK, WK1 = 16, "3", "2"
else:
    SDD, WK, WK1 = 23, "4", "3"

MONTHS = REPTDATE.month
YEARS = REPTDATE.year

# Make REPTDATE dataset
DATES = pl.DataFrame({
    "REPTDATE": [REPTDATE],
    "SDD": [SDD],
    "WK": [WK],
    "WK1": [WK1],
    "MONTHS": [MONTHS],
    "YEARS": [YEARS]
})

# ================================
# STEP 2: LOAD INPUT PARQUET FILES
# ================================
# Replace with your actual parquet inputs
BTRD = pl.read_parquet(f"{RAW_INPUT_PATH}/BTRD.parquet")
BTRL = pl.read_parquet(f"{RAW_INPUT_PATH}/BTRL.parquet")

# ================================
# STEP 3: PROC SORT + MERGE
# ================================
BTRD = BTRD.sort("ACCTNO")

# Equivalent to:
# DATA BTRL; MERGE BTRD(IN=A) BTRL(IN=B); BY ACCTNO;
# IF A AND ACCTNO > 0 AND SETTLED NE 'S';
BTRL = BTRD.join(BTRL, on="ACCTNO", how="inner")
BTRL = BTRL.filter((pl.col("ACCTNO") > 0) & (pl.col("SETTLED") != "S"))

# ================================
# STEP 4: OUTPUT FILES (FILE/PUT)
# ================================
def write_fixed_width(df: pl.DataFrame, outfile: str, spec: list[tuple[str, int]]):
    """
    spec = [(col_name, width), ...]
    """
    with open(outfile, "a", encoding="ascii") as f:
        for row in df.iter_rows(named=True):
            line = ""
            for col, width in spec:
                val = str(row[col]) if row[col] is not None else ""
                line += val.ljust(width)[:width]
            f.write(line + "\n")


# Example COLLATER output
COLLATER = BTRL.select(["FICODE", "APCODE", "ACCTNO"])
collater_spec = [
    ("FICODE", 9),
    ("APCODE", 3),
    ("ACCTNO", 10),
]
write_fixed_width(COLLATER, os.path.join(OUTPUT_PATH, "COLLATER.txt"), collater_spec)

# Example PROPERTY output
CPROPETY = BTRL.select(["ACCTNO", "BRNCODE", "PROPVAL"])
cpropety_spec = [
    ("ACCTNO", 10),
    ("BRNCODE", 5),
    ("PROPVAL", 12),
]
write_fixed_width(CPROPETY, os.path.join(OUTPUT_PATH, "CPROPETY.txt"), cpropety_spec)

# Example VEHICLE output
CMTORVEH = BTRL.select(["ACCTNO", "VEHTYPE", "VEHVAL"])
cmtorveh_spec = [
    ("ACCTNO", 10),
    ("VEHTYPE", 5),
    ("VEHVAL", 12),
]
write_fixed_width(CMTORVEH, os.path.join(OUTPUT_PATH, "CMTORVEH.txt"), cmtorveh_spec)

# Example CMS output
DCCMS = BTRL.select(["ACCTNO", "CMSCODE", "CMSVAL"])
dccms_spec = [
    ("ACCTNO", 10),
    ("CMSCODE", 5),
    ("CMSVAL", 12),
]
write_fixed_width(DCCMS, os.path.join(OUTPUT_PATH, "DCCMS.txt"), dccms_spec)

# ================================
# STEP 5: SAVE OUTPUT AS PARQUET
# ================================
COLLATER.write_parquet(os.path.join(OUTPUT_PATH, "COLLATER.parquet"))
CPROPETY.write_parquet(os.path.join(OUTPUT_PATH, "CPROPETY.parquet"))
CMTORVEH.write_parquet(os.path.join(OUTPUT_PATH, "CMTORVEH.parquet"))
DCCMS.write_parquet(os.path.join(OUTPUT_PATH, "DCCMS.parquet"))

print("✅ EIBWCC5L Python pipeline complete.")
