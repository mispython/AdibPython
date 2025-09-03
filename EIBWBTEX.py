EIBWBTEX

import polars as pl
from pathlib import Path
from datetime import datetime

# ======================================
# 1. CONFIGURATION
# ======================================
BASE_INPUT = Path("data/input")
BASE_OUTPUT = Path("data/output")
BASE_OUTPUT.mkdir(parents=True, exist_ok=True)

REPTDATE_FILE = BASE_INPUT / "LOAN_REPTDATE.parquet"
CRFTABL_FILE  = BASE_INPUT / "BTRADE_CRFTABL.parquet"

# ======================================
# 2. READ REPORT DATE (REPTDATE)
# ======================================
reptdate_df = pl.read_parquet(REPTDATE_FILE)
reptdate_value = reptdate_df["REPTDATE"][0]

# SAS macro variable equivalents
day_val = reptdate_value.day
month_val = reptdate_value.month
year_val = reptdate_value.year

if day_val == 8:
    SDD, WK, WK1 = 1, "1", "4"
elif day_val == 15:
    SDD, WK, WK1 = 9, "2", "1"
elif day_val == 22:
    SDD, WK, WK1 = 16, "3", "2"
else:
    SDD, WK, WK1 = 23, "4", "3"

MM1 = month_val - 1 if month_val > 1 else 12
SDATE = datetime(year_val, month_val, SDD)
STARTDT = datetime(year_val, month_val, 1)

NOWK     = WK
NOWKS    = "4"
NOWK1    = WK1
REPTMON  = f"{month_val:02d}"
REPTMON1 = f"{MM1:02d}"
REPTYEAR = str(year_val)
REPTDAY  = f"{day_val:02d}"
RDATE    = reptdate_value.strftime("%d%m%y")
TDATE    = reptdate_value

# ======================================
# 3. READ CRFTABL (TABLE dataset)
# ======================================
table = pl.read_parquet(CRFTABL_FILE).select([
    "TFNUM", "TFID", "SUBACCT", "TFINDR02", "TFINDR03", "TFINDR05", "ACCTNO"
]).unique(subset=["ACCTNO", "SUBACCT"])

# ======================================
# 4. READ & TRANSFORM COMM (PRNGL)
# ======================================
COMM_FILE = BASE_INPUT / f"BTRADE_COMM{REPTDAY}{REPTMON}.parquet"
prngl = pl.read_parquet(COMM_FILE)

prngl = (
    prngl.with_columns([
        pl.when((pl.col("CREATDS").is_not_null()) & (pl.col("CREATDS") != 0))
          .then(pl.col("CREATDS").cast(pl.Date))
          .alias("ISSDTE"),
        pl.when((pl.col("EXPIRDS").is_not_null()) & (pl.col("EXPIRDS") != 0))
          .then(pl.col("EXPIRDS").cast(pl.Date))
          .alias("EXPRDATE"),
        pl.when(pl.col("GLMNEM").str.starts_with("0"))
          .then(pl.col("GLMNEM") + "0")
          .otherwise(pl.col("GLMNEM"))
          .alias("GLMNEMO")
    ])
    .filter(~pl.col("GLMNEM").is_in(["CONI", "CONM", "CONP", "DDA", "HOE", "IBS"]))
    .filter(~pl.col("GLMNEM").str.starts_with("9"))
    .filter(pl.col("ISSDTE") <= pl.lit(TDATE))
)

# ======================================
# 5. BRANCH REMAPPING (EXTCOMM2)
# ======================================
branch_map = {
    119: 207, 132: 197, 182: 58, 212: 165, 213: 57,
    227: 81, 229: 151, 255: 68, 223: 24, 236: 69,
    246: 146, 250: 92, 200: 122
}
extcomm2 = prngl.with_columns(
    pl.col("BRANCH").replace(branch_map).alias("BRANCH")
)

# ======================================
# 6. PROC SUMMARY (Group Totals)
# ======================================
extcomm2_summary = (
    extcomm2.groupby(["BRANCH", "GLMNEMO", "ACCTNO", "SUBACCT"])
            .agg(pl.sum("GLTRNTOT").alias("GLTRNTOT"))
)

# ======================================
# 7. PROC TABULATE Equivalent
# ======================================
tab = (
    extcomm2.groupby(["GLMNEMO", "BRANCH"])
            .agg(pl.sum("GLTRNTOT").alias("GLTRNTOT"))
)

# ======================================
# 8. SAVE FINAL OUTPUT
# ======================================
csv_out = BASE_OUTPUT / f"BTDTL_{REPTMON}{NOWK}.csv"
parquet_out = BASE_OUTPUT / f"BTDTL_{REPTMON}{NOWK}.parquet"

tab.write_csv(csv_out)
tab.write_parquet(parquet_out)

print(f"Completed BTRWH Extraction for {RDATE}")
print(f"CSV saved to: {csv_out}")
print(f"Parquet saved to: {parquet_out}")
