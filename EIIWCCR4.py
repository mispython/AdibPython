import polars as pl
from pathlib import Path
from datetime import datetime

# =========================================
# CONFIGURATION
# =========================================
BASE_INPUT = Path("data/input")
BASE_OUTPUT = Path("data/output")
BASE_OUTPUT.mkdir(parents=True, exist_ok=True)

# File mappings (update with actual parquet sources)
REPTDATE_FILE = BASE_INPUT / "LOAN_REPTDATE.parquet"
CRFTABL_FILE = BASE_INPUT / "BTRADE_CRFTABL.parquet"
COMM_FILE = BASE_INPUT / "COMM.parquet"
HSTR_FILE = BASE_INPUT / "HSTR.parquet"
LOAN_FILE = BASE_INPUT / "LOAN.parquet"

# =========================================
# READ REPORT DATE
# =========================================
reptdate_df = pl.read_parquet(REPTDATE_FILE)
reptdate_value = reptdate_df["REPTDATE"][0]

if isinstance(reptdate_value, str):
    reptdate_value = datetime.strptime(reptdate_value, "%Y-%m-%d")

REPTYEAR = reptdate_value.year
REPTMON = reptdate_value.month
REPTDAY = reptdate_value.day
RDATE = reptdate_value.strftime("%d%m%y")

# =========================================
# CRFTABL TABLE HANDLING
# =========================================
table = pl.read_parquet(CRFTABL_FILE)
table = table.select(["TFNUM", "TFID", "SUBACCT", "TFINDR02", "TFINDR03", "TFINDR05", "ACCTNO"]).unique(
    subset=["ACCTNO", "SUBACCT"]
)

# =========================================
# COMM DATASET (TRANSACTIONS)
# =========================================
prngl = pl.read_parquet(COMM_FILE)

prngl = (
    prngl.with_columns([
        pl.when((pl.col("CREATDS").is_not_null()) & (pl.col("CREATDS") != 0))
          .then(pl.col("CREATDS").cast(pl.Date)).otherwise(None).alias("ISSDTE"),
        pl.when((pl.col("EXPIRDS").is_not_null()) & (pl.col("EXPIRDS") != 0))
          .then(pl.col("EXPIRDS").cast(pl.Date)).otherwise(None).alias("EXPRDATE"),
        pl.when(pl.col("GLMNEM").str.starts_with("0"))
          .then(pl.col("GLMNEM") + "0")
          .otherwise(pl.col("GLMNEM"))
          .alias("GLMNEMO")
    ])
    .filter(~pl.col("GLMNEM").is_in(["CONI", "CONM", "CONP", "DDA", "HOE", "IBS"]))
    .filter(~pl.col("GLMNEM").str.starts_with("9"))
    .filter(pl.col("ISSDTE") <= pl.lit(reptdate_value))
)

# =========================================
# HSTR DATASET (HISTORICAL)
# =========================================
hstr = pl.read_parquet(HSTR_FILE)
hstr = hstr.filter(pl.col("REPTDATE") <= reptdate_value)

# =========================================
# LOAN DATASET
# =========================================
loan = pl.read_parquet(LOAN_FILE)
loan = loan.filter(pl.col("REPTDATE") == reptdate_value)

# =========================================
# BRANCH REMAPPING
# =========================================
branch_map = {
    119: 207, 132: 197, 182: 58, 212: 165, 213: 57,
    227: 81, 229: 151, 255: 68, 223: 24, 236: 69,
    246: 146, 250: 92, 200: 122
}

if "BRANCH" in prngl.columns:
    prngl = prngl.with_columns(
        pl.col("BRANCH").replace(branch_map).alias("BRANCH")
    )

# =========================================
# PROC SUMMARY (example aggregation)
# =========================================
if {"BRANCH", "GLMNEMO", "ACCTNO", "SUBACCT"}.issubset(prngl.columns):
    extcomm2_summary = (
        prngl.groupby(["BRANCH", "GLMNEMO", "ACCTNO", "SUBACCT"])
             .agg(pl.sum("GLTRNTOT").alias("GLTRNTOT"))
    )
else:
    extcomm2_summary = pl.DataFrame()

# =========================================
# PROC TABULATE (example pivot)
# =========================================
if {"GLMNEMO", "BRANCH", "GLTRNTOT"}.issubset(prngl.columns):
    tab = (
        prngl.groupby(["GLMNEMO", "BRANCH"])
             .agg(pl.sum("GLTRNTOT").alias("GLTRNTOT"))
    )
else:
    tab = pl.DataFrame()

# =========================================
# OUTPUTS
# =========================================
prngl.write_parquet(BASE_OUTPUT / f"COMM_{RDATE}.parquet")
prngl.write_csv(BASE_OUTPUT / f"COMM_{RDATE}.csv")

hstr.write_parquet(BASE_OUTPUT / f"HSTR_{RDATE}.parquet")
hstr.write_csv(BASE_OUTPUT / f"HSTR_{RDATE}.csv")

loan.write_parquet(BASE_OUTPUT / f"LOAN_{RDATE}.parquet")
loan.write_csv(BASE_OUTPUT / f"LOAN_{RDATE}.csv")

extcomm2_summary.write_parquet(BASE_OUTPUT / f"SUMMARY_{RDATE}.parquet")
extcomm2_summary.write_csv(BASE_OUTPUT / f"SUMMARY_{RDATE}.csv")

tab.write_parquet(BASE_OUTPUT / f"TAB_{RDATE}.parquet")
tab.write_csv(BASE_OUTPUT / f"TAB_{RDATE}.csv")

print(f"✅ Completed EIIWBTCR pipeline for {RDATE}")
