import polars as pl
from pathlib import Path
from datetime import datetime

# =========================================================
# 1. CONFIGURATION
# =========================================================
BASE_INPUT = Path("data/input")
BASE_OUTPUT = Path("data/output")
BASE_OUTPUT.mkdir(parents=True, exist_ok=True)

# Input files (replace with your actual parquet file names)
REPTDATE_FILE = BASE_INPUT / "LOAN_REPTDATE.parquet"
IMAST_FILE    = BASE_INPUT / "IMAST.parquet"
ICRED_FILE    = BASE_INPUT / "ICRED.parquet"
ISUBA_FILE    = BASE_INPUT / "ISUBA.parquet"
BNMSUM_FILE   = BASE_INPUT / "BNMSUM.parquet"
BTRSA_FILE    = BASE_INPUT / "BTRSA.parquet"

# =========================================================
# 2. READ REPTDATE AND CREATE VARIABLES
# =========================================================
reptdate_df = pl.read_parquet(REPTDATE_FILE)
reptdate = reptdate_df["REPTDATE"][0]  # assume single value
if isinstance(reptdate, str):
    reptdate = datetime.strptime(reptdate, "%Y-%m-%d")

day_val, month_val, year_val = reptdate.day, reptdate.month, reptdate.year

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

NOWK, NOWKS, NOWK1 = WK, "4", WK1
REPTMON, REPTMON1 = f"{month_val:02d}", f"{MM1:02d}"
REPTYEAR, REPTDAY = str(year_val), f"{day_val:02d}"
RDATE, TDATE = reptdate.strftime("%d%m%y"), reptdate

# =========================================================
# 3. MAST (Loan Master)
# =========================================================
mast = pl.read_parquet(IMAST_FILE)

mast = (
    mast.with_columns([
        pl.when(pl.col("CUSTCODE").is_null() | (pl.col("CUSTCODE") == ""))
          .then("0000000000").otherwise(pl.col("CUSTCODE")).alias("CUSTCODE"),
        pl.when(pl.col("SECTOR").is_null() | (pl.col("SECTOR") == ""))
          .then("0000").otherwise(pl.col("SECTOR")).alias("SECTOR")
    ])
)

# =========================================================
# 4. CREDIT (Loan Credit)
# =========================================================
cred = pl.read_parquet(ICRED_FILE)

cred = cred.with_columns([
    pl.when(pl.col("MATUDTE") < TDATE)
      .then(pl.lit(1))
      .otherwise(0).alias("ARREARS")
])

# Example: merge with BNMSUM if needed
bnmsum = pl.read_parquet(BNMSUM_FILE)
cred = cred.join(bnmsum, on="ACCTNO", how="left")

# =========================================================
# 5. SUBA (Subaccounts)
# =========================================================
suba = pl.read_parquet(ISUBA_FILE)

# Join with BTRSA summaries (example)
btrsa = pl.read_parquet(BTRSA_FILE)
suba = suba.join(btrsa, on="ACCTNO", how="left")

# =========================================================
# 6. OUTPUT DATASETS
# =========================================================
# ACCTCRED (account-level credit)
acctcred = mast.join(cred, on="ACCTNO", how="inner")
acctcred.write_parquet(BASE_OUTPUT / f"ACCTCRED_{REPTYEAR}{REPTMON}{REPTDAY}.parquet")
acctcred.write_csv(BASE_OUTPUT / f"ACCTCRED_{REPTYEAR}{REPTMON}{REPTDAY}.csv")

# SUBACRED (subaccount-level credit)
subacred = suba
subacred.write_parquet(BASE_OUTPUT / f"SUBACRED_{REPTYEAR}{REPTMON}{REPTDAY}.parquet")
subacred.write_csv(BASE_OUTPUT / f"SUBACRED_{REPTYEAR}{REPTMON}{REPTDAY}.csv")

# CREDITPO (credit position summary)
creditpo = (
    acctcred.groupby("ACCTNO")
    .agg([
        pl.sum("OUTSTAND").alias("TOTAL_OUTSTAND"),
        pl.sum("ARREARS").alias("TOTAL_ARREARS")
    ])
)
creditpo.write_parquet(BASE_OUTPUT / f"CREDITPO_{REPTYEAR}{REPTMON}{REPTDAY}.parquet")
creditpo.write_csv(BASE_OUTPUT / f"CREDITPO_{REPTYEAR}{REPTMON}{REPTDAY}.csv")

print(f"✅ Completed EIIWBTCR extraction for {RDATE}")
