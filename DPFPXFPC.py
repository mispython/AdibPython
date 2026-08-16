import pyreadstat
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta

# =========================================================
# 1. CONFIGURATION
# =========================================================
BASE_INPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIWBTCR")
BASE_OUTPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIWBTCR")
BASE_OUTPUT.mkdir(parents=True, exist_ok=True)

# Input files (SAS7BDAT files)
IMAST_FILE    = BASE_INPUT / "IMAST.sas7bdat"
ICRED_FILE    = BASE_INPUT / "ICRED.sas7bdat"
ISUBA_FILE    = BASE_INPUT / "ISUBA.sas7bdat"
BNMSUM_FILE   = BASE_INPUT / "BNMSUM.sas7bdat"
BTRSA_FILE    = BASE_INPUT / "BTRSA.sas7bdat"

# =========================================================
# 2. CREATE DATE VARIABLES USING TIMEDELTA
# =========================================================
TDATE = datetime.now() - timedelta(days=1)  # Yesterday's date
RDATE = TDATE.strftime("%d%m%y")

day_val, month_val, year_val = TDATE.day, TDATE.month, TDATE.year

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

# =========================================================
# 3. READ SAS DATASETS AND CONVERT TO POLARS
# =========================================================
def read_sas_to_polars(file_path):
    """Read SAS7BDAT file and convert to Polars DataFrame"""
    df, meta = pyreadstat.read_sas7bdat(str(file_path))
    return pl.from_pandas(df)

# MAST (Loan Master)
mast = read_sas_to_polars(IMAST_FILE)
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
cred = read_sas_to_polars(ICRED_FILE)

cred = cred.with_columns([
    pl.when(pl.col("MATUDTE") < TDATE)
      .then(pl.lit(1))
      .otherwise(0).alias("ARREARS")
])

# Merge with BNMSUM if needed
bnmsum = read_sas_to_polars(BNMSUM_FILE)
cred = cred.join(bnmsum, on="ACCTNO", how="left")

# =========================================================
# 5. SUBA (Subaccounts)
# =========================================================
suba = read_sas_to_polars(ISUBA_FILE)

# Join with BTRSA summaries
btrsa = read_sas_to_polars(BTRSA_FILE)
suba = suba.join(btrsa, on="ACCTNO", how="left")

# =========================================================
# 6. OUTPUT DATASETS AS TEXT FILES
# =========================================================
# ACCTCRED (account-level credit)
acctcred = mast.join(cred, on="ACCTNO", how="inner")
output_suffix = f"{REPTYEAR}{REPTMON}{REPTDAY}"

# Write as pipe-delimited text files (common in SAS environments)
# ACCTCRED
acctcred.write_csv(
    BASE_OUTPUT / f"ACCTCRED_{output_suffix}.txt",
    separator="|",
    include_header=True
)

# SUBACRED (subaccount-level credit)
subacred = suba
subacred.write_csv(
    BASE_OUTPUT / f"SUBACRED_{output_suffix}.txt",
    separator="|",
    include_header=True
)

# CREDITPO (credit position summary)
creditpo = (
    acctcred.group_by("ACCTNO")
    .agg([
        pl.sum("OUTSTAND").alias("TOTAL_OUTSTAND"),
        pl.sum("ARREARS").alias("TOTAL_ARREARS")
    ])
)
creditpo.write_csv(
    BASE_OUTPUT / f"CREDITPO_{output_suffix}.txt",
    separator="|",
    include_header=True
)

# Optional: Print summary
print(f"Processed date: {TDATE.strftime('%Y-%m-%d')}")
print(f"ACCTCRED records: {acctcred.height}")
print(f"SUBACRED records: {subacred.height}")
print(f"CREDITPO records: {creditpo.height}")
