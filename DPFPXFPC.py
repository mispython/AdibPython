# -*- coding: utf-8 -*-
import os
# Use all CPU cores for Polars (set BEFORE importing polars)
os.environ.setdefault("POLARS_MAX_THREADS", str(os.cpu_count() or 4))

import polars as pl
import pyreadstat
import saspy
from datetime import datetime, timedelta
from pathlib import Path

# -----------------------------
# CONFIGURATION
# -----------------------------
parquet_in_dir    = "/stgsrcsys/host/holding"                 # dir containing DPDARPGS_FB_*.parquet
input_dyibu_dir   = "/stgsrcsys/host/uat/maa/python/input"    # dir containing dyibu*{mon}.sas7bdat
output_dir        = "/stgsrcsys/host/uat/maa/python/output"

# REPTDATE = yesterday
run_date = datetime.now() - timedelta(days=1)
reptyear = run_date.strftime("%Y")
reptmon  = run_date.strftime("%m")
reptday  = run_date.strftime("%d")
rdate    = run_date.strftime("%Y-%m-%d")

# Deposit Parquet: DPDARPGS_FB_{YYYY}{MM}{DD}.parquet
parquet_file = f"{parquet_in_dir}/DPDARPGS_FB_{reptyear}{reptmon}{reptday}.parquet"

if not Path(parquet_file).exists():
    raise SystemExit(f"ERROR: Parquet file not found: {parquet_file}. Run flatfile_to_parquet.py first.")

# SAS session for writing SAS7BDAT
sas = saspy.SASsession()

# -----------------------------
# STEP 0: Read existing dyibu*{reptmon}.sas7bdat inputs (lowercase columns)
# -----------------------------
dyibu_names = ["DYIBUF", "DYIBUB", "DYIBUA", "DYIBUN", "DYIBUY"]

existing_dyibu = {}
for name in dyibu_names:
    fname = f"{name.lower()}{reptmon}.sas7bdat"
    path = Path(input_dyibu_dir) / fname

    if path.exists():
        pdf, meta = pyreadstat.read_sas7bdat(str(path))
        pdf.columns = [c.lower() for c in pdf.columns]
        existing_dyibu[name] = pl.from_pandas(pdf)
        print(f"Loaded {name} from {path.name}: {len(existing_dyibu[name])} rows")
    else:
        existing_dyibu[name] = None
        print(f"No existing {name} at {path} - will create new.")

# -----------------------------
# STEP 1: Lazy-scan the deposit Parquet
# -----------------------------
# The Parquet file already contains:
#   BANKNO, REPTNO, FMTCODE, BRANCH, ACCTNO, OPENIND, CURBAL, INTPLAN, LMATDATE
# No need for COMP-3 or EBCDIC decoding here.
print(f"Reading deposit parquet: {parquet_file}")
lf = pl.scan_parquet(parquet_file)

# -----------------------------
# STEP 2: Base filter (same logic as SAS)
# -----------------------------
lf = lf.filter(
    (pl.col("BANKNO") == 33) &
    (pl.col("REPTNO") == 4001) &
    (pl.col("FMTCODE").is_in([1, 2])) &
    (pl.col("OPENIND").is_in(["D", "O"])) &
    (
        ((pl.col("INTPLAN") >= 340) & (pl.col("INTPLAN") <= 359)) |
        ((pl.col("INTPLAN") >= 448) & (pl.col("INTPLAN") <= 459)) |
        ((pl.col("INTPLAN") >= 461) & (pl.col("INTPLAN") <= 469)) |
        ((pl.col("INTPLAN") >= 580) & (pl.col("INTPLAN") <= 599)) |
        ((pl.col("INTPLAN") >= 660) & (pl.col("INTPLAN") <= 740))
    )
)

# -----------------------------
# STEP 3: Period boundaries as integers (YYYYMMDD)
# -----------------------------
# Avoid date parsing entirely - integer comparisons are ~10x faster.
P_2004_09_04 = 20040904
P_2006_04_15 = 20060415
P_2006_04_16 = 20060416
P_2008_09_15 = 20080915
P_2008_09_16 = 20080916

# Common filtering on LMATDATE > 0 (valid maturity date)
lf_valid = lf.filter(pl.col("LMATDATE") > 0)

# -----------------------------
# STEP 4: Two-pass aggregation (specific periods, then DYIBUF)
# -----------------------------

# --- Pass 1: DYIBUB, DYIBUA, DYIBUN, DYIBUY (specific periods) ---
lf_tagged = lf_valid.with_columns(
    pl.when(pl.col("LMATDATE") < P_2004_09_04).then(pl.lit("DYIBUB"))
      .when(pl.col("LMATDATE") <= P_2006_04_15).then(pl.lit("DYIBUA"))
      .when(pl.col("LMATDATE") <= P_2008_09_15).then(pl.lit("DYIBUN"))
      .otherwise(pl.lit("DYIBUY"))
      .alias("PERIOD")
)

agg_specific = (
    lf_tagged
      .group_by(["PERIOD", "BRANCH", "INTPLAN"])
      .agg([
          pl.len().alias("FDINO"),
          pl.sum("CURBAL").alias("FDI"),
      ])
      .collect(streaming=True)
)
print(f"Specific periods aggregate: {len(agg_specific)} rows")

# --- Pass 2: DYIBUF (all rows with LMATDATE > 0) ---
agg_all = (
    lf_valid
      .group_by(["BRANCH", "INTPLAN"])
      .agg([
          pl.len().alias("FDINO"),
          pl.sum("CURBAL").alias("FDI"),
      ])
      .collect(streaming=True)
)
print(f"DYIBUF aggregate: {len(agg_all)} rows")

# -----------------------------
# STEP 5: Split into named DataFrames
# -----------------------------
def split_period(df: pl.DataFrame, name: str) -> pl.DataFrame:
    return (
        df.filter(pl.col("PERIOD") == name)
          .drop("PERIOD")
          .with_columns(pl.lit(run_date).alias("REPTDATE"))
    )

DYIBUF = agg_all.with_columns(pl.lit(run_date).alias("REPTDATE"))
DYIBUB = split_period(agg_specific, "DYIBUB")
DYIBUA = split_period(agg_specific, "DYIBUA")
DYIBUN = split_period(agg_specific, "DYIBUN")
DYIBUY = split_period(agg_specific, "DYIBUY")

new_results = {
    "DYIBUF": DYIBUF,
    "DYIBUB": DYIBUB,
    "DYIBUA": DYIBUA,
    "DYIBUN": DYIBUN,
    "DYIBUY": DYIBUY,
}
for k, v in new_results.items():
    print(f"{k}: {len(v)} rows")

# -----------------------------
# STEP 6: Merge with existing + write SAS7BDAT via saspy
# -----------------------------
def save_via_saspy(df: pl.DataFrame, name: str):
    """Write a polars DataFrame to a SAS dataset via saspy with lowercase columns."""
    pdf = df.to_pandas()
    pdf.columns = [c.lower() for c in pdf.columns]

    # Convert REPTDATE to SAS-friendly date string
    if "reptdate" in pdf.columns:
        pdf["reptdate"] = pdf["reptdate"].dt.strftime("%Y-%m-%d")

    sas.df2sd(pdf, table=name, libref="WORK")
    print(f"Wrote {name} -> WORK.{name} ({len(pdf)} rows)")

for name, new_df in new_results.items():
    existing = existing_dyibu.get(name)

    if existing is not None and len(existing) > 0:
        if reptday == "01":
            combined = new_df
            print(f"{name}: REPTDAY=01 - replacing all existing rows.")
        else:
            if "reptdate" in existing.columns:
                existing = existing.filter(
                    pl.col("reptdate").cast(pl.Utf8) != rdate
                )
            combined = pl.concat([existing, new_df], how="diagonal_relaxed")
            print(f"{name}: appended {len(new_df)} rows to {len(existing)} retained rows.")

        save_via_saspy(combined, name)
    else:
        save_via_saspy(new_df, name)

print("All summaries successfully exported as SAS7BDAT via saspy.")
