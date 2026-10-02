# -*- coding: utf-8 -*-
import os
os.environ.setdefault("POLARS_MAX_THREADS", str(os.cpu_count() or 4))

import polars as pl
import pyreadstat
import saspy
from datetime import datetime, timedelta
from pathlib import Path

# -----------------------------
# CONFIGURATION
# -----------------------------
parquet_in_dir  = "/stgsrcsys/host/holding"
input_dyibu_dir = "/stgsrcsys/host/uat/maa/python/input"
output_dir      = "/stgsrcsys/host/uat/maa/python/output"

run_date = datetime.now() - timedelta(days=1)
reptyear = run_date.strftime("%Y")
reptmon  = run_date.strftime("%m")
reptday  = run_date.strftime("%d")
rdate    = run_date.strftime("%Y-%m-%d")

# Partitioned Parquet directory
parquet_dir = f"{parquet_in_dir}/DPDARPGS_FB_{reptyear}{reptmon}{reptday}.parquet.dir"

if not Path(parquet_dir).is_dir():
    raise SystemExit(
        f"ERROR: Parquet directory not found: {parquet_dir}. "
        f"Run flatfile_to_parquet.py first."
    )

# SAS session
sas = saspy.SASsession()

# -----------------------------
# STEP 0: existing dyibu*{mon}.sas7bdat
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
# STEP 1: Lazy-scan the PARTITIONED Parquet
# -----------------------------
print(f"Reading deposit parquet dir: {parquet_dir}")
lf = pl.scan_parquet(parquet_dir)

# -----------------------------
# STEP 2: Base filter
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
# STEP 3: Period boundaries as integers
# -----------------------------
P_2004_09_04 = 20040904
P_2006_04_15 = 20060415
P_2006_04_16 = 20060416
P_2008_09_15 = 20080915
P_2008_09_16 = 20080916

lf_valid = lf.filter(pl.col("LMATDATE") > 0)

# -----------------------------
# STEP 4: Two-pass aggregation
# -----------------------------
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
def split_period(df, name):
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
# STEP 6: Merge + write via saspy
# -----------------------------
def save_via_saspy(df, name):
    pdf = df.to_pandas()
    pdf.columns = [c.lower() for c in pdf.columns]
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
                existing = existing.filter(pl.col("reptdate").cast(pl.Utf8) != rdate)
            combined = pl.concat([existing, new_df], how="diagonal_relaxed")
            print(f"{name}: appended {len(new_df)} rows to {len(existing)} retained rows.")
        save_via_saspy(combined, name)
    else:
        save_via_saspy(new_df, name)

print("All summaries successfully exported as SAS7BDAT via saspy.")
