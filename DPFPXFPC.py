# -*- coding: utf-8 -*-
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
# The Parquet file written by flatfile_to_parquet.py already contains:
#   BANKNO, REPTNO, FMTCODE, BRANCH, ACCTNO, OPENIND, CURBAL, INTPLAN, LMATDATE
print(f"Reading deposit parquet: {parquet_file}")
lf = pl.scan_parquet(parquet_file)

# Parse LMATDATE (YYYYMMDD int or 0) into a date column, lazily
lf = lf.with_columns(
    pl.when(pl.col("LMATDATE") > 0)
      .then(
          pl.col("LMATDATE").cast(pl.Utf8).str.zfill(8)
            .str.strptime(pl.Date, "%Y%m%d", strict=False)
      )
      .otherwise(None)
      .alias("LMATDT")
)

# -----------------------------
# STEP 2: Filter valid deposits (same logic as SAS)
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
# STEP 3: Helper - summarize by period (streaming collect)
# -----------------------------
def summarise_period(lf: pl.LazyFrame, label: str, condition) -> pl.DataFrame:
    df = (
        lf.filter(condition)
          .group_by(["BRANCH", "INTPLAN"])
          .agg([
              pl.len().alias("FDINO"),
              pl.sum("CURBAL").alias("FDI"),
          ])
          .with_columns(pl.lit(run_date).alias("REPTDATE"))
          .collect(streaming=True)          # <- key: bounded memory
    )
    print(f"{label}: {len(df)} rows summarized.")
    return df

# -----------------------------
# STEP 4: Period definitions
# -----------------------------
D_2004_09_04 = datetime(2004, 9, 4).date()
D_2006_04_15 = datetime(2006, 4, 15).date()
D_2006_04_16 = datetime(2006, 4, 16).date()
D_2008_09_15 = datetime(2008, 9, 15).date()
D_2008_09_16 = datetime(2008, 9, 16).date()

DYIBUF = summarise_period(lf, "DYIBUF", pl.col("LMATDT").is_not_null())
DYIBUB = summarise_period(lf, "DYIBUB", pl.col("LMATDT") < D_2004_09_04)
DYIBUA = summarise_period(
    lf, "DYIBUA",
    (pl.col("LMATDT") >= D_2004_09_04) & (pl.col("LMATDT") <= D_2006_04_15)
)
DYIBUN = summarise_period(
    lf, "DYIBUN",
    (pl.col("LMATDT") >= D_2006_04_16) & (pl.col("LMATDT") <= D_2008_09_15)
)
DYIBUY = summarise_period(lf, "DYIBUY", pl.col("LMATDT") >= D_2008_09_16)

new_results = {
    "DYIBUF": DYIBUF,
    "DYIBUB": DYIBUB,
    "DYIBUA": DYIBUA,
    "DYIBUN": DYIBUN,
    "DYIBUY": DYIBUY,
}

# -----------------------------
# STEP 5: Merge with existing + write SAS7BDAT via saspy
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
            # SAS logic: on the 1st, delete ALL existing and replace
            combined = new_df
            print(f"{name}: REPTDAY=01 - replacing all existing rows.")
        else:
            # SAS logic: delete rows with the same REPTDATE, then append
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
