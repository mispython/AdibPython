# isolate_slow_step.py
# -*- coding: utf-8 -*-
import os
os.environ.setdefault("POLARS_MAX_THREADS", "8")

import time
from pathlib import Path
import polars as pl
import pyreadstat
import saspy

PDIR = "/stgsrcsys/host/holding/DPDARPGS_FB_20261001.parquet.dir"
DYIBU_DIR = "/stgsrcsys/host/uat/maa/python/input"
REPTMON = "10"

def sep(title):
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60, flush=True)

# ---------- 1. Read existing dyibu* files ----------
sep("1. Read existing dyibu* files")

dyibu_names = ["DYIBUF", "DYIBUB", "DYIBUA", "DYIBUN", "DYIBUY"]
existing = {}
for name in dyibu_names:
    fname = f"{name.lower()}{REPTMON}.sas7bdat"
    path = Path(DYIBU_DIR) / fname
    if not path.exists():
        print(f"{fname}: not found")
        existing[name] = None
        continue
    size_mb = path.stat().st_size / 1e6
    t0 = time.time()
    try:
        pdf, meta = pyreadstat.read_sas7bdat(str(path))
        dt = time.time() - t0
        print(f"{fname:20s} {size_mb:8.1f} MB  {len(pdf):>10,} rows  {dt:6.2f}s", flush=True)
        existing[name] = pdf
    except Exception as e:
        print(f"{fname:20s}  ERROR: {e}")

# ---------- 2. Do the full filter + aggregation ----------
sep("2. Full aggregation on Parquet")

lf = pl.scan_parquet(PDIR)

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
).filter(pl.col("LMATDATE") > 0)

lf = lf.with_columns(
    pl.when(pl.col("LMATDATE") < 20040904).then(pl.lit("DYIBUB"))
      .when(pl.col("LMATDATE") <= 20060415).then(pl.lit("DYIBUA"))
      .when(pl.col("LMATDATE") <= 20080915).then(pl.lit("DYIBUN"))
      .otherwise(pl.lit("DYIBUY"))
      .alias("PERIOD")
)

t0 = time.time()
agg_specific = (
    lf.group_by(["PERIOD", "BRANCH", "INTPLAN"])
      .agg([pl.len().alias("FDINO"), pl.sum("CURBAL").alias("FDI")])
      .collect()
)
print(f"agg_specific: {len(agg_specific):,} rows in {time.time()-t0:.2f}s", flush=True)

t0 = time.time()
agg_all = (
    lf.group_by(["BRANCH", "INTPLAN"])
      .agg([pl.len().alias("FDINO"), pl.sum("CURBAL").alias("FDI")])
      .collect()
)
print(f"agg_all: {len(agg_all):,} rows in {time.time()-t0:.2f}s", flush=True)

# ---------- 3. Convert to pandas ----------
sep("3. to_pandas conversion")

t0 = time.time()
pdf = agg_all.to_pandas()
pdf.columns = [c.lower() for c in pdf.columns]
print(f"to_pandas (agg_all): {len(pdf):,} rows in {time.time()-t0:.2f}s", flush=True)

# ---------- 4. saspy session ----------
sep("4. saspy session init")

t0 = time.time()
try:
    sas = saspy.SASsession()
    print(f"saspy init: {time.time()-t0:.2f}s", flush=True)
except Exception as e:
    print(f"saspy init failed: {e}")
    sas = None

# ---------- 5. saspy write ----------
if sas is not None:
    sep("5. sas.df2sd upload")

    t0 = time.time()
    try:
        sas.df2sd(pdf, table="TEST_DYIBUF", libref="WORK")
        print(f"df2sd (WORK.TEST_DYIBUF, {len(pdf):,} rows): {time.time()-t0:.2f}s", flush=True)
    except Exception as e:
        print(f"df2sd failed: {e}")

# ---------- 6. Merge test ----------
sep("6. Merge with existing dyibu*")

if existing.get("DYIBUF") is not None:
    ex = pl.from_pandas(existing["DYIBUF"])
    ex.columns = [c.lower() for c in ex.columns]
    print(f"existing DYIBUF: {len(ex):,} rows")

    t0 = time.time()
    combined = pl.concat([ex, agg_all.with_columns(pl.lit("2026-10-02").alias("REPTDATE"))],
                         how="diagonal_relaxed")
    print(f"concat: {len(combined):,} rows in {time.time()-t0:.2f}s", flush=True)

print("\nDONE - paste full output")
