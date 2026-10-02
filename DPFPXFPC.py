# -*- coding: utf-8 -*-
import os
os.environ["POLARS_MAX_THREADS"] = "16"     # 16 is plenty; 80 causes contention

import time
import polars as pl
import pyreadstat
import saspy
from datetime import datetime, timedelta
from pathlib import Path

def tic(): return time.time()
def toc(label, t0): print(f"[{time.time()-t0:6.2f}s] {label}", flush=True)

# ---------- CONFIG ----------
parquet_in_dir  = "/stgsrcsys/host/holding"
input_dyibu_dir = "/stgsrcsys/host/uat/maa/python/input"
output_dir      = "/stgsrcsys/host/uat/maa/python/output"

run_date = datetime.now() - timedelta(days=1)
reptyear = run_date.strftime("%Y")
reptmon  = run_date.strftime("%m")
reptday  = run_date.strftime("%d")
rdate    = run_date.strftime("%Y-%m-%d")

parquet_dir = f"{parquet_in_dir}/DPDARPGS_FB_{reptyear}{reptmon}{reptday}.parquet.dir"
if not Path(parquet_dir).is_dir():
    raise SystemExit(f"Parquet dir not found: {parquet_dir}")

print(f"Polars threads: {pl.thread_pool_size()}", flush=True)

# ---------- Existing dyibu* files ----------
t0 = tic()
existing_dyibu = {}
for name in ["DYIBUF", "DYIBUB", "DYIBUA", "DYIBUN", "DYIBUY"]:
    fname = f"{name.lower()}{reptmon}.sas7bdat"
    path = Path(input_dyibu_dir) / fname
    if path.exists():
        pdf, _ = pyreadstat.read_sas7bdat(str(path))
        pdf.columns = [c.lower() for c in pdf.columns]
        existing_dyibu[name] = pl.from_pandas(pdf)
        print(f"  loaded {fname}: {len(existing_dyibu[name])} rows", flush=True)
    else:
        existing_dyibu[name] = None
toc("read existing dyibu*", t0)

# ---------- Build lazy pipeline ----------
lf = pl.scan_parquet(parquet_dir)

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

lf_tagged = lf.with_columns(
    pl.when(pl.col("LMATDATE") < 20040904).then(pl.lit("DYIBUB"))
      .when(pl.col("LMATDATE") <= 20060415).then(pl.lit("DYIBUA"))
      .when(pl.col("LMATDATE") <= 20080915).then(pl.lit("DYIBUN"))
      .otherwise(pl.lit("DYIBUY"))
      .alias("PERIOD")
)

# ---------- Aggregations (default engine, no streaming=) ----------
t0 = tic()
agg_specific = (
    lf_tagged
      .group_by(["PERIOD", "BRANCH", "INTPLAN"])
      .agg([pl.len().alias("FDINO"), pl.sum("CURBAL").alias("FDI")])
      .collect()
)
toc(f"agg_specific ({len(agg_specific)} rows)", t0)

t0 = tic()
agg_all = (
    lf.group_by(["BRANCH", "INTPLAN"])
      .agg([pl.len().alias("FDINO"), pl.sum("CURBAL").alias("FDI")])
      .collect()
)
toc(f"agg_all ({len(agg_all)} rows)", t0)

# ---------- Split ----------
def split_period(df, name):
    return (
        df.filter(pl.col("PERIOD") == name)
          .drop("PERIOD")
          .with_columns(pl.lit(run_date).alias("REPTDATE"))
    )

new_results = {
    "DYIBUF": agg_all.with_columns(pl.lit(run_date).alias("REPTDATE")),
    "DYIBUB": split_period(agg_specific, "DYIBUB"),
    "DYIBUA": split_period(agg_specific, "DYIBUA"),
    "DYIBUN": split_period(agg_specific, "DYIBUN"),
    "DYIBUY": split_period(agg_specific, "DYIBUY"),
}
for k, v in new_results.items():
    print(f"  {k}: {len(v)} rows", flush=True)

# ---------- saspy session ----------
t0 = tic()
sas = saspy.SASsession()
toc("saspy session", t0)

# ---------- Merge + write ----------
def save_via_saspy(df, name):
    t0 = tic()
    pdf = df.to_pandas()
    pdf.columns = [c.lower() for c in pdf.columns]
    if "reptdate" in pdf.columns:
        pdf["reptdate"] = pdf["reptdate"].dt.strftime("%Y-%m-%d")
    toc(f"  to_pandas {name}", t0)

    t0 = tic()
    sas.df2sd(pdf, table=name, libref="WORK")
    toc(f"  sas.df2sd {name} ({len(pdf)} rows)", t0)

for name, new_df in new_results.items():
    existing = existing_dyibu.get(name)
    if existing is not None and len(existing) > 0:
        if reptday == "01":
            combined = new_df
        else:
            if "reptdate" in existing.columns:
                existing = existing.filter(pl.col("reptdate").cast(pl.Utf8) != rdate)
            combined = pl.concat([existing, new_df], how="diagonal_relaxed")
        save_via_saspy(combined, name)
    else:
        save_via_saspy(new_df, name)

print("All summaries exported.", flush=True)
