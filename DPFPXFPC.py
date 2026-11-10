# -*- coding: utf-8 -*-
"""
Python equivalent of SAS job: DYIBU summaries (DYIBUF/B/A/N/Y) by BRANCH, INTPLAN.

Usage:
    python dyibu_summary.py                 # report date = yesterday
    python dyibu_summary.py --date 20261109 # explicit report date (YYYYMMDD)

Requires polars >= 1.25, pyreadstat, saspy, pandas, pyarrow.
"""
import os
os.environ.setdefault("POLARS_MAX_THREADS", str(os.cpu_count() or 4))

import argparse
from datetime import datetime, timedelta, date
from pathlib import Path

import polars as pl
import pyreadstat
import saspy

# =============================================================
# CONFIGURATION
# =============================================================
PARQUET_IN_DIR  = "/stgsrcsys/host/holding"
INPUT_DYIBU_DIR = "/stgsrcsys/host/uat/maa/python/input"     # existing dyibu*MM.sas7bdat
OUTPUT_LIBREF   = "WORK"                                      # SAS libref to write to (e.g. MIS)

# How LMATDATE is stored in the parquet:
#   "z11"      -> raw SAS PD6 value; first 8 digits of Z11. = MMDDYYYY  (e.g. 09152008000)
#   "yyyymmdd" -> converter already produced 20080915
LMATDATE_ENCODING = "z11"

SAS_EPOCH = date(1960, 1, 1)

# Period boundaries (YYYYMMDD ints) - same as SAS
D_04SEP04, D_15APR06 = 20040904, 20060415
D_16APR06, D_15SEP08 = 20060416, 20080915
D_16SEP08            = 20080916

NEEDED_COLS = ["BANKNO", "REPTNO", "FMTCODE", "BRANCH", "ACCTNO",
               "OPENIND", "CURBAL", "INTPLAN", "LMATDATE"]
KEYS = ["BRANCH", "INTPLAN"]
OUT_COLS = ["BRANCH", "INTPLAN", "FDINO", "FDINO2", "FDI", "REPTDATE"]
NAMES = ["DYIBUF", "DYIBUB", "DYIBUA", "DYIBUN", "DYIBUY"]


# =============================================================
# REPORT DATE
# =============================================================
def get_reptdate() -> date:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="Report date YYYYMMDD (default: yesterday)")
    args = ap.parse_args()
    if args.date:
        return datetime.strptime(args.date, "%Y%m%d").date()
    return (datetime.now() - timedelta(days=1)).date()


# =============================================================
# STEP 1: ONE PASS OVER THE BIG PARQUET -> small FD table
# =============================================================
def lmatdt_expr() -> pl.Expr:
    """LMATDT as YYYYMMDD integer; 0 when LMATDATE = 0 (same as SAS LMATDT=0)."""
    v = pl.col("LMATDATE").cast(pl.Int64)
    if LMATDATE_ENCODING == "yyyymmdd":
        return pl.when(v == 0).then(0).otherwise(v).alias("LMATDT")
    mmddyyyy = v // 1000                       # drop last 3 digits of Z11.
    mm = mmddyyyy // 1_000_000
    dd = (mmddyyyy // 10_000) % 100
    yy = mmddyyyy % 10_000
    return (pl.when(v == 0).then(0)
              .otherwise(yy * 10_000 + mm * 100 + dd)
              .alias("LMATDT"))


def build_fd(parquet_dir: str) -> pl.DataFrame:
    lf = pl.scan_parquet(f"{parquet_dir}/**/*.parquet")

    lf = (
        lf.select(NEEDED_COLS)                         # column pruning
          .filter(                                      # predicate pushdown
              (pl.col("BANKNO") == 33) &
              (pl.col("REPTNO") == 4001) &
              (pl.col("FMTCODE").is_in([1, 2])) &
              (pl.col("OPENIND").is_in(["D", "O"])) &
              (
                  pl.col("INTPLAN").is_between(340, 359) |
                  pl.col("INTPLAN").is_between(448, 459) |
                  pl.col("INTPLAN").is_between(461, 469) |
                  pl.col("INTPLAN").is_between(580, 599) |
                  pl.col("INTPLAN").is_between(660, 740)
              )
          )
          .with_columns(
              pl.col("BRANCH").cast(pl.Int64),
              pl.col("INTPLAN").cast(pl.Int64),
              pl.col("CURBAL").cast(pl.Float64),
              lmatdt_expr(),
          )
          .select(["BRANCH", "ACCTNO", "CURBAL", "INTPLAN", "LMATDT"])
    )
    return lf.collect(engine="streaming")


# =============================================================
# STEP 2: SUMMARISE (all in memory, from the small FD)
# =============================================================
def summarise(fd: pl.DataFrame, reptdate: date,
              lo: int | None = None, hi: int | None = None,
              exclude_zero: bool = False) -> pl.DataFrame:
    df = fd
    if exclude_zero:
        df = df.filter(pl.col("LMATDT") != 0)
    if lo is not None:
        df = df.filter(pl.col("LMATDT") >= lo)
    if hi is not None:
        df = df.filter(pl.col("LMATDT") <= hi)

    return (
        df.group_by(KEYS)
          .agg(
              pl.col("ACCTNO").n_unique().alias("FDINO"),   # NODUPKEYS count
              pl.len().alias("FDINO2"),                      # raw row count
              pl.col("CURBAL").sum().alias("FDI"),
          )
          .with_columns(pl.lit(reptdate).cast(pl.Date).alias("REPTDATE"))
          .select(OUT_COLS)
          .sort(KEYS)
    )


def build_all(fd: pl.DataFrame, reptdate: date) -> dict[str, pl.DataFrame]:
    return {
        "DYIBUF": summarise(fd, reptdate),
        "DYIBUB": summarise(fd, reptdate, hi=D_04SEP04 - 1, exclude_zero=True),
        "DYIBUA": summarise(fd, reptdate, lo=D_04SEP04, hi=D_15APR06),
        "DYIBUN": summarise(fd, reptdate, lo=D_16APR06, hi=D_15SEP08),
        "DYIBUY": summarise(fd, reptdate, lo=D_16SEP08),
    }


# =============================================================
# STEP 3: EXISTING MONTHLY HISTORY (MIS.DYIBUx&REPTMON)
# =============================================================
def normalise_existing(pdf) -> pl.DataFrame:
    pdf.columns = [c.upper() for c in pdf.columns]
    df = pl.from_pandas(pdf)
    dt = df.schema["REPTDATE"]
    if dt == pl.Date:
        pass
    elif isinstance(dt, pl.Datetime):
        df = df.with_columns(pl.col("REPTDATE").cast(pl.Date))
    elif dt.is_numeric():                                  # SAS numeric date
        df = df.with_columns(
            (pl.lit(SAS_EPOCH) + pl.duration(days=pl.col("REPTDATE").cast(pl.Int64)))
            .alias("REPTDATE"))
    else:                                                  # string
        df = df.with_columns(pl.col("REPTDATE").cast(pl.Utf8).str.to_date("%Y-%m-%d"))

    return df.select(
        pl.col("BRANCH").cast(pl.Int64),
        pl.col("INTPLAN").cast(pl.Int64),
        pl.col("FDINO").cast(pl.UInt32),
        pl.col("FDINO2").cast(pl.UInt32),
        pl.col("FDI").cast(pl.Float64),
        pl.col("REPTDATE"),
    )


def load_existing(name: str, reptmon: str) -> pl.DataFrame | None:
    path = Path(INPUT_DYIBU_DIR) / f"{name.lower()}{reptmon}.sas7bdat"
    if not path.exists():
        print(f"No existing {name}{reptmon} at {path} - will create new.")
        return None
    pdf, _ = pyreadstat.read_sas7bdat(str(path))
    df = normalise_existing(pdf)
    print(f"Loaded {name}{reptmon}: {len(df)} rows")
    return df


# =============================================================
# STEP 4: MERGE (MISAPPD macro logic) + WRITE
# =============================================================
def combine(name: str, new_df: pl.DataFrame, existing: pl.DataFrame | None,
            reptday: str, reptdate: date) -> pl.DataFrame:
    new_df = new_df.with_columns(
        pl.col("FDINO").cast(pl.UInt32), pl.col("FDINO2").cast(pl.UInt32))
    if reptday == "01" or existing is None:
        # REPTDAY=01: SAS deletes the month's datasets, then appends -> fresh month
        print(f"{name}: new month / no history - {len(new_df)} rows.")
        return new_df
    kept = existing.filter(pl.col("REPTDATE") != reptdate)   # IF REPTDATE EQ &RDATE THEN DELETE
    print(f"{name}: {len(kept)} retained + {len(new_df)} new rows.")
    return pl.concat([kept, new_df], how="vertical_relaxed")  # PROC APPEND


def save_via_saspy(sas, df: pl.DataFrame, table: str):
    pdf = (df.with_columns(
               (pl.col("REPTDATE") - pl.lit(SAS_EPOCH)).dt.total_days().alias("REPTDATE"))
             .to_pandas())
    sas.df2sd(pdf, table=table, libref=OUTPUT_LIBREF)
    sas.submit(f"proc datasets lib={OUTPUT_LIBREF} nolist; "
               f"modify {table}; format REPTDATE mmddyy10.; run; quit;")
    print(f"Wrote {table} -> {OUTPUT_LIBREF}.{table} ({len(pdf)} rows)")


# =============================================================
# MAIN
# =============================================================
def main():
    reptdate = get_reptdate()
    reptyear, reptmon, reptday = (reptdate.strftime("%Y"),
                                  reptdate.strftime("%m"),
                                  reptdate.strftime("%d"))

    parquet_dir = f"{PARQUET_IN_DIR}/DPDARPGS_FB_{reptyear}{reptmon}{reptday}.parquet.dir"
    if not Path(parquet_dir).is_dir():
        raise SystemExit(f"ERROR: Parquet directory not found: {parquet_dir}. "
                         f"Run flatfile_to_parquet.py first.")

    print(f"Report date: {reptdate}  |  source: {parquet_dir}")

    # One pass over the huge data
    fd = build_fd(parquet_dir)
    print(f"FD (filtered accounts): {len(fd)} rows")

    # All summaries from the in-memory FD
    results = build_all(fd, reptdate)
    for k, v in results.items():
        print(f"{k}: {len(v)} rows")

    # Merge with monthly history and write
    sas = saspy.SASsession()
    try:
        for name, new_df in results.items():
            existing = None if reptday == "01" else load_existing(name, reptmon)
            combined = combine(name, new_df, existing, reptday, reptdate)
            save_via_saspy(sas, combined, f"{name}{reptmon}")
    finally:
        sas.endsas()

    print("Done.")


if __name__ == "__main__":
    main()
