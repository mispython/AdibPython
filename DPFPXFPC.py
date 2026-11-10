# -*- coding: utf-8 -*-
"""
EIBDFALE.py  -  flat file -> parquet (if needed) -> DYIBU summaries -> SAS

Usage:
    python EIBDFALE.py                    # report date = yesterday
    python EIBDFALE.py --date 20261109    # explicit report date
    python EIBDFALE.py --force-convert    # rebuild parquet even if it exists

Requires: polars>=1.25, pyarrow, numpy, pandas, pyreadstat, saspy
"""
import os
os.environ.setdefault("POLARS_MAX_THREADS", str(os.cpu_count() or 4))

import argparse
import shutil
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta, date
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pyreadstat
import saspy

# =============================================================
# CONFIGURATION
# =============================================================
FLATFILE_PATTERN = "/stgsrcsys/host/holding/DPDARPGS_FB_{ymd}"   # <-- set to your real flat file name
PARQUET_IN_DIR   = "/stgsrcsys/host/holding"
INPUT_DYIBU_DIR  = "/stgsrcsys/host/uat/maa/python/input"        # existing dyibu*MM.sas7bdat
OUTPUT_LIBREF    = "WORK"                                         # e.g. MIS if saspy can see it

# ---- flat file layout / conversion settings ----
LRECL          = 1693
OPENIND_EBCDIC = True        # True if the file was binary-transferred from the mainframe (text still EBCDIC)
KEEP_ALL_ROWS  = True        # False = only keep BANKNO=33, REPTNO=4001, FMTCODE 1/2 (much smaller parquet)
CHUNK_RECORDS  = 250_000     # ~420 MB raw per chunk per worker
WORKERS        = min(os.cpu_count() or 4, 8)

# How LMATDATE is stored in the parquet (this converter writes the raw PD6 value -> "z11")
LMATDATE_ENCODING = "z11"

SAS_EPOCH = date(1960, 1, 1)
D_04SEP04, D_15APR06 = 20040904, 20060415
D_16APR06, D_15SEP08 = 20060416, 20080915
D_16SEP08            = 20080916

NEEDED_COLS = ["BANKNO", "REPTNO", "FMTCODE", "BRANCH", "ACCTNO",
               "OPENIND", "CURBAL", "INTPLAN", "LMATDATE"]
KEYS     = ["BRANCH", "INTPLAN"]
OUT_COLS = ["BRANCH", "INTPLAN", "FDINO", "FDINO2", "FDI", "REPTDATE"]


# =============================================================
# PART 1: FLAT FILE -> PARQUET
# =============================================================
def _pd(a: np.ndarray, start: int, length: int) -> np.ndarray:
    """Vectorised packed-decimal decode. a = (n, LRECL) uint8, start = 0-based offset."""
    b = a[:, start:start + length]
    hi, lo = b >> 4, b & 0x0F
    digits = np.empty((a.shape[0], 2 * length - 1), dtype=np.int64)
    digits[:, 0::2] = hi
    digits[:, 1::2] = lo[:, :-1]
    powers = 10 ** np.arange(2 * length - 2, -1, -1, dtype=np.int64)
    val = digits @ powers
    sign = lo[:, -1]
    return np.where((sign == 0x0D) | (sign == 0x0B), -val, val)


_EBCDIC = np.array([ord(c) for c in bytes(range(256)).decode("cp037")], dtype=np.uint32)


def _char1(a: np.ndarray, pos: int) -> pa.Array:
    col = a[:, pos]
    codes = _EBCDIC[col] if OPENIND_EBCDIC else col.astype(np.uint32)
    return pa.array(np.ascontiguousarray(codes).view("U1"), type=pa.string())


def _decode_chunk(args):
    """Worker: read records [start, start+n) and write one parquet part."""
    path, start, n, out_path = args
    a = np.fromfile(path, dtype=np.uint8, count=n * LRECL,
                    offset=start * LRECL).reshape(n, LRECL)

    # SAS: @3 BANKNO PD2, @24 REPTNO PD3, @27 FMTCODE PD2
    bankno, reptno, fmtcode = _pd(a, 2, 2), _pd(a, 23, 3), _pd(a, 26, 2)

    if not KEEP_ALL_ROWS:
        m = (bankno == 33) & (reptno == 4001) & ((fmtcode == 1) | (fmtcode == 2))
        a, bankno, reptno, fmtcode = a[m], bankno[m], reptno[m], fmtcode[m]
    if a.shape[0] == 0:
        return n, 0

    table = pa.table({
        "BANKNO":   pa.array(bankno, pa.int32()),
        "REPTNO":   pa.array(reptno, pa.int32()),
        "FMTCODE":  pa.array(fmtcode, pa.int16()),
        "BRANCH":   pa.array(_pd(a, 105, 4), pa.int32()),             # @106 PD4
        "ACCTNO":   pa.array(_pd(a, 109, 6), pa.int64()),             # @110 PD6
        "OPENIND":  _char1(a, 154),                                   # @155 $1
        "CURBAL":   pa.array(_pd(a, 155, 6) / 100.0, pa.float64()),   # @156 PD6.2
        "INTPLAN":  pa.array(_pd(a, 207, 2), pa.int32()),             # @208 PD2
        "LMATDATE": pa.array(_pd(a, 391, 6), pa.int64()),             # @392 PD6 (raw)
    })
    pq.write_table(table, out_path, compression="zstd", row_group_size=1_000_000)
    return n, a.shape[0]


def _read_tbdate(path: str) -> date:
    """SAS REPTDATE step: @106 TBDATE PD6 in the first record, first 8 digits of Z11. = MMDDYYYY."""
    a = np.fromfile(path, dtype=np.uint8, count=LRECL).reshape(1, LRECL)
    z11 = f"{int(_pd(a, 105, 6)[0]):011d}"
    return datetime.strptime(z11[:8], "%m%d%Y").date()


def convert_flatfile_to_parquet(flatfile: str, parquet_dir: str,
                                reptdate: date, force: bool = False):
    final = Path(parquet_dir)
    if final.is_dir() and not force:
        print(f"Parquet already exists, skipping conversion: {final}")
        return
    if not Path(flatfile).is_file():
        raise SystemExit(f"ERROR: flat file not found: {flatfile}")

    # Header check: same date the SAS job would have used
    tb = _read_tbdate(flatfile)
    if tb != reptdate:
        raise SystemExit(f"ERROR: TBDATE in flat file is {tb}, expected {reptdate}. "
                         f"Wrong file, or pass --date {tb:%Y%m%d}.")

    size = Path(flatfile).stat().st_size
    nrec = size // LRECL
    if size % LRECL:
        print(f"WARNING: file size is not a multiple of LRECL={LRECL}; "
              f"{size % LRECL} trailing bytes ignored (RDW/newline bytes in file?).")
    print(f"Converting {flatfile}: {nrec:,} records, {WORKERS} workers")

    tmp = final.with_name(final.name + ".tmp")
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True)

    tasks = [(flatfile, s, min(CHUNK_RECORDS, nrec - s), str(tmp / f"part-{i:05d}.parquet"))
             for i, s in enumerate(range(0, nrec, CHUNK_RECORDS))]

    done_in = done_out = 0
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for k, (n_in, n_out) in enumerate(ex.map(_decode_chunk, tasks), 1):
            done_in += n_in
            done_out += n_out
            if k % 10 == 0 or k == len(tasks):
                print(f"  {k}/{len(tasks)} chunks | read {done_in:,} | kept {done_out:,}")

    if done_out == 0:
        shutil.rmtree(tmp)
        raise SystemExit("ERROR: conversion produced 0 rows. Check LRECL / layout / OPENIND_EBCDIC.")

    if final.exists():
        shutil.rmtree(final)
    tmp.rename(final)          # only now does the directory count as "complete"
    print(f"Parquet ready: {final}")


# =============================================================
# PART 2: BUILD FD (one lazy pass over the parquet)
# =============================================================
def lmatdt_expr() -> pl.Expr:
    v = pl.col("LMATDATE").cast(pl.Int64)
    if LMATDATE_ENCODING == "yyyymmdd":
        return pl.when(v == 0).then(0).otherwise(v).alias("LMATDT")
    mmddyyyy = v // 1000
    mm, dd, yy = mmddyyyy // 1_000_000, (mmddyyyy // 10_000) % 100, mmddyyyy % 10_000
    return pl.when(v == 0).then(0).otherwise(yy * 10_000 + mm * 100 + dd).alias("LMATDT")


def build_fd(parquet_dir: str) -> pl.DataFrame:
    lf = (
        pl.scan_parquet(f"{parquet_dir}/**/*.parquet")
          .select(NEEDED_COLS)
          .filter(
              (pl.col("BANKNO") == 33) & (pl.col("REPTNO") == 4001) &
              (pl.col("FMTCODE").is_in([1, 2])) &
              (pl.col("OPENIND").is_in(["D", "O"])) &
              (pl.col("INTPLAN").is_between(340, 359) |
               pl.col("INTPLAN").is_between(448, 459) |
               pl.col("INTPLAN").is_between(461, 469) |
               pl.col("INTPLAN").is_between(580, 599) |
               pl.col("INTPLAN").is_between(660, 740))
          )
          .with_columns(pl.col("BRANCH").cast(pl.Int64),
                        pl.col("INTPLAN").cast(pl.Int64),
                        pl.col("CURBAL").cast(pl.Float64),
                        lmatdt_expr())
          .select(["BRANCH", "ACCTNO", "CURBAL", "INTPLAN", "LMATDT"])
    )
    return lf.collect(engine="streaming")


# =============================================================
# PART 3: SUMMARISE
# =============================================================
def summarise(fd, reptdate, lo=None, hi=None, exclude_zero=False):
    df = fd
    if exclude_zero:
        df = df.filter(pl.col("LMATDT") != 0)
    if lo is not None:
        df = df.filter(pl.col("LMATDT") >= lo)
    if hi is not None:
        df = df.filter(pl.col("LMATDT") <= hi)
    return (df.group_by(KEYS)
              .agg(pl.col("ACCTNO").n_unique().alias("FDINO"),
                   pl.len().alias("FDINO2"),
                   pl.col("CURBAL").sum().alias("FDI"))
              .with_columns(pl.lit(reptdate).cast(pl.Date).alias("REPTDATE"))
              .select(OUT_COLS).sort(KEYS))


def build_all(fd, reptdate):
    return {
        "DYIBUF": summarise(fd, reptdate),
        "DYIBUB": summarise(fd, reptdate, hi=D_04SEP04 - 1, exclude_zero=True),
        "DYIBUA": summarise(fd, reptdate, lo=D_04SEP04, hi=D_15APR06),
        "DYIBUN": summarise(fd, reptdate, lo=D_16APR06, hi=D_15SEP08),
        "DYIBUY": summarise(fd, reptdate, lo=D_16SEP08),
    }


# =============================================================
# PART 4: MONTHLY HISTORY MERGE + WRITE
# =============================================================
def normalise_existing(pdf) -> pl.DataFrame:
    pdf.columns = [c.upper() for c in pdf.columns]
    df = pl.from_pandas(pdf)
    dt = df.schema["REPTDATE"]
    if dt == pl.Date:
        pass
    elif isinstance(dt, pl.Datetime):
        df = df.with_columns(pl.col("REPTDATE").cast(pl.Date))
    elif dt.is_numeric():
        df = df.with_columns((pl.lit(SAS_EPOCH) +
                              pl.duration(days=pl.col("REPTDATE").cast(pl.Int64))).alias("REPTDATE"))
    else:
        df = df.with_columns(pl.col("REPTDATE").cast(pl.Utf8).str.to_date("%Y-%m-%d"))
    return df.select(pl.col("BRANCH").cast(pl.Int64), pl.col("INTPLAN").cast(pl.Int64),
                     pl.col("FDINO").cast(pl.UInt32), pl.col("FDINO2").cast(pl.UInt32),
                     pl.col("FDI").cast(pl.Float64), pl.col("REPTDATE"))


def load_existing(name, reptmon):
    path = Path(INPUT_DYIBU_DIR) / f"{name.lower()}{reptmon}.sas7bdat"
    if not path.exists():
        print(f"No existing {name}{reptmon} - will create new.")
        return None
    pdf, _ = pyreadstat.read_sas7bdat(str(path))
    df = normalise_existing(pdf)
    print(f"Loaded {name}{reptmon}: {len(df)} rows")
    return df


def combine(name, new_df, existing, reptday, reptdate):
    new_df = new_df.with_columns(pl.col("FDINO").cast(pl.UInt32), pl.col("FDINO2").cast(pl.UInt32))
    if reptday == "01" or existing is None:
        print(f"{name}: new month / no history - {len(new_df)} rows.")
        return new_df
    kept = existing.filter(pl.col("REPTDATE") != reptdate)
    print(f"{name}: {len(kept)} retained + {len(new_df)} new rows.")
    return pl.concat([kept, new_df], how="vertical_relaxed")


def save_via_saspy(sas, df, table):
    pdf = (df.with_columns((pl.col("REPTDATE") - pl.lit(SAS_EPOCH)).dt.total_days().alias("REPTDATE"))
             .to_pandas())
    sas.df2sd(pdf, table=table, libref=OUTPUT_LIBREF)
    sas.submit(f"proc datasets lib={OUTPUT_LIBREF} nolist; "
               f"modify {table}; format REPTDATE mmddyy10.; run; quit;")
    print(f"Wrote {table} -> {OUTPUT_LIBREF}.{table} ({len(pdf)} rows)")


# =============================================================
# MAIN
# =============================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="Report date YYYYMMDD (default: yesterday)")
    ap.add_argument("--force-convert", action="store_true", help="Rebuild parquet even if it exists")
    args = ap.parse_args()

    reptdate = (datetime.strptime(args.date, "%Y%m%d").date() if args.date
                else (datetime.now() - timedelta(days=1)).date())
    ymd = reptdate.strftime("%Y%m%d")
    reptmon, reptday = reptdate.strftime("%m"), reptdate.strftime("%d")

    flatfile    = FLATFILE_PATTERN.format(ymd=ymd)
    parquet_dir = f"{PARQUET_IN_DIR}/DPDARPGS_FB_{ymd}.parquet.dir"
    print(f"Report date: {reptdate}")

    # STEP A: convert if needed (skips automatically when parquet exists)
    convert_flatfile_to_parquet(flatfile, parquet_dir, reptdate, force=args.force_convert)

    # STEP B: summaries
    fd = build_fd(parquet_dir)
    print(f"FD (filtered accounts): {len(fd):,} rows")
    if len(fd) == 0:
        raise SystemExit("ERROR: FD is empty. If OPENIND never matches D/O, flip OPENIND_EBCDIC.")
    results = build_all(fd, reptdate)

    # STEP C: merge with monthly history and write
    sas = saspy.SASsession()
    try:
        for name, new_df in results.items():
            existing = None if reptday == "01" else load_existing(name, reptmon)
            save_via_saspy(sas, combine(name, new_df, existing, reptday, reptdate),
                           f"{name}{reptmon}")
    finally:
        sas.endsas()
    print("Done.")


if __name__ == "__main__":
    main()
