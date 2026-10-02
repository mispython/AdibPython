# -*- coding: utf-8 -*-
"""
Runtime diagnostics for the deposit pipeline.
Checks storage, CPU, memory, Polars threads, and does a minimal Parquet read.
"""
import os
import sys
import time
import shutil
import subprocess
from pathlib import Path

PARQUET_DIR = "/stgsrcsys/host/holding/DPDARPGS_FB_20261001.parquet.dir"

def header(title):
    print("=" * 78)
    print(title)
    print("=" * 78)

# ---------------- 1. System basics ----------------
header("1. SYSTEM BASICS")

print(f"Python version : {sys.version.split()[0]}")
print(f"CPU cores      : {os.cpu_count()}")
print(f"cwd            : {os.getcwd()}")

# Memory
try:
    meminfo = {}
    with open("/proc/meminfo") as f:
        for line in f:
            k, _, v = line.partition(":")
            meminfo[k.strip()] = v.strip()
    print(f"Mem total      : {meminfo.get('MemTotal', '?')}")
    print(f"Mem free       : {meminfo.get('MemFree', '?')}")
    print(f"Mem available  : {meminfo.get('MemAvailable', '?')}")
except Exception as e:
    print(f"Mem info failed: {e}")

# Disk
try:
    usage = shutil.disk_usage(PARQUET_DIR)
    print(f"Disk total     : {usage.total / 1e9:.1f} GB")
    print(f"Disk used      : {usage.used / 1e9:.1f} GB")
    print(f"Disk free      : {usage.free / 1e9:.1f} GB")
except Exception as e:
    print(f"Disk info failed: {e}")

# Mount info
try:
    with open("/proc/mounts") as f:
        for line in f:
            parts = line.split()
            if len(parts) >= 3 and Path(PARQUET_DIR).is_relative_to(parts[1]) if hasattr(Path, "is_relative_to") else False:
                print(f"Mount          : {parts[0]} on {parts[1]} ({parts[2]})")
except Exception:
    # is_relative_to needs py3.9+, so fallback
    pass

# Manual mount lookup (works on py3.8+)
with open("/proc/mounts") as f:
    best = None
    for line in f:
        parts = line.split()
        if len(parts) < 3:
            continue
        mnt = parts[1]
        if PARQUET_DIR.startswith(mnt):
            if best is None or len(mnt) > len(best[1]):
                best = (parts[0], mnt, parts[2])
    if best:
        print(f"Mount          : {best[0]} on {best[1]} ({best[2]})")

# ---------------- 2. Parquet directory ----------------
header("2. PARQUET DIRECTORY")

pdir = Path(PARQUET_DIR)
if not pdir.is_dir():
    print(f"NOT A DIRECTORY: {PARQUET_DIR}")
    sys.exit(1)

parts = sorted(pdir.glob("part-*.parquet"))
print(f"Part files      : {len(parts)}")
if parts:
    sizes = [p.stat().st_size for p in parts]
    print(f"Total size      : {sum(sizes) / 1e6:.1f} MB")
    print(f"Avg part size   : {sum(sizes) / len(sizes) / 1e6:.2f} MB")
    print(f"First part      : {parts[0].name} ({sizes[0]/1e6:.2f} MB)")
    print(f"Last part       : {parts[-1].name} ({sizes[-1]/1e6:.2f} MB)")

# ---------------- 3. Polars ----------------
header("3. POLARS")

# Force a thread count before importing polars
os.environ.setdefault("POLARS_MAX_THREADS", str(os.cpu_count() or 4))

import polars as pl

print(f"Polars version  : {pl.__version__}")
print(f"Thread pool     : {pl.threadpool_size()}")

# ---------------- 4. Row count (pure I/O) ----------------
header("4. ROW COUNT (pure scan)")

t0 = time.time()
n = (
    pl.scan_parquet(PARQUET_DIR)
      .select(pl.len())
      .collect(streaming=True)
      .item()
)
dt = time.time() - t0
print(f"Total rows      : {n:,}")
print(f"Elapsed         : {dt:.1f}s")
print(f"Throughput      : {n/dt/1e6:.1f} M rows/s")

# ---------------- 5. Simple filter ----------------
header("5. SIMPLE FILTER (BANKNO==33)")

t0 = time.time()
m = (
    pl.scan_parquet(PARQUET_DIR)
      .select([pl.col("BANKNO")])
      .filter(pl.col("BANKNO") == 33)
      .select(pl.len())
      .collect(streaming=True)
      .item()
)
dt = time.time() - t0
print(f"BANKNO==33 count: {m:,}")
print(f"Elapsed         : {dt:.1f}s")

# ---------------- 6. Full filter + group-by ----------------
header("6. FULL FILTER + GROUP-BY")

t0 = time.time()
df = (
    pl.scan_parquet(PARQUET_DIR)
      .filter(
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
      .group_by(["BRANCH", "INTPLAN"])
      .agg([
          pl.len().alias("FDINO"),
          pl.sum("CURBAL").alias("FDI"),
      ])
      .collect(streaming=True)
)
dt = time.time() - t0
print(f"Groups          : {len(df)}")
print(f"Elapsed         : {dt:.1f}s")

# ---------------- 7. Existing dyibu* files ----------------
header("7. EXISTING DYIBU* FILES")

import pyreadstat
input_dyibu_dir = "/stgsrcsys/host/uat/maa/python/input"
reptmon = "10"   # adjust to current month

for name in ["DYIBUF", "DYIBUB", "DYIBUA", "DYIBUN", "DYIBUY"]:
    fname = f"{name.lower()}{reptmon}.sas7bdat"
    path = Path(input_dyibu_dir) / fname
    if path.exists():
        size_mb = path.stat().st_size / 1e6
        t0 = time.time()
        try:
            pdf, meta = pyreadstat.read_sas7bdat(str(path))
            dt = time.time() - t0
            print(f"{fname:24s}  {size_mb:8.2f} MB  {len(pdf):>8,} rows  {dt:6.2f}s")
        except Exception as e:
            print(f"{fname:24s}  ERROR: {e}")
    else:
        print(f"{fname:24s}  not found")

# ---------------- 8. saspy session ----------------
header("8. SASPY SESSION")

try:
    t0 = time.time()
    import saspy
    sas = saspy.SASsession()
    dt = time.time() - t0
    print(f"saspy init      : {dt:.1f}s")
except Exception as e:
    print(f"saspy failed    : {e}")

# ---------------- 9. Summary ----------------
header("9. SUMMARY")
print("Paste the entire output of this script back for diagnosis.")
