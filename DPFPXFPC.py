# read_all_pyarrow.py
# -*- coding: utf-8 -*-
import os, time
from pathlib import Path
import pyarrow.parquet as pq

PDIR = "/stgsrcsys/host/holding/DPDARPGS_FB_20261001.parquet.dir"
parts = sorted(Path(PDIR).glob("part-*.parquet"))
print(f"Found {len(parts)} part files", flush=True)

total = 0
t_start = time.time()
for i, p in enumerate(parts):
    t0 = time.time()
    pf = pq.ParquetFile(p)
    n = pf.metadata.num_rows
    total += n
    dt = time.time() - t0
    if i < 5 or i % 20 == 0 or dt > 1.0:
        print(f"  [{i:3d}/{len(parts)}] {p.name}: {n:,} rows in {dt:.3f}s", flush=True)

dt = time.time() - t_start
print(f"\nTotal {total:,} rows across {len(parts)} parts in {dt:.1f}s", flush=True)
