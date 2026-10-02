# read_all_polars.py
# -*- coding: utf-8 -*-
import os, time
os.environ["POLARS_MAX_THREADS"] = "8"   # limit threads to avoid memory blowup

import polars as pl
from pathlib import Path

PDIR = "/stgsrcsys/host/holding/DPDARPGS_FB_20261001.parquet.dir"

print(f"Threads: {pl.thread_pool_size()}", flush=True)
t0 = time.time()
df = pl.read_parquet(PDIR)
print(f"Read {len(df):,} rows in {time.time()-t0:.1f}s", flush=True)
