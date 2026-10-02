# read_all_polars_8threads.py
# -*- coding: utf-8 -*-
import os, time
os.environ["POLARS_MAX_THREADS"] = "8"   # <-- key change

import polars as pl

PDIR = "/stgsrcsys/host/holding/DPDARPGS_FB_20261001.parquet.dir"

print(f"Threads: {pl.thread_pool_size()}", flush=True)
t0 = time.time()
n = pl.scan_parquet(PDIR).select(pl.len()).collect(streaming=True).item()
print(f"Counted {n:,} rows in {time.time()-t0:.1f}s", flush=True)
