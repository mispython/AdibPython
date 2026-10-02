# minimal_test.py
# -*- coding: utf-8 -*-
import os, time, sys

print("Step A: importing pyarrow", flush=True)
t0 = time.time()
import pyarrow.parquet as pq
print(f"  pyarrow imported in {time.time()-t0:.1f}s", flush=True)

print("Step B: opening ONE part file", flush=True)
PART = "/stgsrcsys/host/holding/DPDARPGS_FB_20261001.parquet.dir/part-00000.parquet"

t0 = time.time()
pf = pq.ParquetFile(PART)
print(f"  opened in {time.time()-t0:.1f}s", flush=True)
print(f"  rows: {pf.metadata.num_rows}", flush=True)
print(f"  row groups: {pf.num_row_groups}", flush=True)

print("Step C: reading metadata only", flush=True)
t0 = time.time()
meta = pf.metadata
print(f"  metadata in {time.time()-t0:.1f}s", flush=True)

print("Step D: reading ONE row group", flush=True)
t0 = time.time()
table = pf.read_row_group(0)
print(f"  read in {time.time()-t0:.1f}s, {table.num_rows} rows", flush=True)

print("Step E: importing polars", flush=True)
t0 = time.time()
import polars as pl
print(f"  polars imported in {time.time()-t0:.1f}s", flush=True)
print(f"  threadpool: {pl.threadpool_size()}", flush=True)

print("Step F: scan one part file", flush=True)
t0 = time.time()
df = pl.read_parquet(PART)
print(f"  read in {time.time()-t0:.1f}s, {len(df)} rows", flush=True)

print("DONE", flush=True)
