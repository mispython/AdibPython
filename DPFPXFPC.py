# diag.py
import os

FLAT_FILE = "/host_pq/dwh/input/DEPOSIT/DPDARPGS_FB_20260924"
RECORD_LEN = 1693

# ---------- 1. File size ----------
size = os.path.getsize(FLAT_FILE)
print(f"1. File size    : {size:,} bytes ({size/1e6:.1f} MB, {size/1e9:.2f} GB)")
print(f"   Expected records if RECORD_LEN={RECORD_LEN}: {size // RECORD_LEN:,}")

# ---------- 2. Line count ----------
line_count = 0
with open(FLAT_FILE, "rb") as f:
    for _ in f:
        line_count += 1
print(f"2. Line count   : {line_count:,}")

# ---------- 3. First five 1693-byte blocks, show tail bytes ----------
print("3. First five 1693-byte blocks:")
with open(FLAT_FILE, "rb") as f:
    for i in range(5):
        block = f.read(RECORD_LEN)
        print(f"   rec {i}: len={len(block)}  tail={block[-8:]!r}")

# ---------- 4. Parquet row count ----------
import pyarrow.parquet as pq
PARQUET = "/stgsrcsys/host/holding/DPDARPGS_FB_20260924.parquet"
pf = pq.ParquetFile(PARQUET)
print(f"4. Parquet rows : {pf.metadata.num_rows:,}")
print(f"   Parquet size : {os.path.getsize(PARQUET):,} bytes")
