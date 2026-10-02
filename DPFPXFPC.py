# -*- coding: utf-8 -*-
"""
Convert the deposit flat file to a PARTITIONED Parquet dataset.
Writes one small Parquet file per read-chunk, into a directory.
Reads in parallel across CPU cores when consumed by pl.scan_parquet(dir).
"""
import os
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from pathlib import Path

# ---------- CONFIG ----------
input_dir       = "/host_pq/dwh/input/DEPOSIT"
parquet_out_dir = "/stgsrcsys/host/holding"

run_date = datetime.now() - timedelta(days=1)
reptyear = run_date.strftime("%Y")
reptmon  = run_date.strftime("%m")
reptday  = run_date.strftime("%d")

flat_file    = f"{input_dir}/DPDARPGS_FB_{reptyear}{reptmon}{reptday}"
parquet_root = f"{parquet_out_dir}/DPDARPGS_FB_{reptyear}{reptmon}{reptday}.parquet.dir"

RECORD_LEN  = 1693
CHUNK_BYTES = 500 * 1024 * 1024   # 500 MB per read -> one part file per chunk

FIELDS = {
    "BANKNO":   (2,   4,   0),
    "REPTNO":   (23,  26,  0),
    "FMTCODE":  (26,  28,  0),
    "BRANCH":   (105, 109, 0),
    "ACCTNO":   (109, 115, 0),
    "OPENIND":  (154, 155, -1),
    "CURBAL":   (155, 161, 2),
    "INTPLAN":  (207, 209, 0),
    "LMATDATE": (391, 397, 0),
}

EBCDIC_TO_CHAR = np.array([bytes([i]).decode("cp037") for i in range(256)])

# ---------- DECODERS ----------
def decode_packed_2d(raw, scale=0):
    high = (raw >> 4) & 0x0F
    low  = raw & 0x0F
    n_rows, n_bytes = raw.shape
    vals = np.zeros(n_rows, dtype=np.int64)
    for i in range(n_bytes - 1):
        vals = vals * 100 + high[:, i] * 10 + low[:, i]
    vals = vals * 10 + high[:, -1]
    sign = low[:, -1]
    vals[(sign == 0x0D) | (sign == 0x0B)] *= -1
    if scale:
        return vals.astype(np.float64) / (10 ** scale)
    return vals

def decode_chunk(arr: np.ndarray) -> pa.Table:
    cols = {}
    for name, (a, b, scale) in FIELDS.items():
        if scale == -1:
            cols[name] = EBCDIC_TO_CHAR[arr[:, a]]
        else:
            cols[name] = decode_packed_2d(arr[:, a:b], scale=scale)
    return pa.table(cols)

# ---------- MAIN ----------
def main():
    root = Path(parquet_root)
    # Remove any stale files from a previous run
    if root.exists():
        for f in root.glob("part-*.parquet"):
            f.unlink()
    root.mkdir(parents=True, exist_ok=True)

    # Pre-flight writability
    test = root / ".writetest"
    try:
        test.write_bytes(b"")
        test.unlink()
    except PermissionError:
        raise SystemExit(f"ERROR: Cannot write to {root}")

    print(f"Reading  : {flat_file}")
    print(f"Writing  : {parquet_root}/part-NNNNN.parquet")

    total = 0
    part  = 0
    leftover = b""

    with open(flat_file, "rb") as f:
        while True:
            buf = f.read(CHUNK_BYTES)
            if not buf:
                break
            buf = leftover + buf
            n_records = len(buf) // RECORD_LEN
            if n_records == 0:
                leftover = buf
                continue
            usable = n_records * RECORD_LEN
            leftover = buf[usable:]

            arr = np.frombuffer(buf[:usable], dtype=np.uint8).reshape(n_records, RECORD_LEN)
            table = decode_chunk(arr)

            part_path = root / f"part-{part:05d}.parquet"
            pq.write_table(
                table,
                part_path,
                compression="zstd",
                use_dictionary=True,
                row_group_size=1_000_000,
            )

            total += n_records
            part  += 1
            print(f"  part-{part - 1:05d}.parquet : {n_records:,} rows (total {total:,})",
                  flush=True)

    if leftover:
        print(f"WARNING: {len(leftover)} unaligned trailing bytes ignored")

    print(f"Done. {total:,} rows across {part} part files -> {parquet_root}")

if __name__ == "__main__":
    main()
