# -*- coding: utf-8 -*-
"""
Convert the 70 GB fixed-width, packed-decimal, EBCDIC deposit flat file
to a columnar Parquet file.

- Input : {input_dir}/DPDARPGS_FB_{YYYY}{MM}{DD}
- Output: {parquet_out_dir}/DPDARPGS_FB_{YYYY}{MM}{DD}.parquet

Streaming, bounded memory (~1 GB peak).
"""
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from pathlib import Path

# ---------- CONFIG ----------
input_dir       = "/host_pq/dwh/input/DEPOSIT"
parquet_out_dir = "/stgsrcsys/host/uat/maa/python/input"

run_date = datetime.now() - timedelta(days=1)
reptyear = run_date.strftime("%Y")
reptmon  = run_date.strftime("%m")
reptday  = run_date.strftime("%d")

flat_file   = f"{input_dir}/DPDARPGS_FB_{reptyear}{reptmon}{reptday}"
parquet_out = f"{parquet_out_dir}/DPDARPGS_FB_{reptyear}{reptmon}{reptday}.parquet"

RECORD_LEN  = 1693
CHUNK_BYTES = 500 * 1024 * 1024   # 500 MB per read

# Byte offsets (0-based) for the fields we keep
FIELDS = {
    "BANKNO":   (2,   4,   0),
    "REPTNO":   (23,  26,  0),
    "FMTCODE":  (26,  28,  0),
    "BRANCH":   (105, 109, 0),
    "ACCTNO":   (109, 115, 0),
    "OPENIND":  (154, 155, -1),   # -1 = single byte EBCDIC char
    "CURBAL":   (155, 161, 2),
    "INTPLAN":  (207, 209, 0),
    "LMATDATE": (391, 397, 0),
}

# ---------- EBCDIC LOOKUP TABLE ----------
EBCDIC_TO_CHAR = np.array([bytes([i]).decode("cp037") for i in range(256)])

# ---------- VECTORIZED COMP-3 DECODE ----------
def decode_packed_2d(raw: np.ndarray, scale: int = 0) -> np.ndarray:
    """raw: uint8 array (n_rows, n_bytes) -> int64 (or float64 if scale>0)."""
    high = (raw >> 4) & 0x0F
    low  = raw & 0x0F
    n_rows, n_bytes = raw.shape

    vals = np.zeros(n_rows, dtype=np.int64)
    for i in range(n_bytes - 1):
        vals = vals * 100 + high[:, i] * 10 + low[:, i]
    vals = vals * 10 + high[:, -1]

    sign = low[:, -1]
    neg = (sign == 0x0D) | (sign == 0x0B)
    vals[neg] = -vals[neg]

    if scale:
        return vals.astype(np.float64) / (10 ** scale)
    return vals

def decode_chunk(arr: np.ndarray) -> pa.Table:
    """arr: uint8 (n_rows, RECORD_LEN) -> pyarrow Table."""
    cols = {}
    for name, (a, b, scale) in FIELDS.items():
        if scale == -1:
            cols[name] = EBCDIC_TO_CHAR[arr[:, a]]
        else:
            cols[name] = decode_packed_2d(arr[:, a:b], scale=scale)
    return pa.table(cols)

# ---------- MAIN ----------
def main():
    Path(parquet_out_dir).mkdir(parents=True, exist_ok=True)

    print(f"Reading  : {flat_file}")
    print(f"Writing  : {parquet_out}")

    writer = None
    total = 0
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

            if writer is None:
                writer = pq.ParquetWriter(
                    parquet_out, table.schema,
                    compression="zstd",
                    use_dictionary=True,
                    row_group_size=1_000_000,
                )
            writer.write_table(table)

            total += n_records
            print(f"  {total:,} rows written", flush=True)

    if writer is not None:
        writer.close()
    print(f"Done. {total:,} rows -> {parquet_out}")

if __name__ == "__main__":
    main()
