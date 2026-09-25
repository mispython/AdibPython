# -*- coding: utf-8 -*-
"""
Convert the deposit flat file to Parquet.
Includes first-chunk diagnostics to verify decoding is correct.

- Input : {input_dir}/DPDARPGS_FB_{YYYY}{MM}{DD}
- Output: {parquet_out_dir}/DPDARPGS_FB_{YYYY}{MM}{DD}.parquet

Streaming, bounded memory.
"""
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

flat_file   = f"{input_dir}/DPDARPGS_FB_{reptyear}{reptmon}{reptday}"
parquet_out = f"{parquet_out_dir}/DPDARPGS_FB_{reptyear}{reptmon}{reptday}.parquet"

RECORD_LEN  = 1693
CHUNK_BYTES = 500 * 1024 * 1024   # 500 MB per read

# Byte offsets (0-based)
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

# EBCDIC lookup
EBCDIC_TO_CHAR = np.array([bytes([i]).decode("cp037") for i in range(256)])

# ---------- DECODERS ----------
def decode_packed_2d(raw: np.ndarray, scale: int = 0) -> np.ndarray:
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

# ---------- DIAGNOSTIC (first chunk only) ----------
_diag_done = False

def run_diagnostics(arr: np.ndarray):
    global _diag_done
    if _diag_done:
        return
    _diag_done = True

    n = arr.shape[0]
    print("=" * 70)
    print("FIRST CHUNK DIAGNOSTIC")
    print("=" * 70)
    print(f"Records in first chunk : {n:,}")
    print(f"Bytes per record       : {arr.shape[1]}")

    print("\n--- First row, raw bytes 0..100 ---")
    print(bytes(arr[0, :100]).hex())

    print("\n--- First row, raw bytes 100..250 ---")
    print(bytes(arr[0, 100:250]).hex())

    print("\n--- First row, raw bytes 380..400 ---")
    print(bytes(arr[0, 380:400]).hex())

    # BANKNO
    bankno = decode_packed_2d(arr[:, 2:4])
    print("\nBANKNO")
    print(f"  unique[:20] : {np.unique(bankno)[:20].tolist()}")
    print(f"  ==33 count  : {(bankno == 33).sum():,} / {n:,}")

    # REPTNO
    reptno = decode_packed_2d(arr[:, 23:26])
    print("\nREPTNO")
    print(f"  unique[:20] : {np.unique(reptno)[:20].tolist()}")
    print(f"  ==4001 count: {(reptno == 4001).sum():,} / {n:,}")

    # FMTCODE
    fmtcode = decode_packed_2d(arr[:, 26:28])
    print("\nFMTCODE")
    print(f"  unique      : {np.unique(fmtcode).tolist()}")
    print(f"  in (1,2)    : {np.isin(fmtcode, [1, 2]).sum():,} / {n:,}")

    # OPENIND
    openind = EBCDIC_TO_CHAR[arr[:, 154]]
    u, c = np.unique(openind, return_counts=True)
    pairs = sorted(zip(u.tolist(), c.tolist()), key=lambda x: -x[1])[:10]
    print("\nOPENIND (top 10 by freq)")
    for ch, cnt in pairs:
        print(f"  {ch!r} : {cnt:,}")
    in_do = np.isin(openind, ["D", "O"]).sum()
    print(f"  in ('D','O') : {in_do:,} / {n:,}")

    # INTPLAN
    intplan = decode_packed_2d(arr[:, 207:209])
    print("\nINTPLAN")
    print(f"  min / max   : {intplan.min()} / {intplan.max()}")
    print(f"  sample[:20] : {intplan[:20].tolist()}")

    # LMATDATE
    lmat = decode_packed_2d(arr[:, 391:397])
    print("\nLMATDATE")
    print(f"  min / max   : {lmat.min()} / {lmat.max()}")
    print(f"  sample[:20] : {lmat[:20].tolist()}")
    print(f"  > 0 count   : {(lmat > 0).sum():,} / {n:,}")

    # Combined filter results
    print("\nFILTER FUNNEL")
    m_bankno = (bankno == 33)
    m_reptno = (reptno == 4001)
    m_fmtcode = np.isin(fmtcode, [1, 2])
    m_openind = np.isin(openind, ["D", "O"])
    m_intplan = (
        ((intplan >= 340) & (intplan <= 359)) |
        ((intplan >= 448) & (intplan <= 459)) |
        ((intplan >= 461) & (intplan <= 469)) |
        ((intplan >= 580) & (intplan <= 599)) |
        ((intplan >= 660) & (intplan <= 740))
    )
    m_all = m_bankno & m_reptno & m_fmtcode & m_openind & m_intplan

    print(f"  BANKNO==33                 : {m_bankno.sum():,} / {n:,}")
    print(f"  REPTNO==4001               : {m_reptno.sum():,} / {n:,}")
    print(f"  FMTCODE in (1,2)           : {m_fmtcode.sum():,} / {n:,}")
    print(f"  OPENIND in (D,O)           : {m_openind.sum():,} / {n:,}")
    print(f"  INTPLAN in ranges          : {m_intplan.sum():,} / {n:,}")
    print(f"  ALL filters combined       : {m_all.sum():,} / {n:,}")

    print("\nIf ALL filters show very small counts, the byte offsets or")
    print("EBCDIC code page may be wrong. Compare the raw hex dumps above")
    print("against a known-good record to locate the correct offsets.")
    print("=" * 70)


# ---------- CHUNK DECODER ----------
def decode_chunk(arr: np.ndarray) -> pa.Table:
    run_diagnostics(arr)

    cols = {}
    for name, (a, b, scale) in FIELDS.items():
        if scale == -1:
            cols[name] = EBCDIC_TO_CHAR[arr[:, a]]
        else:
            cols[name] = decode_packed_2d(arr[:, a:b], scale=scale)
    return pa.table(cols)


# ---------- MAIN ----------
def main():
    out_dir = Path(parquet_out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Pre-flight writability
    test = out_dir / ".writetest"
    try:
        test.write_bytes(b"")
        test.unlink()
    except PermissionError:
        raise SystemExit(f"ERROR: Cannot write to {out_dir}.")

    print(f"Reading  : {flat_file}")
    print(f"Writing  : {parquet_out}")

    writer = None
    total = 0
    leftover = b""
    chunk_idx = 0

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
                )
            writer.write_table(table, row_group_size=1_000_000)

            total += n_records
            chunk_idx += 1
            print(f"  chunk {chunk_idx}: {total:,} rows written so far", flush=True)

    if writer is not None:
        writer.close()
    print(f"Done. {total:,} rows -> {parquet_out}")

if __name__ == "__main__":
    main()
