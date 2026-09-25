# -*- coding: utf-8 -*-
"""
Combined diagnostic for the deposit flat file and Parquet output.
Prints a full report so we can pinpoint the bug.
"""
import os
import numpy as np
import pyarrow.parquet as pq

# ---------- CONFIG ----------
FLAT_FILE   = "/host_pq/dwh/input/DEPOSIT/DPDARPGS_FB_20260924"
PARQUET     = "/stgsrcsys/host/holding/DPDARPGS_FB_20260924.parquet"
RECORD_LEN  = 1693
SAMPLE_MB   = 200                # how much of the flat file to sample

# Byte offsets (0-based) matching the converter
FIELDS = {
    "BANKNO":   (2,   4),
    "REPTNO":   (23,  26),
    "FMTCODE":  (26,  28),
    "BRANCH":   (105, 109),
    "ACCTNO":   (109, 115),
    "OPENIND":  (154, 155),
    "CURBAL":   (155, 161),
    "INTPLAN":  (207, 209),
    "LMATDATE": (391, 397),
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

# ---------- SECTION 1: file basics ----------
def section_file_basics():
    print("=" * 78)
    print("SECTION 1: FILE BASICS")
    print("=" * 78)

    size = os.path.getsize(FLAT_FILE)
    print(f"Flat file size         : {size:,} bytes ({size/1e9:.2f} GB)")
    print(f"Expected records @1693 : {size // RECORD_LEN:,}")

    # Line count in the first SAMPLE_MB bytes (extrapolate)
    with open(FLAT_FILE, "rb") as f:
        sample = f.read(SAMPLE_MB * 1024 * 1024)
    newlines_sample = sample.count(b"\n")
    ratio = size / len(sample)
    print(f"Newlines in first {SAMPLE_MB} MB : {newlines_sample:,}")
    print(f"  extrapolated total lines    : {int(newlines_sample * ratio):,}")
    print(f"  bytes per newline (avg)     : {len(sample) / max(newlines_sample,1):,.0f}")

    # Parquet
    if os.path.exists(PARQUET):
        pf = pq.ParquetFile(PARQUET)
        print(f"\nParquet size           : {os.path.getsize(PARQUET):,} bytes")
        print(f"Parquet rows           : {pf.metadata.num_rows:,}")
        print(f"Parquet row groups     : {pf.num_row_groups}")
        print(f"Parquet schema         :")
        for f_ in pf.schema_arrow:
            print(f"    {f_.name:12s} {f_.type}")
    else:
        print(f"\nParquet not found: {PARQUET}")

    print()

# ---------- SECTION 2: first-row hex dumps ----------
def section_hex_dump():
    print("=" * 78)
    print("SECTION 2: HEX DUMPS (first 3 records)")
    print("=" * 78)

    with open(FLAT_FILE, "rb") as f:
        data = f.read(RECORD_LEN * 3)

    for i in range(3):
        rec = data[i*RECORD_LEN : (i+1)*RECORD_LEN]
        if len(rec) < RECORD_LEN:
            break
        print(f"\nRecord {i} (bytes {i*RECORD_LEN}..{(i+1)*RECORD_LEN}):")
        print(f"  bytes   0- 10 : {rec[  0: 10].hex()}")
        print(f"  bytes  23- 28 : {rec[ 23: 28].hex()}")
        print(f"  bytes 105-115 : {rec[105:115].hex()}")
        print(f"  bytes 154-161 : {rec[154:161].hex()}")
        print(f"  bytes 207-209 : {rec[207:209].hex()}")
        print(f"  bytes 391-397 : {rec[391:397].hex()}")
        print(f"  tail (1685-1693): {rec[-8:].hex()}  ({rec[-8:]!r})")
    print()

# ---------- SECTION 3: sample decode & filter funnel ----------
def section_decode_and_filter():
    print("=" * 78)
    print(f"SECTION 3: SAMPLE DECODE & FILTER FUNNEL (first {SAMPLE_MB} MB)")
    print("=" * 78)

    with open(FLAT_FILE, "rb") as f:
        buf = f.read(SAMPLE_MB * 1024 * 1024)
    n = len(buf) // RECORD_LEN
    arr = np.frombuffer(buf[:n*RECORD_LEN], dtype=np.uint8).reshape(n, RECORD_LEN)

    print(f"Sample records         : {n:,}")

    # Decode each field
    bankno   = decode_packed_2d(arr[:, 2:4])
    reptno   = decode_packed_2d(arr[:, 23:26])
    fmtcode  = decode_packed_2d(arr[:, 26:28])
    branch   = decode_packed_2d(arr[:, 105:109])
    acctno   = decode_packed_2d(arr[:, 109:115])
    openind  = EBCDIC_TO_CHAR[arr[:, 154]]
    curbal   = decode_packed_2d(arr[:, 155:161], scale=2)
    intplan  = decode_packed_2d(arr[:, 207:209])
    lmat     = decode_packed_2d(arr[:, 391:397])

    def stats(name, vals, is_char=False):
        print(f"\n{name}")
        if is_char:
            u, c = np.unique(vals, return_counts=True)
            pairs = sorted(zip(u.tolist(), c.tolist()), key=lambda x: -x[1])[:10]
            print("  top 10 values (count):")
            for ch, cnt in pairs:
                print(f"      {ch!r} : {cnt:,}")
        else:
            u = np.unique(vals)
            print(f"  unique count   : {len(u)}")
            print(f"  min / max      : {vals.min():,} / {vals.max():,}")
            print(f"  first 10 uniq  : {u[:10].tolist()}")
            print(f"  first 10 vals  : {vals[:10].tolist()}")

    stats("BANKNO", bankno)
    stats("REPTNO", reptno)
    stats("FMTCODE", fmtcode)
    stats("BRANCH", branch)
    stats("ACCTNO", acctno)
    stats("OPENIND", openind, is_char=True)
    stats("CURBAL", curbal)
    stats("INTPLAN", intplan)
    stats("LMATDATE", lmat)

    # Filter funnel
    print("\n--- FILTER FUNNEL ---")
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

    print(f"  total records              : {n:,}")
    print(f"  BANKNO == 33               : {m_bankno.sum():,}  ({m_bankno.sum()/n:.2%})")
    print(f"  REPTNO == 4001             : {m_reptno.sum():,}  ({m_reptno.sum()/n:.2%})")
    print(f"  FMTCODE in (1,2)           : {m_fmtcode.sum():,}  ({m_fmtcode.sum()/n:.2%})")
    print(f"  OPENIND in (D,O)           : {m_openind.sum():,}  ({m_openind.sum()/n:.2%})")
    print(f"  INTPLAN in ranges          : {m_intplan.sum():,}  ({m_intplan.sum()/n:.2%})")

    m_step1 = m_bankno
    m_step2 = m_step1 & m_reptno
    m_step3 = m_step2 & m_fmtcode
    m_step4 = m_step3 & m_openind
    m_step5 = m_step4 & m_intplan
    print(f"\n  cumulative funnel:")
    print(f"    after BANKNO             : {m_step1.sum():,}")
    print(f"    after +REPTNO            : {m_step2.sum():,}")
    print(f"    after +FMTCODE           : {m_step3.sum():,}")
    print(f"    after +OPENIND           : {m_step4.sum():,}")
    print(f"    after +INTPLAN           : {m_step5.sum():,}")

    # Extrapolate to full file
    ratio = os.path.getsize(FLAT_FILE) / len(buf)
    print(f"\n  extrapolated to full file:")
    print(f"    expected total records   : {int(n * ratio):,}")
    print(f"    expected passing records : {int(m_step5.sum() * ratio):,}")
    print()

# ---------- SECTION 4: peek at the actual Parquet ----------
def section_parquet_peek():
    print("=" * 78)
    print("SECTION 4: PARQUET CONTENT")
    print("=" * 78)

    if not os.path.exists(PARQUET):
        print(f"Parquet not found: {PARQUET}")
        return

    try:
        import polars as pl
    except ImportError:
        print("polars not installed — skipping content peek")
        return

    df = pl.read_parquet(PARQUET)
    print(f"Rows         : {len(df):,}")
    print(f"Columns      : {df.columns}")
    print(f"Head:")
    print(df.head(5))

    print(f"\nNull counts:")
    print(df.null_count())

    print(f"\nDescribe:")
    print(df.describe())
    print()

# ---------- MAIN ----------
def main():
    section_file_basics()
    section_hex_dump()
    section_decode_and_filter()
    section_parquet_peek()

    print("=" * 78)
    print("DONE — paste this entire output back for diagnosis.")
    print("=" * 78)


if __name__ == "__main__":
    main()
