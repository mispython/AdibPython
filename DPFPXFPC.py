"""
inspect_formats.py — Detect the real format of DATEFILE, NFEEFILE, ACCTFILE, LKP_BRANCH
Run this BEFORE modifying EIBDLNS2.py.
"""

from pathlib import Path
import binascii
import sys

# --- Configure the paths (same macros as in EIBDLNS2) ---
DATEFILE  = Path("/host_pq/dwh/input/LOAN/DATEFILE")
# Adjust to today's actual file names if different:
FEEFILE   = Path("/host_pq/dwh/input/LOAN/NFEEFILE_20250912")
ACCTFILE  = Path("/host_pq/dwh/input/LOAN/ACCTFILE_20250912")
BRANCHF   = Path("/sasdata/rawdata/lookup/LKP_BRANCH")

def peek(path: Path, nbytes: int = 256):
    print("=" * 100)
    print(f"FILE : {path}")
    if not path.exists():
        print("  !! NOT FOUND")
        return
    size = path.stat().st_size
    print(f"SIZE : {size:,} bytes ({size/1024/1024/1024:.2f} GB)")

    with open(path, "rb") as f:
        head = f.read(nbytes)

    # 1. Magic bytes for common binary formats
    magic_map = {
        b"PAR1": "Parquet",
        b"PK\x03\x04": "ZIP / XLSX / ODS",
        b"\x1f\x8b": "GZIP",
        b"BZh": "BZIP2",
        b"\xfd7zXZ\x00": "XZ",
        b"SAS": "SAS7BDAT (possible)",
        b"\x00\x00\x00\x00": "SAS7BDAT (typical zero header)",
        b"ARROW1": "Arrow IPC",
        b"\x89PNG": "PNG",
    }
    detected = None
    for magic, name in magic_map.items():
        if head.startswith(magic):
            detected = name
            break
    print(f"MAGIC: {detected or 'none of the known magic bytes'}")

    # 2. Hex dump of first 64 bytes
    print("HEX  :", binascii.hexlify(head[:64]).decode())

    # 3. Printable ASCII rendering
    ascii_repr = "".join(chr(b) if 32 <= b < 127 else "." for b in head[:64])
    print("ASCII:", ascii_repr)

    # 4. EBCDIC heuristic:
    #    Common EBCDIC bytes for digits 0-9: F0-F9
    #    EBCDIC space: 0x40
    #    If > 50% of first 64 bytes are in the F0-F9 / 0x40-0x4F range -> EBCDIC
    ebcdic_hits = sum(1 for b in head[:64] if 0xF0 <= b <= 0xF9 or b == 0x40)
    print(f"EBCDIC digit/space ratio in first 64 bytes: {ebcdic_hits}/64")

    # 5. Packed-decimal (COMP-3) heuristic:
    #    Packed decimal digits are nibbles 0x0-0x9; last nibble is sign (C/D/F).
    #    If the first 64 bytes are mostly in 0x00-0x99 range and NOT printable ASCII -> likely COMP-3.
    non_print = sum(1 for b in head[:64] if b < 32 or b > 126)
    print(f"Non-printable bytes in first 64: {non_print}/64")

    # 6. Show first line as text (if it parses as UTF-8/ASCII)
    try:
        text = head.decode("ascii", errors="replace")
        first_line = text.splitlines()[0] if text else ""
        print(f"TEXT : {first_line[:200]!r}")
    except Exception as e:
        print(f"TEXT : <decode failed: {e}>")

    print()

for p in (DATEFILE, FEEFILE, ACCTFILE, BRANCHF):
    peek(p)
