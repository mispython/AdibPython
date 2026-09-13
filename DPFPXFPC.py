# check_file.py
from pathlib import Path
import binascii

for p in [
    "/host_pq/dwh/input/LOAN/NFEEFILE_20260912",
    "/host_pq/dwh/input/LOAN/ACCTFILE_20260912",
    "/sasdata/rawdata/lookup/LKP_BRANCH",
]:
    path = Path(p)
    print("=" * 100)
    print(f"FILE: {path}")
    if not path.exists():
        print("  NOT FOUND")
        continue
    size = path.stat().st_size
    print(f"SIZE: {size:,} bytes ({size/1024/1024/1024:.2f} GB)")

    # Read first 4 KB and last 4 KB
    with open(path, "rb") as f:
        head = f.read(4096)
    with open(path, "rb") as f:
        f.seek(max(0, size - 4096))
        tail = f.read(4096)

    def show(label, b):
        print(f"--- {label} (first 256 bytes) ---")
        print("HEX  :", binascii.hexlify(b[:256]).decode())
        print("ASCII:", "".join(chr(c) if 32 <= c < 127 else "." for c in b[:256]))
        print("EBCDIC digit ratio (0xF0-0xF9 / 0x40):",
              sum(1 for c in b[:256] if 0xF0 <= c <= 0xF9 or c == 0x40), "/256")
        print("Non-printable bytes:", sum(1 for c in b[:256] if c < 32 or c > 126), "/256")
        print("Newline count (\\n):", b.count(b"\n"))
        print("CR count       (\\r):", b.count(b"\r"))

    show("HEAD", head)
    show("TAIL", tail)

    # File command equivalent
    try:
        import magic
        print("MAGIC:", magic.from_file(str(path)))
    except Exception:
        pass
