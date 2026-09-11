#!/usr/bin/env python3
"""
EIBTNPGS Diagnostic - Run this FIRST to determine file layouts
"""
from pathlib import Path

# ============================================================================
# CRFTABL.TXT - inspect fixed-width layout
# ============================================================================
print("=" * 80)
print("CRFTABL.TXT INSPECTION")
print("=" * 80)

crftabl = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/crftabl.txt")
with open(crftabl, 'r') as f:
    lines = f.readlines()

print(f"Total lines: {len(lines)}")
print(f"\nLine 0 (header?): {repr(lines[0])}")
print(f"\nLine 1 (data):    {repr(lines[1])}")
print(f"Line 1 length: {len(lines[1].rstrip())}")

# Show position ruler
line1 = lines[1].rstrip('\n')
print("\nPosition ruler:")
for start in range(0, len(line1), 10):
    print(f"  {start:4d}: {line1[start:start+10]!r}")

# ============================================================================
# LCCRISEX - inspect EBCDIC layout
# ============================================================================
print("\n" + "=" * 80)
print("LCCRISEX INSPECTION")
print("=" * 80)

import glob
ebcdic_files = sorted(glob.glob(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_*"
))
print("Found EBCDIC files:")
for f in ebcdic_files:
    print(f"  {f}  size={Path(f).stat().st_size}")

# Pick first non-DESC file
coll_file = [f for f in ebcdic_files if 'DESC' not in f]
if coll_file:
    with open(coll_file[0], 'rb') as f:
        raw = f.read(2000)
    print(f"\nFile: {coll_file[0]}")
    print(f"Raw bytes (first 200): {raw[:200]!r}")
    decoded = raw.decode('cp500', errors='replace')
    print(f"\nDecoded (cp500) first 500 chars:")
    print(repr(decoded[:500]))

    # Determine record length by looking for repeating pattern
    print(f"\nTotal file size: {Path(coll_file[0]).stat().st_size}")
    # Try common record lengths
    size = Path(coll_file[0]).stat().st_size
    for rl in [80, 100, 128, 150, 200, 250, 300, 350, 400, 500]:
        if size % rl == 0:
            print(f"  Possible record length: {rl} ({size // rl} records)")

# ============================================================================
# BOPESS.TXT - inspect
# ============================================================================
print("\n" + "=" * 80)
print("BOPESS.TXT INSPECTION")
print("=" * 80)

bopess = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/BOPESS.txt")
if bopess.exists():
    with open(bopess, 'r') as f:
        lines = f.readlines()
    print(f"Total lines: {len(lines)}")
    for i in range(min(3, len(lines))):
        print(f"  Line {i}: {repr(lines[i])}")
        print(f"          length={len(lines[i].rstrip())}")
else:
    print(f"NOT FOUND: {bopess}")

# ============================================================================
# SAS7BDAT COLUMNS - inspect
# ============================================================================
print("\n" + "=" * 80)
print("SAS7BDAT FILE COLUMNS")
print("=" * 80)

import pyreadstat

sas_files = {
    'MAST': "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/mast0809.sas7bdat",
    'CRED': "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/cred0809.sas7bdat",
    'PROV': "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/prov0809.sas7bdat",
    'SUBA': "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/suba0809.sas7bdat",
    'NPLA': "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBTNPGS/npla.sas7bdat",
}

for name, path in sas_files.items():
    p = Path(path)
    if p.exists():
        df, meta = pyreadstat.read_sas7bdat(p, metadataonly=True)
        print(f"\n{name}: {p.name}")
        print(f"  Columns: {meta.column_names}")
    else:
        print(f"\n{name}: NOT FOUND at {p}")
