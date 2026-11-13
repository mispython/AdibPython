#!/usr/bin/env python3
"""
Deep diagnostic — verify the file, function, and behavior on real data.
Run as: python3 /tmp/deep_diag.py
"""
import sys
import os
import inspect
import traceback

EIBM_PATH_DIR = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS"
if EIBM_PATH_DIR not in sys.path:
    sys.path.insert(0, EIBM_PATH_DIR)

print("=" * 70)
print("1. WHICH FILE IS ACTUALLY BEING IMPORTED")
print("=" * 70)
for mod_name in list(sys.modules):
    if mod_name == 'EIBMHPFS' or mod_name.startswith('EIBMHPFS.'):
        del sys.modules[mod_name]

try:
    import EIBMHPFS
except Exception:
    traceback.print_exc()
    sys.exit(1)

print(f"  module file: {EIBMHPFS.__file__}")
print(f"  file mtime:  {__import__('time').ctime(os.path.getmtime(EIBMHPFS.__file__))}")
print(f"  file size:   {os.path.getsize(EIBMHPFS.__file__):,} bytes")

print()
print("=" * 70)
print("2. WHICH FUNCTION IS DEFINED IN THE IMPORTED MODULE")
print("=" * 70)
print(f"  has parse_sas_date:    {hasattr(EIBMHPFS, 'parse_sas_date')}")
print(f"  has parse_z11_to_date: {hasattr(EIBMHPFS, 'parse_z11_to_date')}")

if hasattr(EIBMHPFS, 'parse_sas_date'):
    print()
    print("  --- Source of parse_sas_date ---")
    try:
        src = inspect.getsource(EIBMHPFS.parse_sas_date)
        for line in src.splitlines()[:20]:
            print(f"  {line}")
    except Exception:
        traceback.print_exc()

print()
print("=" * 70)
print("3. DOES THE FUNCTION ACTUALLY WORK ON SAS DATES")
print("=" * 70)
if hasattr(EIBMHPFS, 'parse_sas_date'):
    for v in [14633, 23197, 14435.0, 0, None]:
        try:
            print(f"  parse_sas_date({v!r:12}) = {EIBMHPFS.parse_sas_date(v)}")
        except Exception as e:
            print(f"  parse_sas_date({v!r:12}) RAISED {type(e).__name__}: {e}")
    print()
    print("  Expected: 14633 -> 2000-01-20, 23197 -> 2023-07-05, 14435.0 -> 1999-07-11")
    print("            If any show 'None' for the numeric values, the fix is NOT loaded.")
elif hasattr(EIBMHPFS, 'parse_z11_to_date'):
    print("  >>> parse_z11_to_date is STILL in the module (old broken version)")
    for v in [14633, 23197]:
        try:
            print(f"  parse_z11_to_date({v!r}) = {EIBMHPFS.parse_z11_to_date(v)}")
        except Exception as e:
            print(f"  parse_z11_to_date({v!r}) RAISED {type(e).__name__}: {e}")

print()
print("=" * 70)
print("4. OTHER COPIES OF EIBMHPFS.py ON THE FILESYSTEM")
print("=" * 70)
import subprocess
try:
    result = subprocess.run(
        ["find", "/sas", "/opt", "/home", "/tmp", "-name", "EIBMHPFS*.py",
         "-type", "f", "2>/dev/null"],
        capture_output=True, text=True, timeout=30,
    )
    print(result.stdout)
except Exception as e:
    print(f"  find failed: {e}")

print()
print("=" * 70)
print("DONE")
print("=" * 70)
