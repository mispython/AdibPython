#!/usr/bin/env python3
"""
Diagnostic: confirm which EIBMHPFS.py is being imported and whether
parse_sas_date (the fixed function) is present.
"""
import inspect
import sys

print("=" * 70)
print("STEP 1 — which EIBMHPFS.py is Python importing?")
print("=" * 70)
try:
    import EIBMHPFS
    print(f"  module file: {EIBMHPFS.__file__}")
except Exception as e:
    print(f"  IMPORT FAILED: {e}")
    sys.exit(1)

print()
print("=" * 70)
print("STEP 2 — does that file contain the fixed function?")
print("=" * 70)
has_new = hasattr(EIBMHPFS, 'parse_sas_date')
has_old = hasattr(EIBMHPFS, 'parse_z11_to_date')
print(f"  has parse_sas_date:    {has_new}   <- should be True")
print(f"  has parse_z11_to_date: {has_old}   <- should be False")

if not has_new:
    print()
    print("  >>> BUG CONFIRMED: the fixed function is NOT loaded.")
    print("  >>> The file you edited is not the file Python is importing,")
    print("      or the file was not saved, or .pyc cache is stale.")
elif has_new and has_old:
    print()
    print("  >>> WARNING: both old and new functions exist.")
    print("  >>> The file may be a partial merge. Check the source.")
else:
    print()
    print("  >>> OK: only the fixed function is present.")

print()
print("=" * 70)
print("STEP 3 — what does the function source look like?")
print("=" * 70)
if has_new:
    print(inspect.getsource(EIBMHPFS.parse_sas_date))
elif has_old:
    print(inspect.getsource(EIBMHPFS.parse_z11_to_date))

print()
print("=" * 70)
print("STEP 4 — sanity-test the function on your actual data values")
print("=" * 70)
try:
    if has_new:
        for v in [14633, 23197, 14435.0, 0, None]:
            print(f"  parse_sas_date({v!r}) = {EIBMHPFS.parse_sas_date(v)}")
        print()
        print("  Expected:")
        print("    parse_sas_date(14633)   = 2000-01-20")
        print("    parse_sas_date(23197)   = 2023-07-05")
        print("    parse_sas_date(14435.0) = 1999-07-11")
        print("    parse_sas_date(0)       = 1960-01-01")
        print("    parse_sas_date(None)    = None")
    else:
        for v in [14633, 23197]:
            print(f"  parse_z11_to_date({v!r}) = "
                  f"{EIBMHPFS.parse_z11_to_date(v)}   (probably None = bug)")
except Exception as e:
    print(f"  Test failed: {e}")

print()
print("=" * 70)
print("DONE")
print("=" * 70)
