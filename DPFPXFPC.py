#!/usr/bin/env python3
"""
Diagnostic #2 — catch the actual import-time error.
"""
import sys
import traceback

print("=" * 70)
print("Importing EIBMHPFS with full traceback capture...")
print("=" * 70)

# Force Python to forget any cached EIBMHPFS
for mod_name in list(sys.modules):
    if mod_name == 'EIBMHPFS' or mod_name.startswith('EIBMHPFS.'):
        del sys.modules[mod_name]

try:
    import EIBMHPFS
    print("Import succeeded")
except Exception as exc:
    print(f"IMPORT FAILED: {type(exc).__name__}: {exc}")
    print()
    print("--- Full traceback ---")
    traceback.print_exc()
    sys.exit(1)

print()
print(f"module file: {EIBMHPFS.__file__}")

# What's actually defined?
names = sorted(n for n in dir(EIBMHPFS) if not n.startswith('__'))
print(f"\nDefined names ({len(names)} total):")
for n in names:
    obj = getattr(EIBMHPFS, n)
    kind = type(obj).__name__
    print(f"  {n:30s} ({kind})")

# Was there anything weird left over?
print()
print(f"Has 'parse_sas_date':    {hasattr(EIBMHPFS, 'parse_sas_date')}")
print(f"Has 'parse_z11_to_date': {hasattr(EIBMHPFS, 'parse_z11_to_date')}")
print(f"Has 'main':              {hasattr(EIBMHPFS, 'main')}")
print(f"Has 'get_report_vars':   {hasattr(EIBMHPFS, 'get_report_vars')}")
print(f"Has 'build_hpsettle':    {hasattr(EIBMHPFS, 'build_hpsettle')}")
print(f"Has 'LNNOTE':            {hasattr(EIBMHPFS, 'LNNOTE')}")
print(f"Has 'BNM_LOAN':          {hasattr(EIBMHPFS, 'BNM_LOAN')}")
