import pyreadstat
import os

paths = [
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/a_lnnote/enrh_ln_note_m10.sas7bdat",
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWHP01/loan104.sas7bdat",
]

for p in paths:
    print(f"\n=== {p} ===")
    if not os.path.exists(p):
        print("  FILE NOT FOUND")
        continue
    print(f"  size on disk: {os.path.getsize(p):,} bytes")

    # Metadata only
    _, meta = pyreadstat.read_sas7bdat(p, metadataonly=True)
    print(f"  columns: {meta.column_names[:10]}...")
    print(f"  rows reported by metadata: {meta.number_rows}")

    # Plain full read
    try:
        df, _ = pyreadstat.read_sas7bdat(p)
        print(f"  full read -> {len(df)} rows, columns: {list(df.columns)[:10]}")
    except Exception as e:
        print(f"  full read FAILED: {e}")

    # Chunked read (first chunk only)
    try:
        df, _ = pyreadstat.read_sas7bdat(p, row_limit=250_000, row_offset=0)
        print(f"  row_limit=250_000 read -> {len(df)} rows")
    except Exception as e:
        print(f"  chunk read FAILED: {e}")

    # Chunked read with usecols = lowercase (matching your current code)
    try:
        cols_lower = ['acctno','noteno','fisspurp','prodcd']
        df, _ = pyreadstat.read_sas7bdat(p, usecols=cols_lower,
                                          row_limit=250_000, row_offset=0)
        print(f"  lowercase usecols read -> {len(df)} rows")
    except Exception as e:
        print(f"  lowercase usecols FAILED: {e}")

    # Chunked read with usecols = actual stored names (from metadata)
    actual = meta.column_names
    wanted = [c for c in actual if c.upper() in
              ('ACCTNO','NOTENO','FISSPURP','PRODCD','ENTITY_CD')]
    try:
        df, _ = pyreadstat.read_sas7bdat(p, usecols=wanted,
                                          row_limit=250_000, row_offset=0)
        print(f"  actual-case usecols {wanted} -> {len(df)} rows")
    except Exception as e:
        print(f"  actual-case usecols FAILED: {e}")
