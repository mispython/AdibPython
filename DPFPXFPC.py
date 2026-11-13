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
    print(f"  column_names[:10]: {meta.column_names[:10]}")
    print(f"  number_rows: {meta.number_rows}")
    print(f"  number_columns: {meta.number_columns}")

    # Chunked read — no usecols
    try:
        df, _ = pyreadstat.read_sas7bdat(p, row_limit=250_000, row_offset=0)
        print(f"  row_limit=250k (no usecols) -> {len(df)} rows, "
              f"cols: {list(df.columns)[:8]}")
    except Exception as e:
        print(f"  row_limit=250k (no usecols) FAILED: {e}")

    # Chunked read — lowercase usecols (what the current EIBMHPFS code passes)
    try:
        cols_lower = ['acctno','noteno','fisspurp','prodcd']
        df, _ = pyreadstat.read_sas7bdat(p, usecols=cols_lower,
                                          row_limit=250_000, row_offset=0)
        print(f"  lowercase usecols -> {len(df)} rows")
    except Exception as e:
        print(f"  lowercase usecols FAILED: {e}")

    # Chunked read — actual stored names from metadata (case-exact)
    actual = meta.column_names
    wanted = [c for c in actual if c.upper() in
              ('ACCTNO','NOTENO','FISSPURP','PRODCD','ENTITY_CD')]
    print(f"  actual-case wanted list: {wanted}")
    try:
        df, _ = pyreadstat.read_sas7bdat(p, usecols=wanted,
                                          row_limit=250_000, row_offset=0)
        print(f"  actual-case usecols -> {len(df)} rows, "
              f"cols: {list(df.columns)}")
    except Exception as e:
        print(f"  actual-case usecols FAILED: {e}")
