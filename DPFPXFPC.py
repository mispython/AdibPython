# diagnose_repo1.py
"""
Standalone diagnostic for the REPO1 (empty/1-obs) issue.

Run:
    python diagnose_repo1.py

Prints exactly how many rows survive at each stage, so you can tell whether:
  (A) The data genuinely only has 1 matching row  -> code is fine
  (B) LOANTYPE isn't matching '983'/'993'         -> normalize_code bug
  (C) LNNOTE has almost no 983/993 rows           -> data-feed issue
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import pyreadstat

import PBBLNFMT
import PBBELF


# ----------------------------------------------------------------------
# Same date logic as the main pipeline
# ----------------------------------------------------------------------
def compute_dates():
    reptdate = datetime.now() - timedelta(days=1)
    day, month, year = reptdate.day, reptdate.month, reptdate.year
    reptmon = f"{month:02d}"
    return reptdate, reptmon


# ----------------------------------------------------------------------
# Robust normalizer for LOANTYPE — handles int, float, str, bytes
# ----------------------------------------------------------------------
def normalize_code_scalar(x):
    if pd.isna(x):
        return None
    if isinstance(x, bytes):
        x = x.decode('utf-8', errors='ignore')
    try:
        return str(int(float(x)))
    except (ValueError, TypeError):
        return str(x).strip()


def normalize_code(series):
    return series.apply(normalize_code_scalar)


# ----------------------------------------------------------------------
# Main diagnostic
# ----------------------------------------------------------------------
def main():
    reptdate, reptmon = compute_dates()
    print(f"=== Diagnostic run for reptmon={reptmon} "
          f"(reptdate={reptdate.strftime('%Y-%m-%d')}) ===\n")

    # ---- Paths (same as main script) ----
    lnnote_path   = Path(f"/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m{reptmon}.sas7bdat")
    loantemp_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMREPOI/loantemp_iccd2.sas7bdat")

    if not lnnote_path.exists():
        print(f"!! LNNOTE file not found: {lnnote_path}")
        sys.exit(1)
    if not loantemp_path.exists():
        print(f"!! LOANTEMP file not found: {loantemp_path}")
        sys.exit(1)

    hp_values_int = PBBLNFMT.MACROS['HP']
    hp_values_str = [str(x) for x in hp_values_int]
    print(f"HP codes from PBBLNFMT.MACROS['HP'] ({len(hp_values_int)}): "
          f"{hp_values_int}\n")

    # ==================================================================
    # A. LNNOTE — how many rows survive each filter?
    # ==================================================================
    print("=" * 70)
    print("A. LNNOTE filtering stages")
    print("=" * 70)

    print(f"Reading {lnnote_path.name} ...")
    lnnote_df, _ = pyreadstat.read_sas7bdat(
        str(lnnote_path),
        usecols=['ACCTNO', 'LOANTYPE', 'NTBRCH', 'COLLDESC', 'COLLYEAR',
                 'BALANCE', 'BORSTAT'],
    )
    lnnote_df.columns = lnnote_df.columns.str.lower()
    total = len(lnnote_df)
    print(f"  Total rows                        : {total:,}")

    # Inspect raw LOANTYPE dtype and sample values
    print(f"\n  LOANTYPE dtype: {lnnote_df['loantype'].dtype}")
    print(f"  LOANTYPE first 10 raw values:")
    for v in lnnote_df['loantype'].head(10):
        print(f"     {v!r}  (type={type(v).__name__})")

    # Normalize LOANTYPE
    lt_norm = normalize_code(lnnote_df['loantype'])
    print(f"\n  LOANTYPE first 10 normalized:")
    print(f"     {lt_norm.head(10).tolist()}")

    # Stage 1: LOANTYPE in HP
    mask_lt = lt_norm.isin(hp_values_str)
    print(f"\n  Stage 1: LOANTYPE IN &HP          : {mask_lt.sum():,}")

    # Stage 2: BALANCE > 0
    mask_bal = lnnote_df['balance'] > 0
    print(f"  Stage 2: BALANCE > 0              : {mask_bal.sum():,}")

    # Stage 3: BORSTAT NOT IN (F,I,R)
    mask_bor = ~lnnote_df['borstat'].isin(['F', 'I', 'R'])
    print(f"  Stage 3: BORSTAT NOT IN (F,I,R)   : {mask_bor.sum():,}")

    # Combined
    mask_all = mask_lt & mask_bal & mask_bor
    print(f"  ALL THREE (the real filter)       : {mask_all.sum():,}")

    # How many of THOSE are 983/993?
    filtered = lnnote_df[mask_all]
    filtered_lt = normalize_code(filtered['loantype'])
    n_983_993 = filtered_lt.isin(['983', '993']).sum()
    print(f"  Of the filtered rows, 983/993      : {n_983_993:,}")

    # What loan types DID survive?
    print(f"\n  Loan type distribution after all filters:")
    dist = filtered_lt.value_counts().sort_index()
    for lt, cnt in dist.items():
        marker = '  <-- 983/993' if lt in ('983', '993') else ''
        print(f"     {lt:>6}: {cnt:>8,}{marker}")

    # Show whether 983 or 993 even exist in the RAW file at all
    print(f"\n  Raw LOANTYPE value counts (top 20) — before any filtering:")
    raw_dist = lt_norm.value_counts().head(20)
    for lt, cnt in raw_dist.items():
        marker = '  <-- 983/993' if lt in ('983', '993') else ''
        print(f"     {lt!r:>10}: {cnt:>10,}{marker}")

    # ==================================================================
    # B. LOANTEMP — how many rows have ARREAR >= 10?
    # ==================================================================
    print("\n" + "=" * 70)
    print("B. LOANTEMP filtering stages")
    print("=" * 70)

    print(f"Reading {loantemp_path.name} ...")
    arrear_df, _ = pyreadstat.read_sas7bdat(
        str(loantemp_path),
        usecols=['ACCTNO', 'ARREAR'],
    )
    arrear_df.columns = arrear_df.columns.str.lower()
    print(f"  Total rows                        : {len(arrear_df):,}")
    print(f"  ARREAR dtype                      : {arrear_df['arrear'].dtype}")
    print(f"  ARREAR >= 10                      : {(arrear_df['arrear'] >= 10).sum():,}")
    print(f"  ARREAR null                       : {arrear_df['arrear'].isna().sum():,}")
    print(f"  ACCTNO dtype                      : {arrear_df['acctno'].dtype}")
    print(f"  ACCTNO first 5 raw                : {arrear_df['acctno'].head(5).tolist()}")

    # ==================================================================
    # C. Intersection of the two — that's the MAX possible REPO1 size
    # ==================================================================
    print("\n" + "=" * 70)
    print("C. Intersection (the ceiling for REPO1)")
    print("=" * 70)

    def clean_acctno(v):
        if pd.isna(v):
            return None
        try:
            return str(int(float(v)))
        except (ValueError, TypeError):
            return str(v).strip()

    lnnote_acctno  = set(filtered['acctno'].apply(clean_acctno).dropna())
    arrear_acctno  = set(
        arrear_df.loc[arrear_df['arrear'] >= 10, 'acctno']
                 .apply(clean_acctno).dropna()
    )

    # Only 983/993 from LNNOTE
    lnnote_983_993 = filtered[filtered_lt.isin(['983', '993'])]
    lnnote_983_acctno = set(lnnote_983_993['acctno'].apply(clean_acctno).dropna())

    print(f"  LNNOTE filtered acctnos           : {len(lnnote_acctno):,}")
    print(f"  LOANTEMP acctnos w/ ARREAR >= 10  : {len(arrear_acctno):,}")
    print(f"  LNNOTE 983/993 acctnos            : {len(lnnote_983_acctno):,}")

    both_acctno = lnnote_acctno & arrear_acctno
    both_983 = lnnote_983_acctno & arrear_acctno

    print(f"\n  INTERSECTION (ARREAR>=10 AND in LNNOTE filtered): "
          f"{len(both_acctno):,}   <- max REPO size")
    print(f"  INTERSECTION (ARREAR>=10 AND 983/993):            "
          f"{len(both_983):,}   <- max REPO1 size")

    # ==================================================================
    # Verdict
    # ==================================================================
    print("\n" + "=" * 70)
    print("VERDICT")
    print("=" * 70)

    n_983_raw   = (lt_norm.isin(['983', '993'])).sum()
    n_983_post  = n_983_993
    n_repo_max  = len(both_acctno)
    n_repo1_max = len(both_983)

    if n_repo1_max <= 2:
        print(f"  => DATA LIMITATION: only {n_repo1_max} account(s) in the input")
        print(f"     satisfy (ARREAR>=10 AND LOANTYPE IN 983/993).")
        print(f"     Your code is CORRECT. REPO1 is intentionally a tiny subset.")
    elif n_983_raw == 0:
        print(f"  => LOANTYPE MATCH FAILURE: no rows in LNNOTE have LOANTYPE")
        print(f"     983 or 993 at all. Check the raw LOANTYPE values above.")
    elif n_983_post == 0:
        print(f"  => FILTER TOO STRICT: {n_983_raw} raw 983/993 rows exist,")
        print(f"     but ALL are being dropped by BALANCE>0 / BORSTAT filters.")
    elif n_repo1_max > 2:
        print(f"  => CODE BUG: {n_repo1_max} accounts should appear in REPO1,")
        print(f"     but your pipeline outputs only 1. Check the merge/key types.")
    else:
        print(f"  => Ambiguous. Paste the full output above for analysis.")

    print("\nDone.")


if __name__ == "__main__":
    main()
