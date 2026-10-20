#!/usr/bin/env python3
"""
diagnose_repo_parity.py

Standalone diagnostic to identify why Python REPOTXT has ~301 records
but production SAS REPOTXT has 1000+.

Traces the pipeline stage-by-stage and specifically tests:
  1. KKI (branch 117) accounts in LNNOTE — do they survive all filters?
  2. Sample prod REGNOs (AAM8888, ABF5555, etc.) — traced end-to-end
  3. Whether the LNNOTE -> LOANTEMP ACCTNO join is dropping accounts
  4. Whether prod's LOANTEMP might differ from iccd2

Reads:
  - enrh_ln_note_m09.sas7bdat    (LNNOTE)
  - loantemp_iccd2.sas7bdat      (LOANTEMP)

Prints a report. Does not modify any files.
"""

import sys
from pathlib import Path
import pandas as pd
import pyreadstat

import PBBLNFMT


# =====================================================================
# Configuration
# =====================================================================
INPUT_BASE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod")

LNNOTE_PATH   = INPUT_BASE / "EIBRCGCS" / "enrh_ln_note_m09.sas7bdat"
LOANTEMP_PATH = INPUT_BASE / "EIMREPOI" / "loantemp_iccd2.sas7bdat"

HP_SET = list(PBBLNFMT.MACROS['HP'])   # 17 HP loan types
HP_SET_INT = set(int(x) for x in HP_SET)

# Prod REGNOs from your sample that Python is missing
PROD_SAMPLE_REGNOS = ['AAM8888', 'ABF5555', 'ABT3993', 'ACN712',
                      'ACV7557', 'ACW6576', 'ACA4798', 'ACM6676']

# KKI branch code
KKI_BRANCH = 117


# =====================================================================
# Helpers
# =====================================================================
def norm(x):
    """Normalize ACCTNO/LOANTYPE to clean string form."""
    if pd.isna(x):
        return None
    try:
        return str(int(float(x)))
    except (ValueError, TypeError):
        return str(x).strip()


def section(title):
    print("\n" + "=" * 72)
    print(f"  {title}")
    print("=" * 72)


def subsection(title):
    print(f"\n--- {title} ---")


# =====================================================================
# Diagnostic
# =====================================================================
def main():
    print("EIMREPOI REPOTXT Parity Diagnostic")
    print("=" * 72)
    print(f"LNNOTE:   {LNNOTE_PATH}")
    print(f"LOANTEMP: {LOANTEMP_PATH}")
    print(f"HP set ({len(HP_SET)}): {HP_SET}")

    if not LNNOTE_PATH.exists():
        print(f"\nFATAL: LNNOTE not found: {LNNOTE_PATH}")
        return 1
    if not LOANTEMP_PATH.exists():
        print(f"\nFATAL: LOANTEMP not found: {LOANTEMP_PATH}")
        return 1

    # =================================================================
    # SECTION 1: Load LNNOTE
    # =================================================================
    section("SECTION 1: Load LNNOTE (filtered to HP + BALANCE > 0 + BORSTAT)")

    print("Reading LNNOTE...")
    lnnote, _ = pyreadstat.read_sas7bdat(
        str(LNNOTE_PATH),
        usecols=['ACCTNO', 'LOANTYPE', 'NTBRCH', 'BALANCE',
                 'BORSTAT', 'COLLDESC', 'COLLYEAR'],
    )
    lnnote.columns = lnnote.columns.str.lower()
    lnnote['lt'] = pd.to_numeric(lnnote['loantype'], errors='coerce')
    lnnote['acctno_s'] = lnnote['acctno'].apply(norm)

    total = len(lnnote)
    print(f"  Total LNNOTE rows: {total:,}")

    # Stage-by-stage
    m_hp    = lnnote['lt'].isin(HP_SET_INT)
    m_bal   = lnnote['balance'] > 0
    m_bor   = ~lnnote['borstat'].isin(['F', 'I', 'R'])

    print(f"  LOANTYPE IN HP:        {m_hp.sum():>12,}")
    print(f"  BALANCE > 0:           {m_bal.sum():>12,}")
    print(f"  BORSTAT NOT IN F,I,R:  {m_bor.sum():>12,}")
    print(f"  ALL THREE (filtered):  {(m_hp & m_bal & m_bor).sum():>12,}")

    lnnote_f = lnnote[m_hp & m_bal & m_bor].copy()
    print(f"  Filtered LNNOTE retained: {len(lnnote_f):,}")

    # Loan type distribution after filter
    subsection("Loan type distribution in filtered LNNOTE")
    lt_dist = lnnote_f['lt'].value_counts().sort_index()
    for lt, cnt in lt_dist.items():
        print(f"    {int(lt):>6}: {cnt:>10,}")

    # =================================================================
    # SECTION 2: KKI (branch 117) analysis
    # =================================================================
    section(f"SECTION 2: KKI branch ({KKI_BRANCH}) accounts — prod has these")

    kki_raw = lnnote[lnnote['ntbrch'] == KKI_BRANCH]
    print(f"  KKI accounts in raw LNNOTE: {len(kki_raw):,}")

    if len(kki_raw) > 0:
        subsection("KKI loan type distribution (raw LNNOTE)")
        for lt, cnt in kki_raw['lt'].value_counts().head(15).items():
            marker = ' <-- HP' if int(lt) in HP_SET_INT else ''
            print(f"    {int(lt):>6}: {cnt:>6,}{marker}")

    # Apply filters
    kki_filtered = lnnote_f[lnnote_f['ntbrch'] == KKI_BRANCH]
    print(f"\n  KKI after ALL filters:     {len(kki_filtered):,}")

    if len(kki_filtered) > 0:
        subsection("KKI loan types after ALL filters")
        for lt, cnt in kki_filtered['lt'].value_counts().items():
            print(f"    {int(lt):>6}: {cnt:>6,}")
    else:
        print("  ⚠  NO KKI accounts survive LNNOTE filters")
        print("     -> If prod REPOTXT has KKI records, prod's LNNOTE")
        print("        or filters differ from yours.")

    # =================================================================
    # SECTION 3: Load LOANTEMP
    # =================================================================
    section("SECTION 3: Load LOANTEMP (iccd2)")

    print("Reading LOANTEMP...")
    lt, _ = pyreadstat.read_sas7bdat(
        str(LOANTEMP_PATH),
        usecols=['ACCTNO', 'PRODUCT', 'ARREAR', 'BRANCH'],
    )
    lt.columns = lt.columns.str.lower()
    lt['acctno_s'] = lt['acctno'].apply(norm)

    print(f"  LOANTEMP total rows: {len(lt):,}")
    print(f"  ARREAR >= 10:        {(lt['arrear'] >= 10).sum():,}")
    print(f"  product=700 count:   {(lt['product'] == 700).sum()}")

    subsection("LOANTEMP: ARREAR >= 10 by branch (top 20)")
    big = lt[lt['arrear'] >= 10]
    for br, cnt in big['branch'].value_counts().head(20).items():
        print(f"    branch {int(br):>6}: {cnt:>6,}")

    # KKI (branch 117) in LOANTEMP
    kki_lt = lt[(lt['branch'] == KKI_BRANCH) & (lt['arrear'] >= 10)]
    print(f"\n  KKI (branch {KKI_BRANCH}) with ARREAR >= 10: {len(kki_lt):,}")
    if len(kki_lt) > 0:
        print("  KKI products with arrears:")
        for p, cnt in kki_lt['product'].value_counts().items():
            print(f"    product {int(p):>6}: {cnt:>6,}")

    # =================================================================
    # SECTION 4: Merge analysis (LNNOTE filtered + LOANTEMP)
    # =================================================================
    section("SECTION 4: Merge LNNOTE ↔ LOANTEMP on ACCTNO")

    n_lnnote_acct = lnnote_f['acctno_s'].nunique()
    n_lt_acct     = lt['acctno_s'].nunique()
    overlap       = len(set(lnnote_f['acctno_s'].dropna()) &
                        set(lt['acctno_s'].dropna()))

    print(f"  LNNOTE filtered unique acctnos: {n_lnnote_acct:,}")
    print(f"  LOANTEMP unique acctnos:        {n_lt_acct:,}")
    print(f"  Overlap (matched):              {overlap:,} "
          f"({100*overlap/max(n_lnnote_acct,1):.1f}% of LNNOTE)")

    merged = lnnote_f.merge(
        lt[['acctno_s', 'arrear', 'product', 'branch']],
        on='acctno_s', how='left'
    )
    arr10 = merged[merged['arrear'] >= 10]
    print(f"\n  After merge, ARREAR >= 10: {len(arr10):,}")

    if len(arr10) > 0:
        subsection("Merged ARREAR >= 10 — by LNNOTE loantype")
        for lt_v, cnt in arr10['lt'].value_counts().items():
            print(f"    LNNOTE loantype {int(lt_v):>6}: {cnt:>6,}")

        subsection("Merged ARREAR >= 10 — by LOANTEMP product")
        for p, cnt in arr10['product'].value_counts().head(20).items():
            if pd.notna(p):
                print(f"    LOANTEMP product {int(p):>6}: {cnt:>6,}")

        subsection("Merged ARREAR >= 10 — by branch (top 20)")
        for br, cnt in arr10['branch'].value_counts().head(20).items():
            if pd.notna(br):
                print(f"    branch {int(br):>6}: {cnt:>6,}")

    # =================================================================
    # SECTION 5: Trace specific prod REGNOs
    # =================================================================
    section("SECTION 5: Trace prod REGNOs missing from Python REPOTXT")

    for regno in PROD_SAMPLE_REGNOS:
        subsection(f"REGNO = {regno}")

        # Find in raw LNNOTE by COLLDESC substring
        match = lnnote[lnnote['colldesc'].astype(str).str.contains(regno, na=False)]

        if len(match) == 0:
            print(f"  ✗ NOT FOUND in raw LNNOTE (searched COLLDESC)")
            continue

        print(f"  Found {len(match)} row(s) in raw LNNOTE:")
        for _, r in match.head(3).iterrows():
            acctno = r['acctno_s']
            lt_v = r['loantype']
            br = r['ntbrch']
            bal = r['balance']
            bor = r['borstat']

            # Apply LNNOTE filters
            is_hp = int(float(lt_v)) in HP_SET_INT if pd.notna(lt_v) else False
            bal_ok = bal > 0 if pd.notna(bal) else False
            bor_ok = bor not in ('F', 'I', 'R') if pd.notna(bor) else True

            print(f"    ACCTNO={acctno}  LOANTYPE={lt_v}  NTBRCH={br}")
            print(f"      BALANCE={bal}  BORSTAT={bor!r}")
            print(f"      LOANTYPE IN HP: {is_hp}  |  BALANCE>0: {bal_ok}  |  BORSTAT ok: {bor_ok}")

            # Check LOANTEMP
            if acctno:
                hit = lt[lt['acctno_s'] == acctno]
                if len(hit) == 0:
                    print(f"      LOANTEMP: ✗ NOT FOUND for acctno {acctno}")
                else:
                    for _, h in hit.head(2).iterrows():
                        print(f"      LOANTEMP: product={h['product']}  "
                              f"arrear={h['arrear']}  branch={h['branch']}")
                        if pd.notna(h['arrear']):
                            if h['arrear'] >= 10:
                                print(f"        → WOULD APPEAR in REPOTXT if ACCTNO joined")
                            else:
                                print(f"        → ARREAR < 10, correctly excluded")

    # =================================================================
    # SECTION 6: Verdict
    # =================================================================
    section("VERDICT")

    n_final = len(arr10)
    kki_in_final = (arr10['branch'] == KKI_BRANCH).sum() if len(arr10) > 0 else 0
    n_700_in_lt = (lt['product'] == 700).sum()

    print(f"  Python REPOTXT expected rows: {n_final}")
    print(f"  Prod  REPOTXT reported rows:  1000+ (per user)")
    print(f"  Gap:                         {1000 - n_final}+")

    print()
    if n_700_in_lt == 0:
        print("  🔴 LOANTEMP.iccd2 has ZERO product=700 rows.")
        print("     If prod REPOTXT contains 700-type auto-HP, prod's LOANTEMP")
        print("     is DIFFERENT from iccd2.")
    else:
        print(f"  ✅ LOANTEMP has {n_700_in_lt:,} product=700 rows.")

    print()
    if kki_in_final == 0 and len(kki_lt) > 0:
        print(f"  🔴 KKI (branch {KKI_BRANCH}) has {len(kki_lt):,} accounts with ARREAR>=10")
        print(f"     in LOANTEMP, but ZERO survive to merged ARREAR>=10 set.")
        print("     → Join or LNNOTE filter is dropping them.")
    elif kki_in_final > 0:
        print(f"  ✅ {kki_in_final} KKI records survive to merged ARREAR>=10.")

    print()
    print("  Next steps based on trace results above:")
    print("  - If prod REGNOs found in LNNOTE but NOT in LOANTEMP: prod uses different LOANTEMP")
    print("  - If found in LOANTEMP but ARREAR < 10: prod's ARREAR definition differs")
    print("  - If found in both but not in merged output: ACCTNO format mismatch bug")

    print("\nDiagnostic complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
