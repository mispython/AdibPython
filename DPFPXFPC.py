#!/usr/bin/env python3
"""
diag_alm.py — Diagnose why build_alm() returns zero rows.

Prints:
  1. Column dtypes and sample values for the columns used in the filter
     chain (prodcd, paidind, eir_adj, bal_aft_eir, oribal, acctype, etc.)
  2. Per-mask row counts after each stage of the filter chain, so we see
     which condition eliminates everything.
  3. Distribution of prodcd prefixes (top 20), so we know if PRODCD is
     arriving as character '34xxx' or as numeric like 34180.0.
  4. LNFEE file summary (rows, unique feeplan values, CL row count) so we
     can plan a SAS-side filter.
"""

import os
import time
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pyreadstat


# =============================================================================
# CONFIG — adjust the base dir if needed
# =============================================================================

BASE_DIR = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNM01"

BNM_LOAN_MM_WK = os.path.join(BASE_DIR, "loan084.sas7bdat")
LNFEE_PATH     = "/stgsrcsys/host/uat/maa/lnfee084.sas7bdat"


def sep(title):
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def sas_date_to_pydate(val):
    if val is None or (isinstance(val, float) and val != val):
        return None
    if isinstance(val, (int, float, np.integer, np.floating)):
        return date(1960, 1, 1) + timedelta(days=int(val))
    return None


# =============================================================================
# 1. Read BNM_LOAN
# =============================================================================

sep("1. Reading BNM_LOAN (loan084.sas7bdat)")

t0 = time.time()
df, meta = pyreadstat.read_sas7bdat(BNM_LOAN_MM_WK)
print(f"Read {len(df):,} rows x {len(df.columns)} cols in {time.time()-t0:.1f}s")

# Lowercase column names, as EIMBNM01 does
df.columns = [c.lower() for c in df.columns]

print()
print("Columns actually present (lowercased):")
print(df.columns.tolist())

# ---- dtype / sample for the columns we care about --------------------------
sep("2. Dtypes and sample values for filter-chain columns")

focus_cols = [
    'acctno', 'noteno', 'prodcd', 'paidind', 'acctype',
    'balance', 'bal_aft_eir', 'oribal',
    'eir_adj', 'cjfee', 'cusedamt', 'rleasamt',
    'product', 'commno', 'noacct', 'dnbfisme', 'retailid',
]

for c in focus_cols:
    if c in df.columns:
        s = df[c]
        sample = s.dropna().head(5).tolist()
        print(f"  {c:12s}  dtype={str(s.dtype):10s}  "
              f"nulls={s.isna().sum():>10,}  sample={sample}")
    else:
        print(f"  {c:12s}  -- NOT PRESENT --")

# ---- PRODCD detail ---------------------------------------------------------
sep("3. PRODCD breakdown")

if 'prodcd' in df.columns:
    prodcd = df['prodcd']
    print(f"raw dtype: {prodcd.dtype}")

    # If numeric, that's the likely cause. Show both string forms.
    as_str      = prodcd.astype(str)
    as_str_2ch  = as_str.str[:2]

    print()
    print("top 20 PRODCD raw values:")
    print(prodcd.value_counts(dropna=False).head(20).to_string())

    print()
    print("top 20 PRODCD[:2] values (after astype(str)):")
    print(as_str_2ch.value_counts(dropna=False).head(20).to_string())

    # Compare the two candidate '34' tests
    m_str34 = as_str_2ch == '34'
    m_54120 = as_str == '54120'
    print()
    print(f"matches  prodcd[:2] == '34' : {int(m_str34.sum()):>10,}")
    print(f"matches  prodcd == '54120'  : {int(m_54120.sum()):>10,}")
    print(f"matches  prodcd[:2]=='34' OR prodcd=='54120' : {int((m_str34 | m_54120).sum()):>10,}")

# ---- PAIDIND detail --------------------------------------------------------
sep("4. PAIDIND breakdown")

if 'paidind' in df.columns:
    print(df['paidind'].value_counts(dropna=False).head(20).to_string())

# ---- ACCTYPE detail --------------------------------------------------------
sep("5. ACCTYPE breakdown")

if 'acctype' in df.columns:
    print(df['acctype'].value_counts(dropna=False).head(20).to_string())

# ---- EIR_ADJ detail --------------------------------------------------------
sep("6. EIR_ADJ detail")

if 'eir_adj' in df.columns:
    s = df['eir_adj']
    print(f"dtype: {s.dtype}")
    print(f"nulls: {s.isna().sum():,} / {len(s):,}  ({100*s.isna().mean():.1f}%)")
    print(f"nonnull sample: {s.dropna().head(10).tolist()}")
    print(f"describe: \n{s.describe()}")

# ---- BALANCE detail --------------------------------------------------------
sep("7. BALANCE (bal_aft_eir) detail")

for c in ['bal_aft_eir', 'balance', 'oribal']:
    if c in df.columns:
        s = pd.to_numeric(df[c], errors='coerce')
        print(f"{c}:")
        print(f"  dtype: {df[c].dtype}")
        print(f"  nulls: {s.isna().sum():,}")
        print(f"  describe:")
        print(f"  {s.describe().to_string()}")
        # rounded-to-0 count = candidate for the drop_zero mask
        rounded = s.round(2)
        n_zero = int((rounded == 0.0).sum() + (rounded == -0.0).sum())
        print(f"  round(2) == 0 or -0 : {n_zero:,}")
        print()

# =============================================================================
# 8. Emulate the build_alm filter chain step by step
# =============================================================================

sep("8. Emulating build_alm() filter chain")

# Reproduce the renaming build_alm does
work = df.copy()
if 'balance' in work.columns:
    work = work.rename(columns={'balance': 'oribal'})
if 'bal_aft_eir' in work.columns:
    work = work.rename(columns={'bal_aft_eir': 'balance'})

n_total = len(work)
print(f"Starting rows: {n_total:,}")

# Ensure the columns exist
for c in ['oribal', 'cjfee', 'cusedamt', 'rleasamt', 'clfee',
          'product', 'commno', 'noacct', 'eir_adj', 'acctno', 'noteno']:
    if c in work.columns:
        work[c] = pd.to_numeric(work[c], errors='coerce')
    else:
        work[c] = np.nan
for c in ['oribal', 'cjfee', 'cusedamt', 'rleasamt', 'clfee']:
    work[c] = work[c].fillna(0.0)
for c in ['noacct', 'product', 'commno']:
    work[c] = work[c].fillna(0).astype(np.int64)
for c in ['paidind', 'prodcd', 'acctype']:
    if c not in work.columns:
        work[c] = ''
    work[c] = work[c].fillna('').astype(str)
if 'retailid' not in work.columns:
    work['retailid'] = ''
work['retailid'] = work['retailid'].fillna('').astype(str)

# --- The three masks --------------------------------------------------------
paidind = work['paidind']
eir_adj = work['eir_adj']
oribal  = work['oribal']
prodcd  = work['prodcd']
prodcd_str = prodcd.astype(str)

drop_pc   = paidind.isin(['P', 'C']) & eir_adj.isna()
oribal_r  = oribal.round(2)
drop_zero = oribal_r.isin([0.0, -0.0])
keep_prodcd = (prodcd_str.str[:2] == '34') | (prodcd_str == '54120')

print()
print(f"mask  drop_pc          : drops {int(drop_pc.sum()):>10,}  keeps {int((~drop_pc).sum()):>10,}")
print(f"mask  drop_zero        : drops {int(drop_zero.sum()):>10,}  keeps {int((~drop_zero).sum()):>10,}")
print(f"mask  keep_prodcd      : keeps {int(keep_prodcd.sum()):>10,}  drops {int((~keep_prodcd).sum()):>10,}")

base_mask = ~drop_pc & ~drop_zero & keep_prodcd
print()
print(f"combined base_mask     : keeps {int(base_mask.sum()):>10,}  of {n_total:,}")

# --- Remaining mask components ----------------------------------------------
in_pc = paidind.isin(['P', 'C'])

cond_ln1 = (work['rleasamt'] != 0.0) & ~in_pc & (oribal > 0) & (work['cjfee'] != oribal)
cond_ln2 = (work['rleasamt'] == 0.0) & ~in_pc & (oribal > 0) & \
           (work['product'] >= 600) & (work['product'] <= 699)
cond_ln3 = (work['rleasamt'] == 0.0) & ~in_pc & (oribal > 0) & \
           (work['commno'] > 0) & (work['cusedamt'] > 0)
ln_eligible = cond_ln1 | cond_ln2 | cond_ln3
ln_acctype  = (work['acctype'] == 'LN')

print()
print(f"  cond_ln1 (rleasamt!=0 ...)      : {int(cond_ln1.sum()):>10,}")
print(f"  cond_ln2 (rleasamt==0 ... 600:699): {int(cond_ln2.sum()):>10,}")
print(f"  cond_ln3 (commno>0 & cusedamt>0): {int(cond_ln3.sum()):>10,}")
print(f"  ln_acctype                      : {int(ln_acctype.sum()):>10,}")
print(f"  ln_eligible (any of 1,2,3)      : {int(ln_eligible.sum()):>10,}")

# After base_mask, how many are LN and are NOT eligible? They get noacct=0.
base_rows = work[base_mask]
ln_rows = base_rows[base_rows['acctype'] == 'LN']
ln_elig_after = ln_eligible[base_mask][base_rows['acctype'] == 'LN']
print()
print(f"After base_mask: {len(base_rows):,} rows total, "
      f"{len(ln_rows):,} are acctype='LN', "
      f"{int((~ln_elig_after).sum()):,} of those are NOT eligible.")

# =============================================================================
# 9. Sample rows that survive base_mask (if any)
# =============================================================================

sep("9. Sample rows surviving base_mask")

if base_mask.sum() > 0:
    sample = work[base_mask].head(5)
    print(sample[['acctno','noteno','prodcd','paidind','acctype','oribal','product']].to_string())
else:
    print("NO ROWS SURVIVE. Sample of first 5 rows for inspection:")
    print(work[['acctno','noteno','prodcd','paidind','acctype','oribal','product']].head(5).to_string())

# =============================================================================
# 10. LNFEE file profile
# =============================================================================

sep("10. LNFEE file profile")

if os.path.exists(LNFEE_PATH):
    t0 = time.time()
    fee, _ = pyreadstat.read_sas7bdat(LNFEE_PATH)
    print(f"Read {len(fee):,} rows x {len(fee.columns)} cols in {time.time()-t0:.1f}s")
    fee.columns = [c.lower() for c in fee.columns]
    print()
    print("Columns:", fee.columns.tolist())
    print()
    if 'feeplan' in fee.columns:
        print("feeplan value_counts (top 20):")
        print(fee['feeplan'].value_counts(dropna=False).head(20).to_string())
        print()
        cl = fee[fee['feeplan'] == 'CL']
        print(f"rows with feeplan == 'CL'        : {len(cl):,}")
        if 'duetotal' in cl.columns:
            cl2 = cl[pd.to_numeric(cl['duetotal'], errors='coerce') > 0]
            print(f"rows with feeplan=='CL' & due>0  : {len(cl2):,}")
    else:
        print("'feeplan' column not present!")
else:
    print(f"File not found: {LNFEE_PATH}")

sep("Done.")
