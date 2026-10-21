#!/usr/bin/env python3
"""
EIBWHP01 - Products 131,132,720,725 Report (All & SMI)
No-cache version: direct .sas7bdat reads via pyreadstat with column pruning.
"""

import sys
import gc
import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta

# ======================================================================
# PATHS
# ======================================================================

INPUT_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWHP01")
LNNOTE_INPUT_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS")
OUTPUT_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWHP01")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / "EIBWHP01.txt"

LOAN_FILE = INPUT_DIR / "intg_ln_loan.sas7bdat"

PRODUCT_FILTER = [131, 132, 720, 725]
SMI_CUSTCD = ['66', '67', '68', '69']

# If the single loan file contains both current and previous snapshots,
# set PERIOD_COL to the column that distinguishes them and PERIOD_CUR /
# PERIOD_PRV to the values. Set PERIOD_COL = None if it's a single snapshot
# (in which case current == previous and DISBURSE will be 0).
PERIOD_COL = None          # e.g. 'REPTMON' or 'SNAPSHOT'
PERIOD_CUR = None          # e.g. '09'
PERIOD_PRV = None          # e.g. '08'

# ----------------------------------------------------------------------
# PBBLNFMT formats
# ----------------------------------------------------------------------

def _fmt(maybe_callable):
    if callable(maybe_callable):
        try:
            return maybe_callable()
        except Exception as e:
            print(f"[WARN] format call failed: {e}")
            return {}
    return maybe_callable or {}

SECTCD, SECTA, SECTB = {}, {}, {}
try:
    import PBBLNFMT
    SECTCD = _fmt(getattr(PBBLNFMT, 'SECTCD', {}))
    SECTA = _fmt(getattr(PBBLNFMT, 'SECTA', {}))
    SECTB = _fmt(getattr(PBBLNFMT, 'SECTB', {}))
    print(f"[FMT] SECTCD={len(SECTCD)} SECTA={len(SECTA)} SECTB={len(SECTB)}")
except ImportError:
    print("[WARN] PBBLNFMT not found")
except Exception as e:
    print(f"[WARN] PBBLNFMT error: {e}")

# ----------------------------------------------------------------------
# Date logic
# ----------------------------------------------------------------------

def get_sas_macros(reptdate):
    day = reptdate.day
    if day == 8:
        sdd, wk, wk1 = 1, '1', '4'
    elif day == 15:
        sdd, wk, wk1 = 9, '2', '1'
    elif day == 22:
        sdd, wk, wk1 = 16, '3', '2'
    else:
        sdd, wk, wk1 = 23, '4', '3'
    mm = reptdate.month
    mm1 = mm - 1 if wk == '1' else mm
    if mm1 == 0:
        mm1 = 12
    start = datetime(reptdate.year, mm, 1)
    sdate = start + timedelta(days=sdd - 1)
    return {
        'NOWK': wk, 'NOWK1': wk1,
        'REPTMON': f"{mm:02d}", 'REPTMON1': f"{mm1:02d}",
        'REPTDAY': f"{day:02d}",
        'REPTYEAR': f"{reptdate.year:04d}",
        'RDATE': reptdate.strftime("%d/%m/%y"),
        'SDATE': sdate.strftime("%d/%m/%y"),
    }

# ----------------------------------------------------------------------
# Direct read helpers
# ----------------------------------------------------------------------

def read_sas_columns(path, columns):
    import pyreadstat
    df, _ = pyreadstat.read_sas7bdat(
        str(path), usecols=columns, encoding="latin1",
    )
    for c in df.columns:
        if df[c].dtype == np.float64:
            df[c] = df[c].astype(np.float32)
        elif df[c].dtype == np.int64:
            df[c] = df[c].astype(np.int32)
    return df

# ----------------------------------------------------------------------
# File resolution
# ----------------------------------------------------------------------

def resolve_loan_file():
    if LOAN_FILE.exists():
        return LOAN_FILE
    raise FileNotFoundError(f"Loan file not found: {LOAN_FILE}")


def resolve_lnnote_file(reptmon):
    p = LNNOTE_INPUT_DIR / f"enrh_ln_note_m{reptmon}.sas7bdat"
    if p.exists():
        return p
    alt = INPUT_DIR / "lnnote.sas7bdat"
    if alt.exists():
        return alt
    raise FileNotFoundError(f"LNNOTE not found: {p}")

# ----------------------------------------------------------------------
# EFFAPR (vectorised)
# ----------------------------------------------------------------------

def compute_effapr(intamt, intrate, netproc, noteterm, intearn2):
    noteterm = noteterm.astype(float)
    intamt = intamt.astype(float)
    intrate = intrate.astype(float)
    netproc = netproc.astype(float)
    intearn2 = intearn2.astype(float)

    intamt = np.where(intamt <= 0.01,
                      (intrate * netproc * noteterm / 1200) - intearn2,
                      intamt)
    term = np.where(noteterm > 12, 12.0, noteterm)
    with np.errstate(divide='ignore', invalid='ignore'):
        efffact = (100 * term * intamt) / (noteterm * (netproc + intearn2))
        denom = (noteterm * noteterm * efffact) + (150 * term * (noteterm + 1))
        effapr = (noteterm * efffact * (300 * term + noteterm * efffact)) / denom
    effapr = np.where((noteterm == 0) | ~np.isfinite(effapr), 0.0, effapr)
    return effapr.astype(np.float32)

# ----------------------------------------------------------------------
# Main processing
# ----------------------------------------------------------------------

def process(curr_path, lnnote_path):
    import pyreadstat
    con = duckdb.connect(":memory:")

    print(f"[PROCESS] Reading loan file: {curr_path.name}")

    # Columns for loan file. Include PERIOD_COL if it exists.
    loan_cols = ['ACCTNO', 'NOTENO', 'PRODUCT', 'BALANCE', 'CUSTCD']
    if PERIOD_COL:
        loan_cols.append(PERIOD_COL)

    loan = read_sas_columns(curr_path, loan_cols)
    loan = loan[loan['PRODUCT'].isin(PRODUCT_FILTER)].copy()
    print(f"  Loan after PRODUCT filter: {len(loan):,}")

    # Split into current / previous snapshots
    if PERIOD_COL:
        curr = loan[loan[PERIOD_COL].astype(str) == str(PERIOD_CUR)].copy()
        prev = loan[loan[PERIOD_COL].astype(str) == str(PERIOD_PRV)].copy()
        print(f"  Current snapshot rows : {len(curr):,}")
        print(f"  Previous snapshot rows: {len(prev):,}")
    else:
        curr = loan.copy()
        prev = loan.copy()
        print(f"  No PERIOD_COL — current and previous use the same snapshot "
              f"({len(curr):,} rows each)")

    # Prepare column names
    curr = curr[['ACCTNO', 'NOTENO', 'BALANCE', 'CUSTCD']].copy()
    prev = prev[['ACCTNO', 'NOTENO', 'BALANCE', 'CUSTCD']].copy()
    prev = prev.rename(columns={'BALANCE': 'LASTBAL'})

    # Keys for LNNOTE lookup
    keys = pd.concat([
        curr[['ACCTNO', 'NOTENO']],
        prev[['ACCTNO', 'NOTENO']],
    ]).drop_duplicates()
    print(f"  Distinct (ACCTNO, NOTENO) keys needing LNNOTE: {len(keys):,}")
    con.register('keys_df', keys)

    # Stream LNNOTE, filter to matching keys only
    print("[PROCESS] Reading LNNOTE – pruned columns, only matching keys...")
    lnnote_parts = []
    ln_cols = ['ACCTNO', 'NOTENO', 'SECTOR', 'INTAMT', 'INTRATE',
               'NETPROC', 'NOTETERM', 'INTEARN2']
    with pyreadstat.read_file_in_chunks(
        pyreadstat.read_sas7bdat, str(lnnote_path),
        chunksize=500_000, encoding="latin1", usecols=ln_cols,
    ) as reader:
        for i, (chunk, _) in enumerate(reader):
            for c in chunk.columns:
                if chunk[c].dtype == np.float64:
                    chunk[c] = chunk[c].astype(np.float32)
                elif chunk[c].dtype == np.int64:
                    chunk[c] = chunk[c].astype(np.int32)
            con.register('chunk_df', chunk)
            matched = con.execute("""
                SELECT c.*
                FROM chunk_df c
                SEMI JOIN keys_df k
                  ON c.ACCTNO = k.ACCTNO
                 AND c.NOTENO = k.NOTENO
            """).df()
            con.unregister('chunk_df')
            if len(matched) > 0:
                lnnote_parts.append(matched)
            if (i + 1) % 5 == 0:
                print(f"    LNNOTE chunks read: {i+1}")
            del chunk, matched
            gc.collect()

    lnnote = (pd.concat(lnnote_parts, ignore_index=True)
              if lnnote_parts else pd.DataFrame(columns=ln_cols))
    del lnnote_parts
    gc.collect()
    print(f"  LNNOTE filtered: {len(lnnote):,}")

    # Derive SECTORCD from SECTOR
    if SECTCD:
        lnnote['SECTORCD'] = lnnote['SECTOR'].map(SECTCD)
        lnnote = lnnote.dropna(subset=['SECTORCD'])
    else:
        lnnote['SECTORCD'] = lnnote['SECTOR'].astype(str)

    print("[PROCESS] Computing EFFAPR...")
    lnnote['EFFAPR'] = compute_effapr(
        lnnote['INTAMT'].values, lnnote['INTRATE'].values,
        lnnote['NETPROC'].values, lnnote['NOTETERM'].values,
        lnnote['INTEARN2'].values,
    )
    lnnote = lnnote[['ACCTNO', 'NOTENO', 'SECTORCD', 'EFFAPR']]

    # Merge current with EFFAPR (INNER = SAS "IF A")
    curr_merged = curr.merge(lnnote, on=['ACCTNO', 'NOTENO'], how='inner')
    print(f"  Current merged with EFFAPR: {len(curr_merged):,}")

    # Outer merge previous + current on ACCTNO, NOTENO
    merged = pd.merge(
        prev[['ACCTNO', 'NOTENO', 'LASTBAL', 'CUSTCD']],
        curr_merged[['ACCTNO', 'NOTENO', 'SECTORCD', 'BALANCE', 'EFFAPR', 'CUSTCD']],
        on=['ACCTNO', 'NOTENO'],
        how='outer', suffixes=('_prev', '_curr'),
    )
    print(f"  Outer merged: {len(merged):,}")

    merged['LASTBAL'] = merged['LASTBAL'].fillna(0.0).astype(np.float64)
    merged['BALANCE'] = merged['BALANCE'].fillna(0.0).astype(np.float64)
    merged['EFFAPR'] = merged['EFFAPR_curr'].fillna(merged['EFFAPR_prev']).fillna(0.0)
    merged['CUSTCD'] = merged['CUSTCD_curr'].fillna(merged['CUSTCD_prev']).fillna('')

    both = (merged['LASTBAL'] > 0) & (merged['BALANCE'] > 0)
    curr_only = (merged['LASTBAL'] == 0) & (merged['BALANCE'] > 0)

    merged['DISBURSE'] = 0.0
    merged.loc[both, 'DISBURSE'] = np.where(
        merged.loc[both, 'LASTBAL'] > merged.loc[both, 'BALANCE'], 0.0,
        merged.loc[both, 'BALANCE'] - merged.loc[both, 'LASTBAL'])
    merged.loc[curr_only, 'DISBURSE'] = merged.loc[curr_only, 'BALANCE']

    merged['PRODUCT'] = merged['DISBURSE'] * merged['EFFAPR']
    merged = merged[merged['DISBURSE'] > 0].copy()
    print(f"  Records with DISBURSE > 0: {len(merged):,}")

    if len(merged) == 0:
        con.close()
        return pd.DataFrame(columns=['SECTCD', 'DISBURSE', 'PRODUCT', 'CUSTCD'])

    rows = []
    for _, r in merged.iterrows():
        sc = r['SECTORCD']
        if pd.isna(sc) or sc == '':
            continue
        if SECTA:
            a = SECTA.get(sc, '')
            if a and a != ' ':
                rows.append({'SECTCD': a, 'DISBURSE': r['DISBURSE'],
                             'PRODUCT': r['PRODUCT'], 'CUSTCD': r['CUSTCD']})
        if SECTB:
            b = SECTB.get(sc, '')
            if b and b != ' ':
                rows.append({'SECTCD': b, 'DISBURSE': r['DISBURSE'],
                             'PRODUCT': r['PRODUCT'], 'CUSTCD': r['CUSTCD']})

    con.close()
    return pd.DataFrame(rows)

# ----------------------------------------------------------------------
# Summarise + report
# ----------------------------------------------------------------------

def summarise(expanded, custcd_filter=None):
    if len(expanded) == 0:
        return pd.DataFrame(columns=['BNMCODE', 'AMOUNT', 'WEIGHTED'])
    if custcd_filter is not None:
        expanded = expanded[expanded['CUSTCD'].astype(str).isin(custcd_filter)].copy()
        if len(expanded) == 0:
            return pd.DataFrame(columns=['BNMCODE', 'AMOUNT', 'WEIGHTED'])
    s = expanded.groupby('SECTCD', as_index=False).agg(
        DISBURSE=('DISBURSE', 'sum'), PRODUCT=('PRODUCT', 'sum'))
    s = s[s['DISBURSE'] > 0].copy()
    if len(s) == 0:
        return pd.DataFrame(columns=['BNMCODE', 'AMOUNT', 'WEIGHTED'])
    s['WEIGHTED'] = s['PRODUCT'] / s['DISBURSE']
    s['WEIGHTED'] = s['WEIGHTED'].fillna(0.0)
    s['BNMCODE'] = '673400000' + s['SECTCD'].astype(str) + 'Y'
    s['AMOUNT'] = s['DISBURSE']
    return s[['BNMCODE', 'AMOUNT', 'WEIGHTED']].sort_values('BNMCODE').reset_index(drop=True)

def write_report(df, title, rdate, fh):
    fh.write(f"EIBWHP01: {title} AS AT {rdate}\n\n")
    fh.write("Obs    BNMCODE                         AMOUNT          WEIGHTED\n\n")
    if len(df) == 0:
        fh.write("  No records found\n\n")
        return
    for i, r in df.iterrows():
        amt = f"{r['AMOUNT']:,.2f}"
        wgt = f"{r['WEIGHTED']:.6f}" if r['WEIGHTED'] != 0 else "."
        fh.write(f"{i+1:>4}  {r['BNMCODE']:<14}  {amt:>20}  {wgt:>12}\n")
    fh.write("\n")

# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def main():
    print("========== START JOB EIBWHP01 ==========")
    reptdate = datetime.now() - timedelta(days=1)
    m = get_sas_macros(reptdate)
    print(f"[DATE] {m['RDATE']}  REPTMON={m['REPTMON']} NOWK={m['NOWK']}")

    loan_path = resolve_loan_file()
    lnnote_path = resolve_lnnote_file(m['REPTMON'])
    print(f"[INFO] Loan   : {loan_path.name}")
    print(f"[INFO] LNNOTE : {lnnote_path.name}")

    expanded = process(loan_path, lnnote_path)
    all_sum = summarise(expanded)
    smi_sum = summarise(expanded, SMI_CUSTCD)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(f"EIBWHP01 REPORT GENERATED {reptdate.strftime('%d-%m-%Y')}\n")
        f.write(f"REPTMON: {m['REPTMON']}, NOWK: {m['NOWK']}\n")
        f.write("=" * 80 + "\n\n")
        write_report(all_sum, "REPORT ON PRODUCTS 131,132,720,725", m['RDATE'], f)
        write_report(smi_sum, "SMI ACCTS (CUSTCD 66,67,68,69)", m['RDATE'], f)

    print(f"[DONE] {OUTPUT_FILE}")
    print("========== END JOB EIBWHP01 ==========")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[JOB FAILED] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(8)
