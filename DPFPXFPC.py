#!/usr/bin/env python3
"""
EIBWHP01 - Products 131,132,720,725 Report (All & SMI)
"""

import os
import sys
import gc
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime, timedelta

BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/")
INPUT_DIR = BASE_DIR / "input/prod/EIBWHP01"
OUTPUT_DIR = BASE_DIR / "output/EIBWHP01"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CACHE_DIR = BASE_DIR / "cache" / "EIBWHP01"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / "EIBWHP01.txt"
CHUNK_ROWS = 500_000
ROW_LIMIT = int(os.environ.get("ROW_LIMIT", 0))

try:
    import PBBLNFMT
    SECTCD = PBBLNFMT.SECTCD
    SECTA = PBBLNFMT.SECTA
    SECTB = PBBLNFMT.SECTB
except ImportError:
    print("[WARN] PBBLNFMT not found – sector formats unavailable.")
    SECTCD = SECTA = SECTB = {}

# ----------------------------------------------------------------------
# Date logic (replicates SAS DATA REPTDATE)
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
        'RDATE': reptdate.strftime("%d/%m/%y"),
        'SDATE': sdate.strftime("%d/%m/%y")
    }

# ----------------------------------------------------------------------
# Parquet caching helpers
# ----------------------------------------------------------------------

def cache_fresh(sas_path, cache_path):
    return cache_path.exists() and cache_path.stat().st_mtime >= sas_path.stat().st_mtime

def sas_to_parquet(sas_path, cache_path, tag):
    print(f"  [{tag}] Converting {sas_path.name} -> {cache_path.name} ...")
    writer = None
    schema = None
    total = 0
    rows_read = 0
    reader = pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS)
    for chunk in reader:
        if ROW_LIMIT and rows_read >= ROW_LIMIT:
            break
        if ROW_LIMIT:
            chunk = chunk.iloc[:ROW_LIMIT - rows_read]
        rows_read += len(chunk)
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        if schema is None:
            schema = table.schema
            writer = pq.ParquetWriter(cache_path, schema, compression="snappy")
        else:
            cast_arrays = []
            for i, field in enumerate(schema):
                col = table.column(field.name)
                if col.type != field.type:
                    try:
                        col = col.cast(field.type, safe=False)
                    except:
                        col = pa.nulls(len(col), type=field.type)
                cast_arrays.append(col)
            table = pa.Table.from_arrays(cast_arrays, schema=schema)
        writer.write_table(table)
        total += len(chunk)
        del chunk, table
        gc.collect()
    writer.close()
    print(f"  [{tag}] Done – {total:,} rows.")

def read_sas_cached(sas_path):
    cache_path = CACHE_DIR / f"{sas_path.stem}.parquet"
    if cache_fresh(sas_path, cache_path):
        print(f"[READ] Using cache: {cache_path.name}")
        return pd.read_parquet(cache_path)
    sas_to_parquet(sas_path, cache_path, sas_path.stem.upper())
    return pd.read_parquet(cache_path)

# ----------------------------------------------------------------------
# EFFAPR calculation
# ----------------------------------------------------------------------

def compute_effapr(row):
    intamt = row.get('INTAMT', 0.0)
    intrate = row.get('INTRATE', 0.0)
    netproc = row.get('NETPROC', 0.0)
    noteterm = row.get('NOTETERM', 0)
    intearn2 = row.get('INTEARN2', 0.0)

    if intamt <= 0.01:
        intamt = (intrate * netproc * noteterm / 1200) - intearn2

    term = 12 if noteterm > 12 else noteterm
    efffact = (100 * term * intamt) / (noteterm * (netproc + intearn2))
    denom = (noteterm * noteterm * efffact) + (150 * term * (noteterm + 1))
    return 0.0 if denom == 0 else (noteterm * efffact * (300 * term + noteterm * efffact)) / denom

def add_effapr(df):
    df['EFFAPR'] = df.apply(compute_effapr, axis=1)
    return df

# ----------------------------------------------------------------------
# Core processing
# ----------------------------------------------------------------------

def build_expanded(lnnote, curr, prev):
    # LNNOTE: map SECTOR -> SECTORCD
    lnnote['SECTORCD'] = lnnote['SECTOR'].map(SECTCD)
    lnnote = lnnote.dropna(subset=['SECTORCD']).copy()
    lnnote = add_effapr(lnnote)
    keep = ['ACCTNO', 'NOTENO', 'SECTORCD', 'EFFAPR']
    lnnote = lnnote[keep]

    # Merge current and previous with lnnote
    curr_merged = curr.merge(lnnote, on=['ACCTNO', 'NOTENO', 'SECTORCD'], how='left')
    curr_merged['EFFAPR'] = curr_merged['EFFAPR'].fillna(0.0)

    prev_merged = prev.rename(columns={'BALANCE': 'LASTBAL', 'NOTETERM': 'LASTNOTE'})
    prev_merged = prev_merged.merge(lnnote, on=['ACCTNO', 'NOTENO', 'SECTORCD'], how='left')
    prev_merged['EFFAPR'] = prev_merged['EFFAPR'].fillna(0.0)

    # Full outer merge
    merged = pd.merge(
        prev_merged[['ACCTNO', 'NOTENO', 'SECTORCD', 'LASTBAL', 'EFFAPR']],
        curr_merged[['ACCTNO', 'NOTENO', 'SECTORCD', 'BALANCE', 'EFFAPR',
                     'CUSTCD', 'AMTIND', 'APPRLIM2']],
        on=['ACCTNO', 'NOTENO', 'SECTORCD'],
        how='outer',
        suffixes=('_prev', '_curr')
    )

    # Determine flags
    merged['A'] = merged['LASTBAL'].notna()
    merged['B'] = merged['BALANCE'].notna()

    # Compute DISBURSE / REPAID
    merged['DISBURSE'] = 0.0
    merged['REPAID'] = 0.0

    mask_ab = merged['A'] & merged['B']
    merged.loc[mask_ab, 'REPAID'] = np.where(
        merged.loc[mask_ab, 'LASTBAL'] > merged.loc[mask_ab, 'BALANCE'],
        merged.loc[mask_ab, 'LASTBAL'] - merged.loc[mask_ab, 'BALANCE'],
        0.0
    )
    merged.loc[mask_ab, 'DISBURSE'] = np.where(
        merged.loc[mask_ab, 'LASTBAL'] > merged.loc[mask_ab, 'BALANCE'],
        0.0,
        merged.loc[mask_ab, 'BALANCE'] - merged.loc[mask_ab, 'LASTBAL']
    )

    mask_only_prev = merged['A'] & ~merged['B']
    merged.loc[mask_only_prev, 'REPAID'] = merged.loc[mask_only_prev, 'LASTBAL']

    mask_only_curr = ~merged['A'] & merged['B']
    merged.loc[mask_only_curr, 'DISBURSE'] = merged.loc[mask_only_curr, 'BALANCE']

    # PRODUCT = DISBURSE * EFFAPR (use current if available)
    merged['EFFAPR'] = merged['EFFAPR_curr'].fillna(merged['EFFAPR_prev'])
    merged['PRODUCT'] = merged['DISBURSE'] * merged['EFFAPR']

    # Expand by SECTA and SECTB
    rows = []
    for _, row in merged.iterrows():
        sectcd = row['SECTORCD']
        a = SECTA.get(sectcd, '')
        b = SECTB.get(sectcd, '')
        if a:
            rows.append({
                'SECTCD': a,
                'DISBURSE': row['DISBURSE'],
                'PRODUCT': row['PRODUCT'],
                'CUSTCD': row.get('CUSTCD', ''),
                'AMTIND': row.get('AMTIND', 0.0),
                'APPRLIM2': row.get('APPRLIM2', 0.0)
            })
        if b:
            rows.append({
                'SECTCD': b,
                'DISBURSE': row['DISBURSE'],
                'PRODUCT': row['PRODUCT'],
                'CUSTCD': row.get('CUSTCD', ''),
                'AMTIND': row.get('AMTIND', 0.0),
                'APPRLIM2': row.get('APPRLIM2', 0.0)
            })
    return pd.DataFrame(rows)

def summarise(expanded, label, custcd_filter=None):
    if custcd_filter is not None:
        expanded = expanded[expanded['CUSTCD'].isin(custcd_filter)].copy()
    summary = expanded.groupby('SECTCD', as_index=False).agg({
        'DISBURSE': 'sum',
        'PRODUCT': 'sum'
    })
    summary['WEIGHTED'] = summary['PRODUCT'] / summary['DISBURSE']
    summary['WEIGHTED'] = summary['WEIGHTED'].fillna(0.0)
    summary['BNMCODE'] = '673400000' + summary['SECTCD'] + 'Y'
    summary['AMOUNT'] = summary['DISBURSE']
    summary = summary[['BNMCODE', 'AMOUNT', 'WEIGHTED']].sort_values('BNMCODE').reset_index(drop=True)
    return summary

# ----------------------------------------------------------------------
# Report generation
# ----------------------------------------------------------------------

def write_report(df, title, rdate, fh):
    fh.write(f"EIBWHP01: {title} AS AT {rdate}\n")
    fh.write("\n")
    fh.write("Obs    BNMCODE                         AMOUNT          WEIGHTED\n")
    fh.write("\n")
    for i, row in df.iterrows():
        amt = f"{row['AMOUNT']:,.2f}"
        wgt = f"{row['WEIGHTED']:.6f}" if row['WEIGHTED'] != 0 else "."
        fh.write(f"{i+1:>4}  {row['BNMCODE']:<14}  {amt:>20}  {wgt:>12}\n")
    fh.write("\n")

# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def main():
    print(f"========== START JOB EIBWHP01 ==========")

    reptdate = datetime.now() - timedelta(days=1)
    macros = get_sas_macros(reptdate)
    print(f"[DATE] Report: {macros['RDATE']}")

    # Build file paths
    curr_path = INPUT_DIR / f"loan{macros['REPTMON']}{macros['NOWK']}.sas7bdat"
    prev_path = INPUT_DIR / f"loan{macros['REPTMON1']}{macros['NOWK1']}.sas7bdat"
    lnnote_path = INPUT_DIR / "lnnote.sas7bdat"

    if not curr_path.exists():
        candidates = list(INPUT_DIR.glob("loan*.sas7bdat"))
        if candidates:
            curr_path = max(candidates, key=lambda p: p.stat().st_mtime)
            print(f"[WARN] Using latest loan as current: {curr_path.name}")
        else:
            raise FileNotFoundError("No loan file found")
    if not prev_path.exists():
        raise FileNotFoundError(f"Previous BNM not found: {prev_path}")
    if not lnnote_path.exists():
        raise FileNotFoundError(f"LNNOTE not found: {lnnote_path}")

    print("[READ] Loading files (parquet cache)...")
    lnnote = read_sas_cached(lnnote_path)
    curr = read_sas_cached(curr_path)
    prev = read_sas_cached(prev_path)

    print("[PROCESS] Building expanded loan data...")
    expanded = build_expanded(lnnote, curr, prev)

    print("[PROCESS] Summarising all customers...")
    all_summary = summarise(expanded, "ALL")

    print("[PROCESS] Summarising SMI (CUSTCD 66-69)...")
    smi_summary = summarise(expanded, "SMI", custcd_filter=['66','67','68','69'])

    print("[OUTPUT] Writing report...")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        write_report(all_summary, "REPORT ON PRODUCTS 131,132,720,725", macros['RDATE'], f)
        write_report(smi_summary, "SMI ACCTS (CUSTCD 66,67,68,69)", macros['RDATE'], f)

    print(f"  Output written: {OUTPUT_FILE}")
    print("========== END JOB EIBWHP01 ==========")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[JOB FAILED] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(8)
