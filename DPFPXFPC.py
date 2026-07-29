#!/usr/bin/env python3
"""
EIBWHP01 - Products 131,132,720,725 Report (All & SMI)
"""

import os
import sys
import gc
import duckdb
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
CHUNK_ROWS = 200_000

PRODUCT_FILTER = [131, 132, 720, 725]
SMI_CUSTCD = ['66', '67', '68', '69']

# ----------------------------------------------------------------------
# Load PBBLNFMT formats
# ----------------------------------------------------------------------

SECTCD = {}
SECTA = {}
SECTB = {}

try:
    import PBBLNFMT
    if hasattr(PBBLNFMT, 'SECTCD'):
        SECTCD = PBBLNFMT.SECTCD
    if hasattr(PBBLNFMT, 'SECTA'):
        SECTA = PBBLNFMT.SECTA
    if hasattr(PBBLNFMT, 'SECTB'):
        SECTB = PBBLNFMT.SECTB
    print(f"[FMT] Loaded SECTCD: {len(SECTCD)} entries")
except ImportError:
    print("[WARN] PBBLNFMT not found")
except Exception as e:
    print(f"[WARN] Error loading PBBLNFMT: {e}")

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
    
    try:
        reader = pd.read_sas(sas_path, encoding="latin1", chunksize=CHUNK_ROWS)
        for chunk in reader:
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
    except Exception as e:
        print(f"  [{tag}] ERROR: {e}")
        if cache_path.exists():
            cache_path.unlink()
        raise

def read_sas_cached(sas_path):
    cache_path = CACHE_DIR / f"{sas_path.stem}.parquet"
    if cache_fresh(sas_path, cache_path):
        print(f"[READ] Using cache: {cache_path.name}")
        return cache_path
    sas_to_parquet(sas_path, cache_path, sas_path.stem.upper())
    return cache_path

# ----------------------------------------------------------------------
# File finding helpers
# ----------------------------------------------------------------------

def find_loan_file(directory, reptmon, nowk):
    pattern = f"loan{reptmon}{nowk}*.sas7bdat"
    files = list(directory.glob(pattern))
    if files:
        return files[0]
    return None

def find_latest_loan_file(directory):
    files = list(directory.glob("loan*.sas7bdat"))
    if files:
        return max(files, key=lambda p: p.stat().st_mtime)
    return None

# ----------------------------------------------------------------------
# EFFAPR calculation (exact SAS logic)
# ----------------------------------------------------------------------

def compute_effapr_row(row):
    """Compute EFFAPR exactly as SAS does"""
    intamt = row.get('INTAMT', 0.0)
    intrate = row.get('INTRATE', 0.0)
    netproc = row.get('NETPROC', 0.0)
    noteterm = row.get('NOTETERM', 0)
    intearn2 = row.get('INTEARN2', 0.0)

    if noteterm == 0:
        return 0.0

    # SAS: IF INTAMT LE 0.01 THEN INTAMT = (INTRATE*NETPROC*NOTETERM/1200)-INTEARN2
    if intamt <= 0.01:
        intamt = (intrate * netproc * noteterm / 1200) - intearn2

    # SAS: IF NOTETERM > 12 THEN TERM = 12; ELSE TERM = NOTETERM
    term = 12 if noteterm > 12 else noteterm
    
    # SAS: EFFFACT = (100*TERM*INTAMT)/(NOTETERM*(NETPROC+INTEARN2))
    denom_eff = noteterm * (netproc + intearn2)
    if denom_eff == 0:
        return 0.0
    efffact = (100 * term * intamt) / denom_eff
    
    # SAS: EFFAPR=(NOTETERM*EFFFACT*(300*TERM+NOTETERM*EFFFACT))/
    #      ((NOTETERM*NOTETERM*EFFFACT)+(150*TERM*(NOTETERM+1)))
    denom_apr = (noteterm * noteterm * efffact) + (150 * term * (noteterm + 1))
    if denom_apr == 0:
        return 0.0
    effapr = (noteterm * efffact * (300 * term + noteterm * efffact)) / denom_apr
    return effapr

# ----------------------------------------------------------------------
# Main processing
# ----------------------------------------------------------------------

def process_data(curr_path, prev_path, lnnote_path):
    """Process data following SAS logic exactly"""
    
    print("[PROCESS] Starting data processing...")
    
    # Read parquet files (they're already cached)
    print("[READ] Reading parquet files...")
    curr_df = pd.read_parquet(curr_path)
    prev_df = pd.read_parquet(prev_path)
    lnnote_df = pd.read_parquet(lnnote_path)
    
    print(f"  Current BNM: {len(curr_df):,} rows")
    print(f"  Previous BNM: {len(prev_df):,} rows")
    print(f"  LNNOTE: {len(lnnote_df):,} rows")
    
    # Debug: Show columns
    print(f"\n[DEBUG] Current BNM columns: {list(curr_df.columns)}")
    print(f"[DEBUG] Previous BNM columns: {list(prev_df.columns)}")
    print(f"[DEBUG] LNNOTE columns: {list(lnnote_df.columns)}")
    
    # Map SECTOR to SECTORCD using SECTCD format
    print("\n[PROCESS] Mapping SECTOR to SECTORCD...")
    if SECTCD:
        lnnote_df['SECTORCD'] = lnnote_df['SECTOR'].map(SECTCD)
        lnnote_df = lnnote_df.dropna(subset=['SECTORCD']).copy()
    else:
        # If no SECTCD, use SECTOR as string
        lnnote_df['SECTORCD'] = lnnote_df['SECTOR'].astype(str)
    print(f"  LNNOTE after SECTOR mapping: {len(lnnote_df):,}")
    
    # Compute EFFAPR for LNNOTE
    print("[PROCESS] Computing EFFAPR for LNNOTE...")
    lnnote_df['EFFAPR'] = lnnote_df.apply(compute_effapr_row, axis=1)
    lnnote_df = lnnote_df[['ACCTNO', 'NOTENO', 'SECTORCD', 'EFFAPR']]
    print(f"  LNNOTE processed: {len(lnnote_df):,}")
    
    # Filter BNM data by PRODUCT (SAS: WHERE PRODUCT IN (131,132,720,725))
    print("[PROCESS] Filtering BNM by PRODUCT...")
    if 'PRODUCT' in curr_df.columns:
        curr_df = curr_df[curr_df['PRODUCT'].isin(PRODUCT_FILTER)].copy()
        prev_df = prev_df[prev_df['PRODUCT'].isin(PRODUCT_FILTER)].copy()
        print(f"  Current BNM filtered: {len(curr_df):,}")
        print(f"  Previous BNM filtered: {len(prev_df):,}")
    else:
        print("[WARN] PRODUCT column not found - skipping product filter")
    
    # Merge BNM with LNNOTE (SAS: MERGE ALW(IN=A) LOAN; BY ACCTNO NOTENO SECTORCD; IF A;)
    print("[PROCESS] Merging current BNM with LNNOTE...")
    curr_merged = curr_df.merge(
        lnnote_df[['ACCTNO', 'NOTENO', 'SECTORCD', 'EFFAPR']],
        on=['ACCTNO', 'NOTENO', 'SECTORCD'],
        how='inner'  # IF A - only keep BNM records
    )
    print(f"  Current merged: {len(curr_merged):,}")
    
    # Prepare ALW1 (previous period) with renamed columns
    prev_renamed = prev_df.rename(columns={
        'BALANCE': 'LASTBAL',
        'NOTETERM': 'LASTNOTE'
    })
    
    # Merge ALW1 and ALW (SAS: MERGE ALW1(IN=A) ALW(IN=B); BY ACCTNO NOTENO SECTORCD;)
    print("[PROCESS] Merging previous and current periods...")
    merged = pd.merge(
        prev_renamed[['ACCTNO', 'NOTENO', 'SECTORCD', 'LASTBAL', 'CUSTCD']],
        curr_merged[['ACCTNO', 'NOTENO', 'SECTORCD', 'BALANCE', 'EFFAPR', 'CUSTCD']],
        on=['ACCTNO', 'NOTENO', 'SECTORCD'],
        how='outer',
        suffixes=('_prev', '_curr')
    )
    print(f"  Merged rows: {len(merged):,}")
    
    # Compute DISBURSE and REPAID (SAS logic)
    print("[PROCESS] Computing DISBURSE/REPAID...")
    
    # Fill NaN with 0
    merged['LASTBAL'] = merged['LASTBAL'].fillna(0)
    merged['BALANCE'] = merged['BALANCE'].fillna(0)
    merged['EFFAPR'] = merged['EFFAPR_curr'].fillna(merged['EFFAPR_prev']).fillna(0)
    merged['CUSTCD'] = merged['CUSTCD_curr'].fillna(merged['CUSTCD_prev']).fillna('')
    
    # Initialize
    merged['DISBURSE'] = 0.0
    merged['REPAID'] = 0.0
    
    # A & B (both exist)
    mask_both = (merged['LASTBAL'] > 0) & (merged['BALANCE'] > 0)
    merged.loc[mask_both, 'REPAID'] = np.where(
        merged.loc[mask_both, 'LASTBAL'] > merged.loc[mask_both, 'BALANCE'],
        merged.loc[mask_both, 'LASTBAL'] - merged.loc[mask_both, 'BALANCE'],
        0.0
    )
    merged.loc[mask_both, 'DISBURSE'] = np.where(
        merged.loc[mask_both, 'LASTBAL'] > merged.loc[mask_both, 'BALANCE'],
        0.0,
        merged.loc[mask_both, 'BALANCE'] - merged.loc[mask_both, 'LASTBAL']
    )
    
    # ^B (only previous exists)
    mask_prev_only = (merged['LASTBAL'] > 0) & (merged['BALANCE'] == 0)
    merged.loc[mask_prev_only, 'REPAID'] = merged.loc[mask_prev_only, 'LASTBAL']
    
    # ^A (only current exists)
    mask_curr_only = (merged['LASTBAL'] == 0) & (merged['BALANCE'] > 0)
    merged.loc[mask_curr_only, 'DISBURSE'] = merged.loc[mask_curr_only, 'BALANCE']
    
    # Compute PRODUCT = DISBURSE * EFFAPR
    merged['PRODUCT'] = merged['DISBURSE'] * merged['EFFAPR']
    
    print(f"  DISBURSE sum: {merged['DISBURSE'].sum():,.2f}")
    print(f"  REPAID sum: {merged['REPAID'].sum():,.2f}")
    print(f"  Records with DISBURSE > 0: {(merged['DISBURSE'] > 0).sum()}")
    
    # Filter where DISBURSE > 0
    merged = merged[merged['DISBURSE'] > 0].copy()
    print(f"  After filtering DISBURSE > 0: {len(merged):,}")
    
    if len(merged) == 0:
        print("[WARN] No records with DISBURSE > 0")
        return pd.DataFrame(columns=['SECTCD', 'DISBURSE', 'PRODUCT', 'CUSTCD'])
    
    # Apply SECTA and SECTB formats (SAS logic)
    print("[PROCESS] Applying SECTA and SECTB formats...")
    rows = []
    
    for _, row in merged.iterrows():
        sectcd = row['SECTORCD']
        if pd.isna(sectcd) or sectcd == '':
            continue
        
        # Try SECTA first
        if SECTA:
            sect_a = SECTA.get(sectcd, '')
            if sect_a and sect_a != ' ':
                rows.append({
                    'SECTCD': sect_a,
                    'DISBURSE': row['DISBURSE'],
                    'PRODUCT': row['PRODUCT'],
                    'CUSTCD': row['CUSTCD']
                })
        
        # Try SECTB
        if SECTB:
            sect_b = SECTB.get(sectcd, '')
            if sect_b and sect_b != ' ':
                rows.append({
                    'SECTCD': sect_b,
                    'DISBURSE': row['DISBURSE'],
                    'PRODUCT': row['PRODUCT'],
                    'CUSTCD': row['CUSTCD']
                })
    
    expanded = pd.DataFrame(rows)
    print(f"  Expanded rows: {len(expanded):,}")
    
    return expanded

# ----------------------------------------------------------------------
# Summarise function
# ----------------------------------------------------------------------

def summarise(expanded, label, custcd_filter=None):
    if len(expanded) == 0:
        return pd.DataFrame(columns=['BNMCODE', 'AMOUNT', 'WEIGHTED'])
    
    if custcd_filter is not None:
        expanded = expanded[expanded['CUSTCD'].astype(str).isin(custcd_filter)].copy()
        if len(expanded) == 0:
            return pd.DataFrame(columns=['BNMCODE', 'AMOUNT', 'WEIGHTED'])
    
    summary = expanded.groupby('SECTCD', as_index=False).agg({
        'DISBURSE': 'sum',
        'PRODUCT': 'sum'
    })
    
    summary = summary[summary['DISBURSE'] > 0].copy()
    
    if len(summary) == 0:
        return pd.DataFrame(columns=['BNMCODE', 'AMOUNT', 'WEIGHTED'])
    
    summary['WEIGHTED'] = summary['PRODUCT'] / summary['DISBURSE']
    summary['WEIGHTED'] = summary['WEIGHTED'].fillna(0.0)
    summary['BNMCODE'] = '673400000' + summary['SECTCD'].astype(str) + 'Y'
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
    
    if len(df) == 0:
        fh.write("  No records found\n\n")
        return
        
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
    print(f"[DATE] REPTMON={macros['REPTMON']}, NOWK={macros['NOWK']}")
    print(f"[DATE] REPTMON1={macros['REPTMON1']}, NOWK1={macros['NOWK1']}")

    # Find input files
    curr_path = find_loan_file(INPUT_DIR, macros['REPTMON'], macros['NOWK'])
    prev_path = find_loan_file(INPUT_DIR, macros['REPTMON1'], macros['NOWK1'])
    lnnote_path = INPUT_DIR / "lnnote.sas7bdat"

    if not curr_path:
        curr_path = find_latest_loan_file(INPUT_DIR)
        if curr_path:
            print(f"[WARN] Using latest loan as current: {curr_path.name}")
        else:
            raise FileNotFoundError(f"No loan file found in {INPUT_DIR}")
    
    if not prev_path:
        all_files = list(INPUT_DIR.glob("loan*.sas7bdat"))
        for f in all_files:
            if f != curr_path:
                prev_path = f
                print(f"[WARN] Using alternative as previous: {prev_path.name}")
                break
        if not prev_path:
            prev_path = curr_path
            print(f"[WARN] Using same file for previous (no alternative found)")
    
    if not lnnote_path.exists():
        raise FileNotFoundError(f"LNNOTE not found: {lnnote_path}")

    print("\n[READ] Loading files to parquet cache (if needed)...")
    curr_parquet = read_sas_cached(curr_path)
    prev_parquet = read_sas_cached(prev_path)
    lnnote_parquet = read_sas_cached(lnnote_path)

    # Process data
    expanded = process_data(curr_parquet, prev_parquet, lnnote_parquet)

    print("\n[PROCESS] Summarising all customers...")
    all_summary = summarise(expanded, "ALL")

    print("[PROCESS] Summarising SMI (CUSTCD 66-69)...")
    smi_summary = summarise(expanded, "SMI", custcd_filter=SMI_CUSTCD)

    print(f"\n[OUTPUT] Writing report to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(f"EIBWHP01 REPORT GENERATED {reptdate.strftime('%d-%m-%Y')}\n")
        f.write(f"REPTMON: {macros['REPTMON']}, NOWK: {macros['NOWK']}\n")
        f.write("="*80 + "\n\n")
        
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
