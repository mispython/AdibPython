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

# Product filter - only these products should be included
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

# If formats are empty, use fallback based on production output
if not SECTCD:
    print("[FMT] Using fallback SECTCD mapping")
    # Production output shows these BNMCODES:
    # 6734000001000Y, 6734000002000Y, 6734000003000Y, 6734000005001Y, etc.
    # So SECTCD values are like: 1000, 2000, 3000, 5001, 5002, 5003, 5004, 5006, 6100, 6300, 7000, 8310, 8320, 9000
    SECTCD = {
        1: '1000', 2: '2000', 3: '3000', 4: '4000', 5: '5001',
        6: '6000', 7: '7000', 8: '8000', 9: '9000'
    }

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
    
    try:
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
    except Exception as e:
        print(f"  [{tag}] ERROR: {e}")
        if cache_path.exists():
            cache_path.unlink()
        raise

def read_sas_cached(sas_path):
    cache_path = CACHE_DIR / f"{sas_path.stem}.parquet"
    if cache_fresh(sas_path, cache_path):
        print(f"[READ] Using cache: {cache_path.name}")
        try:
            return pd.read_parquet(cache_path)
        except Exception as e:
            print(f"[WARN] Cache read failed: {e}")
            cache_path.unlink()
    
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

    if noteterm == 0:
        return 0.0

    if intamt <= 0.01:
        intamt = (intrate * netproc * noteterm / 1200) - intearn2

    term = 12 if noteterm > 12 else noteterm
    
    if netproc + intearn2 == 0:
        return 0.0
        
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
    print("[PROCESS] Filtering products...")
    
    # Filter current BNM by PRODUCT
    if 'PRODUCT' in curr.columns:
        curr = curr[curr['PRODUCT'].isin(PRODUCT_FILTER)].copy()
        print(f"  Current BNM after product filter: {len(curr):,}")
    else:
        print("[WARN] PRODUCT column not found in current BNM")
    
    # Filter previous BNM by PRODUCT
    if 'PRODUCT' in prev.columns:
        prev = prev[prev['PRODUCT'].isin(PRODUCT_FILTER)].copy()
        print(f"  Previous BNM after product filter: {len(prev):,}")
    else:
        print("[WARN] PRODUCT column not found in previous BNM")
    
    print("[PROCESS] Processing LNNOTE...")
    
    # Map SECTOR to SECTORCD using SECTCD format
    if SECTCD:
        lnnote['SECTORCD'] = lnnote['SECTOR'].map(SECTCD)
        lnnote = lnnote.dropna(subset=['SECTORCD']).copy()
    else:
        print("[WARN] No SECTCD mapping available – using SECTOR as string")
        lnnote['SECTORCD'] = lnnote['SECTOR'].astype(str)
    
    print(f"  LNNOTE rows after sector mapping: {len(lnnote):,}")
    
    # Add EFFAPR
    lnnote = add_effapr(lnnote)
    keep = ['ACCTNO', 'NOTENO', 'SECTORCD', 'EFFAPR']
    lnnote = lnnote[keep]
    
    print("[PROCESS] Merging with BNM files...")
    
    # Merge current BNM with lnnote
    curr_merged = curr.merge(lnnote, on=['ACCTNO', 'NOTENO', 'SECTORCD'], how='left')
    curr_merged['EFFAPR'] = curr_merged['EFFAPR'].fillna(0.0)
    
    # Merge previous BNM with lnnote
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
    
    print(f"  Merged rows: {len(merged):,}")
    
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
    
    # PRODUCT = DISBURSE * EFFAPR
    merged['EFFAPR'] = merged['EFFAPR_curr'].fillna(merged['EFFAPR_prev'])
    merged['PRODUCT'] = merged['DISBURSE'] * merged['EFFAPR']
    
    # Expand by SECTA and SECTB (only if they exist)
    print("[PROCESS] Expanding by SECTA and SECTB...")
    rows = []
    
    if SECTA or SECTB:
        for _, row in merged.iterrows():
            sectcd = row['SECTORCD']
            # Skip if SECTORCD is None or empty
            if pd.isna(sectcd) or sectcd == '':
                continue
                
            a = SECTA.get(sectcd, '') if SECTA else ''
            b = SECTB.get(sectcd, '') if SECTB else ''
            
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
    else:
        # No formats available, use SECTORCD as-is
        for _, row in merged.iterrows():
            sectcd = row['SECTORCD']
            if pd.isna(sectcd) or sectcd == '':
                continue
            rows.append({
                'SECTCD': sectcd,
                'DISBURSE': row['DISBURSE'],
                'PRODUCT': row['PRODUCT'],
                'CUSTCD': row.get('CUSTCD', ''),
                'AMTIND': row.get('AMTIND', 0.0),
                'APPRLIM2': row.get('APPRLIM2', 0.0)
            })
    
    expanded = pd.DataFrame(rows)
    print(f"  Expanded rows: {len(expanded):,}")
    return expanded

def summarise(expanded, label, custcd_filter=None):
    if custcd_filter is not None:
        expanded = expanded[expanded['CUSTCD'].isin(custcd_filter)].copy()
        print(f"  {label} filtered rows: {len(expanded):,}")
    
    if len(expanded) == 0:
        print(f"  {label}: No data found")
        return pd.DataFrame(columns=['BNMCODE', 'AMOUNT', 'WEIGHTED'])
    
    summary = expanded.groupby('SECTCD', as_index=False).agg({
        'DISBURSE': 'sum',
        'PRODUCT': 'sum'
    })
    
    # Filter out rows where DISBURSE is 0 (matches SAS logic where SECTCD ^= ' ')
    summary = summary[summary['DISBURSE'] > 0].copy()
    
    if len(summary) == 0:
        print(f"  {label}: No data with non-zero DISBURSE")
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

    # Build file paths
    curr_path = INPUT_DIR / f"loan{macros['REPTMON']}{macros['NOWK']}.sas7bdat"
    prev_path = INPUT_DIR / f"loan{macros['REPTMON1']}{macros['NOWK1']}.sas7bdat"
    lnnote_path = INPUT_DIR / "lnnote.sas7bdat"

    # Check and find files
    if not curr_path.exists():
        candidates = list(INPUT_DIR.glob("loan*.sas7bdat"))
        if candidates:
            curr_path = max(candidates, key=lambda p: p.stat().st_mtime)
            print(f"[WARN] Using latest loan as current: {curr_path.name}")
        else:
            raise FileNotFoundError(f"No loan file found in {INPUT_DIR}")
    
    if not prev_path.exists():
        print(f"[WARN] Previous BNM not found: {prev_path}")
        candidates = list(INPUT_DIR.glob("loan*.sas7bdat"))
        if candidates:
            for c in candidates:
                if c != curr_path:
                    prev_path = c
                    print(f"[WARN] Using alternative as previous: {prev_path.name}")
                    break
            else:
                prev_path = curr_path
                print(f"[WARN] Using same file for previous (no alternative found)")
        else:
            raise FileNotFoundError(f"No previous loan file found")
    
    if not lnnote_path.exists():
        raise FileNotFoundError(f"LNNOTE not found: {lnnote_path}")

    print("\n[READ] Loading files (parquet cache)...")
    lnnote = read_sas_cached(lnnote_path)
    print(f"  LNNOTE: {len(lnnote):,} rows")
    
    curr = read_sas_cached(curr_path)
    print(f"  Current BNM: {len(curr):,} rows")
    
    prev = read_sas_cached(prev_path)
    print(f"  Previous BNM: {len(prev):,} rows")

    print("\n[PROCESS] Building expanded loan data...")
    expanded = build_expanded(lnnote, curr, prev)

    print("\n[PROCESS] Summarising all customers...")
    all_summary = summarise(expanded, "ALL")

    print("[PROCESS] Summarising SMI (CUSTCD 66-69)...")
    smi_summary = summarise(expanded, "SMI", custcd_filter=SMI_CUSTCD)

    print(f"\n[OUTPUT] Writing report to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(f"EIBWHP01 REPORT GENERATED {reptdate.strftime('%d-%m-%Y')}\n")
        f.write(f"REPTMON: {macros['REPTMON']}, NOWK: {macros['NOWK']}\n")
        f.write("="*80 + "\n")
        f.write(f"BNM RECORDS: {len(curr):>10}\n")
        f.write(f"LOAN RECORDS: {len(lnnote):>10}\n")
        f.write(f"REPORT DATE: {reptdate.strftime('%d-%m-%Y')}\n")
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
