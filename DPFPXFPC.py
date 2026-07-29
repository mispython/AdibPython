#!/usr/bin/env python3
"""
EIBWHP01 - Products 131,132,720,725 Report (All & SMI)
Optimized for memory efficiency using DuckDB and chunking
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
ROW_LIMIT = int(os.environ.get("ROW_LIMIT", 0))

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
# EFFAPR calculation
# ----------------------------------------------------------------------

def compute_effapr(intamt, intrate, netproc, noteterm, intearn2):
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

# ----------------------------------------------------------------------
# Main processing using DuckDB
# ----------------------------------------------------------------------

def process_with_duckdb(curr_path, prev_path, lnnote_path):
    """Process data using DuckDB to minimize memory usage"""
    
    print("[PROCESS] Starting DuckDB processing...")
    con = duckdb.connect(database=":memory:")
    
    # Register parquet files as views
    con.execute(f"CREATE VIEW curr_bnm AS SELECT * FROM read_parquet('{curr_path}')")
    con.execute(f"CREATE VIEW prev_bnm AS SELECT * FROM read_parquet('{prev_path}')")
    con.execute(f"CREATE VIEW lnnote_view AS SELECT * FROM read_parquet('{lnnote_path}')")
    
    print("[PROCESS] Filtering products...")
    
    # Step 1: Filter and prepare current BNM with ALL needed columns
    curr_filtered = con.execute(f"""
        SELECT 
            ACCTNO, 
            NOTENO, 
            SECTORCD, 
            BALANCE, 
            CUSTCD,
            AMTIND, 
            APPRLIM2
        FROM curr_bnm
        WHERE PRODUCT IN ({','.join(map(str, PRODUCT_FILTER))})
    """).df()
    print(f"  Current BNM filtered: {len(curr_filtered):,}")
    
    # Step 2: Filter and prepare previous BNM
    prev_filtered = con.execute(f"""
        SELECT 
            ACCTNO, 
            NOTENO, 
            SECTORCD, 
            BALANCE as LASTBAL,
            CUSTCD,
            AMTIND, 
            APPRLIM2
        FROM prev_bnm
        WHERE PRODUCT IN ({','.join(map(str, PRODUCT_FILTER))})
    """).df()
    print(f"  Previous BNM filtered: {len(prev_filtered):,}")
    
    # Step 3: Get LNNOTE data
    print("[PROCESS] Getting LNNOTE data...")
    lnnote_cols = con.execute("""
        SELECT ACCTNO, NOTENO, SECTOR, INTAMT, INTRATE, NETPROC, NOTETERM, INTEARN2
        FROM lnnote_view
    """).df()
    print(f"  LNNOTE rows: {len(lnnote_cols):,}")
    
    # Step 4: Compute EFFAPR for LNNOTE in chunks
    print("[PROCESS] Computing EFFAPR for LNNOTE...")
    
    chunk_size = 200000
    lnnote_processed = []
    total_chunks = (len(lnnote_cols) + chunk_size - 1) // chunk_size
    
    for i in range(0, len(lnnote_cols), chunk_size):
        chunk = lnnote_cols.iloc[i:i+chunk_size].copy()
        chunk['EFFAPR'] = chunk.apply(
            lambda row: compute_effapr(
                row['INTAMT'], row['INTRATE'], row['NETPROC'], 
                row['NOTETERM'], row['INTEARN2']
            ), axis=1
        )
        lnnote_processed.append(chunk[['ACCTNO', 'NOTENO', 'SECTOR', 'EFFAPR']])
        if (i // chunk_size + 1) % 5 == 0:
            print(f"  Processed chunk {i//chunk_size + 1}/{total_chunks}")
        gc.collect()
    
    lnnote = pd.concat(lnnote_processed, ignore_index=True)
    del lnnote_cols, lnnote_processed
    gc.collect()
    
    # Map SECTOR to SECTORCD
    if SECTCD:
        lnnote['SECTORCD'] = lnnote['SECTOR'].map(SECTCD)
        lnnote = lnnote.dropna(subset=['SECTORCD']).copy()
    else:
        lnnote['SECTORCD'] = lnnote['SECTOR'].astype(str)
    
    lnnote = lnnote[['ACCTNO', 'NOTENO', 'SECTORCD', 'EFFAPR']]
    print(f"  LNNOTE processed: {len(lnnote):,}")
    
    # Step 5: Merge with BNM data
    print("[PROCESS] Merging data...")
    
    # Merge current with lnnote
    curr_merged = curr_filtered.merge(lnnote, on=['ACCTNO', 'NOTENO', 'SECTORCD'], how='left')
    curr_merged['EFFAPR'] = curr_merged['EFFAPR'].fillna(0.0)
    
    # Merge previous with lnnote
    prev_merged = prev_filtered.merge(lnnote, on=['ACCTNO', 'NOTENO', 'SECTORCD'], how='left')
    prev_merged['EFFAPR'] = prev_merged['EFFAPR'].fillna(0.0)
    
    # Clean up
    del lnnote
    gc.collect()
    
    # Step 6: Full outer merge
    merged = pd.merge(
        prev_merged[['ACCTNO', 'NOTENO', 'SECTORCD', 'LASTBAL', 'EFFAPR', 'CUSTCD']],
        curr_merged[['ACCTNO', 'NOTENO', 'SECTORCD', 'BALANCE', 'EFFAPR', 'CUSTCD']],
        on=['ACCTNO', 'NOTENO', 'SECTORCD'],
        how='outer',
        suffixes=('_prev', '_curr')
    )
    
    del curr_filtered, prev_filtered, curr_merged, prev_merged
    gc.collect()
    
    print(f"  Merged rows: {len(merged):,}")
    
    # Step 7: Compute DISBURSE/REPAID
    print("[PROCESS] Computing DISBURSE/REPAID...")
    
    # Fill NaN values
    merged['LASTBAL'] = merged['LASTBAL'].fillna(0.0)
    merged['BALANCE'] = merged['BALANCE'].fillna(0.0)
    merged['CUSTCD'] = merged['CUSTCD_curr'].fillna(merged['CUSTCD_prev']).fillna('')
    merged['EFFAPR'] = merged['EFFAPR_curr'].fillna(merged['EFFAPR_prev'])
    
    # Compute DISBURSE and REPAID
    merged['DISBURSE'] = 0.0
    merged['REPAID'] = 0.0
    
    # Case 1: Both previous and current exist
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
    
    # Case 2: Only previous exists
    mask_prev_only = (merged['LASTBAL'] > 0) & (merged['BALANCE'] == 0)
    merged.loc[mask_prev_only, 'REPAID'] = merged.loc[mask_prev_only, 'LASTBAL']
    
    # Case 3: Only current exists
    mask_curr_only = (merged['LASTBAL'] == 0) & (merged['BALANCE'] > 0)
    merged.loc[mask_curr_only, 'DISBURSE'] = merged.loc[mask_curr_only, 'BALANCE']
    
    # Compute PRODUCT
    merged['PRODUCT'] = merged['DISBURSE'] * merged['EFFAPR']
    
    # Filter out zero DISBURSE
    merged = merged[merged['DISBURSE'] > 0].copy()
    print(f"  After filtering zero DISBURSE: {len(merged):,}")
    
    if len(merged) == 0:
        print("[WARN] No records with DISBURSE > 0")
        return pd.DataFrame(columns=['SECTCD', 'DISBURSE', 'PRODUCT', 'CUSTCD'])
    
    # Step 8: Expand by SECTA and SECTB
    print("[PROCESS] Expanding by SECTA/SECTB...")
    rows = []
    
    for _, row in merged.iterrows():
        sectcd = row['SECTORCD']
        if pd.isna(sectcd) or sectcd == '':
            continue
            
        if SECTA and SECTB:
            a = SECTA.get(sectcd, '')
            b = SECTB.get(sectcd, '')
            
            if not a and not b:
                a = str(sectcd)
            
            if a:
                rows.append({
                    'SECTCD': a,
                    'DISBURSE': row['DISBURSE'],
                    'PRODUCT': row['PRODUCT'],
                    'CUSTCD': row.get('CUSTCD', '')
                })
            if b:
                rows.append({
                    'SECTCD': b,
                    'DISBURSE': row['DISBURSE'],
                    'PRODUCT': row['PRODUCT'],
                    'CUSTCD': row.get('CUSTCD', '')
                })
        else:
            rows.append({
                'SECTCD': sectcd,
                'DISBURSE': row['DISBURSE'],
                'PRODUCT': row['PRODUCT'],
                'CUSTCD': row.get('CUSTCD', '')
            })
    
    expanded = pd.DataFrame(rows)
    print(f"  Expanded rows: {len(expanded):,}")
    
    # Clean up
    del merged
    gc.collect()
    con.close()
    
    return expanded

# ----------------------------------------------------------------------
# Summarise function
# ----------------------------------------------------------------------

def summarise(expanded, label, custcd_filter=None):
    if len(expanded) == 0:
        print(f"  {label}: No data found")
        return pd.DataFrame(columns=['BNMCODE', 'AMOUNT', 'WEIGHTED'])
    
    # Check if CUSTCD column exists
    if 'CUSTCD' not in expanded.columns:
        print(f"  {label}: CUSTCD column not found - treating all as non-SMI")
        if custcd_filter is not None:
            print(f"  {label}: SMI filter skipped - CUSTCD missing")
            return pd.DataFrame(columns=['BNMCODE', 'AMOUNT', 'WEIGHTED'])
    
    # Apply SMI filter if specified
    if custcd_filter is not None and 'CUSTCD' in expanded.columns:
        expanded = expanded[expanded['CUSTCD'].astype(str).isin(custcd_filter)].copy()
        print(f"  {label} filtered rows: {len(expanded):,}")
        
        if len(expanded) == 0:
            print(f"  {label}: No SMI records found")
            return pd.DataFrame(columns=['BNMCODE', 'AMOUNT', 'WEIGHTED'])
    
    # Summarise
    summary = expanded.groupby('SECTCD', as_index=False).agg({
        'DISBURSE': 'sum',
        'PRODUCT': 'sum'
    })
    
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

    # Find input files
    curr_path = find_loan_file(INPUT_DIR, macros['REPTMON'], macros['NOWK'])
    prev_path = find_loan_file(INPUT_DIR, macros['REPTMON1'], macros['NOWK1'])
    lnnote_path = INPUT_DIR / "lnnote.sas7bdat"

    # Handle current file
    if not curr_path:
        curr_path = find_latest_loan_file(INPUT_DIR)
        if curr_path:
            print(f"[WARN] Using latest loan as current: {curr_path.name}")
        else:
            raise FileNotFoundError(f"No loan file found in {INPUT_DIR}")
    
    # Handle previous file
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
    
    # Get row counts
    curr_count = pd.read_parquet(curr_parquet).shape[0]
    prev_count = pd.read_parquet(prev_parquet).shape[0]
    lnnote_count = pd.read_parquet(lnnote_parquet).shape[0]
    
    print(f"  Current BNM: {curr_count:,} rows")
    print(f"  Previous BNM: {prev_count:,} rows")
    print(f"  LNNOTE: {lnnote_count:,} rows")

    # Process using DuckDB
    expanded = process_with_duckdb(curr_parquet, prev_parquet, lnnote_parquet)

    print("\n[PROCESS] Summarising all customers...")
    all_summary = summarise(expanded, "ALL")

    print("[PROCESS] Summarising SMI (CUSTCD 66-69)...")
    smi_summary = summarise(expanded, "SMI", custcd_filter=SMI_CUSTCD)

    print(f"\n[OUTPUT] Writing report to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(f"EIBWHP01 REPORT GENERATED {reptdate.strftime('%d-%m-%Y')}\n")
        f.write(f"REPTMON: {macros['REPTMON']}, NOWK: {macros['NOWK']}\n")
        f.write("="*80 + "\n")
        f.write(f"BNM RECORDS: {curr_count:>10}\n")
        f.write(f"LOAN RECORDS: {lnnote_count:>10}\n")
        f.write(f"REPORT DATE: {reptdate.strftime('%d-%m-%Y')}\n")
        f.write("="*80 + "\n\n")
        
        write_report(all_summary, "REPORT ON PRODUCTS 131,132,720,725", macros['RDATE'], f)
        write_report(smi_summary, "SMI ACCTS (CUSTCD 66,67,68,69)", macros['RDATE'], f)

    print(f"  Output written: {OUTPUT_FILE}")
    
    # Clean up
    del expanded, all_summary, smi_summary
    gc.collect()
    
    print("========== END JOB EIBWHP01 ==========")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[JOB FAILED] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(8)
