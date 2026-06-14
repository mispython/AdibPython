#!/usr/bin/env python3
"""BNM Liquidity Report for Trade Finance - Processes BA and TR transactions"""

import pyreadstat
import polars as pl
from datetime import datetime, timedelta, date
from pathlib import Path
import warnings
import re

# Import from PBBLNFMT (only what's actually available)
from PBBLNFMT import (
    get_days_in_month,      # ✅ This exists
    format_liqpfmt,         # ✅ This exists (LIQPFMT format)
    format_btcustcd,        # ✅ This exists (BTCUSTCD format)
)

# Import from PBBELF
from PBBELF import (
    calculate_next_bldate,
    calculate_remmth,
)

# REMFMT is NOT in PBBLNFMT - defined locally (from SAS PROC FORMAT)
def get_remfmt(remmth):
    """Format remaining months into BNM codes (REMFMT from SAS)"""
    if remmth is None:
        return '07'  # MISSING
    elif remmth <= 0.1:
        return '01'  # UP TO 1 WK
    elif remmth <= 1:
        return '02'  # >1 WK - 1 MTH
    elif remmth <= 3:
        return '03'  # >1 MTH - 3 MTHS
    elif remmth <= 6:
        return '04'  # >3 - 6 MTHS
    elif remmth <= 12:
        return '05'  # >6 MTHS - 1 YR
    else:
        return '06'  # > 1 YEAR

warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURATION
# ============================================================================
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

BTDTL_BASE = INPUT_DIR / "bt_btdtl"
PBA01_BASE = INPUT_DIR / "pba01"

OUTPUT_PARQUET = OUTPUT_DIR / "bt.parquet"
OUTPUT_CSV = OUTPUT_DIR / "bt.csv"
OUTPUT_LOG = OUTPUT_DIR / "bt_processing.log"

BASE_DATE = date(1960, 1, 1)
USE_LATEST_FALLBACK = True


# ============================================================================
# SAS FILE READERS WITH LATEST FILE DETECTION
# ============================================================================
def find_latest_file(base_path):
    """Find the latest file matching pattern (YYYYMMDD)"""
    parent = base_path.parent
    stem = base_path.stem
    
    patterns = [
        rf"{stem}(\d{{8}})\.sas7bdat$",
        rf"{stem}_(\d{{8}})\.sas7bdat$",
        rf"{stem}(\d{{6}})\.sas7bdat$",
        rf"{stem}_(\d{{6}})\.sas7bdat$",
    ]
    
    latest_file, latest_date = None, None
    for f in parent.glob(f"{stem}*"):
        if not f.suffix == '.sas7bdat':
            continue
        for pattern in patterns:
            match = re.search(pattern, str(f.name))
            if match:
                date_str = match.group(1)
                try:
                    if len(date_str) == 8:
                        file_date = datetime.strptime(date_str, '%Y%m%d').date()
                    else:
                        year = 2000 + int(date_str[:2])
                        file_date = datetime.strptime(f"{year}{date_str[2:]}", '%Y%m%d').date()
                    if latest_date is None or file_date > latest_date:
                        latest_date, latest_file = file_date, f
                except:
                    pass
    return latest_file, latest_date


def read_sas(filepath):
    print(f"  Reading: {filepath.name}")
    df, _ = pyreadstat.read_sas7bdat(str(filepath))
    return pl.from_pandas(df)


def get_sas_file(base_path, y, m, d):
    """Get SAS file - exact match first, then latest if fallback enabled"""
    y2, m2, d2 = f"{y%100:02d}", f"{m:02d}", f"{d:02d}"
    y4 = f"{y:04d}"
    
    exact_patterns = [
        base_path.parent / f"{base_path.stem}{y4}{m2}{d2}.sas7bdat",
        base_path.parent / f"{base_path.stem}{y2}{m2}{d2}.sas7bdat",
        base_path.parent / f"{base_path.stem}_{y4}{m2}{d2}.sas7bdat",
        base_path.parent / f"{base_path.stem}_{y2}{m2}{d2}.sas7bdat",
    ]
    
    for filepath in exact_patterns:
        if filepath.exists():
            print(f"  Using exact file: {filepath.name}")
            return read_sas(filepath)
    
    if USE_LATEST_FALLBACK:
        latest_file, latest_date = find_latest_file(base_path)
        if latest_file:
            print(f"  WARNING: Using latest: {latest_file.name} (dated {latest_date})")
            with open(OUTPUT_LOG, 'a') as log:
                log.write(f"{datetime.now()}: Using {latest_file.name}\n")
            return read_sas(latest_file)
    
    raise FileNotFoundError(f"No file found for date {y2}{m2}{d2}")


# ============================================================================
# RECORD PROCESSING FUNCTION
# ============================================================================
def add_record(records, prefix, item, cust, remmth, amount):
    """Add a single BNM record"""
    records.append({
        'BNMCODE': f"{prefix}{item}{cust}{get_remfmt(remmth)}0000Y",
        'AMOUNT': amount
    })


# ============================================================================
# MAIN PROCESSING
# ============================================================================
def main(reptdate=None):
    global USE_LATEST_FALLBACK
    
    # Step 1: Set report date
    reptdate = reptdate or date.today()
    rpyr, rpmth, rpday = reptdate.year, reptdate.month, reptdate.day
    reptdate_sas = (reptdate - BASE_DATE).days
    print(f"\n{'='*60}")
    print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
    print(f"Report Year: {rpyr}, Month: {rpmth}, Day: {rpday}")
    print(f"{'='*60}")
    
    # Step 2: Calculate runoff date (last day of month)
    last_day = get_days_in_month(rpyr, rpmth)
    runoff_dt = date(rpyr, rpmth, last_day)
    runoff_sas = (runoff_dt - BASE_DATE).days
    print(f"Runoff Date: {runoff_dt.strftime('%d/%m/%Y')}")
    
    records = []
    
    # Step 3-4: Process BA data
    print(f"\n--- Processing BA Transactions ---")
    try:
        # Read BTDTL
        btdtl = get_sas_file(BTDTL_BASE, rpyr, rpmth, rpday)
        btdtl = btdtl.filter((pl.col('ISSDTE') > 0) | (pl.col('EXPRDATE') > 0))
        btdtl = btdtl.select(['TRANSREF', 'ISSDTE', 'EXPRDATE', 'PAYAMT'])
        btdtl = btdtl.sort(['TRANSREF', 'ISSDTE'], descending=[False, True]).unique('TRANSREF', keep='first')
        
        # Read PBA01
        pba = get_sas_file(PBA01_BASE, rpyr, rpmth, rpday)
        pba = pba.with_columns(pl.col('TRANSREF').cast(pl.Utf8).str.slice(1, 8).alias('TRANSREF'))
        ba_data = pba.join(btdtl, on='TRANSREF', how='left')
        
        ba_count = 0
        for row in ba_data.iter_rows(named=True):
            balance = (row.get('FCVALUE',0) or 0) - (row.get('UNEARNED',0) or 0)
            if balance == 0:
                continue
            
            # Customer code using format_btcustcd from PBBLNFMT
            custcd = row.get('CUSTCD', 0)
            cust = format_btcustcd(custcd) if custcd else '79'
            
            # Product type using format_liqpfmt from PBBLNFMT (LIQPFMT format)
            product = row.get('PRODUCT', 0) or 0
            prod_type = format_liqpfmt(product)
            
            # Determine item code (matching SAS logic from BA data step)
            if cust in ['77', '78', '95', '96']:  # Bumiputra/Non-Bumiputra/Foreign individuals
                if prod_type == 'HL':
                    item = '214'
                else:
                    item = '219'
            else:
                if prod_type == 'FL':
                    item = '211'
                elif prod_type == 'RC':
                    item = '212'
                else:
                    item = '219'
            
            # Hardcode override for product 100
            if product == 100:
                item = '212'
            
            # Calculate days past due
            days = (reptdate_sas - (row.get('BLDATE',0) or 0)) if (row.get('BLDATE',0) or 0) > 0 else 0
            
            remmth, cur_bal, cur_bldate, expr_sas = None, balance, row.get('BLDATE',0) or 0, row.get('EXPRDATE',0) or 0
            payamt = row.get('PAYAMT',0) or 0
            issdte = row.get('ISSDTE',0) or 0
            
            if expr_sas and expr_sas <= runoff_sas:
                remmth = None
            elif expr_sas and (expr_sas - runoff_sas) < 8:
                remmth = 0.1
            elif expr_sas:
                if cur_bldate <= 0 and issdte > 0:
                    cur_bldate = issdte
                    while cur_bldate > 0 and cur_bldate <= reptdate_sas:
                        bl_date = BASE_DATE + timedelta(days=int(cur_bldate))
                        iss_date = BASE_DATE + timedelta(days=issdte) if issdte > 0 else bl_date
                        nxt = calculate_next_bldate(bl_date, iss_date, '3', 6)
                        cur_bldate = (nxt - BASE_DATE).days if nxt else 0
                
                if payamt < 0:
                    payamt = 0
                if cur_bldate > 0 and (cur_bldate > expr_sas or cur_bal <= payamt):
                    cur_bldate = expr_sas
                
                while cur_bldate > 0 and cur_bldate <= expr_sas:
                    if cur_bldate <= runoff_sas:
                        remmth = None
                    elif (cur_bldate - runoff_sas) < 8:
                        remmth = 0.1
                    else:
                        mat_date = BASE_DATE + timedelta(days=int(cur_bldate))
                        remmth = calculate_remmth(mat_date, runoff_dt, runoff_dt.year, runoff_dt.month, runoff_dt.day)
                    
                    if (remmth and remmth > 1) or cur_bldate == expr_sas:
                        break
                    
                    if payamt > 0 and remmth is not None:
                        cur_bal -= payamt
                        # Part 2-RM (95) - standard
                        add_record(records, '95', item, cust, remmth, payamt)
                        # Part 1-RM (93) - with NPL adjustment if days > 89
                        npl_rem = 13 if days > 89 else remmth
                        add_record(records, '93', item, cust, npl_rem, payamt)
                        ba_count += 2
                    
                    bl_date = BASE_DATE + timedelta(days=int(cur_bldate))
                    iss_date = BASE_DATE + timedelta(days=issdte) if issdte > 0 else bl_date
                    nxt = calculate_next_bldate(bl_date, iss_date, '3', 6)
                    cur_bldate = (nxt - BASE_DATE).days if nxt else 0
                    if cur_bldate > 0 and (cur_bldate > expr_sas or cur_bal <= payamt):
                        cur_bldate = expr_sas
                
                if cur_bldate > 0:
                    if cur_bldate <= runoff_sas:
                        remmth = None
                    elif (cur_bldate - runoff_sas) < 8:
                        remmth = 0.1
                    else:
                        mat_date = BASE_DATE + timedelta(days=int(cur_bldate))
                        remmth = calculate_remmth(mat_date, runoff_dt, runoff_dt.year, runoff_dt.month, runoff_dt.day)
            
            if cur_bal != 0:
                add_record(records, '95', item, cust, remmth, cur_bal)
                npl_rem = 13 if days > 89 else remmth
                add_record(records, '93', item, cust, npl_rem, cur_bal)
                ba_count += 2
        
        print(f"  BA records created: {ba_count}")
        
    except Exception as e:
        print(f"  BA Processing Error: {e}")
    
    # Step 5: Process TR data
    print(f"\n--- Processing TR Transactions ---")
    try:
        tr_full = get_sas_file(BTDTL_BASE, rpyr, rpmth, rpday)
        tr_data = tr_full.filter((~pl.col('LIABCODE').cast(pl.Utf8).is_in(['BAI','BAP','BAS','BAE'])) & 
                                  (pl.col('DIRCTIND').cast(pl.Utf8) == 'D'))
        
        tr_count = 0
        for row in tr_data.iter_rows(named=True):
            outstand = row.get('OUTSTAND',0) or 0
            if outstand == 0:
                continue
            
            custcd = row.get('CUSTCD', 0)
            cust = format_btcustcd(custcd) if custcd else '79'
            
            product = row.get('PRODUCT', 0) or 0
            prod_type = format_liqpfmt(product)
            
            if cust in ['77', '78', '95', '96']:
                if prod_type == 'HL':
                    item = '214'
                else:
                    item = '219'
            else:
                if prod_type == 'FL':
                    item = '211'
                elif prod_type == 'RC':
                    item = '212'
                else:
                    item = '219'
            
            if product == 100:
                item = '212'
            
            days = (reptdate_sas - (row.get('BLDATE',0) or 0)) if (row.get('BLDATE',0) or 0) > 0 else 0
            
            remmth, cur_out, cur_bldate, expr_sas = None, outstand, row.get('BLDATE',0) or 0, row.get('EXPRDATE',0) or 0
            payamt = row.get('PAYAMT',0) or 0
            issdte = row.get('ISSDTE',0) or 0
            
            if expr_sas and expr_sas <= runoff_sas:
                remmth = None
            elif expr_sas and (expr_sas - runoff_sas) < 8:
                remmth = 0.1
            elif expr_sas:
                if cur_bldate <= 0 and issdte > 0:
                    cur_bldate = issdte
                    while cur_bldate > 0 and cur_bldate <= reptdate_sas:
                        bl_date = BASE_DATE + timedelta(days=int(cur_bldate))
                        iss_date = BASE_DATE + timedelta(days=issdte) if issdte > 0 else bl_date
                        nxt = calculate_next_bldate(bl_date, iss_date, '3', 6)
                        cur_bldate = (nxt - BASE_DATE).days if nxt else 0
                
                if payamt < 0:
                    payamt = 0
                if cur_bldate > 0 and (cur_bldate > expr_sas or cur_out <= payamt):
                    cur_bldate = expr_sas
                
                while cur_bldate > 0 and cur_bldate <= expr_sas:
                    if cur_bldate <= runoff_sas:
                        remmth = None
                    elif (cur_bldate - runoff_sas) < 8:
                        remmth = 0.1
                    else:
                        mat_date = BASE_DATE + timedelta(days=int(cur_bldate))
                        remmth = calculate_remmth(mat_date, runoff_dt, runoff_dt.year, runoff_dt.month, runoff_dt.day)
                    
                    if (remmth and remmth > 1) or cur_bldate == expr_sas:
                        break
                    
                    if payamt > 0 and remmth is not None:
                        cur_out -= payamt
                        add_record(records, '95', item, cust, remmth, payamt)
                        npl_rem = 0.1 if days > 89 else remmth
                        add_record(records, '93', item, cust, npl_rem, payamt)
                        tr_count += 2
                    
                    bl_date = BASE_DATE + timedelta(days=int(cur_bldate))
                    iss_date = BASE_DATE + timedelta(days=issdte) if issdte > 0 else bl_date
                    nxt = calculate_next_bldate(bl_date, iss_date, '3', 6)
                    cur_bldate = (nxt - BASE_DATE).days if nxt else 0
                    if cur_bldate > 0 and (cur_bldate > expr_sas or cur_out <= payamt):
                        cur_bldate = expr_sas
                
                if cur_bldate > 0:
                    if cur_bldate <= runoff_sas:
                        remmth = None
                    elif (cur_bldate - runoff_sas) < 8:
                        remmth = 0.1
                    else:
                        mat_date = BASE_DATE + timedelta(days=int(cur_bldate))
                        remmth = calculate_remmth(mat_date, runoff_dt, runoff_dt.year, runoff_dt.month, runoff_dt.day)
            
            if cur_out != 0:
                add_record(records, '95', item, cust, remmth, cur_out)
                npl_rem = 0.1 if days > 89 else remmth
                add_record(records, '93', item, cust, npl_rem, cur_out)
                tr_count += 2
        
        print(f"  TR records created: {tr_count}")
        
    except Exception as e:
        print(f"  TR Processing Error: {e}")
    
    # Step 6: Combine, filter, and output
    print(f"\n--- Final Output ---")
    if records:
        df = pl.DataFrame(records)
        missing = df.filter(pl.col('BNMCODE').str.slice(7,2) == '07')
        if len(missing) > 0:
            print(f"  Missing remmth records (code 07): {len(missing)}")
            print(f"  Missing amount sum: {missing['AMOUNT'].sum():,.2f}")
        
        df = df.filter(pl.col('BNMCODE').str.slice(7,2) != '07')
        result = df.group_by('BNMCODE').agg(pl.col('AMOUNT').sum()).sort('BNMCODE')
        
        result.write_parquet(OUTPUT_PARQUET)
        result.write_csv(OUTPUT_CSV)
        
        print(f"\n  Output files:")
        print(f"    Parquet: {OUTPUT_PARQUET}")
        print(f"    CSV:     {OUTPUT_CSV}")
        print(f"\n  Summary:")
        print(f"    Total BNM Codes: {len(result)}")
        print(f"    Total Amount: {result['AMOUNT'].sum():,.2f}")
        
        if len(result) <= 15:
            print(f"\n  Breakdown:")
            for row in result.iter_rows(named=True):
                print(f"    {row['BNMCODE']}: {row['AMOUNT']:>15,.2f}")
    else:
        print("  No records generated")


if __name__ == "__main__":
    import sys
    
    use_latest = True
    reptdate = None
    
    for arg in sys.argv[1:]:
        if arg == '--exact':
            use_latest = False
        elif arg == '--latest':
            use_latest = True
        else:
            try:
                reptdate = datetime.strptime(arg, '%Y-%m-%d').date()
            except:
                print(f"Usage: python eibdlibt.py [YYYY-MM-DD] [--exact|--latest]")
                print("  --exact:  Require exact date match (no fallback)")
                print("  --latest: Use latest file if exact not found (default)")
                sys.exit(1)
    
    USE_LATEST_FALLBACK = use_latest
    main(reptdate)
