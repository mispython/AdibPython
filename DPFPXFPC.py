#!/usr/bin/env python3
"""
File Name: EIBDLIBT
BNM Liquidity Report for Trade Finance
Processes BA (Banker's Acceptance) and TR (Trade) transactions
Based on SAS original code
"""

import pyreadstat
import polars as pl
from datetime import datetime, timedelta, date
from pathlib import Path
import warnings
import re
import sys

# Import from PBBLNFMT (format definitions)
from PBBLNFMT import (
    get_remfmt,
    get_days_in_month,
    format_liqpfmt,
    format_btcustcd,
)

# Import from PBBELF (macro functions)
try:
    from PBBELF import (
        calculate_next_bldate,
        calculate_remmth,
    )
except ImportError:
    # Fallback definitions if PBBELF not available
    def calculate_next_bldate(bldate, issdte, payfreq, freq):
        """Calculate next billing date (NXTBLDT macro fallback)"""
        from PBBLNFMT import get_days_in_month as _get_days
        
        if payfreq == '6':
            # Fortnightly - add 14 days
            dd, mm, yy = bldate.day + 14, bldate.month, bldate.year
            dim = _get_days(yy, mm)
            if dd > dim:
                dd, mm = dd - dim, mm + 1
                if mm > 12:
                    mm, yy = mm - 12, yy + 1
            return date(yy, mm, dd)
        else:
            # Monthly/quarterly/etc.
            dd, mm, yy = issdte.day, bldate.month + freq, bldate.year
            if mm > 12:
                mm, yy = mm - 12, yy + 1
            dim = _get_days(yy, mm)
            return date(yy, mm, min(dd, dim))
    
    def calculate_remmth(matdate, runoff_dt, rpyr, rpmth, rpday):
        """Calculate remaining months to maturity (REMMTH macro fallback)"""
        from PBBLNFMT import get_days_in_month as _get_days
        
        rpdays = _get_days(rpyr, rpmth)
        mdday = min(matdate.day, rpdays)
        remy = matdate.year - rpyr
        remm = matdate.month - rpmth
        remd = mdday - rpday
        return remy * 12 + remm + remd / rpdays

warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURATION
# ============================================================================
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input SAS files (without date suffix - will be appended)
BTDTL_BASE = INPUT_DIR / "bt_btdtl"
PBA01_BASE = INPUT_DIR / "pba01"

# Output files
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
    """Read SAS .sas7bdat file and return as Polars DataFrame"""
    print(f"  Reading: {filepath.name}")
    df, meta = pyreadstat.read_sas7bdat(str(filepath))
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
            print(f"  WARNING: Exact file not found. Using latest: {latest_file.name} (dated {latest_date})")
            with open(OUTPUT_LOG, 'a') as log:
                log.write(f"{datetime.now()}: Using {latest_file.name}\n")
            return read_sas(latest_file)
    
    raise FileNotFoundError(f"No file found for date {y2}{m2}{d2} at {base_path}")


# ============================================================================
# RECORD PROCESSING FUNCTION
# ============================================================================
def add_record(records, prefix, item, cust, remmth, amount):
    """Add a single BNM record"""
    records.append({
        'BNMCODE': f"{prefix}{item}{cust}{get_remfmt(remmth)}0000Y",
        'AMOUNT': amount
    })


def process_transaction(records, row, is_ba, reptdate_sas, runoff_sas, runoff_dt, rpyr, rpmth, rpday):
    """Process a single BA or TR transaction"""
    if is_ba:
        # BA: balance = FCVALUE - UNEARNED
        balance = (row.get('FCVALUE', 0) or 0) - (row.get('UNEARNED', 0) or 0)
        if balance == 0:
            return 0
        amount_key = balance
    else:
        # TR: use OUTSTAND
        amount_key = row.get('OUTSTAND', 0) or 0
        if amount_key == 0:
            return 0
    
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
    days = 0
    bldate = row.get('BLDATE', 0) or 0
    if bldate > 0:
        days = reptdate_sas - bldate
    
    # Initialize variables
    remmth = None
    current_amount = amount_key
    current_bldate = bldate
    expr_sas = row.get('EXPRDATE', 0) or 0
    payamt = row.get('PAYAMT', 0) or 0
    issdte = row.get('ISSDTE', 0) or 0
    
    record_count = 0
    
    if expr_sas and expr_sas <= runoff_sas:
        remmth = None
    elif expr_sas and (expr_sas - runoff_sas) < 8:
        remmth = 0.1
    elif expr_sas:
        # Payment frequency (hardcoded to '3' = 6 months)
        payfreq = '3'
        freq = 6  # For '3'
        
        # Initialize bldate if needed
        if current_bldate <= 0 and issdte > 0:
            current_bldate = issdte
            while current_bldate > 0 and current_bldate <= reptdate_sas:
                bl_date = BASE_DATE + timedelta(days=int(current_bldate))
                iss_date = BASE_DATE + timedelta(days=issdte) if issdte > 0 else bl_date
                nxt = calculate_next_bldate(bl_date, iss_date, payfreq, freq)
                current_bldate = (nxt - BASE_DATE).days if nxt else 0
        
        if payamt < 0:
            payamt = 0
        
        if current_bldate > 0 and (current_bldate > expr_sas or current_amount <= payamt):
            current_bldate = expr_sas
        
        # Process payment schedule
        while current_bldate > 0 and current_bldate <= expr_sas:
            if current_bldate <= runoff_sas:
                remmth = None
            elif (current_bldate - runoff_sas) < 8:
                remmth = 0.1
            else:
                mat_date = BASE_DATE + timedelta(days=int(current_bldate))
                remmth = calculate_remmth(mat_date, runoff_dt, runoff_dt.year, runoff_dt.month, runoff_dt.day)
            
            if (remmth and remmth > 1) or current_bldate == expr_sas:
                break
            
            if payamt > 0 and remmth is not None:
                current_amount -= payamt
                # Part 2-RM (95) - standard
                add_record(records, '95', item, cust, remmth, payamt)
                # Part 1-RM (93) - with NPL adjustment
                if is_ba:
                    npl_rem = 13 if days > 89 else remmth
                else:
                    npl_rem = 0.1 if days > 89 else remmth
                add_record(records, '93', item, cust, npl_rem, payamt)
                record_count += 2
            
            # Calculate next bldate
            if current_bldate > 0:
                bl_date = BASE_DATE + timedelta(days=int(current_bldate))
                iss_date = BASE_DATE + timedelta(days=issdte) if issdte > 0 else bl_date
                nxt = calculate_next_bldate(bl_date, iss_date, payfreq, freq)
                current_bldate = (nxt - BASE_DATE).days if nxt else 0
            
            if current_bldate > 0 and (current_bldate > expr_sas or current_amount <= payamt):
                current_bldate = expr_sas
        
        # Calculate final remmth for remaining balance
        if current_bldate > 0:
            if current_bldate <= runoff_sas:
                remmth = None
            elif (current_bldate - runoff_sas) < 8:
                remmth = 0.1
            else:
                mat_date = BASE_DATE + timedelta(days=int(current_bldate))
                remmth = calculate_remmth(mat_date, runoff_dt, runoff_dt.year, runoff_dt.month, runoff_dt.day)
    
    # Output remaining balance
    if current_amount != 0:
        add_record(records, '95', item, cust, remmth, current_amount)
        if is_ba:
            npl_rem = 13 if days > 89 else remmth
        else:
            npl_rem = 0.1 if days > 89 else remmth
        add_record(records, '93', item, cust, npl_rem, current_amount)
        record_count += 2
    
    return record_count


# ============================================================================
# MAIN PROCESSING
# ============================================================================
def main(reptdate=None):
    global USE_LATEST_FALLBACK
    
    # Step 1: Set report date
    reptdate = reptdate or date.today()
    rpyr, rpmth, rpday = reptdate.year, reptdate.month, reptdate.day
    reptdate_sas = (reptdate - BASE_DATE).days
    
    print("\n" + "=" * 70)
    print("BNM LIQUIDITY REPORT - TRADE FINANCE PROCESSING")
    print("=" * 70)
    print(f"\nReport Date: {reptdate.strftime('%d/%m/%Y')}")
    print(f"Report Year: {rpyr}, Month: {rpmth:02d}, Day: {rpday:02d}")
    
    # Step 2: Calculate runoff date (last day of month)
    last_day = get_days_in_month(rpyr, rpmth)
    runoff_dt = date(rpyr, rpmth, last_day)
    runoff_sas = (runoff_dt - BASE_DATE).days
    print(f"Runoff Date: {runoff_dt.strftime('%d/%m/%Y')}")
    
    records = []
    
    # Step 3-4: Process BA data
    print("\n" + "-" * 50)
    print("PROCESSING BA TRANSACTIONS (Banker's Acceptance)")
    print("-" * 50)
    
    try:
        # Read BTDTL
        print("\nReading BTDTL data...")
        btdtl = get_sas_file(BTDTL_BASE, rpyr, rpmth, rpday)
        btdtl = btdtl.filter((pl.col('ISSDTE') > 0) | (pl.col('EXPRDATE') > 0))
        btdtl = btdtl.select(['TRANSREF', 'ISSDTE', 'EXPRDATE', 'PAYAMT'])
        btdtl = btdtl.sort(['TRANSREF', 'ISSDTE'], descending=[False, True]).unique('TRANSREF', keep='first')
        print(f"  BTDTL records after filtering: {len(btdtl)}")
        
        # Read PBA01
        print("\nReading PBA01 data...")
        pba = get_sas_file(PBA01_BASE, rpyr, rpmth, rpday)
        pba = pba.with_columns(pl.col('TRANSREF').cast(pl.Utf8).str.slice(1, 8).alias('TRANSREF'))
        
        # Merge
        ba_data = pba.join(btdtl, on='TRANSREF', how='left')
        print(f"  BA records after merge: {len(ba_data)}")
        
        # Process BA records
        print("\nProcessing BA records...")
        ba_count = 0
        for row in ba_data.iter_rows(named=True):
            ba_count += process_transaction(records, row, is_ba=True, 
                                           reptdate_sas=reptdate_sas, 
                                           runoff_sas=runoff_sas, 
                                           runoff_dt=runoff_dt,
                                           rpyr=rpyr, rpmth=rpmth, rpday=rpday)
        
        print(f"  BA records created: {ba_count}")
        
    except Exception as e:
        print(f"  BA Processing Error: {e}")
        import traceback
        traceback.print_exc()
    
    # Step 5: Process TR data
    print("\n" + "-" * 50)
    print("PROCESSING TR TRANSACTIONS (Trade)")
    print("-" * 50)
    
    try:
        # Read BTDTL for TR
        print("\nReading BTDTL data for TR...")
        tr_full = get_sas_file(BTDTL_BASE, rpyr, rpmth, rpday)
        
        # Filter for TR: LIABCODE not in BAI/BAP/BAS/BAE and DIRCTIND='D'
        tr_data = tr_full.filter(
            (~pl.col('LIABCODE').cast(pl.Utf8).is_in(['BAI', 'BAP', 'BAS', 'BAE'])) &
            (pl.col('DIRCTIND').cast(pl.Utf8) == 'D')
        )
        print(f"  TR records before processing: {len(tr_data)}")
        
        # Process TR records
        print("\nProcessing TR records...")
        tr_count = 0
        for row in tr_data.iter_rows(named=True):
            tr_count += process_transaction(records, row, is_ba=False,
                                           reptdate_sas=reptdate_sas,
                                           runoff_sas=runoff_sas,
                                           runoff_dt=runoff_dt,
                                           rpyr=rpyr, rpmth=rpmth, rpday=rpday)
        
        print(f"  TR records created: {tr_count}")
        
    except Exception as e:
        print(f"  TR Processing Error: {e}")
        import traceback
        traceback.print_exc()
    
    # Step 6: Combine, filter, and output
    print("\n" + "-" * 50)
    print("FINAL OUTPUT")
    print("-" * 50)
    
    if records:
        df = pl.DataFrame(records)
        
        # Filter out records with missing remmth (code '07')
        missing_df = df.filter(pl.col('BNMCODE').str.slice(7, 2) == '07')
        if len(missing_df) > 0:
            print(f"\n  Records with MISSING remmth (code '07'): {len(missing_df)}")
            print(f"  Missing amount sum: {missing_df['AMOUNT'].sum():,.2f}")
        else:
            print("\n  Records with MISSING remmth (code '07'): 0")
        
        # Keep only records without missing remmth
        df_valid = df.filter(pl.col('BNMCODE').str.slice(7, 2) != '07')
        
        # Summarize by BNMCODE
        result = df_valid.group_by('BNMCODE').agg([
            pl.col('AMOUNT').sum().alias('AMOUNT')
        ]).sort('BNMCODE')
        
        # Write output files
        print(f"\n  Writing Parquet: {OUTPUT_PARQUET}")
        result.write_parquet(OUTPUT_PARQUET)
        
        print(f"  Writing CSV: {OUTPUT_CSV}")
        result.write_csv(OUTPUT_CSV)
        
        # Summary
        total_amount = result['AMOUNT'].sum() if len(result) > 0 else 0
        
        print("\n" + "=" * 70)
        print("PROCESSING COMPLETE")
        print("=" * 70)
        print(f"\nOutput files:")
        print(f"  Parquet: {OUTPUT_PARQUET}")
        print(f"  CSV:     {OUTPUT_CSV}")
        print(f"\nSummary:")
        print(f"  Total BNM Codes: {len(result)}")
        print(f"  Total Amount:    {total_amount:,.2f}")
        
        if len(result) > 0 and len(result) <= 20:
            print(f"\nBreakdown by BNMCODE:")
            print("-" * 50)
            for row in result.iter_rows(named=True):
                print(f"  {row['BNMCODE']}: {row['AMOUNT']:>15,.2f}")
    else:
        print("\n  No records generated")


# ============================================================================
# COMMAND LINE INTERFACE
# ============================================================================
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='BNM Liquidity Report for Trade Finance')
    parser.add_argument('date', nargs='?', help='Report date in YYYY-MM-DD format (default: today)')
    parser.add_argument('--exact', action='store_true', help='Require exact date match (no fallback)')
    parser.add_argument('--latest', action='store_true', help='Use latest file if exact not found (default)')
    
    args = parser.parse_args()
    
    # Set fallback behavior
    if args.exact:
        USE_LATEST_FALLBACK = False
    else:
        USE_LATEST_FALLBACK = True
    
    # Parse date
    reptdate = None
    if args.date:
        try:
            reptdate = datetime.strptime(args.date, '%Y-%m-%d').date()
        except ValueError:
            print(f"Error: Invalid date format. Use YYYY-MM-DD")
            sys.exit(1)
    
    # Run main processing
    main(reptdate)
