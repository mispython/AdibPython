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
from collections import Counter
import warnings
import re
import sys
import calendar
from typing import Optional

# PBBLNFMT.py is left untouched. This program's CUST/ITEM logic (see
# process_transaction below) matches the real SAS EIBDLIBT source directly
# and does NOT use format_btcustcd/format_liqpfmt - those formats belong to
# other BNM programs, not this one.


# ============================================================================
# GLOBAL DIAGNOSTIC COLLECTORS
# These accumulate counters across all processed rows, then get dumped
# at the end of main() for debugging.
# ============================================================================
DIAG = {
    'ba_rows_in': 0,
    'ba_rows_processed': 0,
    'ba_rows_exprdate_null': 0,
    'tr_rows_in': 0,
    'tr_rows_processed': 0,
    'tr_rows_exprdate_null': 0,
    'payamt_missing': 0,
    'payamt_zero': 0,
    'payamt_negative': 0,
    'payamt_positive': 0,
    'exprdate_le_runoff': 0,
    'exprdate_within_8d': 0,
    'exprdate_far': 0,
    'bldate_zero_or_missing': 0,
    'bldate_before_runoff': 0,
    'bldate_after_runoff': 0,
    'remmth_none_final': 0,
    'remmth_0_1_final': 0,
    'remmth_computed_final': 0,
    'records_emitted_95': 0,
    'records_emitted_93': 0,
    'records_dropped_07': 0,
    'product_100_override': 0,
    'cust_08_count': 0,
    'cust_09_count': 0,
    'item_212_count': 0,
    'item_219_count': 0,
    'item_other_count': 0,
}

# Distribution of BNMCODE buckets before '07' filtering
BNMCODE_BEFORE_FILTER = Counter()
# Distribution of remmth codes for dropped records
DROPPED_REMMTH_CODES = Counter()
# Distribution of why exprdate branch taken
EXPRDATE_BRANCH = Counter()
# Distribution of DAYS past due buckets
DAYS_BUCKETS = Counter()
# Distribution of BALANCE sizes for dropped rows
DROPPED_BALANCE_SUM = {'sum': 0.0, 'count': 0}
# Emitted remmth distribution (non-07)
EMITTED_REMMTH = Counter()
# PAYAMT presence per BA/TR
PAYAMT_PRESENCE = {'ba': 'unknown', 'tr': 'unknown'}


# ============================================================================
# LOCAL FORMAT HELPERS (kept in EIBDLIBT.py so PBBLNFMT.py stays untouched)
# ============================================================================
def get_days_in_month(year: int, month: int) -> int:
    """Return the number of days in the given year/month."""
    return calendar.monthrange(year, month)[1]


def get_remfmt(remmth: Optional[float]) -> str:
    """Format remaining months to maturity into a 2-char BNM REMFMT code."""
    if remmth is None:
        return '07'   # MISSING
    if remmth <= 0.1:
        return '01'   # UP TO 1 WK
    elif remmth <= 1:
        return '02'   # >1 WK - 1 MTH
    elif remmth <= 3:
        return '03'   # >1 MTH - 3 MTHS
    elif remmth <= 6:
        return '04'   # >3 - 6 MTHS
    elif remmth <= 12:
        return '05'   # >6 MTHS - 1 YR
    else:
        return '06'   # > 1 YEAR


# Import from PBBELF (macro functions)
try:
    from PBBELF import (
        calculate_next_bldate,
        calculate_remmth,
    )
except ImportError:
    def calculate_next_bldate(bldate, issdte, payfreq, freq):
        """Calculate next billing date (NXTBLDT macro fallback)"""
        if payfreq == '6':
            dd, mm, yy = bldate.day + 14, bldate.month, bldate.year
            dim = get_days_in_month(yy, mm)
            if dd > dim:
                dd, mm = dd - dim, mm + 1
                if mm > 12:
                    mm, yy = mm - 12, yy + 1
            return date(yy, mm, dd)
        else:
            dd, mm, yy = issdte.day, bldate.month + freq, bldate.year
            if mm > 12:
                mm, yy = mm - 12, yy + 1
            dim = get_days_in_month(yy, mm)
            return date(yy, mm, min(dd, dim))

    def calculate_remmth(matdate, runoff_dt, rpyr, rpmth, rpday):
        rpdays = get_days_in_month(rpyr, rpmth)
        mdday = min(matdate.day, rpdays)
        remy = matdate.year - rpyr
        remm = matdate.month - rpmth
        remd = mdday - rpday
        return remy * 12 + remm + remd / rpdays

try:
    import saspy
    SASPY_AVAILABLE = True
except ImportError:
    saspy = None
    SASPY_AVAILABLE = False

warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURATION
# ============================================================================
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "/dwh/btrade_d/"

BTDTL_BASE = INPUT_DIR / "btdtl"
PBA01_BASE = INPUT_DIR / "pba01"

OUTPUT_SAS7BDAT = Path("/dwh/dwh_m/tempsource/PBB_NLF/daily/bt.sas7bdat")
OUTPUT_SAS7BDAT.parent.mkdir(parents=True, exist_ok=True)

BASE_DATE = date(1960, 1, 1)
USE_LATEST_FALLBACK = True

SAS_CONFIG_NAME = "default"
SAS_OUTPUT_LIBREF = "XMISOUT"
SAS_OUTPUT_DSNAME = "bt"
WRITE_SAS7BDAT = True


# ============================================================================
# SAS FILE READERS
# ============================================================================
def find_latest_file(base_path):
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
    df, meta = pyreadstat.read_sas7bdat(str(filepath))
    return pl.from_pandas(df)


def get_sas_file(base_path, y, m, d):
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
            return read_sas(latest_file)
    raise FileNotFoundError(f"No file found for date {y2}{m2}{d2} at {base_path}")


# ============================================================================
# RECORD PROCESSING
# ============================================================================
def add_record(records, prefix, item, cust, remmth, amount):
    code = get_remfmt(remmth)
    records.append({
        'BNMCODE': f"{prefix}{item}{cust}{code}0000Y",
        'AMOUNT': amount
    })
    EMITTED_REMMTH[code] += 1
    if prefix == '95' or prefix == '96':
        DIAG['records_emitted_95'] += 1
    else:
        DIAG['records_emitted_93'] += 1


def process_transaction(records, row, is_ba, reptdate_sas, runoff_sas, runoff_dt, rpyr, rpmth, rpday):
    """Process a single BA or TR transaction (with diagnostics)."""
    if is_ba:
        amount_key = (row.get('FCVALUE', 0) or 0) - (row.get('UNEARNED', 0) or 0)
        DIAG['ba_rows_in'] += 1
    else:
        amount_key = row.get('OUTSTAND', 0) or 0
        DIAG['tr_rows_in'] += 1

    custcd_raw = row.get('CUSTCODE', row.get('CUSTCD', None))
    custcd = str(custcd_raw).strip() if custcd_raw is not None else ''
    is_individual_group = custcd in ('77', '78', '95', '96')
    cust = '08' if is_individual_group else '09'
    if cust == '08':
        DIAG['cust_08_count'] += 1
    else:
        DIAG['cust_09_count'] += 1

    item = '219'
    product = row.get('PRODUCT', None)
    if product == 100:
        item = '212'
        DIAG['product_100_override'] += 1

    if item == '212':
        DIAG['item_212_count'] += 1
    elif item == '219':
        DIAG['item_219_count'] += 1
    else:
        DIAG['item_other_count'] += 1

    days = 0
    bldate = row.get('BLDATE', 0) or 0
    if bldate > 0:
        days = reptdate_sas - bldate
    # bucket days for diagnostic
    if days < 0:
        DAYS_BUCKETS['<0'] += 1
    elif days <= 30:
        DAYS_BUCKETS['0-30'] += 1
    elif days <= 89:
        DAYS_BUCKETS['31-89'] += 1
    elif days <= 180:
        DAYS_BUCKETS['90-180'] += 1
    else:
        DAYS_BUCKETS['>180'] += 1

    remmth = None
    current_amount = amount_key
    current_bldate = bldate
    expr_sas = row.get('EXPRDATE', 0) or 0
    payamt = row.get('PAYAMT', 0) or 0
    issdte = row.get('ISSDTE', 0) or 0

    # payamt diagnostics
    if payamt == 0:
        DIAG['payamt_zero'] += 1
    elif payamt < 0:
        DIAG['payamt_negative'] += 1
    else:
        DIAG['payamt_positive'] += 1

    # BLDATE diagnostics
    if bldate <= 0:
        DIAG['bldate_zero_or_missing'] += 1
    elif bldate <= runoff_sas:
        DIAG['bldate_before_runoff'] += 1
    else:
        DIAG['bldate_after_runoff'] += 1

    record_count = 0

    if expr_sas and expr_sas <= runoff_sas:
        remmth = None
        EXPRDATE_BRANCH['exprdate_le_runoff'] += 1
        DIAG['exprdate_le_runoff'] += 1
    elif expr_sas and (expr_sas - runoff_sas) < 8:
        remmth = 0.1
        EXPRDATE_BRANCH['exprdate_within_8d'] += 1
        DIAG['exprdate_within_8d'] += 1
    elif expr_sas:
        EXPRDATE_BRANCH['exprdate_far'] += 1
        DIAG['exprdate_far'] += 1

        payfreq = '3'
        freq = 6

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
                add_record(records, '95', item, cust, remmth, payamt)
                if is_ba:
                    npl_rem = 13 if days > 89 else remmth
                else:
                    npl_rem = 0.1 if days > 89 else remmth
                add_record(records, '93', item, cust, npl_rem, payamt)
                record_count += 2

            if current_bldate > 0:
                bl_date = BASE_DATE + timedelta(days=int(current_bldate))
                iss_date = BASE_DATE + timedelta(days=issdte) if issdte > 0 else bl_date
                nxt = calculate_next_bldate(bl_date, iss_date, payfreq, freq)
                current_bldate = (nxt - BASE_DATE).days if nxt else 0

            if current_bldate > 0 and (current_bldate > expr_sas or current_amount <= payamt):
                current_bldate = expr_sas

        if current_bldate > 0:
            if current_bldate <= runoff_sas:
                remmth = None
            elif (current_bldate - runoff_sas) < 8:
                remmth = 0.1
            else:
                mat_date = BASE_DATE + timedelta(days=int(current_bldate))
                remmth = calculate_remmth(mat_date, runoff_dt, runoff_dt.year, runoff_dt.month, runoff_dt.day)
    else:
        # expr_sas == 0 / missing
        EXPRDATE_BRANCH['exprdate_missing'] += 1
        if is_ba:
            DIAG['ba_rows_exprdate_null'] += 1
        else:
            DIAG['tr_rows_exprdate_null'] += 1
        # NOTE: In SAS, an EXPRDATE of 0 would go to the ELSE block
        # (0 <= runoff_sas is true), so remmth stays None.
        remmth = None

    # Track remmth final state
    if remmth is None:
        DIAG['remmth_none_final'] += 1
    elif remmth == 0.1:
        DIAG['remmth_0_1_final'] += 1
    else:
        DIAG['remmth_computed_final'] += 1

    if current_amount != 0:
        add_record(records, '95', item, cust, remmth, current_amount)
        if is_ba:
            npl_rem = 13 if days > 89 else remmth
        else:
            npl_rem = 0.1 if days > 89 else remmth
        add_record(records, '93', item, cust, npl_rem, current_amount)
        record_count += 2

    if is_ba:
        DIAG['ba_rows_processed'] += 1
    else:
        DIAG['tr_rows_processed'] += 1

    return record_count


# ============================================================================
# SAS7BDAT OUTPUT
# ============================================================================
def write_sas7bdat(result_df: pl.DataFrame, output_dir: Path, libref: str,
                    dsname: str, cfgname: str):
    if not SASPY_AVAILABLE:
        print("  WARNING: saspy is not installed - skipping .sas7bdat output.")
        return False
    if len(result_df) == 0:
        print("  No records to write - skipping .sas7bdat output.")
        return False
    sas = None
    try:
        print(f"\n  Starting SAS session (cfgname='{cfgname}')...")
        sas = saspy.SASsession(cfgname=cfgname)
        print(f"  Assigning library {libref} -> {output_dir}")
        lib_rc = sas.saslib(libref, path=str(output_dir))
        if lib_rc is not None and getattr(sas, "SYSERR", 0) not in (0, None):
            print(f"  WARNING: libname assignment may have issues (SYSERR={sas.SYSERR})")
        pdf = result_df.to_pandas()
        print(f"  Uploading DataFrame to SAS dataset {libref}.{dsname} ({len(pdf)} rows)...")
        sas.df2sd(df=pdf, table=dsname, libref=libref)
        out_file = output_dir / f"{dsname}.sas7bdat"
        if out_file.exists():
            print(f"  SAS dataset written: {out_file}")
            return True
        else:
            print(f"  WARNING: Expected output file not found at {out_file}")
            return False
    except Exception as e:
        print(f"  SAS7BDAT Write Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if sas is not None:
            try:
                sas.endsas()
            except Exception:
                pass


# ============================================================================
# DIAGNOSTIC DUMP
# ============================================================================
def dump_diagnostics():
    print("\n" + "=" * 70)
    print("DIAGNOSTIC REPORT")
    print("=" * 70)

    print("\n--- Row counts ---")
    print(f"  BA rows in            : {DIAG['ba_rows_in']}")
    print(f"  BA rows processed     : {DIAG['ba_rows_processed']}")
    print(f"  BA rows exprdate NULL : {DIAG['ba_rows_exprdate_null']}")
    print(f"  TR rows in            : {DIAG['tr_rows_in']}")
    print(f"  TR rows processed     : {DIAG['tr_rows_processed']}")
    print(f"  TR rows exprdate NULL : {DIAG['tr_rows_exprdate_null']}")

    print("\n--- PAYAMT state across all processed rows ---")
    print(f"  PAYAMT == 0           : {DIAG['payamt_zero']}")
    print(f"  PAYAMT < 0            : {DIAG['payamt_negative']}")
    print(f"  PAYAMT > 0            : {DIAG['payamt_positive']}")

    print("\n--- EXPRDATE branch taken ---")
    for k, v in EXPRDATE_BRANCH.items():
        print(f"  {k:28s}: {v}")
    print(f"  exprdate_le_runoff         : {DIAG['exprdate_le_runoff']}")
    print(f"  exprdate_within_8d         : {DIAG['exprdate_within_8d']}")
    print(f"  exprdate_far               : {DIAG['exprdate_far']}")

    print("\n--- BLDATE state ---")
    print(f"  BLDATE <= 0           : {DIAG['bldate_zero_or_missing']}")
    print(f"  BLDATE <= runoff_sas  : {DIAG['bldate_before_runoff']}")
    print(f"  BLDATE >  runoff_sas  : {DIAG['bldate_after_runoff']}")

    print("\n--- DAYS past due buckets ---")
    for k in ['<0', '0-30', '31-89', '90-180', '>180']:
        print(f"  {k:10s}: {DAYS_BUCKETS.get(k, 0)}")

    print("\n--- FINAL remmth state before code '07' filter ---")
    print(f"  remmth is None        : {DIAG['remmth_none_final']}")
    print(f"  remmth == 0.1         : {DIAG['remmth_0_1_final']}")
    print(f"  remmth computed       : {DIAG['remmth_computed_final']}")

    print("\n--- CUST / ITEM distribution ---")
    print(f"  CUST = '08'           : {DIAG['cust_08_count']}")
    print(f"  CUST = '09'           : {DIAG['cust_09_count']}")
    print(f"  ITEM = '212'          : {DIAG['item_212_count']}")
    print(f"  ITEM = '219'          : {DIAG['item_219_count']}")
    print(f"  PRODUCT==100 override : {DIAG['product_100_override']}")

    print("\n--- Records emitted by prefix ---")
    print(f"  prefix 95/96 count    : {DIAG['records_emitted_95']}")
    print(f"  prefix 93/94 count    : {DIAG['records_emitted_93']}")

    print("\n--- Emitted remmth distribution (before 07 filter) ---")
    for code in sorted(EMITTED_REMMTH.keys()):
        print(f"  code {code}: {EMITTED_REMMTH[code]}")

    print("\n--- BNMCODE distribution (before 07 filter) ---")
    for code in sorted(BNMCODE_BEFORE_FILTER.keys()):
        print(f"  {code}: {BNMCODE_BEFORE_FILTER[code]}")

    print("\n--- Records dropped as code '07' ---")
    print(f"  count                 : {DIAG['records_dropped_07']}")
    print(f"  balance sum           : {DROPPED_BALANCE_SUM['sum']:,.2f}")
    for code in sorted(DROPPED_REMMTH_CODES.keys()):
        print(f"  07-drop remmth code {code}: {DROPPED_REMMTH_CODES[code]}")

    print("\n" + "=" * 70)


# ============================================================================
# MAIN PROCESSING
# ============================================================================
def main(reptdate=None, write_sas7bdat_flag=None, sas_cfgname=None):
    global USE_LATEST_FALLBACK

    if write_sas7bdat_flag is None:
        write_sas7bdat_flag = WRITE_SAS7BDAT
    if sas_cfgname is None:
        sas_cfgname = SAS_CONFIG_NAME

    reptdate = reptdate or (date.today() - timedelta(days=1))
    rpyr, rpmth, rpday = reptdate.year, reptdate.month, reptdate.day
    reptdate_sas = (reptdate - BASE_DATE).days

    print("\n" + "=" * 70)
    print("BNM LIQUIDITY REPORT - TRADE FINANCE PROCESSING")
    print("=" * 70)
    print(f"\nReport Date: {reptdate.strftime('%d/%m/%Y')}")
    print(f"Report Year: {rpyr}, Month: {rpmth:02d}, Day: {rpday:02d}")

    last_day = get_days_in_month(rpyr, rpmth)
    runoff_dt = date(rpyr, rpmth, last_day)
    runoff_sas = (runoff_dt - BASE_DATE).days
    print(f"Runoff Date: {runoff_dt.strftime('%d/%m/%Y')}")

    records = []

    # --- BA ---
    print("\n" + "-" * 50)
    print("PROCESSING BA TRANSACTIONS (Banker's Acceptance)")
    print("-" * 50)

    try:
        print("\nReading BTDTL data...")
        btdtl = get_sas_file(BTDTL_BASE, rpyr, rpmth, rpday)

        # --- SCHEMA DIAGNOSTIC: print all columns and dtypes ---------------
        print("\n  [DIAG] BTDTL schema:")
        for col_name, dtype in zip(btdtl.columns, btdtl.dtypes):
            print(f"    {col_name:20s} : {dtype}")
        # -------------------------------------------------------------------

        btdtl = btdtl.filter((pl.col('ISSDTE') > 0) | (pl.col('EXPRDATE') > 0))

        if 'PAYAMT' not in btdtl.columns:
            print("  WARNING: 'PAYAMT' column not found in BTDTL - defaulting to 0 for all rows.")
            PAYAMT_PRESENCE['ba'] = 'MISSING'
            DIAG['payamt_missing'] += 1
            btdtl = btdtl.with_columns(pl.lit(0).alias('PAYAMT'))
        else:
            PAYAMT_PRESENCE['ba'] = 'PRESENT'

        btdtl = btdtl.select(['TRANSREF', 'ISSDTE', 'EXPRDATE', 'PAYAMT'])
        btdtl = btdtl.sort(['TRANSREF', 'ISSDTE'], descending=[False, True]).unique('TRANSREF', keep='first')
        print(f"  BTDTL records after filtering: {len(btdtl)}")

        print("\nReading PBA01 data...")
        pba = get_sas_file(PBA01_BASE, rpyr, rpmth, rpday)

        # --- SCHEMA DIAGNOSTIC for PBA01 -----------------------------------
        print("\n  [DIAG] PBA01 schema:")
        for col_name, dtype in zip(pba.columns, pba.dtypes):
            print(f"    {col_name:20s} : {dtype}")
        # -------------------------------------------------------------------

        pba = pba.with_columns(pl.col('TRANSREF').cast(pl.Utf8).str.slice(1, 8).alias('TRANSREF'))
        ba_data = pba.join(btdtl, on='TRANSREF', how='left')
        print(f"  BA records after merge: {len(ba_data)}")

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

    # --- TR ---
    print("\n" + "-" * 50)
    print("PROCESSING TR TRANSACTIONS (Trade)")
    print("-" * 50)

    try:
        print("\nReading BTDTL data for TR...")
        tr_full = get_sas_file(BTDTL_BASE, rpyr, rpmth, rpday)

        if 'PAYAMT' not in tr_full.columns:
            print("  WARNING: 'PAYAMT' column not found in TR BTDTL - defaulting to 0.")
            PAYAMT_PRESENCE['tr'] = 'MISSING'
            tr_full = tr_full.with_columns(pl.lit(0).alias('PAYAMT'))
        else:
            PAYAMT_PRESENCE['tr'] = 'PRESENT'

        tr_data = tr_full.filter(
            (~pl.col('LIABCODE').cast(pl.Utf8).is_in(['BAI', 'BAP', 'BAS', 'BAE'])) &
            (pl.col('DIRCTIND').cast(pl.Utf8) == 'D')
        )
        print(f"  TR records before processing: {len(tr_data)}")

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

    # --- FINAL OUTPUT ---
    print("\n" + "-" * 50)
    print("FINAL OUTPUT")
    print("-" * 50)

    if records:
        df = pl.DataFrame(records)

        # Populate BNMCODE_BEFORE_FILTER from all records
        for code, amt in zip(df['BNMCODE'].to_list(), df['AMOUNT'].to_list()):
            BNMCODE_BEFORE_FILTER[code] += 1

        # Populate dropped records diagnostic
        missing_df = df.filter(pl.col('BNMCODE').str.slice(7, 2) == '07')
        if len(missing_df) > 0:
            print(f"\n  Records with MISSING remmth (code '07'): {len(missing_df)}")
            print(f"  Missing amount sum: {missing_df['AMOUNT'].sum():,.2f}")
            DIAG['records_dropped_07'] = len(missing_df)
            DROPPED_BALANCE_SUM['sum'] = float(missing_df['AMOUNT'].sum())
            DROPPED_BALANCE_SUM['count'] = len(missing_df)
            for code, amt in zip(missing_df['BNMCODE'].to_list(), missing_df['AMOUNT'].to_list()):
                DROPPED_REMMTH_CODES[code[7:9]] += 1
        else:
            print("\n  Records with MISSING remmth (code '07'): 0")

        df_valid = df.filter(pl.col('BNMCODE').str.slice(7, 2) != '07')

        result = df_valid.group_by('BNMCODE').agg([
            pl.col('AMOUNT').sum().alias('AMOUNT')
        ]).sort('BNMCODE')

        sas7bdat_ok = False
        if write_sas7bdat_flag:
            print(f"\n  Writing SAS7BDAT via saspy: {OUTPUT_SAS7BDAT}")
            sas7bdat_ok = write_sas7bdat(
                result_df=result,
                output_dir=OUTPUT_SAS7BDAT.parent,
                libref=SAS_OUTPUT_LIBREF,
                dsname=SAS_OUTPUT_DSNAME,
                cfgname=sas_cfgname,
            )
        else:
            print("\n  Skipping SAS7BDAT output (disabled).")

        total_amount = result['AMOUNT'].sum() if len(result) > 0 else 0

        print("\n" + "=" * 70)
        print("PROCESSING COMPLETE")
        print("=" * 70)
        print(f"\nOutput file:")
        if write_sas7bdat_flag:
            status = "OK" if sas7bdat_ok else "FAILED/SKIPPED"
            print(f"  SAS7BDAT: {OUTPUT_SAS7BDAT} [{status}]")
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

    # --- DUMP ALL DIAGNOSTICS ---
    dump_diagnostics()

    # Also print PAYAMT presence
    print("\n--- PAYAMT column presence ---")
    print(f"  BA extract PAYAMT : {PAYAMT_PRESENCE['ba']}")
    print(f"  TR extract PAYAMT : {PAYAMT_PRESENCE['tr']}")

    print("\n--- All unique BNMCODEs emitted (before 07 filter) ---")
    for code, cnt in sorted(BNMCODE_BEFORE_FILTER.items()):
       
