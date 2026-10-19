#!/usr/bin/env python3
"""
File Name: EIBDLIBT
BNM Liquidity Report for Trade Finance
Processes BA (Banker's Acceptance) and TR (Trade) transactions
Based on SAS original code

DIAGNOSTIC BUILD — adds extensive sanity checks to compare against SAS production.
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

# ============================================================================
# GLOBAL DIAGNOSTIC COLLECTORS
# ============================================================================
DIAG = {
    'ba_rows_in': 0, 'ba_rows_processed': 0, 'ba_rows_exprdate_null': 0,
    'tr_rows_in': 0, 'tr_rows_processed': 0, 'tr_rows_exprdate_null': 0,
    'payamt_zero': 0, 'payamt_negative': 0, 'payamt_positive': 0,
    'exprdate_within_8d': 0, 'exprdate_far': 0, 'exprdate_missing': 0,
    'remmth_none_final': 0, 'remmth_0_1_final': 0, 'remmth_computed_final': 0,
    'records_emitted_95': 0, 'records_emitted_93': 0,
    'records_dropped_07': 0,
    'product_100_override': 0,
    'cust_08_count': 0, 'cust_09_count': 0,
    'item_212_count': 0, 'item_219_count': 0,
    'npl_override_fired': 0,
    'issdte_used_as_anchor': 0,
    'ba_negative_balance': 0, 'tr_negative_balance': 0,
    'ba_zero_balance': 0, 'tr_zero_balance': 0,
}
BNMCODE_BEFORE_FILTER = Counter()
DROPPED_REMMTH_CODES = Counter()
DAYS_BUCKETS = Counter()
DROPPED_BALANCE_SUM = {'sum': 0.0, 'count': 0}
EMITTED_REMMTH = Counter()
EMITTED_BY_PREFIX_REMMTH = {}
PAYAMT_PRESENCE = {'ba': 'unknown', 'tr': 'unknown'}

# Raw input totals (BEFORE bucketing) — the key comparison point with SAS
RAW_TOTALS = {
    'ba_count': 0,
    'ba_fcvalue_sum': 0.0,
    'ba_unearned_sum': 0.0,
    'ba_balance_sum': 0.0,
    'ba_null_exprdate_count': 0,
    'ba_null_exprdate_balance_sum': 0.0,
    'ba_zero_balance_count': 0,
    'ba_negative_balance_count': 0,

    'tr_count': 0,
    'tr_outstand_sum': 0.0,
    'tr_null_exprdate_count': 0,
    'tr_null_exprdate_balance_sum': 0.0,
    'tr_zero_balance_count': 0,
    'tr_negative_balance_count': 0,
}


# ============================================================================
# FORMAT HELPERS
# ============================================================================
def get_days_in_month(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def get_remfmt(remmth: Optional[float]) -> str:
    if remmth is None:
        return '07'
    if remmth <= 0.1:
        return '01'
    elif remmth <= 1:
        return '02'
    elif remmth <= 3:
        return '03'
    elif remmth <= 6:
        return '04'
    elif remmth <= 12:
        return '05'
    else:
        return '06'


# ============================================================================
# PBBELF MACRO FALLBACKS (inline; PBBELF.py does not define these)
# ============================================================================
def calculate_next_bldate(bldate, issdte, payfreq, freq):
    """SAS %NXTBLDT — literal translation."""
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


def calculate_remmth(matdate, reptdate, rpyr, rpmth, rpday):
    """SAS %REMMTH — literal translation, uses REPORT date as reference."""
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
                except Exception:
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
# HELPERS
# ============================================================================
def _coerce_int_str(v) -> str:
    if v is None:
        return ''
    try:
        return str(int(v))
    except (TypeError, ValueError):
        return str(v).strip()


def _to_sas_date(v) -> int:
    if v is None:
        return 0
    if isinstance(v, date):
        return (v - BASE_DATE).days
    try:
        d = int(v)
    except (TypeError, ValueError):
        return 0
    return d if d > 0 else 0


# ============================================================================
# RECORD PROCESSING
# ============================================================================
def add_record(records, prefix, item, cust, remmth, amount):
    code = get_remfmt(remmth)
    records.append({'BNMCODE': f"{prefix}{item}{cust}{code}0000Y", 'AMOUNT': amount})
    EMITTED_REMMTH[code] += 1
    EMITTED_BY_PREFIX_REMMTH.setdefault(prefix, Counter())[code] += 1
    if prefix in ('95', '96'):
        DIAG['records_emitted_95'] += 1
    else:
        DIAG['records_emitted_93'] += 1


def process_transaction(records, row, is_ba,
                        reptdate_sas, runoff_sas, runoff_dt,
                        rpyr, rpmth, rpday):
    # -------- Amount key --------
    if is_ba:
        fcv = row.get('FCVALUE', 0) or 0
        une = row.get('UNEARNED', 0) or 0
        amount_key = fcv - une
        DIAG['ba_rows_in'] += 1
        RAW_TOTALS['ba_fcvalue_sum'] += fcv
        RAW_TOTALS['ba_unearned_sum'] += une
        RAW_TOTALS['ba_balance_sum'] += amount_key
        RAW_TOTALS['ba_count'] += 1
        if amount_key == 0:
            DIAG['ba_zero_balance'] += 1
        elif amount_key < 0:
            DIAG['ba_negative_balance'] += 1
    else:
        amount_key = row.get('OUTSTAND', 0) or 0
        DIAG['tr_rows_in'] += 1
        RAW_TOTALS['tr_outstand_sum'] += amount_key
        RAW_TOTALS['tr_count'] += 1
        if amount_key == 0:
            DIAG['tr_zero_balance'] += 1
        elif amount_key < 0:
            DIAG['tr_negative_balance'] += 1

    # -------- CUST --------
    custcd = _coerce_int_str(row.get('CUSTCODE', row.get('CUSTCD', None)))
    is_individual_group = custcd in ('77', '78', '95', '96')
    cust = '08' if is_individual_group else '09'
    DIAG['cust_08_count' if cust == '08' else 'cust_09_count'] += 1

    # -------- ITEM --------
    item = '219'
    product = row.get('PRODUCT', None)
    if product == 100:
        item = '212'
        DIAG['product_100_override'] += 1
    DIAG[{'212': 'item_212_count', '219': 'item_219_count'}.get(item, 'item_212_count')] += 1

    # -------- DAYS: missing (BLDATE absent, SAS . → IF . > 0 FALSE) --------
    days = None
    DAYS_BUCKETS['MISSING (BLDATE absent)'] += 1

    # -------- Dates --------
    issdte_sas = _to_sas_date(row.get('ISSDTE', None))
    expr_sas = _to_sas_date(row.get('EXPRDATE', None))

    # -------- PAYAMT --------
    payamt = row.get('PAYAMT', 0) or 0
    if payamt == 0:
        DIAG['payamt_zero'] += 1
    elif payamt < 0:
        DIAG['payamt_negative'] += 1
    else:
        DIAG['payamt_positive'] += 1

    # -------- State --------
    remmth = None
    current_balance = amount_key
    current_bldate = 0
    record_count = 0

    # =====================================================================
    # SAS: IF EXPRDATE - REPTDATE < 8 THEN REMMTH = 0.1; ELSE DO; ... END;
    # =====================================================================
    if expr_sas == 0:
        DIAG['exprdate_missing'] += 1
        if is_ba:
            DIAG['ba_rows_exprdate_null'] += 1
            RAW_TOTALS['ba_null_exprdate_count'] += 1
            RAW_TOTALS['ba_null_exprdate_balance_sum'] += amount_key
        else:
            DIAG['tr_rows_exprdate_null'] += 1
            RAW_TOTALS['tr_null_exprdate_count'] += 1
            RAW_TOTALS['tr_null_exprdate_balance_sum'] += amount_key
        remmth = None
    elif (expr_sas - reptdate_sas) < 8:
        DIAG['exprdate_within_8d'] += 1
        remmth = 0.1
    else:
        DIAG['exprdate_far'] += 1
        payfreq = '3'
        freq = 6

        if current_bldate <= 0:
            current_bldate = issdte_sas
            DIAG['issdte_used_as_anchor'] += 1
            while current_bldate > 0 and current_bldate <= reptdate_sas:
                bl_dt = BASE_DATE + timedelta(days=int(current_bldate))
                iss_dt = BASE_DATE + timedelta(days=int(issdte_sas)) if issdte_sas > 0 else bl_dt
                nxt = calculate_next_bldate(bl_dt, iss_dt, payfreq, freq)
                current_bldate = (nxt - BASE_DATE).days if nxt else 0

        if current_bldate > 0 and (current_bldate > expr_sas or current_balance <= payamt):
            current_bldate = expr_sas

        while current_bldate > 0 and current_bldate <= expr_sas:
            mat_dt = BASE_DATE + timedelta(days=int(current_bldate))
            remmth = calculate_remmth(mat_dt, date(rpyr, rpmth, rpday), rpyr, rpmth, rpday)

            if remmth > 12 or current_bldate == expr_sas:
                break

            if payamt > 0:
                current_balance -= payamt
                add_record(records, '95', item, cust, remmth, payamt)
                npl_rem = 13 if (days is not None and days > 89) else remmth
                if days is not None and days > 89:
                    DIAG['npl_override_fired'] += 1
                add_record(records, '93', item, cust, npl_rem, payamt)
                record_count += 2

            if current_bldate > 0:
                bl_dt = BASE_DATE + timedelta(days=int(current_bldate))
                iss_dt = BASE_DATE + timedelta(days=int(issdte_sas)) if issdte_sas > 0 else bl_dt
                nxt = calculate_next_bldate(bl_dt, iss_dt, payfreq, freq)
                current_bldate = (nxt - BASE_DATE).days if nxt else 0

            if current_bldate > 0 and (current_bldate > expr_sas or current_balance <= payamt):
                current_bldate = expr_sas

    # -------- Final OUTPUT --------
    if remmth is None:
        DIAG['remmth_none_final'] += 1
    elif remmth == 0.1:
        DIAG['remmth_0_1_final'] += 1
    else:
        DIAG['remmth_computed_final'] += 1

    if current_balance != 0:
        add_record(records, '95', item, cust, remmth, current_balance)
        npl_rem = 13 if (days is not None and days > 89) else remmth
        if days is not None and days > 89:
            DIAG['npl_override_fired'] += 1
        add_record(records, '93', item, cust, npl_rem, current_balance)
        record_count += 2

    if is_ba:
        DIAG['ba_rows_processed'] += 1
    else:
        DIAG['tr_rows_processed'] += 1
    return record_count


# ============================================================================
# SAS7BDAT OUTPUT
# ============================================================================
def write_sas7bdat(result_df, output_dir, libref, dsname, cfgname):
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
        sas.saslib(libref, path=str(output_dir))
        pdf = result_df.to_pandas()
        print(f"  Uploading DataFrame to SAS dataset {libref}.{dsname} ({len(pdf)} rows)...")
        sas.df2sd(df=pdf, table=dsname, libref=libref)
        out_file = output_dir / f"{dsname}.sas7bdat"
        if out_file.exists():
            print(f"  SAS dataset written: {out_file}")
            return True
        print(f"  WARNING: Expected output file not found at {out_file}")
        return False
    except Exception as e:
        print(f"  SAS7BDAT Write Error: {e}")
        import traceback; traceback.print_exc()
        return False
    finally:
        if sas is not None:
            try: sas.endsas()
            except Exception: pass


# ============================================================================
# DIAGNOSTIC DUMP
# ============================================================================
def dump_diagnostics():
    print("\n" + "=" * 70)
    print("DIAGNOSTIC REPORT")
    print("=" * 70)

    print("\n--- Row counts ---")
    print(f"  BA rows in               : {DIAG['ba_rows_in']}")
    print(f"  BA rows processed        : {DIAG['ba_rows_processed']}")
    print(f"  BA rows EXPRDATE null    : {DIAG['ba_rows_exprdate_null']}")
    print(f"  TR rows in               : {DIAG['tr_rows_in']}")
    print(f"  TR rows processed        : {DIAG['tr_rows_processed']}")
    print(f"  TR rows EXPRDATE null    : {DIAG['tr_rows_exprdate_null']}")

    print("\n--- PAYAMT state ---")
    print(f"  PAYAMT == 0              : {DIAG['payamt_zero']}")
    print(f"  PAYAMT < 0               : {DIAG['payamt_negative']}")
    print(f"  PAYAMT > 0               : {DIAG['payamt_positive']}")

    print("\n--- NPL override ---")
    print(f"  NPL override fired       : {DIAG['npl_override_fired']}   (expect 0)")

    print("\n--- EXPRDATE branch ---")
    print(f"  exprdate_missing         : {DIAG['exprdate_missing']}")
    print(f"  exprdate_within_8d       : {DIAG['exprdate_within_8d']}")
    print(f"  exprdate_far             : {DIAG['exprdate_far']}")

    print("\n--- DAYS buckets ---")
    for k, v in DAYS_BUCKETS.items():
        print(f"  {k:28s}: {v}")

    print("\n--- FINAL remmth state ---")
    print(f"  remmth is None           : {DIAG['remmth_none_final']}")
    print(f"  remmth == 0.1            : {DIAG['remmth_0_1_final']}")
    print(f"  remmth computed          : {DIAG['remmth_computed_final']}")

    print("\n--- CUST / ITEM distribution ---")
    print(f"  CUST = '08'              : {DIAG['cust_08_count']}")
    print(f"  CUST = '09'              : {DIAG['cust_09_count']}")
    print(f"  ITEM = '212'             : {DIAG['item_212_count']}")
    print(f"  ITEM = '219'             : {DIAG['item_219_count']}")

    print("\n--- Raw input totals (BEFORE bucketing) ---")
    print(f"  BA  count                : {RAW_TOTALS['ba_count']}")
    print(f"  BA  SUM(FCVALUE)         : {RAW_TOTALS['ba_fcvalue_sum']:>20,.2f}")
    print(f"  BA  SUM(UNEARNED)        : {RAW_TOTALS['ba_unearned_sum']:>20,.2f}")
    print(f"  BA  SUM(FCVALUE-UNEARNED): {RAW_TOTALS['ba_balance_sum']:>20,.2f}  <-- compare with SAS BA total")
    print(f"  BA  zero-balance count   : {DIAG['ba_zero_balance']}")
    print(f"  BA  negative-balance cnt : {DIAG['ba_negative_balance']}")
    print(f"  BA  EXPRDATE-null count  : {RAW_TOTALS['ba_null_exprdate_count']}")
    print(f"  BA  EXPRDATE-null balance: {RAW_TOTALS['ba_null_exprdate_balance_sum']:>20,.2f}")

    print()
    print(f"  TR  count                : {RAW_TOTALS['tr_count']}")
    print(f"  TR  SUM(OUTSTAND)        : {RAW_TOTALS['tr_outstand_sum']:>20,.2f}  <-- compare with SAS TR total")
    print(f"  TR  zero-balance count   : {DIAG['tr_zero_balance']}")
    print(f"  TR  negative-balance cnt : {DIAG['tr_negative_balance']}")
    print(f"  TR  EXPRDATE-null count  : {RAW_TOTALS['tr_null_exprdate_count']}")
    print(f"  TR  EXPRDATE-null balance: {RAW_TOTALS['tr_null_exprdate_balance_sum']:>20,.2f}")

    print()
    grand_raw = RAW_TOTALS['ba_balance_sum'] + RAW_TOTALS['tr_outstand_sum']
    print(f"  GRAND RAW TOTAL          : {grand_raw:>20,.2f}  <-- compare with SAS pre-bucket total")

    print("\n--- Records emitted by prefix ---")
    print(f"  prefix 95/96 count       : {DIAG['records_emitted_95']}")
    print(f"  prefix 93/94 count       : {DIAG['records_emitted_93']}")

    print("\n--- Emitted remmth distribution (all) ---")
    for code in sorted(EMITTED_REMMTH.keys()):
        print(f"  code {code}: {EMITTED_REMMTH[code]}")

    print("\n--- Emitted remmth distribution (by prefix) ---")
    for prefix in sorted(EMITTED_BY_PREFIX_REMMTH.keys()):
        c = EMITTED_BY_PREFIX_REMMTH[prefix]
        if c:
            print(f"  prefix {prefix}:")
            for code in sorted(c.keys()):
                print(f"    code {code}: {c[code]}")

    print("\n--- Records dropped as code '07' ---")
    print(f"  count                    : {DIAG['records_dropped_07']}")
    print(f"  balance sum              : {DROPPED_BALANCE_SUM['sum']:>20,.2f}")


# ============================================================================
# MAIN
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

    # ---- BA ----
    print("\n" + "-" * 50)
    print("PROCESSING BA TRANSACTIONS (Banker's Acceptance)")
    print("-" * 50)
    try:
        print("\nReading BTDTL data...")
        btdtl = get_sas_file(BTDTL_BASE, rpyr, rpmth, rpday)
        print(f"  BTDTL raw rows: {len(btdtl)}")

        # Check column presence BEFORE filtering
        has_issdte = 'ISSDTE' in btdtl.columns
        has_exprdate = 'EXPRDATE' in btdtl.columns
        print(f"  [CHK] ISSDTE present: {has_issdte}, EXPRDATE present: {has_exprdate}")

        btdtl = btdtl.filter((pl.col('ISSDTE') > 0) | (pl.col('EXPRDATE') > 0))
        print(f"  BTDTL after filter (ISSDTE>0 OR EXPRDATE>0): {len(btdtl)}")

        if 'PAYAMT' not in btdtl.columns:
            print("  WARNING: 'PAYAMT' not in BTDTL - defaulting to 0.")
            PAYAMT_PRESENCE['ba'] = 'MISSING'
            btdtl = btdtl.with_columns(pl.lit(0).alias('PAYAMT'))
        else:
            PAYAMT_PRESENCE['ba'] = 'PRESENT'

        btdtl = btdtl.select(['TRANSREF', 'ISSDTE', 'EXPRDATE', 'PAYAMT'])
        btdtl = btdtl.sort(['TRANSREF', 'ISSDTE'], descending=[False, True]).unique('TRANSREF', keep='first')
        print(f"  BTDTL after dedup (unique TRANSREF): {len(btdtl)}")

        print("\nReading PBA01 data...")
        pba = get_sas_file(PBA01_BASE, rpyr, rpmth, rpday)
        print(f"  PBA01 raw rows: {len(pba)}")

        pba = pba.with_columns(pl.col('TRANSREF').cast(pl.Utf8).str.slice(1, 8).alias('TRANSREF'))

        ba_data = pba.join(btdtl, on='TRANSREF', how='left')
        print(f"  BA rows after PBA01 ⋈ BTDTL merge: {len(ba_data)}")

        # How many BA rows had a matching BTDTL row?
        matched = ba_data.filter(pl.col('ISSDTE').is_not_null()).height
        print(f"  BA rows with matching BTDTL: {matched} / {len(ba_data)}")

        print("\nProcessing BA records...")
        ba_count = 0
        for row in ba_data.iter_rows(named=True):
            ba_count += process_transaction(
                records, row, is_ba=True,
                reptdate_sas=reptdate_sas, runoff_sas=runoff_sas, runoff_dt=runoff_dt,
                rpyr=rpyr, rpmth=rpmth, rpday=rpday)
        print(f"  BA records created: {ba_count}")
    except Exception as e:
        print(f"  BA Processing Error: {e}")
        import traceback; traceback.print_exc()

    # ---- TR ----
    print("\n" + "-" * 50)
    print("PROCESSING TR TRANSACTIONS (Trade)")
    print("-" * 50)
    try:
        print("\nReading BTDTL data for TR...")
        tr_full = get_sas_file(BTDTL_BASE, rpyr, rpmth, rpday)
        print(f"  BTDTL raw rows for TR: {len(tr_full)}")

        if 'PAYAMT' not in tr_full.columns:
            print("  WARNING: 'PAYAMT' not in TR BTDTL - defaulting to 0.")
            PAYAMT_PRESENCE['tr'] = 'MISSING'
            tr_full = tr_full.with_columns(pl.lit(0).alias('PAYAMT'))
        else:
            PAYAMT_PRESENCE['tr'] = 'PRESENT'

        # SAS: LIABCODE NOT IN ('BAI','BAP','BAS','BAE') AND DIRCTIND='D'
        # Show what each filter does step by step
        pre_liab = tr_full.height
        tr_data = tr_full.filter(~pl.col('LIABCODE').cast(pl.Utf8).is_in(['BAI', 'BAP', 'BAS', 'BAE']))
        print(f"  After LIABCODE filter: {tr_data.height} (was {pre_liab})")

        pre_dir = tr_data.height
        tr_data = tr_data.filter(pl.col('DIRCTIND').cast(pl.Utf8) == 'D')
        print(f"  After DIRCTIND='D' filter: {tr_data.height} (was {pre_dir})")

        # How many distinct LIABCODE values are in the raw table?
        liabcodes = tr_full.select(pl.col('LIABCODE').cast(pl.Utf8)).unique().to_series().to_list()
        print(f"  Distinct LIABCODE values in BTDTL: {sorted(liabcodes)}")

        dirctinds = tr_full.select(pl.col('DIRCTIND').cast(pl.Utf8)).unique().to_series().to_list()
        print(f"  Distinct DIRCTIND values in BTDTL: {sorted(dirctinds)}")

        print(f"  TR records before processing: {len(tr_data)}")

        print("\nProcessing TR records...")
        tr_count = 0
        for row in tr_data.iter_rows(named=True):
            tr_count += process_transaction(
                records, row, is_ba=False,
                reptdate_sas=reptdate_sas, runoff_sas=runoff_sas, runoff_dt=runoff_dt,
                rpyr=rpyr, rpmth=rpmth, rpday=rpday)
        print(f"  TR records created: {tr_count}")
    except Exception as e:
        print(f"  TR Processing Error: {e}")
        import traceback; traceback.print_exc()

    # ---- FINAL ----
    print("\n" + "-" * 50)
    print("FINAL OUTPUT")
    print("-" * 50)

    if records:
        df = pl.DataFrame(records)
        for code in df['BNMCODE'].to_list():
            BNMCODE_BEFORE_FILTER[code] += 1

        missing_df = df.filter(pl.col('BNMCODE').str.slice(7, 2) == '07')
        if len(missing_df) > 0:
            print(f"\n  Records with MISSING remmth (code '07'): {len(missing_df)}")
            print(f"  Missing amount sum: {missing_df['AMOUNT'].sum():,.2f}")
            DIAG['records_dropped_07'] = len(missing_df)
            DROPPED_BALANCE_SUM['sum'] = float(missing_df['AMOUNT'].sum())
            DROPPED_BALANCE_SUM['count'] = len(missing_df)
            for code in missing_df['BNMCODE'].to_list():
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
                result_df=result, output_dir=OUTPUT_SAS7BDAT.parent,
                libref=SAS_OUTPUT_LIBREF, dsname=SAS_OUTPUT_DSNAME, cfgname=sas_cfgname,
            )
        else:
            print("\n  Skipping SAS7BDAT output (disabled).")

        total_amount = result['AMOUNT'].sum() if len(result) > 0 else 0
        print("\n" + "=" * 70)
        print("SUMMARY")
        print("=" * 70)
        print(f"  Total BNM Codes: {len(result)}")
        print(f"  Total Amount:    {total_amount:,.2f}")
        print(f"\n  Breakdown by BNMCODE:")
        for row in result.iter_rows(named=True):
            print(f"    {row['BNMCODE']}: {row['AMOUNT']:>15,.2f}")
        if write_sas7bdat_flag:
            status = "OK" if sas7bdat_ok else "FAILED/SKIPPED"
            print(f"\n  SAS7BDAT: {OUTPUT_SAS7BDAT} [{status}]")
    else:
        print("\n  No records generated")

    dump_diagnostics()

    print("\n--- PAYAMT presence ---")
    print(f"  BA: {PAYAMT_PRESENCE['ba']}, TR: {PAYAMT_PRESENCE['tr']}")

    print("\n--- All emitted BNMCODEs ---")
    for code, cnt in sorted(BNMCODE_BEFORE_FILTER.items()):
        print(f"  {code} : {cnt}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='BNM Liquidity Report for Trade Finance')
    parser.add_argument('date', nargs='?', help='Report date in YYYY-MM-DD (default: yesterday)')
    parser.add_argument('--exact', action='store_true')
    parser.add_argument('--latest', action='store_true')
    parser.add_argument('--no-sas7bdat', action='store_true')
    parser.add_argument('--sas-cfg', default=SAS_CONFIG_NAME)
    args = parser.parse_args()

    USE_LATEST_FALLBACK = not args.exact

    reptdate = None
    if args.date:
        try:
            reptdate = datetime.strptime(args.date, '%Y-%m-%d').date()
        except ValueError:
            print("Error: Invalid date format. Use YYYY-MM-DD")
            sys.exit(1)

    main(reptdate, write_sas7bdat_flag=not args.no_sas7bdat, sas_cfgname=args.sas_cfg)
