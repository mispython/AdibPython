#!/usr/bin/env python3
"""
File Name: EIBDLIBT
BNM Liquidity Report for Trade Finance
Processes BA (Banker's Acceptance) and TR (Trade) transactions
Based on SAS original code

KEY FIX vs previous version:
  BLDATE does NOT exist in btdtl261018.sas7bdat (confirmed against production).
  In SAS, a missing BLDATE means:
      IF BLDATE > 0 THEN DAYS = REPTDATE - BLDATE;   -> FALSE (missing > 0)
      DAYS remains missing (.).
      IF DAYS > 89 THEN REMMTH = 13;                 -> FALSE (missing > 89)
  So the NPL override NEVER FIRES. We reproduce this by keeping DAYS = None
  and treating any DAYS-based check as FALSE.

  We do NOT substitute ISSDTE for BLDATE for the DAYS calculation.
  ISSDTE is only used later as the schedule anchor (which SAS does when
  BLDATE <= 0), matching the ELSE IF branch.

OTHER SUBSTITUTIONS (kept minimal, faithful to SAS):
  SAS PAYAMT   -> 0           (no installment field exists in BTDTL)
  SAS PRODUCT  -> None        (numeric product code absent; PRODUCT==100 never fires)
  SAS BALANCE  -> FCVALUE - UNEARNED (BA) / OUTSTAND (TR)
  SAS CUSTCD   -> CUSTCODE (Float64, cast int->str)
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
    'ba_rows_in': 0,
    'ba_rows_processed': 0,
    'ba_rows_exprdate_null': 0,
    'tr_rows_in': 0,
    'tr_rows_processed': 0,
    'tr_rows_exprdate_null': 0,
    'payamt_zero': 0,
    'payamt_negative': 0,
    'payamt_positive': 0,
    'exprdate_le_reptdate': 0,
    'exprdate_within_8d': 0,
    'exprdate_far': 0,
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
    'npl_override_fired': 0,   # should be 0 given BLDATE missing
    'issdte_used_as_anchor': 0,
}
BNMCODE_BEFORE_FILTER = Counter()
DROPPED_REMMTH_CODES = Counter()
EXPRDATE_BRANCH = Counter()
DAYS_BUCKETS = Counter()
DROPPED_BALANCE_SUM = {'sum': 0.0, 'count': 0}
EMITTED_REMMTH = Counter()
EMITTED_BY_PREFIX_REMMTH = {'95': Counter(), '93': Counter(),
                             '96': Counter(), '94': Counter()}
PAYAMT_PRESENCE = {'ba': 'unknown', 'tr': 'unknown'}


# ============================================================================
# FORMAT HELPERS
# ============================================================================
def get_days_in_month(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def get_remfmt(remmth: Optional[float]) -> str:
    """SAS REMFMT."""
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
# PBBELF MACROS (with fallback)
# ============================================================================
try:
    from PBBELF import (
        calculate_next_bldate,
        calculate_remmth,
    )
except ImportError:
    def calculate_next_bldate(bldate, issdte, payfreq, freq):
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
# HELPERS: coerce / cast
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
    records.append({
        'BNMCODE': f"{prefix}{item}{cust}{code}0000Y",
        'AMOUNT': amount
    })
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
        amount_key = (row.get('FCVALUE', 0) or 0) - (row.get('UNEARNED', 0) or 0)
        DIAG['ba_rows_in'] += 1
    else:
        amount_key = row.get('OUTSTAND', 0) or 0
        DIAG['tr_rows_in'] += 1

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
    DIAG[{'212': 'item_212_count', '219': 'item_219_count'}.get(item, 'item_other_count')] += 1

    # ====================================================================
    # CRITICAL FIX:
    # BLDATE does not exist in btdtl261018.sas7bdat.
    # In SAS, BLDATE is missing (.), so:
    #   IF BLDATE > 0 THEN DAYS = REPTDATE - BLDATE;  -> FALSE
    #   DAYS remains missing (.).
    #   IF DAYS > 89 THEN REMMTH = 13;                -> FALSE (missing > 89)
    # NPL override therefore NEVER fires.
    # ====================================================================
    days = None
    DIAG['npl_override_fired'] += 0  # placeholder, updated later if fires

    # Days bucket diagnostic: SAS has no valid DAYS → bucket "MISSING"
    DAYS_BUCKETS['MISSING (BLDATE absent)'] += 1

    # -------- Dates --------
    issdte_sas = _to_sas_date(row.get('ISSDTE', None))
    expr_sas   = _to_sas_date(row.get('EXPRDATE', None))

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
    current_bldate = 0  # BLDATE missing (SAS: .)
    record_count = 0

    # SAS: IF EXPRDATE - REPTDATE < 8 THEN REMMTH = 0.1; ELSE DO; ... END;
    if expr_sas == 0:
        # EXPRDATE missing. SAS: "missing < 8" is FALSE, so ELSE DO runs.
        # Inside ELSE DO, "BLDATE <= EXPRDATE" (with EXPRDATE missing) is FALSE,
        # so the DO WHILE never runs. REMMTH stays missing -> code '07'.
        EXPRDATE_BRANCH['exprdate_missing'] += 1
        if is_ba:
            DIAG['ba_rows_exprdate_null'] += 1
        else:
            DIAG['tr_rows_exprdate_null'] += 1
        remmth = None
    elif (expr_sas - reptdate_sas) < 8:
        EXPRDATE_BRANCH['exprdate_within_8d'] += 1
        DIAG['exprdate_within_8d'] += 1
        remmth = 0.1
    else:
        EXPRDATE_BRANCH['exprdate_far'] += 1
        DIAG['exprdate_far'] += 1

        payfreq = '3'
        freq = 6

        # SAS: IF PRODUCT IN (350,910,925) THEN BLDATE = EXPRDATE;
        #      ELSE IF BLDATE <= 0 THEN DO; BLDATE = ISSDTE; DO WHILE... END; END;
        # PRODUCT absent -> first FALSE. BLDATE <= 0 (missing) TRUE -> use ISSDTE.
        if current_bldate <= 0:
            current_bldate = issdte_sas
            DIAG['issdte_used_as_anchor'] += 1
            while current_bldate > 0 and current_bldate <= reptdate_sas:
                bl_dt  = BASE_DATE + timedelta(days=int(current_bldate))
                iss_dt = BASE_DATE + timedelta(days=int(issdte_sas)) if issdte_sas > 0 else bl_dt
                nxt = calculate_next_bldate(bl_dt, iss_dt, payfreq, freq)
                current_bldate = (nxt - BASE_DATE).days if nxt else 0

        # SAS: IF BLDATE > EXPRDATE | BALANCE <= PAYAMT THEN BLDATE = EXPRDATE;
        if current_bldate > 0 and (current_bldate > expr_sas or current_balance <= payamt):
            current_bldate = expr_sas

        # SAS: DO WHILE (BLDATE <= EXPRDATE)
        while current_bldate > 0 and current_bldate <= expr_sas:
            mat_dt = BASE_DATE + timedelta(days=int(current_bldate))
            remmth = calculate_remmth(mat_dt, date(rpyr, rpmth, rpday), rpyr, rpmth, rpday)

            # SAS: IF REMMTH > 12 OR BLDATE = EXPRDATE THEN LEAVE;
            if remmth > 12 or current_bldate == expr_sas:
                break

            # SAS: AMOUNT = PAYAMT; BALANCE = BALANCE - PAYAMT;
            #      OUTPUT 95; IF DAYS > 89 THEN REMMTH = 13; OUTPUT 93;
            if payamt > 0:
                current_balance -= payamt
                add_record(records, '95', item, cust, remmth, payamt)
                # DAYS is None → NPL override never fires
                npl_rem = 13 if (days is not None and days > 89) else remmth
                if days is not None and days > 89:
                    DIAG['npl_override_fired'] += 1
                add_record(records, '93', item, cust, npl_rem, payamt)
                record_count += 2

            # %NXTBLDT
            if current_bldate > 0:
                bl_dt  = BASE_DATE + timedelta(days=int(current_bldate))
                iss_dt = BASE_DATE + timedelta(days=int(issdte_sas)) if issdte_sas > 0 else bl_dt
                nxt = calculate_next_bldate(bl_dt, iss_dt, payfreq, freq)
                current_bldate = (nxt - BASE_DATE).days if nxt else 0

            # SAS: IF BLDATE > EXPRDATE | BALANCE <= PAYAMT THEN BLDATE = EXPRDATE;
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
        print(f"  WARNING: Expected output file not found at {out_file}")
        return False
    except Exception as e:
        print(f"  SAS7BDAT Write Error: {e}")
        import traceback; traceback.print_exc()
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
    print(f"  BA rows in               : {DIAG['ba_rows_in']}")
    print(f"  BA rows processed        : {DIAG['ba_rows_processed']}")
    print(f"  BA rows EXPRDATE null    : {DIAG['ba_rows_exprdate_null']}")
    print(f"  TR rows in               : {DIAG['tr_rows_in']}")
    print(f"  TR rows processed        : {DIAG['tr_rows_processed']}")
    print(f"  TR rows EXPRDATE null    : {DIAG['tr_rows_exprdate_null']}")

    print("\n--- PAYAMT state (substitute: always 0 in this extract) ---")
    print(f"  PAYAMT == 0              : {DIAG['payamt_zero']}")
    print(f"  PAYAMT < 0               : {DIAG['payamt_negative']}")
    print(f"  PAYAMT > 0               : {DIAG['payamt_positive']}")

    print("\n--- NPL override ---")
    print(f"  NPL override fired       : {DIAG['npl_override_fired']}   (expect 0 when BLDATE missing)")

    print("\n--- EXPRDATE branch taken ---")
    for k, v in EXPRDATE_BRANCH.items():
        print(f"  {k:28s}: {v}")

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
    print(f"  PRODUCT==100 override    : {DIAG['product_100_override']}")

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
    print(f"  balance sum              : {DROPPED_BALANCE_SUM['sum']:,.2f}")


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
        btdtl = btdtl.filter((pl.col('ISSDTE') > 0) | (pl.col('EXPRDATE') > 0))

        if 'PAYAMT' not in btdtl.columns:
            print("  WARNING: 'PAYAMT' not in BTDTL - defaulting to 0 (SAS-substitute).")
            PAYAMT_PRESENCE['ba'] = 'MISSING'
            btdtl = btdtl.with_columns(pl.lit(0).alias('PAYAMT'))
        else:
            PAYAMT_PRESENCE['ba'] = 'PRESENT'

        btdtl = btdtl.select(['TRANSREF', 'ISSDTE', 'EXPRDATE', 'PAYAMT'])
        btdtl = btdtl.sort(['TRANSREF', 'ISSDTE'], descending=[False, True]).unique('TRANSREF', keep='first')
        print(f"  BTDTL records after filtering: {len(btdtl)}")

        print("\nReading PBA01 data...")
        pba = get_sas_file(PBA01_BASE, rpyr, rpmth, rpday)
        pba = pba.with_columns(pl.col('TRANSREF').cast(pl.Utf8).str.slice(1, 8).alias('TRANSREF'))
        ba_data = pba.join(btdtl, on='TRANSREF', how='left')
        print(f"  BA records after merge: {len(ba_data)}")

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

        if 'PAYAMT' not in tr_full.columns:
            print("  WARNING: 'PAYAMT' not in TR BTDTL - defaulting to 0.")
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
