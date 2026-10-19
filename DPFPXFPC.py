#!/usr/bin/env python3
"""
File Name: EIBDLIBT
BNM Liquidity Report for Trade Finance
Processes BA (Banker's Acceptance) and TR (Trade) transactions
Based on SAS original code (EIBDLIBT)

CORRECTED BUILD - changes versus the previous diagnostic build
  1. RUNOFFDT (month-end of REPTDATE) is now the reference date for every
     maturity test and for %REMMTH, exactly as in SAS.
       EXPRDATE <= RUNOFFDT         -> REMMTH missing  (code '07', dropped)
       EXPRDATE - RUNOFFDT  < 8     -> REMMTH = 0.1
       BLDATE   <= RUNOFFDT         -> REMMTH missing
       BLDATE   - RUNOFFDT  < 8     -> REMMTH = 0.1
       otherwise                    -> %REMMTH measured from RUNOFFDT
     REPTDATE is only used for the "advance BLDATE past REPTDATE" loop and DAYS.
  2. Loop exit is "REMMTH > 1 OR BLDATE = EXPRDATE" (was > 12).
  3. %REMMTH reads MATDATE (as SAS does), not the instalment date. If the
     MATDATE column is absent a WARNING is printed and BLDATE is used instead.
  4. BLDATE (-> DAYS) is read from the data when present, so the NPL (>89 days)
     override can fire. BA override sets REMMTH=13, TR override sets REMMTH=0.1,
     as in the SAS source (they differ).
  5. PAYAMT records and the final balance record are always emitted (SAS has
     no "if > 0" guards).
  6. NODUPKEY now matches SAS: unique on (TRANSREF, ISSDTE), not TRANSREF only.
  7. PBA01 x BTDTL merge: BTDTL values override PBA01 values for matched rows.
  8. Column names are upper-cased on read; date columns are converted to
     SAS day numbers; CUSTCD is preferred over CUSTCODE (SAS uses CUSTCD).
  9. TR LIABCODE filter treats missing LIABCODE as "not in list" (SAS semantics).
"""

import math
import re
import sys
import calendar
import warnings
from collections import Counter
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional

import polars as pl
import pyreadstat

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
INPUT_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLIBT")
BTDTL_BASE = INPUT_DIR / "btdtl"
PBA01_BASE = INPUT_DIR / "pba01"

OUTPUT_SAS7BDAT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT/bt.sas7bdat")
OUTPUT_SAS7BDAT.parent.mkdir(parents=True, exist_ok=True)

BASE_DATE = date(1960, 1, 1)          # SAS epoch
SAS_EPOCH_OFFSET = 3653               # days from 1960-01-01 to 1970-01-01
USE_LATEST_FALLBACK = True

SAS_CONFIG_NAME = "default"
SAS_OUTPUT_LIBREF = "XMISOUT"
SAS_OUTPUT_DSNAME = "bt"
WRITE_SAS7BDAT = True

DATE_COLS = ['ISSDTE', 'EXPRDATE', 'BLDATE', 'MATDATE']

# Production SAS bt.sas7bdat for report date 2026-10-18 (for the comparison table)
SAS_REFERENCE_DATE = date(2026, 10, 18)
SAS_REFERENCE = {
    '9321909010000Y': 213115507.92,
    '9321909020000Y': 837778900.83,
    '9321909030000Y': 1637028753.80,
    '9321909040000Y': 298224787.15,
    '9521909010000Y': 213115507.92,
    '9521909020000Y': 837778900.83,
    '9521909030000Y': 1637028753.80,
    '9521909040000Y': 298224787.15,
}

# ============================================================================
# DIAGNOSTICS
# ============================================================================
DIAG = Counter()
RAW = {'ba_balance': 0.0, 'ba_fcvalue': 0.0, 'ba_unearned': 0.0,
       'tr_balance': 0.0, 'ba_missing_exp_bal': 0.0, 'tr_missing_exp_bal': 0.0}
EMITTED = {}          # prefix -> Counter(code -> count)


# ============================================================================
# DATE / FORMAT HELPERS
# ============================================================================
def days_in_month(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def sas_days(v) -> Optional[int]:
    """Anything date-like or numeric -> SAS day number; None for missing."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return (v.date() - BASE_DATE).days
    if isinstance(v, date):
        return (v - BASE_DATE).days
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(f):
        return None
    return int(f)


def num(v) -> float:
    if v is None:
        return 0.0
    try:
        f = float(v)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if math.isnan(f) else f


def coerce_code(v) -> str:
    if v is None:
        return ''
    try:
        return str(int(v))
    except (TypeError, ValueError):
        return str(v).strip()


def first_present(row, *keys):
    for k in keys:
        v = row.get(k)
        if v is not None:
            return v
    return None


def get_remfmt(remmth: Optional[float]) -> str:
    """SAS REMFMT."""
    if remmth is None or (isinstance(remmth, float) and math.isnan(remmth)):
        return '07'
    if remmth <= 0.1:
        return '01'
    if remmth <= 1:
        return '02'
    if remmth <= 3:
        return '03'
    if remmth <= 6:
        return '04'
    if remmth <= 12:
        return '05'
    return '06'


def next_bldate(bldate: int, issdte: int, freq: int = 6, payfreq: str = '3') -> int:
    """SAS %NXTBLDT on SAS day numbers. SAS DAY(0) = 1 (1960-01-01), kept as-is."""
    bl = BASE_DATE + timedelta(days=bldate)
    if payfreq == '6':
        dd, mm, yy = bl.day + 14, bl.month, bl.year
        dim = days_in_month(yy, mm)
        if dd > dim:
            dd -= dim
            mm += 1
            if mm > 12:
                mm -= 12
                yy += 1
    else:
        iss = BASE_DATE + timedelta(days=max(issdte, 0))
        dd, mm, yy = iss.day, bl.month + freq, bl.year
        if mm > 12:
            mm -= 12
            yy += 1
    dd = min(dd, days_in_month(yy, mm))
    return (date(yy, mm, dd) - BASE_DATE).days


def calc_remmth(matdate: Optional[int], runoff_dt: date) -> Optional[float]:
    """SAS %REMMTH. Reference = RUNOFFDT. Missing MATDATE -> missing REMMTH."""
    if matdate is None:
        return None
    mat = BASE_DATE + timedelta(days=int(matdate))
    rpdays = days_in_month(runoff_dt.year, runoff_dt.month)
    mdday = min(mat.day, rpdays)
    remy = mat.year - runoff_dt.year
    remm = mat.month - runoff_dt.month
    remd = mdday - runoff_dt.day
    return remy * 12 + remm + remd / rpdays


# ============================================================================
# SAS FILE READERS
# ============================================================================
def find_latest_file(base_path):
    parent, stem = base_path.parent, base_path.stem
    patterns = [
        rf"{stem}(\d{{8}})\.sas7bdat$",
        rf"{stem}_(\d{{8}})\.sas7bdat$",
        rf"{stem}(\d{{6}})\.sas7bdat$",
        rf"{stem}_(\d{{6}})\.sas7bdat$",
    ]
    latest_file, latest_date = None, None
    for f in parent.glob(f"{stem}*"):
        if f.suffix != '.sas7bdat':
            continue
        for pattern in patterns:
            m = re.search(pattern, f.name)
            if not m:
                continue
            s = m.group(1)
            try:
                if len(s) == 8:
                    fd = datetime.strptime(s, '%Y%m%d').date()
                else:
                    fd = datetime.strptime(f"{2000 + int(s[:2])}{s[2:]}", '%Y%m%d').date()
                if latest_date is None or fd > latest_date:
                    latest_date, latest_file = fd, f
            except Exception:
                pass
    return latest_file, latest_date


def normalize(df: pl.DataFrame) -> pl.DataFrame:
    """Upper-case column names; convert Date/Datetime columns to SAS day numbers."""
    df = df.rename({c: c.upper() for c in df.columns if c != c.upper()})
    exprs = []
    for c in DATE_COLS:
        if c in df.columns:
            dt = df.schema[c]
            if dt == pl.Datetime or isinstance(dt, pl.Datetime):
                exprs.append((pl.col(c).cast(pl.Date).cast(pl.Int32) + SAS_EPOCH_OFFSET).alias(c))
            elif dt == pl.Date:
                exprs.append((pl.col(c).cast(pl.Int32) + SAS_EPOCH_OFFSET).alias(c))
    return df.with_columns(exprs) if exprs else df


def read_sas(filepath: Path) -> pl.DataFrame:
    print(f"  Reading: {filepath.name}")
    df, _meta = pyreadstat.read_sas7bdat(str(filepath))
    return normalize(pl.from_pandas(df))


def get_sas_file(base_path: Path, y, m, d) -> pl.DataFrame:
    y2, m2, d2, y4 = f"{y % 100:02d}", f"{m:02d}", f"{d:02d}", f"{y:04d}"
    candidates = [
        base_path.parent / f"{base_path.stem}{y4}{m2}{d2}.sas7bdat",
        base_path.parent / f"{base_path.stem}{y2}{m2}{d2}.sas7bdat",
        base_path.parent / f"{base_path.stem}_{y4}{m2}{d2}.sas7bdat",
        base_path.parent / f"{base_path.stem}_{y2}{m2}{d2}.sas7bdat",
    ]
    for fp in candidates:
        if fp.exists():
            print(f"  Using exact file: {fp.name}")
            return read_sas(fp)
    if USE_LATEST_FALLBACK:
        lf, ld = find_latest_file(base_path)
        if lf:
            print(f"  WARNING: Exact file not found. Using latest: {lf.name} (dated {ld})")
            return read_sas(lf)
    raise FileNotFoundError(f"No file found for date {y2}{m2}{d2} at {base_path}")


# ============================================================================
# RECORD PROCESSING
# ============================================================================
def add_record(records, prefix, item, cust, remmth, amount):
    code = get_remfmt(remmth)
    records.append({'BNMCODE': f"{prefix}{item}{cust}{code}0000Y", 'AMOUNT': amount})
    EMITTED.setdefault(prefix, Counter())[code] += 1


def emit_pair(records, item, cust, remmth, amount, days, npl_value):
    """
    SAS:  BNMCODE='95'...; OUTPUT;
          IF DAYS > 89 THEN REMMTH = <npl_value>;
          BNMCODE='93'...; OUTPUT;
    REMMTH stays modified afterwards (SAS does not reset it), so it is returned.
    BA uses 13, TR uses 0.1.
    """
    add_record(records, '95', item, cust, remmth, amount)
    if days is not None and days > 89:
        DIAG['npl_override_fired'] += 1
        remmth = npl_value
    add_record(records, '93', item, cust, remmth, amount)
    return remmth


def process_transaction(records, row, is_ba, reptdate_sas, runoff_sas, runoff_dt):
    tag = 'ba' if is_ba else 'tr'

    # ---- balance ----
    if is_ba:
        fcv, une = num(row.get('FCVALUE')), num(row.get('UNEARNED'))
        balance = fcv - une
        RAW['ba_fcvalue'] += fcv
        RAW['ba_unearned'] += une
    else:
        balance = num(row.get('OUTSTAND'))
    RAW[f'{tag}_balance'] += balance
    DIAG[f'{tag}_rows'] += 1
    if balance == 0:
        DIAG[f'{tag}_zero_balance'] += 1
    elif balance < 0:
        DIAG[f'{tag}_negative_balance'] += 1

    # ---- CUST / ITEM ----
    custcd = coerce_code(first_present(row, 'CUSTCD', 'CUSTCODE'))
    cust = '08' if custcd in ('77', '78', '95', '96') else '09'
    item = '219'
    if num(row.get('PRODUCT')) == 100:
        item = '212'
        DIAG['product_100_override'] += 1
    DIAG[f'cust_{cust}'] += 1
    DIAG[f'item_{item}'] += 1

    # ---- dates ----
    issdte = sas_days(row.get('ISSDTE')) or 0
    exprdate = sas_days(row.get('EXPRDATE'))          # None = missing
    bldate = sas_days(row.get('BLDATE')) or 0
    has_matdate = 'MATDATE' in row
    matdate = sas_days(row.get('MATDATE')) if has_matdate else None

    # ---- DAYS ----
    days = (reptdate_sas - bldate) if bldate > 0 else None
    if days is not None and days > 89:
        DIAG['days_gt_89'] += 1

    payamt = num(row.get('PAYAMT'))
    if payamt == 0:
        DIAG['payamt_zero'] += 1
    elif payamt < 0:
        DIAG['payamt_negative'] += 1
    else:
        DIAG['payamt_positive'] += 1

    npl_value = 13 if is_ba else 0.1
    remmth: Optional[float] = None
    n_before = len(records)

    # =====================================================================
    # IF EXPRDATE <= RUNOFFDT THEN REMMTH = .;
    # ELSE IF EXPRDATE - RUNOFFDT < 8 THEN REMMTH = 0.1;
    # ELSE DO; ... END;
    # =====================================================================
    if exprdate is None or exprdate <= runoff_sas:
        DIAG['expr_missing_or_le_runoff'] += 1
        if exprdate is None:
            DIAG[f'{tag}_expr_null'] += 1
            RAW[f'{tag}_missing_exp_bal'] += balance
        remmth = None
    elif exprdate - runoff_sas < 8:
        DIAG['expr_within_8d_of_runoff'] += 1
        remmth = 0.1
    else:
        DIAG['expr_far'] += 1
        payfreq, freq = '3', 6

        if bldate <= 0:
            bldate = issdte
            DIAG['issdte_used_as_anchor'] += 1
            while bldate <= reptdate_sas:                 # REPTDATE, as in SAS
                bldate = next_bldate(bldate, issdte, freq, payfreq)

        if payamt < 0:
            payamt = 0.0
        if bldate > exprdate or balance <= payamt:
            bldate = exprdate

        while bldate <= exprdate:
            if bldate <= runoff_sas:
                remmth = None
            elif bldate - runoff_sas < 8:
                remmth = 0.1
            else:
                matdt = matdate if has_matdate else bldate     # SAS reads MATDATE
                remmth = calc_remmth(matdt, runoff_dt)

            if (remmth is not None and remmth > 1) or bldate == exprdate:
                break

            balance -= payamt
            remmth = emit_pair(records, item, cust, remmth, payamt, days, npl_value)

            bldate = next_bldate(bldate, issdte, freq, payfreq)
            if bldate > exprdate or balance <= payamt:
                bldate = exprdate

    # ---- final balance record (always emitted in SAS) ----
    if remmth is None:
        DIAG['remmth_missing_final'] += 1
    elif remmth == 0.1:
        DIAG['remmth_0_1_final'] += 1
    else:
        DIAG['remmth_computed_final'] += 1
    emit_pair(records, item, cust, remmth, balance, days, npl_value)

    DIAG[f'{tag}_rows_processed'] += 1
    return len(records) - n_before


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
def dump_diagnostics(dropped_count, dropped_sum):
    print("\n" + "=" * 70)
    print("DIAGNOSTIC REPORT")
    print("=" * 70)

    print("\n--- Row counts ---")
    for k in ('ba_rows', 'ba_rows_processed', 'ba_expr_null',
              'tr_rows', 'tr_rows_processed', 'tr_expr_null'):
        print(f"  {k:28s}: {DIAG[k]}")

    print("\n--- EXPRDATE vs RUNOFFDT branch ---")
    for k in ('expr_missing_or_le_runoff', 'expr_within_8d_of_runoff', 'expr_far'):
        print(f"  {k:28s}: {DIAG[k]}")

    print("\n--- PAYAMT / DAYS ---")
    for k in ('payamt_zero', 'payamt_negative', 'payamt_positive',
              'days_gt_89', 'npl_override_fired'):
        print(f"  {k:28s}: {DIAG[k]}")

    print("\n--- Final REMMTH state ---")
    for k in ('remmth_missing_final', 'remmth_0_1_final', 'remmth_computed_final'):
        print(f"  {k:28s}: {DIAG[k]}")

    print("\n--- CUST / ITEM ---")
    for k in ('cust_08', 'cust_09', 'item_212', 'item_219', 'product_100_override'):
        print(f"  {k:28s}: {DIAG[k]}")

    print("\n--- Raw input totals (BEFORE bucketing) ---")
    print(f"  BA SUM(FCVALUE)          : {RAW['ba_fcvalue']:>20,.2f}")
    print(f"  BA SUM(UNEARNED)         : {RAW['ba_unearned']:>20,.2f}")
    print(f"  BA SUM(FCVALUE-UNEARNED) : {RAW['ba_balance']:>20,.2f}")
    print(f"  BA zero / negative bal   : {DIAG['ba_zero_balance']} / {DIAG['ba_negative_balance']}")
    print(f"  BA EXPRDATE-null balance : {RAW['ba_missing_exp_bal']:>20,.2f}")
    print(f"  TR SUM(OUTSTAND)         : {RAW['tr_balance']:>20,.2f}")
    print(f"  TR zero / negative bal   : {DIAG['tr_zero_balance']} / {DIAG['tr_negative_balance']}")
    print(f"  GRAND RAW TOTAL          : {RAW['ba_balance'] + RAW['tr_balance']:>20,.2f}")

    print("\n--- Emitted records by prefix / REMFMT code ---")
    for prefix in sorted(EMITTED):
        print(f"  prefix {prefix}: " + ", ".join(f"{c}={n}" for c, n in sorted(EMITTED[prefix].items())))

    print("\n--- Dropped as code '07' (missing REMMTH) ---")
    print(f"  records                  : {dropped_count}")
    print(f"  amount (both prefixes)   : {dropped_sum:>20,.2f}")


# ============================================================================
# MAIN
# ============================================================================
def main(reptdate=None, write_sas7bdat_flag=None, sas_cfgname=None):
    if write_sas7bdat_flag is None:
        write_sas7bdat_flag = WRITE_SAS7BDAT
    if sas_cfgname is None:
        sas_cfgname = SAS_CONFIG_NAME

    # NOTE: SAS reads REPTDATE from BNM1.REPTDATE. Pass the same date here.
    reptdate = reptdate or (date.today() - timedelta(days=1))
    rpyr, rpmth, rpday = reptdate.year, reptdate.month, reptdate.day
    reptdate_sas = (reptdate - BASE_DATE).days

    # SAS "DETERMINE RUN-OFF DATE": month-end of REPTDATE
    runoff_dt = date(rpyr, rpmth, days_in_month(rpyr, rpmth))
    runoff_sas = (runoff_dt - BASE_DATE).days

    print("\n" + "=" * 70)
    print("BNM LIQUIDITY REPORT - TRADE FINANCE PROCESSING")
    print("=" * 70)
    print(f"\nReport Date: {reptdate.strftime('%d/%m/%Y')}")
    print(f"Runoff Date: {runoff_dt.strftime('%d/%m/%Y')}  (reference for all REMMTH logic)")

    records = []

    # ---- read BTDTL once (used by both BA and TR) ----
    print("\nReading BTDTL data...")
    btdtl_full = get_sas_file(BTDTL_BASE, rpyr, rpmth, rpday)
    print(f"  BTDTL raw rows: {len(btdtl_full)}")
    print(f"  BTDTL columns : {sorted(btdtl_full.columns)}")
    if 'PAYAMT' not in btdtl_full.columns:
        print("  WARNING: 'PAYAMT' not in BTDTL - defaulting to 0. SAS KEEPs PAYAMT, so "
              "check you are reading the same file/version as SAS (column name/case).")
        btdtl_full = btdtl_full.with_columns(pl.lit(0.0).alias('PAYAMT'))

    # ---- BA ----
    print("\n" + "-" * 50)
    print("PROCESSING BA TRANSACTIONS (Banker's Acceptance)")
    print("-" * 50)
    try:
        # SAS: WHERE ISSDTE > 0 OR EXPRDATE > 0; NODUPKEY BY TRANSREF DESCENDING ISSDTE
        bt = btdtl_full.filter((pl.col('ISSDTE') > 0) | (pl.col('EXPRDATE') > 0))
        print(f"  BTDTL after filter (ISSDTE>0 OR EXPRDATE>0): {len(bt)}")
        bt = (bt.select(['TRANSREF', 'ISSDTE', 'EXPRDATE', 'PAYAMT'])
                .with_columns(pl.col('TRANSREF').cast(pl.Utf8).str.strip_chars_end())
                .sort(['TRANSREF', 'ISSDTE'], descending=[False, True])
                .unique(subset=['TRANSREF', 'ISSDTE'], keep='first', maintain_order=True)
                .with_columns(pl.lit(True).alias('_BT_MATCH')))
        print(f"  BTDTL after NODUPKEY (TRANSREF, ISSDTE): {len(bt)}")

        print("\nReading PBA01 data...")
        pba = get_sas_file(PBA01_BASE, rpyr, rpmth, rpday)
        print(f"  PBA01 raw rows: {len(pba)}")
        print(f"  PBA01 columns : {sorted(pba.columns)}")
        for c in ('FCVALUE', 'UNEARNED', 'CUSTCD', 'CUSTCODE', 'PRODUCT', 'BLDATE', 'MATDATE'):
            print(f"    {c:9s} present in PBA01: {c in pba.columns}")
        if 'MATDATE' not in pba.columns:
            print("  WARNING: MATDATE not in PBA01. SAS %REMMTH reads MATDATE; falling back "
                  "to the instalment date (BLDATE) which will NOT match SAS.")

        pba = pba.with_columns(
            pl.col('TRANSREF').cast(pl.Utf8).str.slice(1, 8).str.strip_chars_end().alias('TRANSREF'))

        # SAS MERGE: variables in BTDTL overwrite same-named PBA01 variables on matched rows
        overlap = [c for c in ('ISSDTE', 'EXPRDATE', 'PAYAMT') if c in pba.columns]
        if overlap:
            pba = pba.rename({c: f"{c}_PBA" for c in overlap})
        ba_data = pba.join(bt, on='TRANSREF', how='left')
        for c in overlap:
            ba_data = ba_data.with_columns(
                pl.when(pl.col('_BT_MATCH').is_not_null()).then(pl.col(c))
                  .otherwise(pl.col(f"{c}_PBA")).alias(c)).drop(f"{c}_PBA")
        print(f"  BA rows after PBA01 x BTDTL merge: {len(ba_data)}")
        print(f"  BA rows with matching BTDTL: {ba_data.filter(pl.col('_BT_MATCH').is_not_null()).height}")

        print("\nProcessing BA records...")
        ba_count = 0
        for row in ba_data.iter_rows(named=True):
            ba_count += process_transaction(records, row, True, reptdate_sas, runoff_sas, runoff_dt)
        print(f"  BA records created: {ba_count}")
    except Exception as e:
        print(f"  BA Processing Error: {e}")
        import traceback
        traceback.print_exc()

    # ---- TR ----
    print("\n" + "-" * 50)
    print("PROCESSING TR TRANSACTIONS (Trade)")
    print("-" * 50)
    try:
        for c in ('OUTSTAND', 'CUSTCD', 'CUSTCODE', 'PRODUCT', 'BLDATE', 'MATDATE', 'LIABCODE', 'DIRCTIND'):
            print(f"    {c:9s} present in BTDTL: {c in btdtl_full.columns}")
        if 'MATDATE' not in btdtl_full.columns:
            print("  WARNING: MATDATE not in BTDTL. SAS %REMMTH reads MATDATE; falling back "
                  "to the instalment date (BLDATE) which will NOT match SAS.")

        pre = btdtl_full.height
        # SAS: LIABCODE NOT IN (...) AND DIRCTIND = 'D'   (missing LIABCODE -> NOT IN is true)
        tr_data = btdtl_full.filter(
            ~pl.col('LIABCODE').cast(pl.Utf8).fill_null('').str.strip_chars().is_in(['BAI', 'BAP', 'BAS', 'BAE']))
        print(f"  After LIABCODE filter: {tr_data.height} (was {pre})")
        pre = tr_data.height
        tr_data = tr_data.filter(pl.col('DIRCTIND').cast(pl.Utf8).str.strip_chars() == 'D')
        print(f"  After DIRCTIND='D' filter: {tr_data.height} (was {pre})")

        print("\nProcessing TR records...")
        tr_count = 0
        for row in tr_data.iter_rows(named=True):
            tr_count += process_transaction(records, row, False, reptdate_sas, runoff_sas, runoff_dt)
        print(f"  TR records created: {tr_count}")
    except Exception as e:
        print(f"  TR Processing Error: {e}")
        import traceback
        traceback.print_exc()

    # ---- FINAL ----
    print("\n" + "-" * 50)
    print("FINAL OUTPUT")
    print("-" * 50)

    dropped_count, dropped_sum = 0, 0.0
    if records:
        df = pl.DataFrame(records)

        # SAS: IF SUBSTR(BNMCODE,8,2) NE '07'
        missing_df = df.filter(pl.col('BNMCODE').str.slice(7, 2) == '07')
        dropped_count = len(missing_df)
        dropped_sum = float(missing_df['AMOUNT'].sum()) if dropped_count else 0.0
        print(f"\n  Records with MISSING remmth (code '07'): {dropped_count}, amount {dropped_sum:,.2f}")

        valid = df.filter(pl.col('BNMCODE').str.slice(7, 2) != '07')
        result = valid.group_by('BNMCODE').agg(pl.col('AMOUNT').sum()).sort('BNMCODE')

        sas7bdat_ok = False
        if write_sas7bdat_flag:
            print(f"\n  Writing SAS7BDAT via saspy: {OUTPUT_SAS7BDAT}")
            sas7bdat_ok = write_sas7bdat(result, OUTPUT_SAS7BDAT.parent,
                                         SAS_OUTPUT_LIBREF, SAS_OUTPUT_DSNAME, sas_cfgname)
        else:
            print("\n  Skipping SAS7BDAT output (disabled).")

        print("\n" + "=" * 70)
        print("SUMMARY")
        print("=" * 70)
        print(f"  Total BNM Codes: {len(result)}")
        print(f"  Total Amount:    {result['AMOUNT'].sum():,.2f}")
        print("\n  Breakdown by BNMCODE:")
        for r in result.iter_rows(named=True):
            print(f"    {r['BNMCODE']}: {r['AMOUNT']:>18,.2f}")

        if reptdate == SAS_REFERENCE_DATE:
            print("\n  Comparison with production SAS bt.sas7bdat (18/10/2026):")
            print(f"    {'BNMCODE':16s}{'PYTHON':>20s}{'SAS':>20s}{'DIFF':>18s}")
            py = {r['BNMCODE']: r['AMOUNT'] for r in result.iter_rows(named=True)}
            for code in sorted(set(py) | set(SAS_REFERENCE)):
                p, s = py.get(code, 0.0), SAS_REFERENCE.get(code, 0.0)
                flag = "" if abs(p - s) < 0.01 else "  <-- DIFF"
                print(f"    {code:16s}{p:>20,.2f}{s:>20,.2f}{p - s:>18,.2f}{flag}")

        if write_sas7bdat_flag:
            print(f"\n  SAS7BDAT: {OUTPUT_SAS7BDAT} [{'OK' if sas7bdat_ok else 'FAILED/SKIPPED'}]")
    else:
        print("\n  No records generated")

    dump_diagnostics(dropped_count, dropped_sum)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='BNM Liquidity Report for Trade Finance')
    parser.add_argument('date', nargs='?', help='Report date YYYY-MM-DD (must equal BNM1.REPTDATE used by SAS; default: yesterday)')
    parser.add_argument('--exact', action='store_true', help='Do not fall back to the latest file')
    parser.add_argument('--latest', action='store_true')
    parser.add_argument('--no-sas7bdat', action='store_true')
    parser.add_argument('--sas-cfg', default=SAS_CONFIG_NAME)
    args = parser.parse_args()

    USE_LATEST_FALLBACK = not args.exact

    rd = None
    if args.date:
        try:
            rd = datetime.strptime(args.date, '%Y-%m-%d').date()
        except ValueError:
            print("Error: Invalid date format. Use YYYY-MM-DD")
            sys.exit(1)

    main(rd, write_sas7bdat_flag=not args.no_sas7bdat, sas_cfgname=args.sas_cfg)
