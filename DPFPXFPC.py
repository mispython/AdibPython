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
import calendar
from typing import Optional

# PBBLNFMT.py is left untouched. This program's CUST/ITEM logic (see
# process_transaction below) matches the real SAS EIBDLIBT source directly
# and does NOT use format_btcustcd/format_liqpfmt - those formats belong to
# other BNM programs, not this one.


# ============================================================================
# LOCAL FORMAT HELPERS (kept in EIBDLIBT.py so PBBLNFMT.py stays untouched)
# ============================================================================
def get_days_in_month(year: int, month: int) -> int:
    """Return the number of days in the given year/month.
    Equivalent to SAS RPDAYS(RPMTH) array lookup built via %DCLVAR."""
    return calendar.monthrange(year, month)[1]


def get_remfmt(remmth: Optional[float]) -> str:
    """Format remaining months to maturity into a 2-char BNM REMFMT code.

    Equivalent to SAS:
        PROC FORMAT;
           VALUE REMFMT
              LOW-0.1 = '01'   /*  UP TO 1 WK       */
              0.1-1   = '02'   /*  >1 WK - 1 MTH    */
              1-3     = '03'   /*  >1 MTH - 3 MTHS  */
              3-6     = '04'   /*  >3 - 6 MTHS      */
              6-12    = '05'   /*  >6 MTHS - 1 YR   */
                 .    = '07'   /*  MISSING          */
              OTHER   = '06';  /*  > 1 YEAR         */

    NOTE: SAS resolves the overlap at the 0.1 boundary in favor of the
    earlier-declared range (LOW-0.1), so remmth == 0.1 maps to '01', not '02'.
    """
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
    # Fallback definitions if PBBELF not available
    def calculate_next_bldate(bldate, issdte, payfreq, freq):
        """Calculate next billing date (NXTBLDT macro fallback)"""
        if payfreq == '6':
            # Fortnightly - add 14 days
            dd, mm, yy = bldate.day + 14, bldate.month, bldate.year
            dim = get_days_in_month(yy, mm)
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
            dim = get_days_in_month(yy, mm)
            return date(yy, mm, min(dd, dim))

    def calculate_remmth(matdate, runoff_dt, rpyr, rpmth, rpday):
        """Calculate remaining months to maturity (REMMTH macro fallback)"""
        rpdays = get_days_in_month(rpyr, rpmth)
        mdday = min(matdate.day, rpdays)
        remy = matdate.year - rpyr
        remm = matdate.month - rpmth
        remd = mdday - rpday
        return remy * 12 + remm + remd / rpdays

# saspy is optional at import time so the rest of the pipeline still works
# (parquet/csv output) even if saspy / a SAS session isn't available.
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
INPUT_DIR = BASE_DIR / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod"
OUTPUT_DIR = BASE_DIR / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input SAS files (without date suffix - will be appended)
BTDTL_BASE = INPUT_DIR / "btdtl"
PBA01_BASE = INPUT_DIR / "pba01"

# Output files
OUTPUT_PARQUET = OUTPUT_DIR / "bt.parquet"
OUTPUT_CSV = OUTPUT_DIR / "bt.csv"
OUTPUT_SAS7BDAT = OUTPUT_DIR / "bt.sas7bdat"
OUTPUT_LOG = OUTPUT_DIR / "bt_processing.log"

BASE_DATE = date(1960, 1, 1)
USE_LATEST_FALLBACK = True

# --- saspy configuration ---
# Name of the SAS config entry defined in your sascfg_personal.py
# (e.g. "default", "oda", "iomlinux" etc.). Override via --sas-cfg on CLI.
SAS_CONFIG_NAME = "default"

# Libref/dataset name to use for the SAS-side output dataset
SAS_OUTPUT_LIBREF = "XMISOUT"
SAS_OUTPUT_DSNAME = "bt"

# Whether to attempt writing the .sas7bdat output by default
WRITE_SAS7BDAT = True


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
        # BA: balance = FCVALUE - UNEARNED (SAS: BALANCE = FCVALUE - UNEARNED;)
        # NOTE: SAS does not skip zero-balance rows - it still falls through
        # to OUTPUT with AMOUNT = BALANCE (possibly 0). Matched exactly here.
        amount_key = (row.get('FCVALUE', 0) or 0) - (row.get('UNEARNED', 0) or 0)
    else:
        # TR: use OUTSTAND (SAS reads OUTSTAND directly from BTDTL)
        # Same note as above - SAS does not skip zero-OUTSTAND rows.
        amount_key = row.get('OUTSTAND', 0) or 0

    # --- CUST: matches SAS exactly -----------------------------------------
    # SAS:  IF CUSTCD IN ('77','78','95','96') THEN CUST = '08';
    #       ELSE CUST = '09';
    # NOTE: this is a plain binary code - NOT a BTCUSTCD/PBBLNFMT lookup.
    # Real BTDTL schema uses 'CUSTCODE' rather than 'CUSTCD'; fall back to
    # 'CUSTCD' for compatibility with any table that still uses that name.
    custcd_raw = row.get('CUSTCODE', row.get('CUSTCD', None))
    custcd = str(custcd_raw).strip() if custcd_raw is not None else ''
    is_individual_group = custcd in ('77', '78', '95', '96')
    cust = '08' if is_individual_group else '09'

    # --- ITEM: matches SAS exactly ------------------------------------------
    # SAS:  PROD = 'BT';   -- hardcoded literal, NOT a LIQPFMT lookup
    #       SELECT (PROD) WHEN ('HL'/'FL'/'RC') ... OTHERWISE ITEM = '219';
    # Since PROD is always the literal 'BT', it never matches 'HL'/'FL'/'RC',
    # so ITEM is always '219' regardless of the CUSTCD branch, UNLESS the
    # PRODUCT = 100 override below applies.
    # TODO: confirm the real column name that plays the role of SAS PRODUCT
    # (used only for this ==100 override) - not present under this name in
    # the BTDTL schema seen so far.
    item = '219'

    product = row.get('PRODUCT', None)

    # Hardcode override for product 100 (SAS: IF PRODUCT = 100 THEN ITEM='212';)
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
# SAS7BDAT OUTPUT VIA SASPY
# ============================================================================
def write_sas7bdat(result_df: pl.DataFrame, output_dir: Path, libref: str,
                    dsname: str, cfgname: str, log_path: Path):
    """
    Write the final Polars result DataFrame out as a native .sas7bdat file
    using saspy. This starts a SAS session, assigns a library pointing at
    `output_dir`, uploads the data as a SAS dataset, then ends the session.
    The library assignment (fileref pointing at output_dir) is what causes
    SAS to physically write <dsname>.sas7bdat into that folder.
    """
    if not SASPY_AVAILABLE:
        msg = "  WARNING: saspy is not installed - skipping .sas7bdat output."
        print(msg)
        with open(log_path, 'a') as log:
            log.write(f"{datetime.now()}: {msg}\n")
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

        # saspy works with pandas, so convert from Polars -> pandas
        pdf = result_df.to_pandas()

        print(f"  Uploading DataFrame to SAS dataset {libref}.{dsname} "
              f"({len(pdf)} rows)...")
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
        with open(log_path, 'a') as log:
            log.write(f"{datetime.now()}: SAS7BDAT write failed: {e}\n")
        return False

    finally:
        if sas is not None:
            try:
                sas.endsas()
            except Exception:
                pass


# ============================================================================
# MAIN PROCESSING
# ============================================================================
def main(reptdate=None, write_sas7bdat_flag=None, sas_cfgname=None):
    global USE_LATEST_FALLBACK

    if write_sas7bdat_flag is None:
        write_sas7bdat_flag = WRITE_SAS7BDAT
    if sas_cfgname is None:
        sas_cfgname = SAS_CONFIG_NAME

    # Step 1: Set report date
    # Source files (BTDTL/PBA01) are produced the day before processing runs,
    # so the default report date is "yesterday", not today.
    reptdate = reptdate or (date.today() - timedelta(days=1))
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

        # PAYAMT is not present in every BTDTL extract. When missing, treat
        # it as 0 for all rows (process_transaction already zeroes out any
        # negative/absent PAYAMT), and log it instead of crashing the run.
        if 'PAYAMT' not in btdtl.columns:
            warn_msg = ("  WARNING: 'PAYAMT' column not found in BTDTL - "
                        "defaulting to 0 for all rows.")
            print(warn_msg)
            with open(OUTPUT_LOG, 'a') as log:
                log.write(f"{datetime.now()}: {warn_msg.strip()}\n")
            btdtl = btdtl.with_columns(pl.lit(0).alias('PAYAMT'))

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

        # Write native SAS dataset via saspy
        sas7bdat_ok = False
        if write_sas7bdat_flag:
            print(f"\n  Writing SAS7BDAT via saspy: {OUTPUT_SAS7BDAT}")
            sas7bdat_ok = write_sas7bdat(
                result_df=result,
                output_dir=OUTPUT_DIR,
                libref=SAS_OUTPUT_LIBREF,
                dsname=SAS_OUTPUT_DSNAME,
                cfgname=sas_cfgname,
                log_path=OUTPUT_LOG,
            )
        else:
            print("\n  Skipping SAS7BDAT output (disabled).")

        # Summary
        total_amount = result['AMOUNT'].sum() if len(result) > 0 else 0

        print("\n" + "=" * 70)
        print("PROCESSING COMPLETE")
        print("=" * 70)
        print(f"\nOutput files:")
        print(f"  Parquet:  {OUTPUT_PARQUET}")
        print(f"  CSV:      {OUTPUT_CSV}")
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


# ============================================================================
# COMMAND LINE INTERFACE
# ============================================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='BNM Liquidity Report for Trade Finance')
    parser.add_argument('date', nargs='?', help='Report date in YYYY-MM-DD format (default: yesterday)')
    parser.add_argument('--exact', action='store_true', help='Require exact date match (no fallback)')
    parser.add_argument('--latest', action='store_true', help='Use latest file if exact not found (default)')
    parser.add_argument('--no-sas7bdat', action='store_true',
                         help='Skip writing the .sas7bdat output via saspy')
    parser.add_argument('--sas-cfg', default=SAS_CONFIG_NAME,
                         help=f'saspy config name to use (default: {SAS_CONFIG_NAME})')

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
    main(
        reptdate,
        write_sas7bdat_flag=not args.no_sas7bdat,
        sas_cfgname=args.sas_cfg,
    )
