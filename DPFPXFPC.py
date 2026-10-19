#!/usr/bin/env python3
"""
File Name: EIBMLIBT
Loan Maturity Profile Processor (BT)
Processes BTRAD loan data for BNM reporting
Based on original SAS code
Outputs to SAS dataset (.sas7bdat) only

DEBUG VERSION v3.2: pinpoint where '07' codes come from
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
import calendar
import sys
import warnings
from collections import Counter

import pyreadstat
import polars as pl
import pandas as pd

try:
    import saspy
    SASPY_AVAILABLE = True
except ImportError:
    saspy = None
    SASPY_AVAILABLE = False

warnings.filterwarnings('ignore')


# ============================================================================
# CODE VERSION MARKER
# ============================================================================
CODE_VERSION = "v3.2-debug (pinpoint '07' sources)"


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "/dwh/btrade"

BT_SAS_PATH = Path("/dwh/dwh_m/tempsource/PBB_NLF/month/bt.sas7bdat")
BT_SAS_PATH.parent.mkdir(parents=True, exist_ok=True)

SAS_CONFIG_NAME = "default"
SAS_OUTPUT_LIBREF = "XMISOUT"
SAS_OUTPUT_DSNAME = "bt"
WRITE_SAS7BDAT = True

# Debug: number of detailed sample lines to print
DEBUG_SAMPLE_MAX = 5


# ============================================================================
# SAS DATE CONVERSION
# ============================================================================
BASE_SAS_DATE = date(1960, 1, 1)

def sas_date_to_python(sas_days):
    if sas_days is None or sas_days <= 0:
        return None
    return BASE_SAS_DATE + timedelta(days=int(sas_days))


def python_date_to_sas(py_date):
    if py_date is None:
        return None
    return (py_date - BASE_SAS_DATE).days


# ============================================================================
# REPORT DATE DERIVATION
# ============================================================================
def get_report_date(run_date: date) -> date:
    if run_date.day == 1:
        return run_date - timedelta(days=1)
    return run_date


# ============================================================================
# FORMAT DEFINITIONS
# ============================================================================
def get_remfmt(remmth):
    """SAS REMFMT format - map remaining months to code"""
    if remmth is None:
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


def get_prdfmt(product):
    hl_products = {4,5,6,7,31,32,100,101,102,103,110,111,112,113,114,115,
                   116,170,200,201,204,205,209,210,211,212,214,215,219,220,
                   225,226,227,228,229,230,231,232,233,234}
    rc_products = {350,910,925}
    if product in hl_products:
        return 'HL'
    if product in rc_products:
        return 'RC'
    return 'FL'


# ============================================================================
# DATE HELPERS
# ============================================================================
def get_days_in_month(year, month):
    if month == 2:
        return 29 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 28
    elif month in [4, 6, 9, 11]:
        return 30
    else:
        return 31


def calculate_next_bldate(bldate, issdte, payfreq, freq):
    if payfreq == '6':
        dd = bldate.day + 14
        mm = bldate.month
        yy = bldate.year
        days_in_month = get_days_in_month(yy, mm)
        if dd > days_in_month:
            dd = dd - days_in_month
            mm += 1
            if mm > 12:
                mm = mm - 12
                yy += 1
        return date(yy, mm, dd)
    else:
        dd = issdte.day
        mm = bldate.month + freq
        yy = bldate.year
        if mm > 12:
            mm = mm - 12
            yy += 1
        days_in_month = get_days_in_month(yy, mm)
        if dd > days_in_month:
            dd = days_in_month
        return date(yy, mm, dd)


def calculate_remmth(matdt, reptdate, rpyr, rpmth, rpday, rpdays):
    if matdt is None:
        return None
    mdyr = matdt.year
    mdmth = matdt.month
    mdday = matdt.day
    if mdday > rpdays:
        mdday = rpdays
    return (mdyr - rpyr) * 12 + (mdmth - rpmth) + (mdday - rpday) / rpdays


def get_week_number(reptdate):
    day = reptdate.day
    if day == 8:
        return "1"
    elif day == 15:
        return "2"
    elif day == 22:
        return "3"
    else:
        return "4"


# ============================================================================
# SAS7BDAT WRITE
# ============================================================================
def write_sas7bdat(result_pdf, output_dir, libref, dsname, cfgname):
    if not SASPY_AVAILABLE:
        print("  WARNING: saspy not installed - skipping .sas7bdat output.")
        return False
    if result_pdf is None or len(result_pdf) == 0:
        print("  No records to write - skipping .sas7bdat output.")
        return False
    sas = None
    try:
        print(f"\n  Starting SAS session (cfgname='{cfgname}')...")
        sas = saspy.SASsession(cfgname=cfgname)
        print(f"  Assigning library {libref} -> {output_dir}")
        sas.saslib(libref, path=str(output_dir))
        print(f"  Uploading DataFrame to SAS dataset {libref}.{dsname} "
              f"({len(result_pdf)} rows)...")
        sas.df2sd(df=result_pdf, table=dsname, libref=libref)
        out_file = output_dir / f"{dsname.lower()}.sas7bdat"
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
# MAIN PROCESSING
# ============================================================================
def main(reptdate=None, write_sas7bdat_flag=None, sas_cfgname=None):
    if write_sas7bdat_flag is None:
        write_sas7bdat_flag = WRITE_SAS7BDAT
    if sas_cfgname is None:
        sas_cfgname = SAS_CONFIG_NAME

    print("\n" + "=" * 70)
    print("EIBMLIBT - LOAN MATURITY PROFILE PROCESSOR")
    print("=" * 70)
    print(f"CODE VERSION: {CODE_VERSION}")

    try:
        # ---------- Step 1: Report date ----------
        if reptdate is None:
            run_date = datetime.now().date()
            reptdate = get_report_date(run_date)
            print(f"\nRun date:    {run_date.strftime('%d/%m/%Y')}")
            print(f"Report date: {reptdate.strftime('%d/%m/%Y')} (derived)")
        elif isinstance(reptdate, datetime):
            reptdate = reptdate.date()
            print(f"\nReport date: {reptdate.strftime('%d/%m/%Y')} (from CLI)")
        else:
            print(f"\nReport date: {reptdate.strftime('%d/%m/%Y')} (from CLI)")

        nowk = get_week_number(reptdate)
        reptyear = reptdate.strftime('%Y')
        reptmon = reptdate.strftime('%m')
        rpyr = reptdate.year
        rpmth = reptdate.month
        rpday = reptdate.day
        rpdays = get_days_in_month(rpyr, rpmth)

        print(f"Week Number: {nowk}")
        print(f"Report Month: {reptmon}")
        print(f"Report Year: {reptyear}")

        # ---------- Step 2: Input file ----------
        year_suffix = reptyear[2:]
        btrad_filename = f"btrad{reptmon}{nowk}{year_suffix}.sas7bdat"
        btrad_path = INPUT_DIR / btrad_filename
        if not btrad_path.exists():
            btrad_filename_alt = f"btrad{reptmon}{nowk}.sas7bdat"
            btrad_path_alt = INPUT_DIR / btrad_filename_alt
            if btrad_path_alt.exists():
                btrad_path = btrad_path_alt
                btrad_filename = btrad_filename_alt
        print(f"\nLooking for BTRAD file: {btrad_path.name}")
        if not btrad_path.exists():
            raise FileNotFoundError(f"BTRAD file not found: {btrad_path}")

        # ---------- Step 3: Read input ----------
        print("  Reading SAS file...")
        df, meta = pyreadstat.read_sas7bdat(str(btrad_path))
        df_pl = pl.from_pandas(df)
        print(f"  Total records read: {len(df_pl)}")

        if "PRODUCT" in df_pl.columns:
            df_note = df_pl.filter(
                (pl.col("PRODCD").cast(pl.Utf8).str.slice(0, 2) == "34") |
                (pl.col("PRODUCT").is_in([225, 226]))
            )
        else:
            df_note = df_pl.filter(
                (pl.col("PRODCD").cast(pl.Utf8).str.slice(0, 2) == "34") |
                (pl.col("PRODCD").cast(pl.Utf8).is_in(["225", "226"]))
            )
        print(f"  Records after filtering: {len(df_note)}")
        if len(df_note) == 0:
            print("  No records after filtering.")
            return 0

        # ---------- Step 4: Process each record ----------
        output_records = []
        processed = 0
        remmth_distribution = Counter()

        # ---------- DEBUG COUNTERS ----------
        # Every output record's remfmt gets tallied here, regardless of source
        debug_remfmt_by_source = Counter()   # source -> remfmt -> count
        debug_samples_printed = 0
        debug_07_source = Counter()          # which branch produced a '07'
        debug_totals = {
            'records_in': 0,
            'records_exprdate_none': 0,
            'records_bal_zero': 0,
            'records_processed': 0,
            'bnm_codes_generated': 0,
        }

        for row in df_note.iter_rows(named=True):
            debug_totals['records_in'] += 1

            custcd = str(row.get("CUSTCD", ""))
            prodcd = str(row.get("PRODCD", ""))
            product = row.get("PRODUCT")
            if product is None or product == 0:
                try:
                    product = int(prodcd) if prodcd else 0
                except (ValueError, TypeError):
                    product = 0

            balance = float(row.get("BALANCE", 0) or 0)
            payamt = float(row.get("PAYAMT", 0) or 0)

            bldate = sas_date_to_python(row.get("BLDATE"))
            issdte = sas_date_to_python(row.get("ISSDTE"))
            exprdate = sas_date_to_python(row.get("EXPRDATE"))

            if exprdate is None:
                debug_totals['records_exprdate_none'] += 1
                continue

            if custcd in ['77', '78', '95', '96']:
                cust = '08'
            else:
                cust = '09'
            item = '219'

            days = 0
            if bldate is not None:
                days = (reptdate - bldate).days

            remmth = None
            days_to_expiry = (exprdate - reptdate).days
            branch_taken = None
            loop_iters = 0

            if days_to_expiry < 8:
                remmth = 0.1
                branch_taken = 'A'
            else:
                branch_taken = 'B'
                payfreq = '3'
                freq = 6
                if product in [350, 910, 925]:
                    bldate = exprdate
                elif bldate is None:
                    bldate = issdte
                    if bldate is not None:
                        while bldate <= reptdate:
                            bldate = calculate_next_bldate(bldate, issdte, payfreq, freq)
                if bldate is None:
                    bldate = exprdate
                if payamt < 0:
                    payamt = 0
                if bldate > exprdate or balance <= payamt:
                    bldate = exprdate

                current_balance = balance
                current_bldate = bldate

                while current_bldate <= exprdate:
                    loop_iters += 1
                    matdt = current_bldate
                    remmth = calculate_remmth(matdt, reptdate, rpyr, rpmth, rpday, rpdays)
                    if remmth is None:
                        break
                    if remmth > 12 or current_bldate == exprdate:
                        break
                    if payamt > 0:
                        amount = payamt
                        current_balance -= payamt
                        # 95-branch
                        remfmt_code = get_remfmt(remmth)
                        bnmcode = f"95{item}{cust}{remfmt_code}0000Y"
                        output_records.append({"BNMCODE": bnmcode, "AMOUNT": amount})
                        remmth_distribution[remfmt_code] += 1
                        debug_remfmt_by_source[('95', 'while_loop')] = debug_remfmt_by_source.get(('95', 'while_loop'), 0) + 0
                        # track per code
                        debug_remfmt_by_source[('95', remfmt_code)] = debug_remfmt_by_source.get(('95', remfmt_code), 0) + 1
                        # 93-branch (note: NPL remfmt via get_remfmt(13))
                        remmth_npl = 13 if days > 89 else remmth
                        remfmt_npl = get_remfmt(remmth_npl)
                        bnmcode = f"93{item}{cust}{remfmt_npl}0000Y"
                        output_records.append({"BNMCODE": bnmcode, "AMOUNT": amount})
                        debug_remfmt_by_source[('93', remfmt_npl)] = debug_remfmt_by_source.get(('93', remfmt_npl), 0) + 1
                    current_bldate = calculate_next_bldate(current_bldate, issdte, payfreq, freq)
                    if current_bldate > exprdate or current_balance <= payamt:
                        current_bldate = exprdate

                balance = current_balance
                bldate = current_bldate

                if remmth is None:
                    remmth = calculate_remmth(bldate, reptdate, rpyr, rpmth, rpday, rpdays)
                    if remmth is None:
                        remmth = 0.1

            # Diagnostic snapshot: what are we about to output for this record?
            if debug_samples_printed < DEBUG_SAMPLE_MAX:
                debug_samples_printed += 1
                print(f"\n--- SAMPLE RECORD #{debug_samples_printed} ---")
                print(f"  custcd={custcd!r} prodcd={prodcd!r} product={product}")
                print(f"  balance={balance!r} payamt={payamt!r} days={days}")
                print(f"  bldate={bldate} issdte={issdte} exprdate={exprdate}")
                print(f"  days_to_expiry={days_to_expiry} branch={branch_taken} loop_iters={loop_iters}")
                print(f"  remmth={remmth!r}  get_remfmt(remmth)={get_remfmt(remmth)!r}")

            # ---------- Final output ----------
            amount = balance
            if amount == 0:
                debug_totals['records_bal_zero'] += 1
                continue

            remfmt_code = get_remfmt(remmth)
            bnmcode = f"95{item}{cust}{remfmt_code}0000Y"
            output_records.append({"BNMCODE": bnmcode, "AMOUNT": amount})
            remmth_distribution[remfmt_code] += 1
            debug_remfmt_by_source[('95', remfmt_code)] += 1
            debug_totals['bnm_codes_generated'] += 1

            remmth_npl = 13 if days > 89 else remmth
            remfmt_npl = get_remfmt(remmth_npl)
            bnmcode = f"93{item}{cust}{remfmt_npl}0000Y"
            output_records.append({"BNMCODE": bnmcode, "AMOUNT": amount})
            debug_remfmt_by_source[('93', remfmt_npl)] += 1
            debug_totals['bnm_codes_generated'] += 1

            if remfmt_code == '07' or remfmt_npl == '07':
                debug_07_source[(branch_taken, loop_iters, remfmt_code, remfmt_npl)] += 1

            processed += 1
            debug_totals['records_processed'] += 1
            if processed % 5000 == 0:
                print(f"  Processed {processed} records...")

        # ---------- Step 5: Debug report ----------
        print("\n" + "=" * 70)
        print("DEBUG REPORT")
        print("=" * 70)
        print(f"  Totals: {debug_totals}")
        print(f"\n  remfmt distribution BY SOURCE (branch, remfmt):")
        for (source, remfmt), count in sorted(debug_remfmt_by_source.items()):
            print(f"    {source}: remfmt='{remfmt}' -> {count} records")

        print(f"\n  '07' sources (branch, loop_iters, remfmt_95, remfmt_93):")
        for key, count in sorted(debug_07_source.items()):
            print(f"    {key}: {count} records")

        print(f"\n  remmth_distribution (95-branch only, in-loop + final):")
        for code, count in sorted(remmth_distribution.items()):
            print(f"    Code {code}: {count}")

        print("\n" + "=" * 70)
        print("CONTINUING TO AGGREGATION")
        print("=" * 70)

        if len(output_records) == 0:
            print("  No output records generated.")
            return 0

        # ---------- Step 6: Aggregate ----------
        df_output = pl.DataFrame(output_records)
        print(f"\n  Raw output rows: {len(df_output)}")

        # Sample BNMCODEs to inspect structure
        print("\n  Sample BNMCODEs (raw, pre-filter):")
        sample = df_output.head(10)
        for r in sample.iter_rows(named=True):
            code = r['BNMCODE']
            print(f"    '{code}' (len={len(code)})  slice(7,2)='{code[7:9]}'")

        # Show distinct slice(7,2) values BEFORE filter
        print("\n  Distinct slice(7,2) values BEFORE filter (raw):")
        df_codes_check = df_output.with_columns(
            pl.col('BNMCODE').str.slice(7, 2).alias('REMFMT')
        )
        dist = df_codes_check.group_by('REMFMT').agg(pl.len().alias('COUNT')).sort('REMFMT')
        for r in dist.iter_rows(named=True):
            print(f"    REMFMT='{r['REMFMT']}': {r['COUNT']} rows")

        df_summary = df_output.group_by('BNMCODE').agg([
            pl.col('AMOUNT').sum()
        ]).sort('BNMCODE')

        # Apply filter
        df_summary = df_summary.filter(pl.col('BNMCODE').str.slice(7, 2) != '07')
        print(f"\n  After aggregation + filter: {len(df_summary)} codes")

        print("\n  Final aggregated records:")
        for row in df_summary.iter_rows(named=True):
            print(f"    {row['BNMCODE']}: {row['AMOUNT']:,.2f}")

        df_pandas = df_summary.to_pandas()

        # ---------- Step 7: Write SAS dataset ----------
        sas7bdat_ok = False
        if write_sas7bdat_flag:
            print(f"\nWriting SAS dataset to: {BT_SAS_PATH}")
            sas7bdat_ok = write_sas7bdat(
                result_pdf=df_pandas,
                output_dir=BT_SAS_PATH.parent,
                libref=SAS_OUTPUT_LIBREF,
                dsname=SAS_OUTPUT_DSNAME,
                cfgname=sas_cfgname,
            )
        else:
            print("\nSkipping SAS7BDAT output (disabled).")

        print("\n" + "=" * 70)
        print("PROCESSING COMPLETED")
        print("=" * 70)
        print(f"\nOutput SAS dataset: {BT_SAS_PATH} [{'OK' if sas7bdat_ok else 'FAILED/SKIPPED'}]")
        print(f"Total BNM codes: {len(df_summary)}")
        print(f"Total amount: {df_summary['AMOUNT'].sum():,.2f}")

        return 0 if sas7bdat_ok else 1

    except FileNotFoundError as e:
        print(f"\nFILE NOT FOUND ERROR: {e}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


# ============================================================================
# CLI
# ============================================================================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description='EIBMLIBT - Loan Maturity Profile Processor (DEBUG)',
        epilog='Example: python EIBMLIBT.py 2026-10-19'
    )
    parser.add_argument('date', nargs='?', help='Report date in YYYY-MM-DD format')
    parser.add_argument('--no-sas7bdat', action='store_true', help='Skip SAS7BDAT output')
    parser.add_argument('--sas-cfg', default=SAS_CONFIG_NAME,
                         help=f'saspy config name (default: {SAS_CONFIG_NAME})')
    args = parser.parse_args()

    reptdate = None
    if args.date:
        try:
            reptdate = datetime.strptime(args.date, '%Y-%m-%d').date()
            print(f"Using command line report date: {reptdate}")
        except ValueError:
            print(f"Error: Invalid date format. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        print("No report date provided - will derive from today's run date")

    sys.exit(main(
        reptdate,
        write_sas7bdat_flag=not args.no_sas7bdat,
        sas_cfgname=args.sas_cfg,
    ))
