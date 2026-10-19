#!/usr/bin/env python3
"""
File Name: EIBMLIBT
Loan Maturity Profile Processor (BT)
Processes BTRAD loan data for BNM reporting
Based on original SAS code
Outputs to SAS dataset (.sas7bdat) only

DEBUG VERSION: includes diagnostics for '07' remmth records
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
import calendar
import sys
import warnings

import pyreadstat
import polars as pl
import pandas as pd

# saspy is optional at import time so the rest of the pipeline still works
# even if saspy / a SAS session isn't available.
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
CODE_VERSION = "v3.1-debug (get_report_date + remmth diagnostics)"


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "/dwh/btrade"

# Output file - SAS7BDAT only (lowercase bt.sas7bdat)
BT_SAS_PATH = Path("/dwh/dwh_m/tempsource/PBB_NLF/month/bt.sas7bdat")

# Create output directory
BT_SAS_PATH.parent.mkdir(parents=True, exist_ok=True)

# --- saspy configuration ---
SAS_CONFIG_NAME = "default"
SAS_OUTPUT_LIBREF = "XMISOUT"
SAS_OUTPUT_DSNAME = "bt"
WRITE_SAS7BDAT = True

# Debug: number of DEBUG_07 lines to print before going quiet
DEBUG_07_MAX = 10


# ============================================================================
# SAS DATE CONVERSION
# ============================================================================
BASE_SAS_DATE = date(1960, 1, 1)

def sas_date_to_python(sas_days):
    """Convert SAS numeric date (days since 1960-01-01) to Python date"""
    if sas_days is None or sas_days <= 0:
        return None
    return BASE_SAS_DATE + timedelta(days=int(sas_days))

def python_date_to_sas(py_date):
    """Convert Python date to SAS numeric date (days since 1960-01-01)"""
    if py_date is None:
        return None
    return (py_date - BASE_SAS_DATE).days


# ============================================================================
# REPORT DATE DERIVATION
# ============================================================================

def get_report_date(run_date: date) -> date:
    """
    Derive the actual reporting date from the run date.

    Business rule:
      - If run on the 1st of the month -> use last day of previous month
        (this is the month-end run, e.g. running on 1-Nov-2026 processes the
        October month-end report using btrad10426.sas7bdat)
      - Otherwise -> use the run date itself (mid-month runs on 8th/15th/22nd)
    """
    if run_date.day == 1:
        return run_date - timedelta(days=1)
    return run_date


# ============================================================================
# FORMAT DEFINITIONS (from SAS PROC FORMAT)
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
    """SAS PRDFMT format - map product to HL/RC/FL"""
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
# DATE HELPER FUNCTIONS (from PBBELF macros)
# ============================================================================

def get_days_in_month(year, month):
    """Get days in month, accounting for leap year"""
    if month == 2:
        return 29 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 28
    elif month in [4, 6, 9, 11]:
        return 30
    else:
        return 31


def calculate_next_bldate(bldate, issdte, payfreq, freq):
    """SAS NXTBLDT macro - calculate next billing date"""
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
    """SAS REMMTH macro - calculate remaining months"""
    if matdt is None:
        return None
    
    mdyr = matdt.year
    mdmth = matdt.month
    mdday = matdt.day
    
    if mdday > rpdays:
        mdday = rpdays
    
    remy = mdyr - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday
    
    return remy * 12 + remm + remd / rpdays


def get_week_number(reptdate):
    """Determine week number based on report day"""
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
# SAS7BDAT OUTPUT VIA SASPY
# ============================================================================
def write_sas7bdat(result_pdf: pd.DataFrame, output_dir: Path, libref: str,
                    dsname: str, cfgname: str):
    """Write the final pandas DataFrame out as a native .sas7bdat file."""
    if not SASPY_AVAILABLE:
        print("  WARNING: saspy is not installed - skipping .sas7bdat output.")
        return False

    if result_pdf is None or len(result_pdf) == 0:
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
    """Main execution function - matches SAS DATA NOTE step"""
    if write_sas7bdat_flag is None:
        write_sas7bdat_flag = WRITE_SAS7BDAT
    if sas_cfgname is None:
        sas_cfgname = SAS_CONFIG_NAME

    print("\n" + "=" * 70)
    print("EIBMLIBT - LOAN MATURITY PROFILE PROCESSOR")
    print("=" * 70)
    print(f"CODE VERSION: {CODE_VERSION}")

    try:
        # Step 1: Get report date - derive from today's date if not provided
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
        reptday = reptdate.strftime('%d')
        
        rpyr = reptdate.year
        rpmth = reptdate.month
        rpday = reptdate.day
        rpdays = get_days_in_month(rpyr, rpmth)
        
        print(f"Week Number: {nowk}")
        print(f"Report Month: {reptmon}")
        print(f"Report Year: {reptyear}")
        
        # Step 2: Build BTRAD filename
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
        
        # Step 3: Read SAS file
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
        
        # Debug: First few records
        print("\n  Debug - First few records:")
        sample_rows = df_note.head(5)
        for row in sample_rows.iter_rows(named=True):
            print(f"    CUSTCD: {row.get('CUSTCD')}, PRODCD: {row.get('PRODCD')}, "
                  f"PRODUCT: {row.get('PRODUCT')}, BALANCE: {row.get('BALANCE')}, "
                  f"PAYAMT: {row.get('PAYAMT')}, BLDATE: {row.get('BLDATE')}, "
                  f"ISSDTE: {row.get('ISSDTE')}, EXPRDATE: {row.get('EXPRDATE')}")
        
        # Step 4: Process each record
        output_records = []
        processed = 0
        remmth_distribution = {}
        
        # DEBUG counters
        debug_07_printed = 0
        debug_07_total = 0
        debug_07_by_reason = {
            'exprdate_none': 0,
            'bal_zero': 0,
            'path_A_unreached': 0,
            'path_B_unreached': 0,
            'zero_iter_else': 0,
            'remmth_still_none': 0,
        }
        
        for row in df_note.iter_rows(named=True):
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
                debug_07_by_reason['exprdate_none'] += 1
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
            
            # Track which branch was taken for debugging
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
                        
                        remfmt_code = get_remfmt(remmth)
                        bnmcode = f"95{item}{cust}{remfmt_code}0000Y"
                        output_records.append({"BNMCODE": bnmcode, "AMOUNT": amount})
                        remmth_distribution[remfmt_code] = remmth_distribution.get(remfmt_code, 0) + 1
                        
                        remmth_npl = 13 if days > 89 else remmth
                        bnmcode = f"93{item}{cust}{get_remfmt(remmth_npl)}0000Y"
                        output_records.append({"BNMCODE": bnmcode, "AMOUNT": amount})
                    
                    current_bldate = calculate_next_bldate(current_bldate, issdte, payfreq, freq)
                    
                    if current_bldate > exprdate or current_balance <= payamt:
                        current_bldate = exprdate
                
                balance = current_balance
                bldate = current_bldate
                
                if remmth is None:
                    remmth = calculate_remmth(bldate, reptdate, rpyr, rpmth, rpday, rpdays)
                    if remmth is None:
                        remmth = 0.1
            
            # DEBUG: catch '07' BEFORE it is written
            if remmth is None:
                debug_07_total += 1
                if branch_taken == 'A':
                    debug_07_by_reason['path_A_unreached'] += 1
                elif branch_taken == 'B' and loop_iters == 0:
                    debug_07_by_reason['zero_iter_else'] += 1
                else:
                    debug_07_by_reason['remmth_still_none'] += 1
                
                if debug_07_printed < DEBUG_07_MAX:
                    debug_07_printed += 1
                    print(f"\nDEBUG_07 #{debug_07_printed}:")
                    print(f"    custcd={custcd!r} prodcd={prodcd!r} product={product!r}")
                    print(f"    balance={balance!r} payamt={payamt!r}")
                    print(f"    bldate={bldate} issdte={issdte} exprdate={exprdate}")
                    print(f"    days_to_expiry={days_to_expiry} days_past_due={days}")
                    print(f"    branch_taken={branch_taken} loop_iters={loop_iters}")
                    print(f"    remmth={remmth!r}")
            
            # Output final balance
            amount = balance
            if amount == 0:
                debug_07_by_reason['bal_zero'] += 1
                continue
            
            if amount != 0:
                remfmt_code = get_remfmt(remmth)
                bnmcode = f"95{item}{cust}{remfmt_code}0000Y"
                output_records.append({"BNMCODE": bnmcode, "AMOUNT": amount})
                remmth_distribution[remfmt_code] = remmth_distribution.get(remfmt_code, 0) + 1
                
                remmth_npl = 13 if days > 89 else remmth
                bnmcode = f"93{item}{cust}{get_remfmt(remmth_npl)}0000Y"
                output_records.append({"BNMCODE": bnmcode, "AMOUNT": amount})
            
            processed += 1
            if processed % 1000 == 0:
                print(f"  Processed {processed} records...")
        
        print(f"\n  Total records processed: {processed}")
        print(f"  Output records created: {len(output_records)}")
        
        # DEBUG summary
        print(f"\n  DEBUG SUMMARY (records producing remmth=None at final output):")
        print(f"    Total DEBUG_07 hits: {debug_07_total}")
        print(f"    By reason:")
        for reason, count in debug_07_by_reason.items():
            print(f"      {reason}: {count}")
        
        # Remmth distribution
        print("\n  Remmth code distribution:")
        for code, count in sorted(remmth_distribution.items()):
            print(f"    Code {code}: {count} records")
        
        if len(output_records) == 0:
            print("  No output records generated.")
            return 0
        
        # Step 5: Aggregate
        df_output = pl.DataFrame(output_records)
        
        print("\n  Unique BNMCODEs before filtering:")
        unique_codes = df_output.select('BNMCODE').unique().sort('BNMCODE')
        for row in unique_codes.iter_rows():
            print(f"    {row[0]}")
        
        df_summary = df_output.group_by('BNMCODE').agg([
            pl.col('AMOUNT').sum()
        ]).sort('BNMCODE')
        
        df_summary = df_summary.filter(pl.col('BNMCODE').str.slice(7, 2) != '07')
        
        missing_count = len(df_output) - len(df_summary)
        if missing_count > 0:
            print(f"\n  Records with missing remmth (code '07'): {missing_count}")
        
        print("\n  Final aggregated records:")
        for row in df_summary.iter_rows(named=True):
            print(f"    {row['BNMCODE']}: {row['AMOUNT']:,.2f}")
        
        df_pandas = df_summary.to_pandas()
        
        # Step 6: Write SAS dataset
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
        print("PROCESSING COMPLETED SUCCESSFULLY")
        print("=" * 70)
        print(f"\nOutput SAS dataset: {BT_SAS_PATH} [{'OK' if sas7bdat_ok else 'FAILED/SKIPPED'}]")
        print(f"Total BNM codes: {len(df_summary)}")
        print(f"Total amount: {df_summary['AMOUNT'].sum():,.2f}")
        
        return 0 if sas7bdat_ok else 1
        
    except FileNotFoundError as e:
        print(f"\nFILE NOT FOUND ERROR: {e}", file=sys.stderr)
        print("\nExpected file pattern: btrad{MM}{WK}{YY}.sas7bdat")
        print("Example: btrad06426.sas7bdat for Month=06, Week=4, Year=2026")
        return 1
    except Exception as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


# ============================================================================
# COMMAND LINE INTERFACE
# ============================================================================
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='EIBMLIBT - Loan Maturity Profile Processor',
        epilog='Example: python EIBMLIBT.py 2026-11-08'
    )
    parser.add_argument('date', nargs='?',
                         help='Report date in YYYY-MM-DD format (overrides auto-derivation)')
    parser.add_argument('--no-sas7bdat', action='store_true',
                         help='Skip writing the .sas7bdat output via saspy')
    parser.add_argument('--sas-cfg', default=SAS_CONFIG_NAME,
                         help=f'saspy config name to use (default: {SAS_CONFIG_NAME})')
    
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
