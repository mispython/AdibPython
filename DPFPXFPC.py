#!/usr/bin/env python3
"""
File Name: EIBMLIBT
Loan Maturity Profile Processor (BT)
Processes BTRAD loan data for BNM reporting
Based on original SAS code
Outputs to SAS dataset and Parquet formats
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
import saspy

warnings.filterwarnings('ignore')


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod"
OUTPUT_DIR = BASE_DIR / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMLIBT"

# Output files
BT_SAS_PATH = OUTPUT_DIR / "BT.sas7bdat"
BT_PARQUET_PATH = OUTPUT_DIR / "BT.parquet"
BT_REPORT_PATH = OUTPUT_DIR / "BT_REPORT.txt"

# Create output directory
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


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
        # Fortnightly - add 14 days
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
        # Monthly/quarterly - use issue date day
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
    
    # Adjust day if it exceeds days in month
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
# MAIN PROCESSING
# ============================================================================

def main(reptdate=None):
    """Main execution function - matches SAS DATA NOTE step"""
    print("\n" + "=" * 70)
    print("EIBMLIBT - LOAN MATURITY PROFILE PROCESSOR")
    print("=" * 70)

    try:
        # Step 1: Get report date - use today's date if not provided
        if reptdate is None:
            reptdate = datetime.now().date()
            print(f"\nUsing today's date: {reptdate.strftime('%d/%m/%Y')}")
        elif isinstance(reptdate, datetime):
            reptdate = reptdate.date()
        
        # Derive macro variables
        nowk = get_week_number(reptdate)
        reptyear = reptdate.strftime('%Y')
        reptmon = reptdate.strftime('%m')
        reptday = reptdate.strftime('%d')
        
        # Calculate runoff date variables (for REMMTH macro)
        rpyr = reptdate.year
        rpmth = reptdate.month
        rpday = reptdate.day
        rpdays = get_days_in_month(rpyr, rpmth)
        
        print(f"\nReport Date: {reptdate.strftime('%d/%m/%Y')}")
        print(f"Week Number: {nowk}")
        print(f"Report Month: {reptmon}")
        print(f"Report Year: {reptyear}")
        
        # Step 2: Build BTRAD filename: btrad{MM}{WK}{YY}.sas7bdat
        # Format: btrad{MM}{WK}{YY} where YY is last 2 digits of year
        year_suffix = reptyear[2:]  # Get last 2 digits of year
        btrad_filename = f"btrad{reptmon}{nowk}{year_suffix}.sas7bdat"
        btrad_path = INPUT_DIR / btrad_filename
        
        # Also try without year suffix for backward compatibility
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
        
        # Convert to Polars and filter
        df_pl = pl.from_pandas(df)
        print(f"  Total records read: {len(df_pl)}")
        
        # Apply filter: SUBSTR(PRODCD,1,2) = '34' OR PRODUCT IN (225,226)
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
        
        # Debug: Check data types and values
        print("\n  Debug - First few records:")
        sample_rows = df_note.head(5)
        for row in sample_rows.iter_rows(named=True):
            print(f"    CUSTCD: {row.get('CUSTCD')}, PRODCD: {row.get('PRODCD')}, "
                  f"PRODUCT: {row.get('PRODUCT')}, BALANCE: {row.get('BALANCE')}, "
                  f"PAYAMT: {row.get('PAYAMT')}, BLDATE: {row.get('BLDATE')}, "
                  f"ISSDTE: {row.get('ISSDTE')}, EXPRDATE: {row.get('EXPRDATE')}")
        
        # Step 4: Process each record (matches SAS DATA NOTE step)
        output_records = []
        processed = 0
        remmth_distribution = {}
        
        for row in df_note.iter_rows(named=True):
            # Get values
            custcd = str(row.get("CUSTCD", ""))
            prodcd = str(row.get("PRODCD", ""))
            
            # Get product (try PRODUCT first, then PRODCD)
            product = row.get("PRODUCT")
            if product is None or product == 0:
                try:
                    product = int(prodcd) if prodcd else 0
                except (ValueError, TypeError):
                    product = 0
            
            balance = float(row.get("BALANCE", 0) or 0)
            payamt = float(row.get("PAYAMT", 0) or 0)
            
            # Convert SAS dates to Python dates
            bldate = sas_date_to_python(row.get("BLDATE"))
            issdte = sas_date_to_python(row.get("ISSDTE"))
            exprdate = sas_date_to_python(row.get("EXPRDATE"))
            
            if exprdate is None:
                continue
            
            # Determine CUST (matches SAS logic)
            if custcd in ['77', '78', '95', '96']:
                cust = '08'
            else:
                cust = '09'
            
            # In the SAS code: PROD = 'BT' (hardcoded)
            # Therefore, the item is always '219' because:
            # - For CUST='08': PROD='BT' doesn't match 'HL' -> OTHERWISE -> '219'
            # - For CUST='09': PROD='BT' doesn't match 'FL' or 'RC' -> OTHERWISE -> '219'
            item = '219'
            
            # Calculate DAYS past due (only if BLDATE > 0)
            days = 0
            if bldate is not None:
                days = (reptdate - bldate).days
            
            # Initialize remmth
            remmth = None
            
            # Calculate days to expiry
            days_to_expiry = (exprdate - reptdate).days
            
            # Process maturity profile (matches SAS IF-ELSE logic)
            if days_to_expiry < 8:
                # Less than 8 days to expiry
                remmth = 0.1
            else:
                payfreq = '3'
                freq = 6  # For payfreq = '3'
                
                # RC products use expiry date as billing date
                if product in [350, 910, 925]:
                    bldate = exprdate
                elif bldate is None:
                    # If no billing date, use issue date and calculate forward
                    bldate = issdte
                    if bldate is not None:
                        while bldate <= reptdate:
                            bldate = calculate_next_bldate(bldate, issdte, payfreq, freq)
                
                # Ensure bldate is not None
                if bldate is None:
                    # Use expiry date as fallback
                    bldate = exprdate
                
                if payamt < 0:
                    payamt = 0
                
                if bldate > exprdate or balance <= payamt:
                    bldate = exprdate
                
                current_balance = balance
                current_bldate = bldate
                
                # Process payment schedule
                while current_bldate <= exprdate:
                    # Calculate remaining months
                    matdt = current_bldate
                    remmth = calculate_remmth(matdt, reptdate, rpyr, rpmth, rpday, rpdays)
                    
                    if remmth is None:
                        break
                    
                    if remmth > 12 or current_bldate == exprdate:
                        break
                    
                    if payamt > 0:
                        amount = payamt
                        current_balance -= payamt
                        
                        # Part 2-RM (95)
                        remfmt_code = get_remfmt(remmth)
                        bnmcode = f"95{item}{cust}{remfmt_code}0000Y"
                        output_records.append({"BNMCODE": bnmcode, "AMOUNT": amount})
                        remmth_distribution[remfmt_code] = remmth_distribution.get(remfmt_code, 0) + 1
                        
                        # Part 1-RM (93) - NPL if days > 89
                        remmth_npl = 13 if days > 89 else remmth
                        bnmcode = f"93{item}{cust}{get_remfmt(remmth_npl)}0000Y"
                        output_records.append({"BNMCODE": bnmcode, "AMOUNT": amount})
                    
                    # Calculate next billing date
                    current_bldate = calculate_next_bldate(current_bldate, issdte, payfreq, freq)
                    
                    if current_bldate > exprdate or current_balance <= payamt:
                        current_bldate = exprdate
                
                # Use final balance and billing date
                balance = current_balance
                bldate = current_bldate
                
                # Calculate final remmth if not set
                if remmth is None:
                    remmth = calculate_remmth(bldate, reptdate, rpyr, rpmth, rpday, rpdays)
                    if remmth is None:
                        remmth = 0.1  # Default to 0.1 if still None
            
            # Output final balance (matches final OUTPUT statements)
            amount = balance
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
        
        # Print remmth distribution for debugging
        print("\n  Remmth code distribution:")
        for code, count in sorted(remmth_distribution.items()):
            print(f"    Code {code}: {count} records")
        
        if len(output_records) == 0:
            print("  No output records generated.")
            return 0
        
        # Step 5: Aggregate (matches PROC SUMMARY NWAY)
        df_output = pl.DataFrame(output_records)
        
        # Print unique BNMCODEs before filtering
        print("\n  Unique BNMCODEs before filtering:")
        unique_codes = df_output.select('BNMCODE').unique().sort('BNMCODE')
        for row in unique_codes.iter_rows():
            print(f"    {row[0]}")
        
        df_summary = df_output.group_by('BNMCODE').agg([
            pl.col('AMOUNT').sum()
        ]).sort('BNMCODE')
        
        # Filter out missing remmth (code '07')
        df_summary = df_summary.filter(pl.col('BNMCODE').str.slice(7, 2) != '07')
        
        missing_count = len(df_output) - len(df_summary)
        if missing_count > 0:
            print(f"\n  Records with missing remmth (code '07'): {missing_count}")
        
        # Print final summary
        print("\n  Final aggregated records:")
        for row in df_summary.iter_rows(named=True):
            print(f"    {row['BNMCODE']}: {row['AMOUNT']:,.2f}")
        
        # Convert to pandas for SAS output
        df_pandas = df_summary.to_pandas()
        
        # Step 6: Write output to SAS dataset using saspy
        print(f"\nWriting SAS dataset to: {BT_SAS_PATH}")
        
        sas = None
        try:
            # Initialize SAS session
            sas = saspy.SASsession(cfgname='default')
            
            # Method 1: Use the sasdata() method with DataFrame as first argument
            sas.sasdata(df_pandas, 'BT', 'WORK')
            
            # Copy to permanent location
            sas.submit(f'''
                libname out "{OUTPUT_DIR}";
                data out.BT;
                    set WORK.BT;
                run;
            ''')
            
            print(f"  SAS dataset written successfully using saspy")
            
        except Exception as e:
            print(f"  Warning: Could not write SAS dataset using saspy: {e}")
            print(f"  Attempting alternative method...")
            
            # Alternative: Use CSV import method
            try:
                # Write CSV as intermediate
                temp_csv = OUTPUT_DIR / "temp_BT.csv"
                df_pandas.to_csv(temp_csv, index=False)
                
                # If SAS session doesn't exist, create one
                if sas is None:
                    sas = saspy.SASsession(cfgname='default')
                
                # Use SAS to read CSV and create dataset
                sas.submit(f'''
                    proc import datafile="{temp_csv}"
                        out=temp_data
                        dbms=csv
                        replace;
                    run;
                    
                    libname out "{OUTPUT_DIR}";
                    data out.BT;
                        set temp_data;
                    run;
                    
                    proc datasets library=work nolist;
                        delete temp_data;
                    run;
                ''')
                
                # Clean up temp file
                temp_csv.unlink()
                print(f"  SAS dataset written successfully using CSV import")
                
            except Exception as e2:
                print(f"  Error writing SAS dataset: {e2}")
                print("  Continuing with Parquet output only...")
        
        # Step 7: Write output to Parquet
        print(f"\nWriting Parquet file to: {BT_PARQUET_PATH}")
        df_summary.write_parquet(BT_PARQUET_PATH)
        print(f"  Parquet file written successfully")
        
        # Step 8: Generate report
        print(f"\nWriting report to: {BT_REPORT_PATH}")
        with open(BT_REPORT_PATH, 'w') as f:
            f.write("1LOAN MATURITY PROFILE REPORT\n")
            f.write(" " + "=" * 60 + "\n")
            f.write(" " + f"{'BNMCODE':<20}{'AMOUNT':>20}\n")
            f.write(" " + "-" * 60 + "\n")
            
            total = 0
            for row in df_summary.iter_rows(named=True):
                amount = float(row['AMOUNT'])
                total += amount
                f.write(" " + f"{row['BNMCODE']:<20}{amount:>20.2f}\n")
            
            f.write(" " + "-" * 60 + "\n")
            f.write(" " + f"{'TOTAL':<20}{total:>20.2f}\n")
            f.write(" " + "=" * 60 + "\n")
        
        print("\n" + "=" * 70)
        print("PROCESSING COMPLETED SUCCESSFULLY")
        print("=" * 70)
        print(f"\nOutput SAS dataset: {BT_SAS_PATH}")
        print(f"Output Parquet file: {BT_PARQUET_PATH}")
        print(f"Report file: {BT_REPORT_PATH}")
        print(f"Total BNM codes: {len(df_summary)}")
        print(f"Total amount: {total:,.2f}")
        
        # Clean up SAS session if it exists
        if sas is not None:
            try:
                sas.terminate()
            except:
                pass
        
        return 0
        
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
        epilog='Example: python EIBMLIBT.py 2026-08-08'
    )
    parser.add_argument('date', nargs='?', help='Report date in YYYY-MM-DD format')
    
    args = parser.parse_args()
    
    reptdate = None
    if args.date:
        try:
            reptdate = datetime.strptime(args.date, '%Y-%m-%d').date()
            print(f"Using command line date: {reptdate}")
        except ValueError:
            print(f"Error: Invalid date format. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        # Use today's date when no date is provided
        reptdate = datetime.now().date()
        print(f"No date provided - using today's date: {reptdate.strftime('%Y-%m-%d')}")
    
    sys.exit(main(reptdate))
