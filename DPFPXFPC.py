#!/usr/bin/env python3
"""
File Name: EIIMABTL
Islamic Loan Maturity Profile Processor (IBT)
Processes IBTRAD loan data for BNM reporting
Based on original SAS code for Islamic products
"""

from datetime import date, datetime, timedelta
from pathlib import Path
import calendar
import sys
import warnings

import pyreadstat
import polars as pl

warnings.filterwarnings('ignore')


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "/dwh/ibtrade"
OUTPUT_DIR = BASE_DIR / "/host/mis/output/report"

# Output file
NLFBT = OUTPUT_DIR / "EIIMABTL_ISLAMIC_NLFBT.txt"

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


# ============================================================================
# FORMAT DEFINITIONS (from SAS PROC FORMAT)
# ============================================================================

def get_remfmt(remmth):
    """
    SAS REMFMT format - map remaining months to code
    Based on the EIIMABTL format (same as EIBMABTL but with different threshold in main logic)
    """
    if remmth is None:
        return '07'
    elif remmth <= 0.1:
        return '01'
    elif remmth <= 1:
        return '02'
    elif remmth <= 3:
        return '03'
    elif remmth <= 6:
        return '04'
    elif remmth <= 12:
        return '05'
    elif remmth <= 36:
        return '06'
    elif remmth <= 60:
        return '07'
    else:
        return '08'


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
# MAIN PROCESSING
# ============================================================================

def main(reptdate=None):
    """Main execution function - matches SAS DATA NOTE step for Islamic products"""
    print("\n" + "=" * 70)
    print("EIIMABTL - ISLAMIC LOAN MATURITY PROFILE PROCESSOR")
    print("=" * 70)

    try:
        # Step 1: Get report date
        if reptdate is None:
            reptdate = date(2025, 8, 8)
            print("\nTESTING MODE: Using fixed date: {}".format(reptdate))
        elif isinstance(reptdate, datetime):
            reptdate = reptdate.date()
        
        # Derive macro variables
        nowk = get_week_number(reptdate)
        reptyear = reptdate.strftime('%Y')
        reptmon = reptdate.strftime('%m')
        reptday = reptdate.strftime('%d')
        yy_2digit = reptdate.strftime('%y')  # 2-digit year for filename
        
        # Calculate runoff date variables (for REMMTH macro)
        rpyr = reptdate.year
        rpmth = reptdate.month
        rpday = reptdate.day
        rpdays = get_days_in_month(rpyr, rpmth)
        
        print("\nReport Date: {}".format(reptdate.strftime('%d/%m/%Y')))
        print("Week Number: {}".format(nowk))
        print("Report Month: {}".format(reptmon))
        print("Report Year: {}".format(reptyear))
        print("2-Digit Year: {}".format(yy_2digit))
        
        # Step 2: Build IBTRAD filename: ibtrad{MM}{WK}{YY}.sas7bdat
        # Example: ibtrad08125.sas7bdat (Month=08, Week=1, Year=2025)
        ibtrad_filename = "ibtrad{}{}{}.sas7bdat".format(reptmon, nowk, yy_2digit)
        ibtrad_path = INPUT_DIR / ibtrad_filename
        
        if not ibtrad_path.exists():
            ibtrad_filename_upper = "IBTRAD{}{}{}.sas7bdat".format(reptmon, nowk, yy_2digit)
            ibtrad_path = INPUT_DIR / ibtrad_filename_upper
        
        print("\nLooking for IBTRAD file: {}".format(ibtrad_path.name))
        
        if not ibtrad_path.exists():
            raise FileNotFoundError("IBTRAD file not found: {}".format(ibtrad_path))
        
        # Step 3: Read SAS file
        print("  Reading SAS file...")
        df, meta = pyreadstat.read_sas7bdat(str(ibtrad_path))
        df_pl = pl.from_pandas(df)
        print("  Total records read: {}".format(len(df_pl)))
        
        # Step 4: Filter for loan products
        # SUBSTR(PRODCD,1,2) = '34' OR PRODUCT IN (225,226)
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
        print("  Records after filtering: {}".format(len(df_note)))
        
        if len(df_note) == 0:
            print("  No records after filtering.")
            return 0
        
        # Step 5: Process each record (matches SAS DATA NOTE step)
        output_records = []
        processed = 0
        
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
            
            # Determine PROD type using PRDFMT
            prod_type = get_prdfmt(product)
            
            # Determine ITEM (matches SAS SELECT logic)
            if custcd in ['77', '78', '95', '96']:
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
            
            # Calculate DAYS past due (only if BLDATE > 0)
            days = 0
            if bldate is not None and bldate > 0:
                days = (reptdate - bldate).days
            
            # Initialize
            remmth = None
            
            # Process maturity profile (matches SAS IF-ELSE logic)
            # KEY DIFFERENCE: Islamic uses > 60 threshold (not > 12)
            if (exprdate - reptdate).days < 8:
                remmth = 0.1
            else:
                payfreq = '3'
                freq = 6  # For payfreq = '3'
                
                # RC products use expiry date as billing date
                if product in [350, 910, 925]:
                    bldate = exprdate
                elif bldate is None or bldate <= 0:
                    bldate = issdte
                    if bldate is not None:
                        while bldate <= reptdate:
                            bldate = calculate_next_bldate(bldate, issdte, payfreq, freq)
                
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
                    
                    # KEY DIFFERENCE: Islamic uses > 60 (not > 12)
                    if remmth > 60 or current_bldate == exprdate:
                        break
                    
                    if payamt > 0:
                        amount = payamt
                        current_balance -= payamt
                        
                        # Islamic uses same 95/93 prefixes (no FCY distinction in this version)
                        bnmcode = "95{}{}{}0000Y".format(item, cust, get_remfmt(remmth))
                        output_records.append({
                            "BNMCODE": bnmcode,
                            "AMOUNT": amount
                        })
                        
                        remmth_npl = 13 if days > 89 else remmth
                        bnmcode = "93{}{}{}0000Y".format(item, cust, get_remfmt(remmth_npl))
                        output_records.append({
                            "BNMCODE": bnmcode,
                            "AMOUNT": amount
                        })
                    
                    # Calculate next billing date
                    current_bldate = calculate_next_bldate(current_bldate, issdte, payfreq, freq)
                    
                    if current_bldate > exprdate or current_balance <= payamt:
                        current_bldate = exprdate
                
                # Use final balance and billing date
                balance = current_balance
                bldate = current_bldate
            
            # Output final balance (matches final OUTPUT statements)
            # Islamic uses same 95/93 prefixes
            bnmcode = "95{}{}{}0000Y".format(item, cust, get_remfmt(remmth))
            output_records.append({
                "BNMCODE": bnmcode,
                "AMOUNT": balance
            })
            
            remmth_npl = 13 if days > 89 else remmth
            bnmcode = "93{}{}{}0000Y".format(item, cust, get_remfmt(remmth_npl))
            output_records.append({
                "BNMCODE": bnmcode,
                "AMOUNT": balance
            })
            
            processed += 1
            if processed % 1000 == 0:
                print("  Processed {} records...".format(processed))
        
        print("\n  Total records processed: {}".format(processed))
        print("  Output records created: {}".format(len(output_records)))
        
        if len(output_records) == 0:
            print("  No output records generated.")
            return 0
        
        # Step 6: Aggregate (matches PROC SUMMARY NWAY)
        df_output = pl.DataFrame(output_records)
        
        df_summary = df_output.group_by('BNMCODE').agg([
            pl.col('AMOUNT').sum()
        ]).sort('BNMCODE')
        
        # Filter out missing remmth (code '07')
        df_summary = df_summary.filter(pl.col('BNMCODE').str.slice(7, 2) != '07')
        
        missing_count = len(df_output) - len(df_summary)
        if missing_count > 0:
            print("\n  Records with missing remmth (code '07'): {}".format(missing_count))
        
        # Step 7: Write output file (matches DATA _NULL_ step)
        print("\nWriting output to: {}".format(NLFBT))
        with open(NLFBT, 'w') as f:
            # Write header: INLFBT{DD}{MM}{YYYY} (I for Islamic)
            f.write("INLFBT{}{}{}\n".format(reptday, reptmon, reptyear))
            
            # Write data rows: BNMCODE;AMOUNT;
            for row in df_summary.iter_rows(named=True):
                f.write("{};{:.2f};\n".format(row['BNMCODE'], row['AMOUNT']))
        
        print("\n" + "=" * 70)
        print("PROCESSING COMPLETED SUCCESSFULLY")
        print("=" * 70)
        print("\nOutput file: {}".format(NLFBT))
        print("Total BNM codes: {}".format(len(df_summary)))
        print("Total amount: {:.2f}".format(df_summary['AMOUNT'].sum()))
        
        return 0
        
    except FileNotFoundError as e:
        print("\nFILE NOT FOUND ERROR: {}".format(e), file=sys.stderr)
        print("\nExpected file pattern: ibtrad{MM}{WK}{YY}.sas7bdat")
        print("Example: ibtrad08125.sas7bdat for Month=08, Week=1, Year=2025")
        return 1
    except Exception as exc:
        print("\nERROR: {}".format(exc), file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


# ============================================================================
# COMMAND LINE INTERFACE
# ============================================================================
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='EIIMABTL - Islamic Loan Maturity Profile Processor',
        epilog='Example: python EIIMABTL.py 2025-08-08'
    )
    parser.add_argument('date', nargs='?', help='Report date in YYYY-MM-DD format')
    
    args = parser.parse_args()
    
    reptdate = None
    if args.date:
        try:
            reptdate = datetime.strptime(args.date, '%Y-%m-%d').date()
            print("Using command line date: {}".format(reptdate))
        except ValueError:
            print("Error: Invalid date format. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        print("No date provided - using August 8, 2025 for testing")
        reptdate = date(2026, 6, 8)
    
    sys.exit(main(reptdate))
