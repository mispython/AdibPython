#!/usr/bin/env python3
"""
File Name: EIBMABTL
Loan Maturity Profile Processor (BT)
Processes BTRAD loan data for BNM reporting
"""

from datetime import date, datetime, timedelta
from pathlib import Path
import calendar
import sys
import warnings
from typing import Optional

import pyreadstat
import polars as pl

# Import from PBBLNFMT (format definitions)
try:
    from PBBLNFMT import (
        get_remfmt,
        get_days_in_month,
        format_liqpfmt,
        format_btcustcd,
    )
    print("Successfully imported from PBBLNFMT")
except ImportError as e:
    print("Warning: Could not import from PBBLNFMT, using local fallback: {}".format(e))
    # Local fallback for get_remfmt
    def get_remfmt(remmth):
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
        else:
            return '06'
    
    def get_days_in_month(year, month):
        if month == 2:
            return 29 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 28
        elif month in [4, 6, 9, 11]:
            return 30
        else:
            return 31
    
    def format_liqpfmt(product):
        hl_products = [4,5,6,7,31,32,100,101,102,103,110,111,112,113,114,115,116,170,
                       200,201,204,205,209,210,211,212,214,215,219,220,225,226,227,228,
                       229,230,231,232,233,234]
        rc_products = [350,910,925]
        if product in hl_products:
            return 'HL'
        elif product in rc_products:
            return 'RC'
        return 'FL'
    
    def format_btcustcd(custcode):
        cust_map = {
            1:'11',2:'02',3:'03',4:'04',5:'05',6:'06',10:'10',11:'11',12:'12',
            13:'13',15:'15',17:'17',20:'20',30:'30',31:'31',32:'32',33:'33',
            34:'34',35:'35',36:'06',37:'37',38:'38',39:'39',40:'40',41:'41',
            42:'42',43:'43',44:'44',46:'46',47:'47',48:'48',49:'49',50:'50',
            51:'51',52:'52',53:'53',54:'54',57:'57',59:'59',60:'60',61:'61',
            62:'62',63:'63',64:'64',65:'65',66:'66',67:'67',68:'68',69:'69',
            70:'70',71:'71',72:'72',73:'73',74:'74',75:'75',76:'76',77:'77',
            78:'78',79:'79',80:'80',81:'81',82:'82',83:'83',84:'84',85:'85',
            86:'86',87:'87',88:'88',89:'89',90:'90',91:'91',92:'92',95:'95',
            96:'96',98:'98',99:'99'
        }
        return cust_map.get(custcode, '79')

# Import from PBBELF (macro functions)
try:
    from PBBELF import (
        calculate_next_bldate,
        calculate_remmth,
    )
    print("Successfully imported from PBBELF")
except ImportError as e:
    print("Warning: Could not import from PBBELF, using local fallback: {}".format(e))
    # Local fallback for calculate_next_bldate
    def calculate_next_bldate(bldate, issdte, payfreq, freq):
        if payfreq == '6':
            return bldate + timedelta(days=14)
        else:
            dd = issdte.day
            mm = bldate.month + freq
            yy = bldate.year
            while mm > 12:
                mm -= 12
                yy += 1
            days_in_target = get_days_in_month(yy, mm)
            dd = min(dd, days_in_target)
            return date(yy, mm, dd)
    
    def calculate_remmth(matdt, reptdate_val):
        rpyr = reptdate_val.year
        rpmth = reptdate_val.month
        rpday = reptdate_val.day
        rpdays = get_days_in_month(rpyr, rpmth)
        mdyr = matdt.year
        mdmth = matdt.month
        mdday = matdt.day
        if mdday > rpdays:
            mdday = rpdays
        remy = mdyr - rpyr
        remm = mdmth - rpmth
        remd = mdday - rpday
        return remy * 12 + remm + remd / rpdays

warnings.filterwarnings('ignore')


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod"
OUTPUT_DIR = BASE_DIR / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMABTL"

# Output file
NLFBT = OUTPUT_DIR / "nlfbt.txt"

# Create output directory
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_week_number(reptdate):
    """Determine week number based on report day."""
    day = reptdate.day
    if day == 8:
        return "1"
    elif day == 15:
        return "2"
    elif day == 22:
        return "3"
    else:
        return "4"


def categorize_remmth(remmth):
    """Categorize remaining months into BNM codes."""
    if remmth is None:
        return '07'
    elif remmth < 0.1:
        return '01'
    elif remmth < 1:
        return '02'
    elif remmth < 3:
        return '03'
    elif remmth < 6:
        return '04'
    elif remmth < 12:
        return '05'
    elif remmth < 36:
        return '06'
    elif remmth < 60:
        return '07'
    else:
        return '08'


def to_date(value):
    """Convert SAS numeric date to Python date."""
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, (int, float)):
        if value <= 0:
            return None
        return date(1960, 1, 1) + timedelta(days=int(value))
    return None


# ============================================================================
# MAIN PROCESSING
# ============================================================================

def main(reptdate=None):
    """Main execution function."""
    print("\n" + "=" * 70)
    print("EIBMABTL - LOAN MATURITY PROFILE PROCESSOR")
    print("=" * 70)

    try:
        # Step 1: Get report date
        if reptdate is None:
            #reptdate = date.today()
            reptdate = datetime(2026, 6, 8)
            print("\nTESTING MODE: Using today's date: {}".format(reptdate))
        
        # Derive macro variables
        nowk = get_week_number(reptdate)
        reptyear = reptdate.strftime('%Y')
        reptmon = reptdate.strftime('%m')
        reptday = reptdate.strftime('%d')
        
        print("\nReport Date: {}".format(reptdate.strftime('%d/%m/%Y')))
        print("Week Number: {}".format(nowk))
        print("Report Month: {}".format(reptmon))
        print("Report Year: {}".format(reptyear))
        
        # Step 2: Build BTRAD filename: btrad{MM}{WK}{YY}.sas7bdat
        yy_2digit = reptdate.strftime('%y')
        btrad_filename = "btrad{}{}{}.sas7bdat".format(reptmon, nowk, yy_2digit)
        btrad_path = INPUT_DIR / btrad_filename
        
        # Also try uppercase
        if not btrad_path.exists():
            btrad_filename_upper = "btrad{}{}{}.sas7bdat".format(reptmon, nowk, yy_2digit)
            btrad_path = INPUT_DIR / btrad_filename_upper
        
        print("\nLooking for BTRAD file: {}".format(btrad_path.name))
        
        if not btrad_path.exists():
            raise FileNotFoundError("BTRAD file not found: {}".format(btrad_path))
        
        # Step 3: Read SAS file
        print("  Reading SAS file...")
        df, meta = pyreadstat.read_sas7bdat(str(btrad_path))
        df_pl = pl.from_pandas(df)
        print("  Total records read: {}".format(len(df_pl)))
        
        # Step 4: Filter for loan products (using PRODCD instead of PRODUCT)
        # PRODCD starts with '34' OR PRODCD in ['225', '226']
        df_note = df_pl.filter(
            (pl.col("PRODCD").cast(pl.Utf8).str.slice(0, 2) == "34") | 
            (pl.col("PRODCD").cast(pl.Utf8).is_in(["225", "226"]))
        )
        print("  Records after filtering: {}".format(len(df_note)))
        
        if len(df_note) == 0:
            print("  No records after filtering.")
            return 0
        
        # Step 5: Add CUST column using format_btcustcd from PBBLNFMT
        cust_codes = []
        for row in df_note.iter_rows(named=True):
            custcd = row.get("CUSTCD", 0)
            if custcd is None:
                custcd = 0
            cust_codes.append(format_btcustcd(custcd))
        df_note = df_note.with_columns(pl.Series("CUST", cust_codes))
        
        # Step 6: Add PROD_TYPE column using format_liqpfmt from PBBLNFMT
        prod_types = []
        for row in df_note.iter_rows(named=True):
            prodcd = row.get("PRODCD", 0)
            # Try to convert PRODCD to integer for product mapping
            try:
                product = int(prodcd) if prodcd else 0
            except (ValueError, TypeError):
                product = 0
            prod_types.append(format_liqpfmt(product))
        df_note = df_note.with_columns(pl.Series("PROD_TYPE", prod_types))
        
        # Step 7: Add ITEM column based on product type and customer
        def get_item_code(prod_type, custcd):
            if str(custcd) in ['77', '78', '95', '96']:
                if prod_type == "HL":
                    return "214"
                else:
                    return "219"
            else:
                if prod_type == "FL":
                    return "211"
                elif prod_type == "RC":
                    return "212"
                else:
                    return "219"
        
        items = []
        for row in df_note.iter_rows(named=True):
            custcd = row.get("CUSTCD", "")
            prod_type = row.get("PROD_TYPE", "FL")
            items.append(get_item_code(prod_type, custcd))
        df_note = df_note.with_columns(pl.Series("ITEM", items))
        
        # Hardcode override for product 100 (but we don't have PRODUCT column, using PRODCD)
        df_note = df_note.with_columns([
            pl.when(pl.col("PRODCD").cast(pl.Utf8) == "100")
              .then(pl.lit("212"))
              .otherwise(pl.col("ITEM"))
              .alias("ITEM")
        ])
        
        # Step 8: Calculate DAYS past due
        reptdate_sas = (reptdate - date(1960, 1, 1)).days
        df_note = df_note.with_columns([
            pl.when(pl.col("BLDATE") > 0)
              .then(reptdate_sas - pl.col("BLDATE"))
              .otherwise(0)
              .alias("DAYS")
        ])
        
        # Step 9: Process each record
        records = []
        processed = 0
        
        for row in df_note.iter_rows(named=True):
            # Extract values
            custcd = str(row.get("CUSTCD", ""))
            item = row.get("ITEM", "219")
            cust = row.get("CUST", "79")
            balance = float(row.get("BALANCE", 0) or 0)
            payamt = float(row.get("PAYAMT", 0) or 0)
            days = row.get("DAYS", 0) or 0
            prodcd = str(row.get("PRODCD", ""))
            forcurr = str(row.get("FORCURR", ""))
            
            # Convert product to integer for RC check
            try:
                product = int(prodcd) if prodcd else 0
            except (ValueError, TypeError):
                product = 0
            
            # Convert dates
            bldate = to_date(row.get("BLDATE"))
            issdte = to_date(row.get("ISSDTE"))
            exprdate = to_date(row.get("EXPRDATE"))
            
            if exprdate is None:
                continue
            
            remmth = None
            current_balance = balance
            current_bldate = bldate
            
            # Process maturity profile
            if (exprdate - reptdate).days < 8:
                remmth = 0.1
            else:
                payfreq = '3'
                
                # RC products use expiry date as billing date
                if product in [350, 910, 925]:
                    current_bldate = exprdate
                elif not current_bldate or current_bldate <= reptdate:
                    current_bldate = issdte
                    if current_bldate is None:
                        current_bldate = reptdate
                    while current_bldate <= reptdate:
                        current_bldate = calculate_next_bldate(current_bldate, issdte, payfreq, 6)
                
                if payamt < 0:
                    payamt = 0
                
                if current_bldate > exprdate or current_balance <= payamt:
                    current_bldate = exprdate
                
                # Process payment schedule
                while current_bldate <= exprdate:
                    matdt = current_bldate
                    remmth = calculate_remmth(matdt, reptdate)
                    
                    if remmth > 12 or current_bldate == exprdate:
                        break
                    
                    remmth_code = categorize_remmth(remmth)
                    
                    # Determine prefix based on currency/product (PRODCD starting with '346' = FCY)
                    if prodcd[:3] == '346':
                        prefix1 = '96'
                        prefix2 = '94'
                    else:
                        prefix1 = '95'
                        prefix2 = '93'
                    
                    # Add payment records
                    records.append({
                        'BNMCODE': "{}{}{}{}0000Y".format(prefix1, item, cust, remmth_code),
                        'AMOUNT': payamt,
                        'AMTUSD': 0,
                        'AMTSGD': 0
                    })
                    
                    remmth_overdue = 13 if days > 89 else remmth
                    remmth_code_overdue = categorize_remmth(remmth_overdue)
                    records.append({
                        'BNMCODE': "{}{}{}{}0000Y".format(prefix2, item, cust, remmth_code_overdue),
                        'AMOUNT': payamt,
                        'AMTUSD': 0,
                        'AMTSGD': 0
                    })
                    
                    current_balance -= payamt
                    current_bldate = calculate_next_bldate(current_bldate, issdte, payfreq, 6)
                    
                    if current_bldate > exprdate or current_balance <= payamt:
                        current_bldate = exprdate
            
            # Calculate final remmth for remaining balance
            remmth_final = remmth if remmth is not None else 0.1
            remmth_code_final = categorize_remmth(remmth_final)
            
            # Determine currency amounts
            amtusd = current_balance if forcurr == 'USD' else 0
            amtsgd = current_balance if forcurr == 'SGD' else 0
            
            # Determine prefix based on currency/product
            if prodcd[:3] == '346':
                prefix1 = '96'
                prefix2 = '94'
            else:
                prefix1 = '95'
                prefix2 = '93'
            
            # Add final balance records
            records.append({
                'BNMCODE': "{}{}{}{}0000Y".format(prefix1, item, cust, remmth_code_final),
                'AMOUNT': current_balance,
                'AMTUSD': amtusd,
                'AMTSGD': amtsgd
            })
            
            remmth_overdue_final = 13 if days > 89 else remmth_final
            remmth_code_overdue_final = categorize_remmth(remmth_overdue_final)
            records.append({
                'BNMCODE': "{}{}{}{}0000Y".format(prefix2, item, cust, remmth_code_overdue_final),
                'AMOUNT': current_balance,
                'AMTUSD': amtusd,
                'AMTSGD': amtsgd
            })
            
            processed += 1
            if processed % 5000 == 0:
                print("  Processed {} records...".format(processed))
        
        print("  Total records processed: {}".format(processed))
        print("  Output records created: {}".format(len(records)))
        
        if len(records) == 0:
            print("  No output records generated.")
            return 0
        
        # Step 10: Aggregate output
        df_output = pl.DataFrame(records)
        
        df_summary = df_output.group_by('BNMCODE').agg([
            pl.col('AMOUNT').sum(),
            pl.col('AMTUSD').sum(),
            pl.col('AMTSGD').sum()
        ]).sort('BNMCODE')
        
        # Fill nulls with 0
        df_summary = df_summary.with_columns([
            pl.col('AMTUSD').fill_null(0),
            pl.col('AMTSGD').fill_null(0)
        ])
        
        # Filter out missing remmth (code '07')
        df_summary = df_summary.filter(pl.col('BNMCODE').str.slice(7, 2) != '07')
        
        # Step 11: Write output file
        print("\nWriting output to: {}".format(NLFBT))
        with open(NLFBT, 'w') as f:
            # Write header
            f.write("NLFBT{}{}{}\n".format(reptday, reptmon, reptyear))
            
            # Write data rows
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
        print("\nExpected file pattern: btrad{MM}{WK}{YY}.sas7bdat")
        print("Example: btrad060126.sas7bdat for Month=06, Week=1, Year=2026")
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
        description='EIBMABTL - Loan Maturity Profile Processor',
        epilog='Example: python EIBMABTL.py 2026-06-08'
    )
    parser.add_argument('date', nargs='?', help='Report date in YYYY-MM-DD format (default: today for testing)')
    
    args = parser.parse_args()
    
    # Parse date if provided
    reptdate = None
    if args.date:
        try:
            reptdate = datetime.strptime(args.date, '%Y-%m-%d').date()
            print("Using command line date: {}".format(reptdate))
        except ValueError:
            print("Error: Invalid date format. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        print("No date provided - using today's date for testing")
    
    sys.exit(main(reptdate))



Looking for BTRAD file: btrad06126.sas7bdat
  Reading SAS file...
  Total records read: 41554
  Records after filtering: 19631

ERROR: unsupported operand type(s) for -: 'datetime.datetime' and 'datetime.date'
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMABTL.py", line 303, in main
    reptdate_sas = (reptdate - date(1960, 1, 1)).days
TypeError: unsupported operand type(s) for -: 'datetime.datetime' and 'datetime.date'
