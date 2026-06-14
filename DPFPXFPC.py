#!/usr/bin/env python3
"""
File Name: EIBMLIBT
Loan Maturity Profile Processor (BT)
Processes BTRAD loan data for BNM reporting
"""

from __future__ import annotations

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
INPUT_DIR = BASE_DIR / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod"
OUTPUT_DIR = BASE_DIR / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMLIBT"

# Input SAS files - pattern: BTRAD{MM}{WK}{YY}.sas7bdat
BTRAD_FILE_TEMPLATE = INPUT_DIR / "btrad{mm}{wk}{yy}.sas7bdat"

# Output files
BT_OUTPUT_PATH = OUTPUT_DIR / "BT.txt"
BT_REPORT_PATH = OUTPUT_DIR / "BT_REPORT.txt"

# Create output directory
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# SAS FILE READER
# ============================================================================
def read_sas_file(filepath: Path) -> pl.DataFrame:
    """Read a SAS .sas7bdat file and return as Polars DataFrame"""
    print(f"  Reading: {filepath.name}")
    df, meta = pyreadstat.read_sas7bdat(str(filepath))
    
    # Convert all date columns from SAS numeric to proper dates
    # SAS dates are days since 1960-01-01
    date_columns = ['BLDATE', 'ISSDTE', 'EXPRDATE', 'MATDATE', 'CREATDS', 'APVDATE']
    for col in date_columns:
        if col in df.columns:
            # Convert SAS numeric dates to datetime, then to date
            df[col] = df[col].apply(
                lambda x: (date(1960, 1, 1) + timedelta(days=int(x))).strftime('%Y-%m-%d') 
                if pd.notna(x) and x > 0 else None
            )
    
    return pl.from_pandas(df)


# ============================================================================
# FORMAT DEFINITIONS
# ============================================================================

def get_rem_format(remmth: float | None) -> str:
    """Map remaining months to SAS REMFMT code."""
    if remmth is None:
        return "07"
    if remmth <= 0.1:
        return "01"
    if remmth <= 1:
        return "02"
    if remmth <= 3:
        return "03"
    if remmth <= 6:
        return "04"
    if remmth <= 12:
        return "05"
    return "06"


def get_prod_format(product: int | None) -> str:
    """Map product to HL/RC/FL buckets based on SAS PRDFMT."""
    hl_products = {
        4, 5, 6, 7, 31, 32, 100, 101, 102, 103, 110, 111, 112, 113, 114, 115,
        116, 170, 200, 201, 204, 205, 209, 210, 211, 212, 214, 215, 219, 220,
        225, 226, 227, 228, 229, 230, 231, 232, 233, 234,
    }
    rc_products = {350, 910, 925}

    if product in hl_products:
        return "HL"
    if product in rc_products:
        return "RC"
    return "FL"


# ============================================================================
# DATE HELPERS
# ============================================================================

def to_date(value) -> date | None:
    """
    Convert any date-like value to a Python date object.
    Handles datetime, date, and SAS numeric values.
    Always returns a date object (never datetime).
    """
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        # Already a date object
        return value
    if isinstance(value, datetime):
        # Convert datetime to date
        return value.date()
    if isinstance(value, str):
        # Try to parse string date
        try:
            return datetime.strptime(value, '%Y-%m-%d').date()
        except ValueError:
            try:
                return datetime.strptime(value, '%d/%m/%Y').date()
            except ValueError:
                return None
    if isinstance(value, (int, float)):
        # SAS numeric date (days since 1960-01-01)
        if value <= 0:
            return None
        return date(1960, 1, 1) + timedelta(days=int(value))
    return None


def days_in_month(year: int, month: int) -> int:
    """Get number of days in a month, accounting for leap year."""
    return calendar.monthrange(year, month)[1]


def calculate_next_bldate(bldate: date, issdte: date | None, payfreq: str, freq: int) -> date:
    """Calculate the next billing date based on SAS NXTBLDT logic."""
    if payfreq == "6":
        # Fortnightly - add 14 days
        return bldate + timedelta(days=14)

    # Use issue date day or billing date day
    dd = issdte.day if issdte else bldate.day
    mm = bldate.month + freq
    yy = bldate.year

    while mm > 12:
        mm -= 12
        yy += 1

    max_day = days_in_month(yy, mm)
    if dd > max_day:
        dd = max_day

    return date(yy, mm, dd)


def calculate_remaining_months(matdt: date, reptdate: date) -> float:
    """Calculate remaining months per SAS REMMTH macro."""
    rpyr = reptdate.year
    rpmth = reptdate.month
    rpday = reptdate.day
    rpdays = days_in_month(rpyr, rpmth)

    mdyr = matdt.year
    mdmth = matdt.month
    mdday = matdt.day

    if mdday > rpdays:
        mdday = rpdays

    remy = mdyr - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday

    return remy * 12 + remm + remd / rpdays


def get_week_number(reptdate: date) -> str:
    """Determine week number based on report day (1st, 2nd, 3rd, or 4th week)."""
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
# GET REPORT DATE (Using datetime instead of file)
# ============================================================================

def get_report_date(reptdate: date | None = None) -> dict:
    """
    Get report date and derive macro variables.
    If reptdate is None, use today's date for testing.
    """
    if reptdate is None:
        # ====================================================================
        # TESTING - Use today's date
        # ====================================================================
        reptdate = date.today()
        print(f"  TESTING MODE: Using today's date: {reptdate}")
        
        # ====================================================================
        # PRODUCTION - Uncomment below for production use with fixed date
        # ====================================================================
        # reptdate = date(2026, 6, 8)  # Example: June 8, 2026
    
    nowk = get_week_number(reptdate)

    return {
        "REPTDATE": reptdate,
        "NOWK": nowk,
        "REPTMON": f"{reptdate.month:02d}",
        "REPTDAY": f"{reptdate.day:02d}",
        "REPTYEAR": f"{reptdate.year:04d}",
        "REPTYEAR2": f"{reptdate.year % 100:02d}",
        "RDATE": reptdate.strftime("%d%m%Y"),
    }


# ============================================================================
# PROCESS LOAN DATA
# ============================================================================

def process_loan_data(macro_vars: dict) -> pl.DataFrame:
    """Process BTRAD loan data and produce BNMCODE/AMOUNT output."""
    reptdate = macro_vars["REPTDATE"]
    mm = macro_vars["REPTMON"]         # 2-digit month (e.g., "06")
    wk = macro_vars["NOWK"]            # week (1-4)
    yy = macro_vars["REPTYEAR2"]       # 2-digit year (e.g., "26")

    # Build filename: btrad{MM}{WK}{YY}.sas7bdat
    btrad_filename = f"btrad{mm}{wk}{yy}.sas7bdat"
    btrad_path = INPUT_DIR / btrad_filename
    
    # Also try uppercase version if lowercase not found
    if not btrad_path.exists():
        btrad_filename_upper = f"BTRAD{mm}{wk}{yy}.sas7bdat"
        btrad_path = INPUT_DIR / btrad_filename_upper
    
    print(f"\nLooking for BTRAD file: {btrad_path.name}")
    
    if not btrad_path.exists():
        raise FileNotFoundError(f"BTRAD file not found: {btrad_path}")

    # Read SAS file
    print(f"  Reading: {btrad_path.name}")
    df, meta = pyreadstat.read_sas7bdat(str(btrad_path))
    print(f"  Total records read: {len(df)}")
    
    # Convert to Polars
    df_pl = pl.from_pandas(df)
    
    # Show available columns for debugging
    print(f"  Available columns: {df_pl.columns[:20]}...")

    # Filter for loan products (PRODCD starts with '34' OR PRODCD in ('225','226'))
    df_filtered = df_pl.filter(
        (pl.col("PRODCD").cast(pl.Utf8).str.slice(0, 2) == "34")
        | (pl.col("PRODCD").cast(pl.Utf8).is_in(["225", "226"]))
    )
    print(f"  Records after filtering: {len(df_filtered)}")

    if len(df_filtered) == 0:
        print("  No records after filtering.")
        return pl.DataFrame({"BNMCODE": [], "AMOUNT": []})

    output_records: list[dict] = []
    processed = 0

    for row in df_filtered.iter_rows(named=True):
        custcd = str(row.get("CUSTCD", ""))
        prodcd = str(row.get("PRODCD", ""))
        balance = float(row.get("BALANCE", 0) or 0)
        payamt = float(row.get("PAYAMT", 0) or 0)

        # Convert all dates to date objects using to_date() function
        bldate_val = row.get("BLDATE")
        issdte_val = row.get("ISSDTE")
        exprdate_val = row.get("EXPRDATE")
        
        bldate = to_date(bldate_val)
        issdte = to_date(issdte_val)
        exprdate = to_date(exprdate_val)

        if exprdate is None:
            continue

        # Convert PRODCD to integer for product mapping
        try:
            product = int(prodcd) if prodcd else 0
        except ValueError:
            product = 0

        # Determine customer code (matches SAS: CUST = '08' or '09')
        if custcd in {"77", "78", "95", "96"}:
            cust = "08"
        else:
            cust = "09"

        # Determine product type using PRDFMT
        prod_type = get_prod_format(product)

        # Determine item code (matches SAS SELECT logic)
        if custcd in {"77", "78", "95", "96"}:
            item = "214" if prod_type == "HL" else "219"
        else:
            if prod_type == "FL":
                item = "211"
            elif prod_type == "RC":
                item = "212"
            else:
                item = "219"

        # Hardcode override for product 100
        if product == 100:
            item = "212"

        # Calculate days past due (matches SAS: DAYS = REPTDATE - BLDATE)
        days = (reptdate - bldate).days if bldate else 0

        # Initialize remaining months
        remmth = None
        current_balance = balance
        current_bldate = bldate

        # Process maturity profile (matches SAS IF-ELSE logic)
        if exprdate <= reptdate:
            remmth = None
        elif (exprdate - reptdate).days < 8:
            remmth = 0.1
        else:
            # Payment frequency (matches SAS: PAYFREQ = '3', FREQ = 6)
            payfreq = "3"
            freq = 6

            # RC products use expiry date as billing date
            if product in (350, 910, 925):
                current_bldate = exprdate
            elif not current_bldate:
                current_bldate = issdte
                if current_bldate is None:
                    continue
                # Advance billing date to after report date
                while current_bldate <= reptdate:
                    current_bldate = calculate_next_bldate(current_bldate, issdte, payfreq, freq)

            if payamt < 0:
                payamt = 0

            if current_bldate > exprdate or current_balance <= payamt:
                current_bldate = exprdate

            # Process payment schedule (matches SAS DO WHILE loop)
            while current_bldate <= exprdate:
                remmth = calculate_remaining_months(current_bldate, reptdate)
                
                if remmth > 12 or current_bldate == exprdate:
                    break

                if payamt > 0 and remmth is not None:
                    amount = payamt
                    current_balance -= payamt
                    
                    # Part 2-RM record (95)
                    bnmcode = f"95{item}{cust}{get_rem_format(remmth)}0000Y"
                    output_records.append({"BNMCODE": bnmcode, "AMOUNT": amount})

                    # Part 1-RM record (93) - NPL if days > 89
                    remmth_code = 13 if days > 89 else remmth
                    bnmcode = f"93{item}{cust}{get_rem_format(remmth_code)}0000Y"
                    output_records.append({"BNMCODE": bnmcode, "AMOUNT": amount})

                # Calculate next billing date
                current_bldate = calculate_next_bldate(current_bldate, issdte, payfreq, freq)
                
                if current_bldate > exprdate or current_balance <= payamt:
                    current_bldate = exprdate

            # Calculate final remaining months for remaining balance
            if current_bldate <= exprdate:
                remmth = calculate_remaining_months(current_bldate, reptdate)

        # Output remaining balance (matches final OUTPUT statements)
        amount = current_balance
        if amount != 0 and amount is not None:
            # Part 2-RM record (95)
            bnmcode = f"95{item}{cust}{get_rem_format(remmth)}0000Y"
            output_records.append({"BNMCODE": bnmcode, "AMOUNT": amount})

            # Part 1-RM record (93)
            remmth_code = 13 if days > 89 else remmth
            bnmcode = f"93{item}{cust}{get_rem_format(remmth_code)}0000Y"
            output_records.append({"BNMCODE": bnmcode, "AMOUNT": amount})

        processed += 1
        if processed % 5000 == 0:
            print(f"  Processed {processed} records...")

    print(f"  Total records processed: {processed}")
    print(f"  Output records created: {len(output_records)}")

    if not output_records:
        return pl.DataFrame({"BNMCODE": [], "AMOUNT": []})

    return pl.DataFrame(output_records)


# ============================================================================
# AGGREGATE AND OUTPUT
# ============================================================================

def aggregate_output(df_output: pl.DataFrame) -> pl.DataFrame:
    """Aggregate amounts by BNMCODE (PROC SUMMARY NWAY)."""
    if df_output.is_empty():
        return df_output

    # Filter out missing remmth (code '07')
    df_valid = df_output.filter(pl.col("BNMCODE").str.slice(7, 2) != "07")
    
    missing = df_output.filter(pl.col("BNMCODE").str.slice(7, 2) == "07")
    if len(missing) > 0:
        print(f"\n  Records with missing remmth (code '07'): {len(missing)}")
        print(f"  Missing amount sum: {missing['AMOUNT'].sum():,.2f}")

    return df_valid.group_by("BNMCODE").agg(pl.col("AMOUNT").sum()).sort("BNMCODE")


def write_output_txt(df_agg: pl.DataFrame) -> None:
    """Write BNMCODE/AMOUNT output to text file (pipe-delimited)."""
    print(f"\nWriting output to: {BT_OUTPUT_PATH}")
    with open(BT_OUTPUT_PATH, "w", encoding="utf-8") as file_handle:
        for row in df_agg.iter_rows(named=True):
            file_handle.write(f"{row['BNMCODE']}|{row['AMOUNT']:.2f}\n")
    
    total_amount = df_agg['AMOUNT'].sum() if len(df_agg) > 0 else 0
    print(f"  Total BNM codes written: {len(df_agg)}")
    print(f"  Total amount: {total_amount:,.2f}")


# ============================================================================
# REPORT GENERATION
# ============================================================================

def build_report_lines(df_agg: pl.DataFrame, macro_vars: dict) -> list[str]:
    """Build report lines with ASA carriage control characters."""
    lines: list[str] = []
    page_length = 60
    reptdate = macro_vars["REPTDATE"]

    def add_line(text: str, new_page: bool = False) -> int:
        control = "1" if new_page else " "
        lines.append(control + text)
        return 1 if new_page else 0

    def add_header() -> int:
        count = 0
        count += add_line(f"LOAN MATURITY PROFILE REPORT - {reptdate.strftime('%d/%m/%Y')}", new_page=True)
        lines.append(" " + "=" * 60)
        lines.append(" " + f"{'BNMCODE':<20}{'AMOUNT':>20}")
        lines.append(" " + "-" * 60)
        return 4

    if df_agg.is_empty():
        lines.append(" " + "NO DATA FOUND FOR THE REPORT PERIOD")
        return lines

    line_count = add_header()
    total_amount = 0.0

    for row in df_agg.iter_rows(named=True):
        if line_count >= page_length:
            line_count = add_header()
        amount = float(row["AMOUNT"])
        total_amount += amount
        lines.append(" " + f"{row['BNMCODE']:<20}{amount:>20.2f}")
        line_count += 1

    # Add total line
    if line_count >= page_length - 2:
        line_count = add_header()

    lines.append(" " + "-" * 60)
    lines.append(" " + f"{'TOTAL':<20}{total_amount:>20.2f}")
    lines.append(" " + "=" * 60)

    return lines


def write_report(lines: list[str]) -> None:
    """Write report with ASA carriage control characters."""
    print(f"Writing report to: {BT_REPORT_PATH}")
    with open(BT_REPORT_PATH, "w", encoding="utf-8") as file_handle:
        for line in lines:
            file_handle.write(line + "\n")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main(reptdate: date | None = None) -> int:
    """Main execution function."""
    print("\n" + "=" * 70)
    print("EIBMLIBT - LOAN MATURITY PROFILE PROCESSOR")
    print("=" * 70)

    try:
        # Step 1: Get report date
        macro_vars = get_report_date(reptdate)
        print(f"\nReport Date: {macro_vars['REPTDATE'].strftime('%d/%m/%Y')}")
        print(f"Week Number: {macro_vars['NOWK']}")
        print(f"Report Month: {macro_vars['REPTMON']}")
        print(f"Report Year (2-digit): {macro_vars['REPTYEAR2']}")
        print(f"Expected BTRAD file: btrad{macro_vars['REPTMON']}{macro_vars['NOWK']}{macro_vars['REPTYEAR2']}.sas7bdat")

        # Step 2: Process loan data
        df_output = process_loan_data(macro_vars)

        # Step 3: Aggregate output
        print("\n" + "-" * 50)
        print("AGGREGATING OUTPUT")
        print("-" * 50)
        df_agg = aggregate_output(df_output)

        if df_agg.is_empty():
            print("\nNo valid records to output.")
            report_lines = build_report_lines(df_agg, macro_vars)
            write_report(report_lines)
            return 0

        # Step 4: Write output files
        print("\n" + "-" * 50)
        print("WRITING OUTPUT")
        print("-" * 50)
        write_output_txt(df_agg)

        # Step 5: Generate report
        report_lines = build_report_lines(df_agg, macro_vars)
        write_report(report_lines)

        print("\n" + "=" * 70)
        print("PROCESSING COMPLETED SUCCESSFULLY")
        print("=" * 70)
        print(f"\nOutput file: {BT_OUTPUT_PATH}")
        print(f"Report file: {BT_REPORT_PATH}")
        
        return 0
        
    except FileNotFoundError as e:
        print(f"\nFILE NOT FOUND ERROR: {e}", file=sys.stderr)
        print("\nExpected file pattern: btrad{MM}{WK}{YY}.sas7bdat")
        print("Example: btrad060126.sas7bdat for Month=06, Week=1, Year=2026")
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
        epilog='Example: python EIBMLIBT.py 2026-06-08'
    )
    parser.add_argument('date', nargs='?', help='Report date in YYYY-MM-DD format (default: today for testing)')
    
    args = parser.parse_args()
    
    # Parse date if provided
    reptdate = None
    if args.date:
        try:
            reptdate = datetime.strptime(args.date, '%Y-%m-%d').date()
            print(f"Using command line date: {reptdate}")
        except ValueError:
            print(f"Error: Invalid date format. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        print("No date provided - using today's date for testing")
    
    sys.exit(main(reptdate))
