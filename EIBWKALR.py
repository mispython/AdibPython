"""
EIBWKALR SAS to Python conversion
Processes loan data and performs validation checks
"""

from pathlib import Path
from datetime import datetime, date
import polars as pl
from typing import Dict, Tuple, Optional

# Setup paths
BASE_PATH = Path("./data")
INPUT_PATH = BASE_PATH / "input"
OUTPUT_PATH = BASE_PATH / "output"
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# Bank configurations (PBB = Public Bank Berhad, PIBB = Public Islamic Bank Berhad)
BANKS = ["PBB", "PIBB"]
BANK_NAMES = {
    "PBB": "PUBLIC BANK BERHAD",
    "PIBB": "PUBLIC ISLAMIC BANK BERHAD"
}

# ============================================================================
# 1. REPTDATE Processing Functions
# ============================================================================

def calculate_week_info(repdate: date) -> Dict:
    """Calculate week info based on day of month (SAS SELECT logic)"""
    day = repdate.day
    
    if day == 8:
        sdd, wk, wk1 = 1, '1', '4'
    elif day == 15:
        sdd, wk, wk1 = 9, '2', '1'
    elif day == 22:
        sdd, wk, wk1 = 16, '3', '2'
    else:  # SAS OTHERWISE case
        sdd, wk, wk1 = 23, '4', '3'
    
    return {"SDD": sdd, "WK": wk, "WK1": wk1}

def get_month_adjustments(wk: str, month: int) -> Tuple[int, int]:
    """Calculate month adjustments (MM and MM1)"""
    mm = month
    mm1 = month
    
    if wk == '1':
        mm1 = month - 1
        if mm1 == 0:
            mm1 = 12
    
    return mm, mm1

def process_repdate(loan_data_path: str, bank: str = "PBB") -> Dict:
    """
    Process REPTDATE similar to SAS DATA REPTDATE step
    Returns all calculated variables
    """
    # Load REPTDATE data
    repdate_path = INPUT_PATH / f"{loan_data_path}/REPTDATE.parquet"
    df = pl.read_parquet(repdate_path)
    
    # Get REPTDATE (assuming single record)
    repdate = df["REPTDATE"][0]
    
    # Calculate week info
    week_info = calculate_week_info(repdate)
    wk = week_info["WK"]
    
    # Get month and adjustments
    month = repdate.month
    mm, mm1 = get_month_adjustments(wk, month)
    
    # Format variables
    variables = {
        "REPTDATE": repdate,
        "NOWK": wk,
        "NOWK1": week_info["WK1"],
        "REPTYEAR": str(repdate.year),
        "REPTMON": f"{repdate.month:02d}",
        "REPTMON1": f"{mm1:02d}",
        "REPTDAY": f"{repdate.day:02d}",
        "RDATE": repdate.strftime("%d%m%y"),  # DDMMYY8.
        "SDESC": BANK_NAMES[bank][:26],  # $26. format
        "SDD": week_info["SDD"],
        "WK": wk,
        "WK1": week_info["WK1"],
        "MM": mm,
        "MM1": mm1
    }
    
    return variables

def process_repdate_previous(loanp_data_path: str) -> Optional[str]:
    """Process previous REPTDATE (PDATE)"""
    repdate_path = INPUT_PATH / f"{loanp_data_path}/REPTDATE.parquet"
    if repdate_path.exists():
        df = pl.read_parquet(repdate_path)
        repdate = df["REPTDATE"][0]
        # Z5. format - 5 digits with leading zeros for day of year
        day_of_year = repdate.timetuple().tm_yday
        return f"{day_of_year:05d}"
    return None

def check_kapiti_date(file_path: str) -> Optional[str]:
    """Check KAPITI file date (reads first record YYMMDD8. format)"""
    kapiti_path = INPUT_PATH / f"{file_path}.parquet"
    if kapiti_path.exists():
        # Read first row to get date
        df = pl.read_parquet(kapiti_path).limit(1)
        # Assuming first column is date in YYMMDD format
        if "REPTDATE" in df.columns:
            repdate = df["REPTDATE"][0]
            return repdate.strftime("%d%m%y")  # DDMMYY8.
    return None

# ============================================================================
# 2. Main Processing Logic
# ============================================================================

def process_bank(bank: str) -> Dict:
    """Process data for a specific bank"""
    print(f"\nProcessing {bank}...")
    
    # Set paths based on bank
    loan_path = f"{bank}.MNILN"  # Current month
    loanp_path = f"{bank}.MNILN_PREV"  # Previous month
    kapitiw_path = f"{bank}.KAPITIW"
    kapiti1_path = f"{bank}.KAPITI1"
    kapiti3_path = f"{bank}.KAPITI3"
    
    # 1. Process REPTDATE
    repdate_vars = process_repdate(loan_path, bank)
    print(f"  Report Date: {repdate_vars['RDATE']}")
    print(f"  Week: {repdate_vars['WK']}, Previous Week: {repdate_vars['WK1']}")
    
    # 2. Process previous REPTDATE (PDATE)
    pdate = process_repdate_previous(loanp_path)
    if pdate:
        repdate_vars["PDATE"] = pdate
        print(f"  Previous Date (PDATE): {pdate}")
    
    # 3. Check KAPITI dates
    kapiti1_date = check_kapiti_date(kapiti1_path)
    kapiti3_date = check_kapiti_date(kapiti3_path)
    kapitiw_date = check_kapiti_date(kapitiw_path)
    
    repdate_vars.update({
        "KAPITI1": kapiti1_date,
        "KAPITI3": kapiti3_date,
        "KAPITIW": kapitiw_date
    })
    
    print(f"  KAPITI1 Date: {kapiti1_date}")
    print(f"  KAPITI3 Date: {kapiti3_date}")
    print(f"  KAPITIW Date: {kapitiw_date}")
    
    return repdate_vars

def validate_dates(repdate_vars: Dict) -> bool:
    """Validate that all KAPITI dates match RDATE (SAS macro logic)"""
    rdate = repdate_vars.get("RDATE")
    kapiti1 = repdate_vars.get("KAPITI1")
    kapiti3 = repdate_vars.get("KAPITI3")
    kapitiw = repdate_vars.get("KAPITIW")
    
    if not all([rdate, kapiti1, kapiti3, kapitiw]):
        print("  Warning: Some dates are missing")
        return False
    
    all_match = (kapiti1 == rdate and kapiti3 == rdate and kapitiw == rdate)
    
    if all_match:
        print("  ✓ All dates match - Processing can proceed")
        return True
    else:
        print("  ✗ Date mismatch detected:")
        if kapiti1 != rdate:
            print(f"    KAPITI1 ({kapiti1}) != RDATE ({rdate})")
        if kapiti3 != rdate:
            print(f"    KAPITI3 ({kapiti3}) != RDATE ({rdate})")
        if kapitiw != rdate:
            print(f"    KAPITIW ({kapitiw}) != RDATE ({rdate})")
        print("  Job will not proceed (simulating ABORT 77)")
        return False

# ============================================================================
# 3. External Program Executors
# ============================================================================

def execute_program(program_name: str, bank: str, repdate_vars: Dict):
    """Execute external program logic"""
    print(f"  Executing {program_name} for {bank}")
    
    # Here you would call the actual program modules
    # For now, we'll just log what would happen
    
    program_path = INPUT_PATH / "PROGRAMS" / f"{program_name}.parquet"
    if program_path.exists():
        # Load program data
        program_data = pl.read_parquet(program_path)
        
        # Apply date filters based on REPTDATE
        result = program_data.filter(
            pl.col("REPTDATE") == repdate_vars["REPTDATE"]
        )
        
        # Save results
        output_file = OUTPUT_PATH / f"{bank}_{program_name}_OUTPUT.parquet"
        result.write_parquet(output_file)
        print(f"    Saved to {output_file}")
    else:
        print(f"    Program {program_name} not found")

def process_valid_case(bank: str, repdate_vars: Dict):
    """Process when all dates match (SAS %IF valid case)"""
    if bank == "PBB":
        # PBB programs
        programs = ["KALWE", "KALWPBBS", "KALWPBBN"]
        # Also check for DCI data if exists
        dciw_path = INPUT_PATH / "PBB.DCIWKLY.parquet"
        dciwh_path = INPUT_PATH / "PBB.DCIWH.parquet"
        
        if dciw_path.exists():
            print("  Processing DCI weekly data...")
            dci_data = pl.read_parquet(dciw_path)
            dci_filtered = dci_data.filter(
                pl.col("REPTDATE") == repdate_vars["REPTDATE"]
            )
            dci_filtered.write_parquet(OUTPUT_PATH / "PBB_DCIWKLY_FILTERED.parquet")
            
    else:  # PIBB
        programs = ["KALWEI", "KALWPIBS", "KALWPIBN"]
    
    # Execute all programs
    for program in programs:
        execute_program(program, bank, repdate_vars)

# ============================================================================
# 4. Main Execution
# ============================================================================

def main():
    """Main execution function"""
    print("=" * 60)
    print("EIBWKALR SAS to Python Conversion")
    print("=" * 60)
    
    results = {}
    
    # Process both banks
    for bank in BANKS:
        print(f"\n{'='*40}")
        print(f"Processing {bank}")
        print('='*40)
        
        # Get REPTDATE and check KAPITI dates
        repdate_vars = process_bank(bank)
        results[bank] = repdate_vars
        
        # Validate dates (SAS macro logic)
        is_valid = validate_dates(repdate_vars)
        
        if is_valid:
            # Process valid case
            process_valid_case(bank, repdate_vars)
            
            # Save report outputs
            report_text = f"""
{repdate_vars['SDESC']}
Report Date: {repdate_vars['RDATE']}
Week: {repdate_vars['WK']}
Year: {repdate_vars['REPTYEAR']}
Month: {repdate_vars['REPTMON']}
Day: {repdate_vars['REPTDAY']}
Previous Month: {repdate_vars['REPTMON1']}
            """
            
            # Save report text (simulating SASLIST)
            report_path = OUTPUT_PATH / f"{bank}_EIBWKALR_REPORT.txt"
            with open(report_path, 'w') as f:
                f.write(report_text)
            print(f"  Report saved to {report_path}")
            
            # Save NSRS text if applicable
            nsrs_path = OUTPUT_PATH / f"{bank}_EIBWKALR_NSRS.txt"
            with open(nsrs_path, 'w') as f:
                f.write(f"NSRS Report for {bank}\n")
                f.write(f"Date: {repdate_vars['RDATE']}\n")
            print(f"  NSRS report saved to {nsrs_path}")
        else:
            # Invalid case - simulate ABORT 77
            print(f"  ⚠ Processing stopped for {bank} (date mismatch)")
            # Create error indicator file
            error_path = OUTPUT_PATH / f"{bank}_ERROR_INDICATOR.txt"
            with open(error_path, 'w') as f:
                f.write(f"ABORT 77 - Date mismatch for {bank}\n")
                f.write(f"RDATE: {repdate_vars.get('RDATE')}\n")
                f.write(f"KAPITI1: {repdate_vars.get('KAPITI1')}\n")
                f.write(f"KAPITI3: {repdate_vars.get('KAPITI3')}\n")
                f.write(f"KAPITIW: {repdate_vars.get('KAPITIW')}\n")
    
    # Save all variables for reference
    variables_df = pl.DataFrame([
        {"BANK": bank, **{k: str(v) for k, v in vars.items()}}
        for bank, vars in results.items()
    ])
    variables_df.write_parquet(OUTPUT_PATH / "EIBWKALR_VARIABLES.parquet")
    
    print("\n" + "=" * 60)
    print("PROCESSING COMPLETE")
    print("=" * 60)
    
    # Summary
    for bank in BANKS:
        status = "COMPLETED" if validate_dates(results[bank]) else "STOPPED (date mismatch)"
        print(f"{bank}: {status}")
    
    print(f"\nOutput files saved to: {OUTPUT_PATH}")

# ============================================================================
# 5. Run the conversion
# ============================================================================

if __name__ == "__main__":
    main()
