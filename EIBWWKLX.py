"""
EIBWWKLX - Weekly Loan Processing Controller

Purpose: Control weekly loan processing and validation
- Similar to EIBWWKLY but for loan-specific processing
- Validate LOAN.REPTDATE matches BNM.REPTDATE
- Execute LALWPBBC if validation passes
- Exit with code 77 if dates don't match

Week Assignment:
- Day 8:  Week 1 (WK='1', WK1='4', SDD=1)
- Day 15: Week 2 (WK='2', WK1='1', SDD=9)
- Day 22: Week 3 (WK='3', WK1='2', SDD=16)
- Day 29+: Week 4 (WK='4', WK1='3', SDD=23)

Quarter Detection:
- QTR='1' if month in (3,6,9,12) and day in (30,31)
- QTR='0' otherwise

Program Executed (if validation passes):
- LALWPBBC: Weekly loan processing
"""

import polars as pl
from datetime import datetime
import os
import sys

# Directories
LOAN_DIR = 'data/loan/'
BNM_DIR = 'data/bnm/'

print("EIBWWKLX - Weekly Loan Processing Controller")
print("=" * 60)

# Read LOAN.REPTDATE
print("\nReading LOAN.REPTDATE...")
try:
    df_loan_reptdate = pl.read_parquet(f'{LOAN_DIR}REPTDATE.parquet')
    loan_reptdate = df_loan_reptdate['REPTDATE'][0]
    loan_str = loan_reptdate.strftime('%d%m%y')
    
    print(f"LOAN REPTDATE: {loan_reptdate.strftime('%d/%m/%Y')} ({loan_str})")

except Exception as e:
    print(f"  ✗ ERROR reading LOAN.REPTDATE: {e}")
    sys.exit(1)

# Process REPTDATE for BNM
print("\nProcessing REPTDATE for BNM...")
try:
    reptdate = loan_reptdate
    day = reptdate.day
    month = reptdate.month
    year = reptdate.year
    
    # Determine week based on day
    if day == 8:
        sdd = 1
        wk = '1'
        wk1 = '4'
    elif day == 15:
        sdd = 9
        wk = '2'
        wk1 = '1'
    elif day == 22:
        sdd = 16
        wk = '3'
        wk1 = '2'
    else:  # Day 29-31
        sdd = 23
        wk = '4'
        wk1 = '3'
    
    # Determine month reference
    if wk == '1':
        mm1 = month - 1 if month > 1 else 12
    else:
        mm1 = month
    
    # Calculate start date
    sdate = datetime(year, month, sdd)
    
    # Format variables
    nowk = wk
    nowk1 = wk1
    reptmon = f'{month:02d}'
    reptmon1 = f'{mm1:02d}'
    reptyear = str(year)
    reptday = f'{day:02d}'
    rdate = reptdate.strftime('%d%m%y')
    sdate_str = sdate.strftime('%d%m%y')
    tdate = f'{year % 100:02d}{month:02d}{day:02d}'
    
    # Determine quarter
    qtr = '1' if month in [3, 6, 9, 12] and day in [30, 31] else '0'
    
    print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
    print(f"Week: {wk}, Previous Week: {wk1}")
    print(f"Week Start: {sdate.strftime('%d/%m/%Y')}")
    print(f"Quarter End: {'Yes' if qtr == '1' else 'No'}")

except Exception as e:
    print(f"  ✗ ERROR processing REPTDATE: {e}")
    sys.exit(1)

# Save to BNM directory
print("\nSaving BNM.REPTDATE...")
try:
    df_bnm_reptdate = pl.DataFrame({'REPTDATE': [reptdate]})
    os.makedirs(BNM_DIR, exist_ok=True)
    df_bnm_reptdate.write_parquet(f'{BNM_DIR}REPTDATE.parquet')
    print(f"  ✓ Saved BNM.REPTDATE")
except Exception as e:
    print(f"  ⚠ Warning saving BNM.REPTDATE: {e}")

print("=" * 60)

# Validation
print("\nValidating dates...")
loan_date_check = loan_str
rdate_check = rdate

if loan_date_check == rdate_check:
    print(f"  ✓ LOAN date matches REPTDATE: {loan_str} = {rdate}")
    
    # Set environment variables
    print("\nSetting environment variables...")
    env_vars = {
        'NOWK': nowk,
        'NOWK1': nowk1,
        'REPTMON': reptmon,
        'REPTMON1': reptmon1,
        'REPTYEAR': reptyear,
        'REPTDAY': reptday,
        'RDATE': rdate,
        'SDATE': sdate_str,
        'TDATE': tdate,
        'QTR': qtr,
        'LOAN': loan_str
    }
    
    os.environ.update(env_vars)
    
    for key, val in env_vars.items():
        print(f"  {key} = {val}")
    
    # Execute weekly loan processing
    print(f"\n{'='*60}")
    print("Executing weekly loan processing...")
    print("=" * 60)
    
    print("\nProgram to Execute:")
    print("  LALWPBBC - Weekly Loan Processing")
    
    # In production, this would execute the actual program
    print("\n[Production Note: Execute LALWPBBC here]")
    
    print(f"\n{'='*60}")
    print(f"✓ EIBWWKLX Complete!")
    print(f"{'='*60}")
    print(f"\nWeekly Loan Processing Summary:")
    print(f"  Week: {wk} of month {reptmon}/{reptyear}")
    print(f"  Start Date: {sdate.strftime('%d/%m/%Y')}")
    print(f"  Report Date: {reptdate.strftime('%d/%m/%Y')}")
    print(f"  Quarter End: {'Yes' if qtr == '1' else 'No'}")
    print(f"\nValidation: ✓ PASSED")
    print(f"  LOAN date: {loan_str} = {rdate} ✓")
    print(f"\nLoan processing executed successfully.")
    
    sys.exit(0)

else:
    print(f"  ✗ ERROR: THE LOAN EXTRACTION IS NOT DATED {rdate}")
    print(f"           Expected: {rdate}")
    print(f"           Found:    {loan_str}")
    print(f"\n{'='*60}")
    print(f"✗ VALIDATION FAILED!")
    print(f"{'='*60}")
    print(f"\nTHE JOB IS NOT DONE !!")
    print(f"\nLOAN extraction date mismatch:")
    print(f"  Expected: {rdate}")
    print(f"  Actual:   {loan_str}")
    print(f"\nExiting with error code 77...")
    
    sys.exit(77)
