"""
EIBX1503 - BNM File Date Validation (Production Ready)

Purpose:
- Validate that BNM NPL files are dated correctly
- Check FMSNPL-PBB (Conventional) file date matches REPTDATE
- Check FMSNPL-PIBB (Islamic) file date matches REPTDATE
- Abort processing if dates don't match

Files Validated:
- FMSGFL: FMSNPL-PBB (Conventional NPL file)
- FMSGFLI: FMSNPL-PIBB (Islamic NPL file)

Critical Logic:
- Read REPTDATE from BNM.REPTDATE
- Read first line of each NPL file to get file date
- Compare file dates against REPTDATE
- Exit with error if mismatch

Exit Codes:
- 0: Success (all dates match)
- 77: Error (date mismatch)
"""

import polars as pl
from datetime import datetime
import sys
import os

# Directories
BNM_DIR = 'data/bnm/'
INPUT_DIR = 'data/input/'

print("EIBX1503 - BNM File Date Validation")
print("=" * 60)

# Read REPTDATE
print("\nReading REPTDATE...")
try:
    df_reptdate = pl.read_parquet(f'{BNM_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    rdate = reptdate.strftime('%d%m%y')
    
    print(f"REPTDATE: {reptdate.strftime('%d/%m/%Y')} ({rdate})")

except Exception as e:
    print(f"  ✗ ERROR: Cannot read REPTDATE: {e}")
    sys.exit(1)

print("=" * 60)

# Read FMSNPL-PBB file date (Conventional)
print("\nValidating FMSNPL-PBB (Conventional) file...")
try:
    fmsgfl_path = f'{INPUT_DIR}FMSGFL'
    
    if not os.path.exists(fmsgfl_path):
        print(f"  ✗ ERROR: FMSNPL-PBB file not found: {fmsgfl_path}")
        sys.exit(77)
    
    # Read first line to get date (format: DDMMYY8. at position 1)
    with open(fmsgfl_path, 'r') as f:
        first_line = f.readline().strip()
    
    # Extract date (first 8 characters, DDMMYY format)
    fmsglf_date_str = first_line[0:8] if len(first_line) >= 8 else None
    
    if not fmsglf_date_str:
        print(f"  ✗ ERROR: Cannot extract date from FMSNPL-PBB file")
        sys.exit(77)
    
    # Parse date (DDMMYY format)
    try:
        fmsglf_date = datetime.strptime(fmsglf_date_str, '%d%m%Y')
        fmsglf = fmsglf_date.strftime('%d%m%y')
    except:
        # Try 6-digit format DDMMYY
        try:
            fmsglf_date = datetime.strptime(fmsglf_date_str[:6], '%d%m%y')
            # Assume 20xx for year
            if fmsglf_date.year < 2000:
                fmsglf_date = fmsglf_date.replace(year=fmsglf_date.year + 2000)
            fmsglf = fmsglf_date.strftime('%d%m%y')
        except:
            print(f"  ✗ ERROR: Invalid date format in FMSNPL-PBB: {fmsglf_date_str}")
            sys.exit(77)
    
    print(f"FMSNPL-PBB date: {fmsglf}")
    
    # Validate
    if fmsglf != rdate:
        print(f"  ✗ ERROR: THE FMSNPL-PBB FILE IS NOT DATED {rdate}")
        print(f"           Expected: {rdate}")
        print(f"           Found:    {fmsglf}")
        sys.exit(77)
    else:
        print(f"  ✓ FMSNPL-PBB date matches REPTDATE")

except Exception as e:
    print(f"  ✗ ERROR: Cannot validate FMSNPL-PBB: {e}")
    sys.exit(77)

# Read FMSNPL-PIBB file date (Islamic)
print("\nValidating FMSNPL-PIBB (Islamic) file...")
try:
    fmsgfli_path = f'{INPUT_DIR}FMSGFLI'
    
    if not os.path.exists(fmsgfli_path):
        print(f"  ✗ ERROR: FMSNPL-PIBB file not found: {fmsgfli_path}")
        sys.exit(77)
    
    # Read first line to get date
    with open(fmsgfli_path, 'r') as f:
        first_line = f.readline().strip()
    
    # Extract date (first 8 characters)
    fmsglfi_date_str = first_line[0:8] if len(first_line) >= 8 else None
    
    if not fmsglfi_date_str:
        print(f"  ✗ ERROR: Cannot extract date from FMSNPL-PIBB file")
        sys.exit(77)
    
    # Parse date
    try:
        fmsglfi_date = datetime.strptime(fmsglfi_date_str, '%d%m%Y')
        fmsglfi = fmsglfi_date.strftime('%d%m%y')
    except:
        try:
            fmsglfi_date = datetime.strptime(fmsglfi_date_str[:6], '%d%m%y')
            if fmsglfi_date.year < 2000:
                fmsglfi_date = fmsglfi_date.replace(year=fmsglfi_date.year + 2000)
            fmsglfi = fmsglfi_date.strftime('%d%m%y')
        except:
            print(f"  ✗ ERROR: Invalid date format in FMSNPL-PIBB: {fmsglfi_date_str}")
            sys.exit(77)
    
    print(f"FMSNPL-PIBB date: {fmsglfi}")
    
    # Validate
    if fmsglfi != rdate:
        print(f"  ✗ ERROR: THE FMSNPL-PIBB FILE IS NOT DATED {rdate}")
        print(f"           Expected: {rdate}")
        print(f"           Found:    {fmsglfi}")
        sys.exit(77)
    else:
        print(f"  ✓ FMSNPL-PIBB date matches REPTDATE")

except Exception as e:
    print(f"  ✗ ERROR: Cannot validate FMSNPL-PIBB: {e}")
    sys.exit(77)

print(f"\n{'='*60}")
print(f"✓ EIBX1503 Complete!")
print(f"{'='*60}")
print(f"\nValidation Summary:")
print(f"  REPTDATE:        {rdate}")
print(f"  FMSNPL-PBB:      {fmsglf} ✓")
print(f"  FMSNPL-PIBB:     {fmsglfi} ✓")
print(f"\nAll BNM NPL files are correctly dated.")
print(f"Ready to proceed with NPL processing.")

sys.exit(0)
