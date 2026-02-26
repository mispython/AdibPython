"""
EIBMDPDE - Deposit Position Date Extraction

Purpose:
- Extract report date from DPPSTRGS file
- Read packed decimal date field from position 34
- Convert to standard date format
- Set environment variables for dependent programs

Based on SAS code:
  DATA REPTDATE;
    INFILE DPPSTRGS LRECL=665 OBS=1;
    INPUT @034 TBDATE PD6.;
    REPTDATE=INPUT(SUBSTR(PUT(TBDATE, Z11.), 1, 8), MMDDYY8.);
    CALL SYMPUT('REPTDAY',PUT(DAY(REPTDATE),Z2.));
    CALL SYMPUT('REPTDT',REPTDATE);
  RUN;
"""

import struct
import polars as pl
from datetime import datetime
import os

# Directories
INPUT_DIR = '/host/dp/parquet/year=2026/month=02/day=25/'
OUTPUT_DIR = '/pythonITD/mis_dev/output/parquet'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIBMDPDE - Deposit Position Date Extraction")
print("=" * 60)

# Read DPPSTRGS file
print("\nReading DPPSTRGS file...")
try:
    dppstrgs_path = f'{INPUT_DIR}MST01_DPPSTRGS.parquet'
    
    if not os.path.exists(dppstrgs_path):
        print(f"  ✗ ERROR: DPPSTRGS file not found: {dppstrgs_path}")
        import sys
        sys.exit(1)
    
    # Read first record only (OBS=1)
    # LRECL=665
    with open(dppstrgs_path, 'rb') as f:
        record = f.read(665)
    
    if len(record) < 665:
        print(f"  ✗ ERROR: Record too short. Expected 665 bytes, got {len(record)}")
        import sys
        sys.exit(1)
    
    print(f"  ✓ DPPSTRGS: Record read ({len(record)} bytes)")

except Exception as e:
    print(f"  ✗ ERROR: Cannot read DPPSTRGS: {e}")
    import sys
    sys.exit(1)

# Extract TBDATE from position 34 (0-indexed = position 33)
# PD6. = 6-byte packed decimal
print("\nExtracting TBDATE (packed decimal)...")
try:
    # Position 34 in SAS (1-indexed) = position 33 in Python (0-indexed)
    tbdate_bytes = record[33:39]  # 6 bytes for PD6.
    
    print(f"  Raw bytes: {' '.join([f'{b:02X}' for b in tbdate_bytes])}")
    
    # Correct packed decimal unpacking for SAS PD6.
    # In SAS, PD6. reads 6 bytes and stores as numeric
    # The bytes contain digits in each nibble, with last nibble as sign
    # For positive numbers, sign nibble is usually 0xC or 0xF
    
    digits = []
    for i, byte in enumerate(tbdate_bytes):
        high_nibble = (byte >> 4) & 0x0F
        low_nibble = byte & 0x0F
        
        # For SAS packed decimal, all nibbles except the last low nibble are digits
        if i < 5:  # First 5 bytes: both nibbles are digits
            digits.append(str(high_nibble))
            digits.append(str(low_nibble))
        else:  # Last byte: high nibble is digit, low nibble is sign (ignore)
            digits.append(str(high_nibble))
            # We ignore the sign nibble
    
    # Join digits to form the number
    digit_string = ''.join(digits)
    print(f"  All digits extracted: {digit_string} (length: {len(digit_string)})")
    
    # In SAS, PD6. should give us 11 digits (5 bytes * 2 + 1 byte * 1 = 11 digits)
    # But we might have extracted too many due to sign nibbles being treated as digits
    # Let's take only the first 11 digits if we have more
    if len(digit_string) > 11:
        print(f"  Warning: Extracted {len(digit_string)} digits, truncating to 11 for Z11. format")
        digit_string = digit_string[:11]
    
    # Convert to integer (this is the numeric value SAS would store)
    tbdate_value = int(digit_string)
    print(f"  TBDATE numeric value: {tbdate_value}")
    
    # Step 2: PUT(TBDATE, Z11.) - Convert to 11-character zero-filled string
    tbdate_z11 = f"{tbdate_value:011d}"
    print(f"  TBDATE Z11.: {tbdate_z11}")
    
    # Step 3: SUBSTR(..., 1, 8) - Take first 8 characters
    date_str = tbdate_z11[0:8]
    print(f"  SUBSTR (1,8): {date_str}")
    
    # Step 4: INPUT(..., MMDDYY8.) - Convert to SAS date
    # MMDDYY8. format expects MMDDYYYY
    if len(date_str) == 8:
        month = int(date_str[0:2])
        day = int(date_str[2:4])
        year = int(date_str[4:8])
        
        print(f"  Parsed: Month={month}, Day={day}, Year={year}")
        
        # Validate
        if 1 <= month <= 12 and 1 <= day <= 31 and 1900 <= year <= 2100:
            reptdate = datetime(year, month, day)
            print(f"  ✓ Valid date: {reptdate.strftime('%m/%d/%Y')}")
        else:
            # Try alternative interpretation - maybe it's YYYYMMDD?
            year = int(date_str[0:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])
            print(f"  Trying YYYYMMDD: Year={year}, Month={month}, Day={day}")
            
            if 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                reptdate = datetime(year, month, day)
                print(f"  ✓ Valid date (YYYYMMDD): {reptdate.strftime('%m/%d/%Y')}")
            else:
                print(f"  ✗ ERROR: Invalid date components")
                print(f"    MMDDYYYY: Month={int(date_str[0:2])}, Day={int(date_str[2:4])}, Year={int(date_str[4:8])}")
                print(f"    YYYYMMDD: Year={int(date_str[0:4])}, Month={int(date_str[4:6])}, Day={int(date_str[6:8])}")
                import sys
                sys.exit(1)
    else:
        print(f"  ✗ ERROR: Expected 8-character date string, got '{date_str}'")
        import sys
        sys.exit(1)

except Exception as e:
    print(f"  ✗ ERROR: Cannot extract/parse TBDATE: {e}")
    print(f"  Raw bytes: {' '.join([f'{b:02X}' for b in tbdate_bytes])}")
    import sys
    sys.exit(1)

# Step 5: CALL SYMPUT - Set environment variables
print("\nSetting environment variables...")

# PUT(DAY(REPTDATE), Z2.) - Day with zero padding (2 digits)
reptday = f"{reptdate.day:02d}"
print(f"  REPTDAY (Z2.): {reptday}")

# REPTDATE (SAS date value) - In SAS, this is days since 1960-01-01
sas_epoch = datetime(1960, 1, 1)
reptdt_sas = (reptdate - sas_epoch).days
print(f"  REPTDT (SAS date value): {reptdt_sas}")

# Set environment variables
os.environ['REPTDAY'] = reptday
os.environ['REPTDT'] = str(reptdt_sas)  # Store as SAS date value

print(f"  Environment variables set:")
print(f"    REPTDAY = {reptday}")
print(f"    REPTDT  = {reptdt_sas}")

# Save REPTDATE for other programs
print("\nSaving REPTDATE...")
try:
    # Store both the Python datetime and the SAS date value
    df_reptdate = pl.DataFrame({
        'REPTDATE': [reptdate],  # Python datetime
        'REPTDT_SAS': [reptdt_sas]  # SAS date value (days since 1960-01-01)
    })
    
    df_reptdate.write_parquet(f'{OUTPUT_DIR}REPTDATE.parquet')
    print(f"  ✓ REPTDATE.parquet saved")
    print(f"    Date: {reptdate.strftime('%d/%m/%Y')}")
    print(f"    SAS value: {reptdt_sas}")

except Exception as e:
    print(f"  ✗ ERROR: Cannot save REPTDATE: {e}")
    import sys
    sys.exit(1)

print(f"\n{'='*60}")
print(f"✓ EIBMDPDE Complete!")
print(f"{'='*60}")
print(f"\nDate Extraction Summary:")
print(f"  Source File: DPPSTRGS")
print(f"  Position: 34 (6-byte packed decimal)")
print(f"  Raw bytes: {' '.join([f'{b:02X}' for b in tbdate_bytes])}")
print(f"  Z11. value: {tbdate_z11}")
print(f"  Date string: {date_str}")
print(f"  Report Date: {reptdate.strftime('%d/%m/%Y')} ({reptdate.strftime('%A')})")
print(f"  SAS date value: {reptdt_sas}")
print(f"\nEnvironment Variables Set:")
print(f"  REPTDAY = {reptday}")
print(f"  REPTDT = {reptdt_sas}")
print(f"\nOutput:")
print(f"  REPTDATE.parquet saved")

import sys
sys.exit(0)
