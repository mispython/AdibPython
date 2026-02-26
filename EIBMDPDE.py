"""
EIBMDPDE - Deposit Position Date Extraction

Purpose:
- Extract report date from DPPSTRGS file
- Read packed decimal date field from position 34
- Convert to standard date format
- Set environment variables for dependent programs
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
    
    # Correct packed decimal unpacking
    # For mainframe packed decimal, each byte (except last) contains 2 digits
    # Last byte contains last digit + sign (0xC = positive, 0xD = negative)
    
    value_str = ""
    for i, byte in enumerate(tbdate_bytes):
        # High nibble (first digit)
        high_nibble = (byte >> 4) & 0x0F
        # Low nibble (second digit, except for last byte where it's sign)
        low_nibble = byte & 0x0F
        
        if i < 5:  # First 5 bytes: both nibbles are digits
            value_str += f"{high_nibble}{low_nibble}"
        else:  # Last byte: high nibble is digit, low nibble is sign
            value_str += f"{high_nibble}"
            # Check sign (C or F = positive, D = negative)
            if low_nibble in [0x0D]:
                print(f"  Warning: Negative value detected (sign={low_nibble:02X})")
    
    tbdate_value = int(value_str)
    print(f"  TBDATE (packed): {tbdate_value}")
    
    # For packed decimal dates in MMDDYYYY format, the value should be 8 digits
    # Pad with leading zeros to ensure 8 digits
    tbdate_str = f'{tbdate_value:08d}'
    print(f"  TBDATE (8-digit): {tbdate_str}")
    
    # Parse as MMDDYYYY format
    if len(tbdate_str) == 8:
        month = int(tbdate_str[0:2])
        day = int(tbdate_str[2:4])
        year = int(tbdate_str[4:8])
        
        print(f"  Extracted: Month={month}, Day={day}, Year={year}")
        
        # Validate date components
        if month < 1 or month > 12:
            raise ValueError(f"Month must be 1-12, got {month}")
        if day < 1 or day > 31:
            raise ValueError(f"Day must be 1-31, got {day}")
        
        reptdate = datetime(year, month, day)
        
        print(f"  Parsed date:     {reptdate.strftime('%m/%d/%Y')}")
    else:
        print(f"  ✗ ERROR: Invalid date string length: {tbdate_str}")
        import sys
        sys.exit(1)

except Exception as e:
    print(f"  ✗ ERROR: Cannot extract/parse TBDATE: {e}")
    print(f"  Please verify the packed decimal format and position")
    import sys
    sys.exit(1)

# Set environment variables
print("\nSetting environment variables...")
reptday = f'{reptdate.day:02d}'
reptdt = reptdate.strftime('%Y-%m-%d')  # Store as ISO format

os.environ['REPTDAY'] = reptday
os.environ['REPTDT'] = reptdt

print(f"  REPTDAY: {reptday}")
print(f"  REPTDT:  {reptdt}")

# Save REPTDATE for other programs
print("\nSaving REPTDATE...")
try:
    df_reptdate = pl.DataFrame({
        'REPTDATE': [reptdate]
    })
    
    df_reptdate.write_parquet(f'{OUTPUT_DIR}REPTDATE.parquet')
    print(f"  ✓ REPTDATE saved: {reptdate.strftime('%d/%m/%Y')}")

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
print(f"  Report Date: {reptdate.strftime('%d/%m/%Y')} ({reptdate.strftime('%A')})")
print(f"  Day: {reptday}")
print(f"\nEnvironment Variables Set:")
print(f"  REPTDAY = {reptday}")
print(f"  REPTDT = {reptdt}")
print(f"\nOutput:")
print(f"  REPTDATE.parquet saved")

import sys
sys.exit(0)
