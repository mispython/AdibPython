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
INPUT_DIR = 'data/input/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIBMDPDE - Deposit Position Date Extraction")
print("=" * 60)

# Read DPPSTRGS file
print("\nReading DPPSTRGS file...")
try:
    dppstrgs_path = f'{INPUT_DIR}DPPSTRGS'
    
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
    
    # Unpack packed decimal (6 bytes)
    # Packed decimal format: each byte contains 2 digits except last nibble is sign
    # For positive numbers, sign nibble is typically 0xC or 0xF
    
    # Convert packed decimal to integer
    tbdate_value = 0
    for i, byte in enumerate(tbdate_bytes):
        if i < 5:  # First 5 bytes have 2 digits each
            high_digit = (byte >> 4) & 0x0F
            low_digit = byte & 0x0F
            tbdate_value = tbdate_value * 100 + high_digit * 10 + low_digit
        else:  # Last byte has 1 digit and sign
            high_digit = (byte >> 4) & 0x0F
            tbdate_value = tbdate_value * 10 + high_digit
            # Ignore sign nibble (low nibble)
    
    print(f"  TBDATE (packed): {tbdate_value}")
    
    # Convert to 11-character zero-filled string (PUT(TBDATE, Z11.))
    tbdate_str = f'{tbdate_value:011d}'
    print(f"  TBDATE (Z11.):   {tbdate_str}")
    
    # Extract first 8 characters (SUBSTR(..., 1, 8))
    date_str = tbdate_str[0:8]
    print(f"  Date substring:  {date_str}")
    
    # Parse as MMDDYY8. format
    # MMDDYY8. = MMDDYYYY
    if len(date_str) == 8:
        month = int(date_str[0:2])
        day = int(date_str[2:4])
        year = int(date_str[4:8])
        
        reptdate = datetime(year, month, day)
        
        print(f"  Parsed date:     {reptdate.strftime('%m/%d/%Y')}")
    else:
        print(f"  ✗ ERROR: Invalid date string length: {date_str}")
        import sys
        sys.exit(1)

except Exception as e:
    print(f"  ✗ ERROR: Cannot extract/parse TBDATE: {e}")
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
