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
from datetime import datetime, timedelta
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
    
    # Method 1: Interpret as packed decimal number (could be SAS date value)
    # For packed decimal, we need to handle the sign nibble correctly
    value_str = ""
    for i, byte in enumerate(tbdate_bytes):
        high_nibble = (byte >> 4) & 0x0F
        low_nibble = byte & 0x0F
        
        if i < 5:  # First 5 bytes: both nibbles should be digits
            if high_nibble <= 9:
                value_str += str(high_nibble)
            else:
                print(f"  Warning: Non-digit high nibble {high_nibble} at byte {i}")
                
            if low_nibble <= 9:
                value_str += str(low_nibble)
            else:
                print(f"  Warning: Non-digit low nibble {low_nibble} at byte {i} (sign nibble in middle?)")
        else:  # Last byte: high nibble is digit, low nibble is sign
            if high_nibble <= 9:
                value_str += str(high_nibble)
            print(f"  Sign nibble: {low_nibble} (0x{low_nibble:X})")
    
    if value_str:
        numeric_value = int(value_str)
        print(f"  Packed decimal value: {numeric_value}")
        
        # Try interpreting as SAS date (days since 1960-01-01)
        sas_epoch = datetime(1960, 1, 1)
        
        # Check if this could be a valid SAS date for 2026
        # For Feb 2026, SAS date would be around 24140-24168
        test_date = sas_epoch + timedelta(days=numeric_value)
        if 2020 <= test_date.year <= 2030:
            print(f"  ✓ Valid SAS date interpretation: {test_date.strftime('%Y-%m-%d')}")
            reptdate = test_date
        else:
            print(f"  SAS date would be: {test_date.strftime('%Y-%m-%d')} (year {test_date.year}) - not in expected range")
            
            # Try interpreting as YYMMDD or MMDDYY numeric
            value_str_padded = value_str.zfill(8)  # Pad to 8 digits
            print(f"  Padded to 8 digits: {value_str_padded}")
            
            # Try as YYMMDD (6 digits)
            if len(value_str) >= 6:
                yymmdd = value_str[-6:]  # Take last 6 digits
                try:
                    yy = int(yymmdd[0:2])
                    mm = int(yymmdd[2:4])
                    dd = int(yymmdd[4:6])
                    
                    # Assume 2000 century for years < 70, 1900 for >= 70
                    if yy < 70:
                        year = 2000 + yy
                    else:
                        year = 1900 + yy
                    
                    if 1 <= mm <= 12 and 1 <= dd <= 31:
                        test_date = datetime(year, mm, dd)
                        if 2020 <= test_date.year <= 2030:
                            print(f"  ✓ Valid YYMMDD interpretation: {test_date.strftime('%Y-%m-%d')}")
                            reptdate = test_date
                except:
                    pass
            
            # Try as MMDDYY (6 digits)
            if len(value_str) >= 6:
                mmddyy = value_str[-6:]
                try:
                    mm = int(mmddyy[0:2])
                    dd = int(mmddyy[2:4])
                    yy = int(mmddyy[4:6])
                    
                    if yy < 70:
                        year = 2000 + yy
                    else:
                        year = 1900 + yy
                    
                    if 1 <= mm <= 12 and 1 <= dd <= 31:
                        test_date = datetime(year, mm, dd)
                        if 2020 <= test_date.year <= 2030:
                            print(f"  ✓ Valid MMDDYY interpretation: {test_date.strftime('%Y-%m-%d')}")
                            reptdate = test_date
                except:
                    pass

except Exception as e:
    print(f"  ✗ ERROR: Cannot extract/parse TBDATE: {e}")
    print(f"  Please verify the packed decimal format and position")
    import sys
    sys.exit(1)

# If we found a date, proceed
if 'reptdate' in locals():
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
else:
    print(f"\n✗ ERROR: Could not determine report date from the packed decimal data")
    print(f"  Raw bytes: {' '.join([f'{b:02X}' for b in tbdate_bytes])}")
    print(f"  This might indicate that:")
    print(f"    1. Position 34 is not the TBDATE field")
    print(f"    2. The date is stored in a different format (maybe character format)")
    print(f"    3. The file has a different record layout")
    print(f"\n  Please check the DPPSTRGS file layout documentation")
    import sys
    sys.exit(1)

import sys
sys.exit(0)
