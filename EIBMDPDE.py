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
    print(f"  Binary:    {' '.join([f'{b:08b}' for b in tbdate_bytes])}")
    
    # Proper packed decimal unpacking
    digits = []
    for i, byte in enumerate(tbdate_bytes):
        high_nibble = (byte >> 4) & 0x0F
        low_nibble = byte & 0x0F
        
        print(f"  Byte {i}: {byte:02X} -> high={high_nibble} (0x{high_nibble:X}), low={low_nibble} (0x{low_nibble:X})")
        
        # Both nibbles are digits (0-9)
        if high_nibble <= 9:
            digits.append(str(high_nibble))
        else:
            print(f"    Warning: High nibble {high_nibble} is not a digit (sign nibble?)")
        
        if low_nibble <= 9 and i < 5:  # All but last byte's low nibble can be digits
            digits.append(str(low_nibble))
        elif i == 5:  # Last byte's low nibble is sign
            print(f"    Sign nibble: {low_nibble} (0x{low_nibble:X}) - {'positive' if low_nibble in [0xC, 0xF] else 'negative' if low_nibble == 0xD else 'unknown'}")
    
    # Join digits to form the number
    tbdate_str = ''.join(digits)
    print(f"  Extracted digits: {tbdate_str} (length: {len(tbdate_str)})")
    
    # Try to find a valid date in the digits
    found_date = False
    
    # Try different interpretations of the date
    interpretations = [
        ("MMDDYYYY (first 8)", tbdate_str[0:8]),
        ("MMDDYYYY (skip 1)", tbdate_str[1:9] if len(tbdate_str) >= 9 else ""),
        ("MMDDYYYY (skip 2)", tbdate_str[2:10] if len(tbdate_str) >= 10 else ""),
        ("YYYYMMDD (first 8)", tbdate_str[0:8]),
        ("YYYYMMDD (skip 1)", tbdate_str[1:9] if len(tbdate_str) >= 9 else ""),
        ("DDMMYYYY (first 8)", tbdate_str[0:8]),
        ("DDMMYYYY (skip 1)", tbdate_str[1:9] if len(tbdate_str) >= 9 else ""),
    ]
    
    for desc, date_candidate in interpretations:
        if len(date_candidate) == 8:
            print(f"  Trying {desc}: {date_candidate}")
            
            # Try MMDDYYYY
            try:
                month = int(date_candidate[0:2])
                day = int(date_candidate[2:4])
                year = int(date_candidate[4:8])
                if 1 <= month <= 12 and 1 <= day <= 31 and 1900 <= year <= 2100:
                    # Validate day based on month
                    if month in [4, 6, 9, 11] and day > 30:
                        continue
                    elif month == 2:
                        # Simple leap year check
                        leap_year = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
                        if day > 29 or (day > 28 and not leap_year):
                            continue
                    
                    print(f"  ✓ Valid MMDDYYYY date found: {month:02d}/{day:02d}/{year}")
                    reptdate = datetime(year, month, day)
                    found_date = True
                    break
            except:
                pass
            
            # Try YYYYMMDD
            try:
                year = int(date_candidate[0:4])
                month = int(date_candidate[4:6])
                day = int(date_candidate[6:8])
                if 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                    # Validate day based on month
                    if month in [4, 6, 9, 11] and day > 30:
                        continue
                    elif month == 2:
                        leap_year = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
                        if day > 29 or (day > 28 and not leap_year):
                            continue
                    
                    print(f"  ✓ Valid YYYYMMDD date found: {year}-{month:02d}-{day:02d}")
                    reptdate = datetime(year, month, day)
                    found_date = True
                    break
            except:
                pass
            
            # Try DDMMYYYY
            try:
                day = int(date_candidate[0:2])
                month = int(date_candidate[2:4])
                year = int(date_candidate[4:8])
                if 1 <= month <= 12 and 1 <= day <= 31 and 1900 <= year <= 2100:
                    # Validate day based on month
                    if month in [4, 6, 9, 11] and day > 30:
                        continue
                    elif month == 2:
                        leap_year = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
                        if day > 29 or (day > 28 and not leap_year):
                            continue
                    
                    print(f"  ✓ Valid DDMMYYYY date found: {day:02d}/{month:02d}/{year}")
                    reptdate = datetime(year, month, day)
                    found_date = True
                    break
            except:
                pass
    
    if not found_date:
        print(f"  ✗ ERROR: No valid date pattern found in digits: {tbdate_str}")
        print(f"  Raw bytes: {' '.join([f'{b:02X}' for b in tbdate_bytes])}")
        print(f"  This might indicate that position 34 is not the date field, or the date is in a different format")
        print(f"  Please check the file layout for DPPSTRGS to confirm the correct position for TBDATE")
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
