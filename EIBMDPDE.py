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
    
    # Alternative packed decimal unpacking for COBOL COMP-3
    # Each byte (except last) contains two digits (high nibble, low nibble)
    # Last byte contains last digit + sign (0xC = positive, 0xD = negative)
    
    digits = []
    for i, byte in enumerate(tbdate_bytes):
        high_nibble = (byte >> 4) & 0x0F
        low_nibble = byte & 0x0F
        
        if i < 5:  # First 5 bytes: both nibbles are digits
            if high_nibble <= 9:
                digits.append(str(high_nibble))
            if low_nibble <= 9:
                digits.append(str(low_nibble))
        else:  # Last byte: high nibble is digit, low nibble is sign
            if high_nibble <= 9:
                digits.append(str(high_nibble))
            # Check sign
            if low_nibble == 0x0D:
                print("  Warning: Negative value detected")
    
    # Join digits to form the number
    tbdate_str = ''.join(digits)
    print(f"  Extracted digits: {tbdate_str}")
    
    # Try different interpretations of the date
    # Option 1: First 8 digits as MMDDYYYY
    if len(tbdate_str) >= 8:
        # Try to find a valid date pattern
        possible_dates = []
        
        # Try positions 0-7
        date_candidate = tbdate_str[0:8]
        try:
            month = int(date_candidate[0:2])
            day = int(date_candidate[2:4])
            year = int(date_candidate[4:8])
            if 1 <= month <= 12 and 1 <= day <= 31:
                possible_dates.append(('First 8', date_candidate, month, day, year))
        except:
            pass
        
        # Try positions 1-8
        if len(tbdate_str) >= 9:
            date_candidate = tbdate_str[1:9]
            try:
                month = int(date_candidate[0:2])
                day = int(date_candidate[2:4])
                year = int(date_candidate[4:8])
                if 1 <= month <= 12 and 1 <= day <= 31:
                    possible_dates.append(('Skip 1', date_candidate, month, day, year))
            except:
                pass
        
        # Try positions 2-9
        if len(tbdate_str) >= 10:
            date_candidate = tbdate_str[2:10]
            try:
                month = int(date_candidate[0:2])
                day = int(date_candidate[2:4])
                year = int(date_candidate[4:8])
                if 1 <= month <= 12 and 1 <= day <= 31:
                    possible_dates.append(('Skip 2', date_candidate, month, day, year))
            except:
                pass
        
        # Try as YYYYMMDD
        if len(tbdate_str) >= 8:
            date_candidate = tbdate_str[0:8]
            try:
                year = int(date_candidate[0:4])
                month = int(date_candidate[4:6])
                day = int(date_candidate[6:8])
                if 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                    possible_dates.append(('YYYYMMDD', date_candidate, month, day, year))
            except:
                pass
        
        # Try as DDMMYYYY
        if len(tbdate_str) >= 8:
            date_candidate = tbdate_str[0:8]
            try:
                day = int(date_candidate[0:2])
                month = int(date_candidate[2:4])
                year = int(date_candidate[4:8])
                if 1 <= month <= 12 and 1 <= day <= 31:
                    possible_dates.append(('DDMMYYYY', date_candidate, month, day, year))
            except:
                pass
        
        if possible_dates:
            # Use the first valid date found
            format_desc, date_str, month, day, year = possible_dates[0]
            print(f"  ✓ Found valid date using {format_desc} format: {date_str}")
            print(f"    Month={month}, Day={day}, Year={year}")
            
            reptdate = datetime(year, month, day)
            print(f"  Parsed date: {reptdate.strftime('%m/%d/%Y')}")
        else:
            print(f"  ✗ ERROR: No valid date pattern found in digits: {tbdate_str}")
            print(f"  Raw bytes: {' '.join([f'{b:02X}' for b in tbdate_bytes])}")
            print(f"  Please check the packed decimal format and position")
            import sys
            sys.exit(1)
    else:
        print(f"  ✗ ERROR: Not enough digits extracted: {tbdate_str}")
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
