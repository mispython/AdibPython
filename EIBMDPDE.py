"""
EIBMDPDE - Deposit Position Date Extraction

Purpose:
- Extract report date from DPPSTRGS file
- Find the correct position for TBDATE
- Convert to standard date format
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

# Expected date from directory path (2026-02-25)
expected_date = datetime(2026, 2, 25)
print(f"\nLooking for date: {expected_date.strftime('%Y-%m-%d')}")

# Function to unpack packed decimal
def unpack_packed_decimal(bytes_data):
    """Unpack mainframe packed decimal to string of digits"""
    digits = []
    for i, byte in enumerate(bytes_data):
        high = (byte >> 4) & 0x0F
        low = byte & 0x0F
        
        # Add high nibble if it's a digit
        if high <= 9:
            digits.append(str(high))
        
        # Add low nibble if it's a digit and not the last byte's low nibble
        if i < len(bytes_data) - 1:
            if low <= 9:
                digits.append(str(low))
        else:
            # Last byte's low nibble is sign
            pass
    
    return ''.join(digits)

# Try different positions around 34
print("\nScanning nearby positions for valid dates...")
print("-" * 60)

found_date = None
found_position = None

for offset in range(-10, 11):
    pos = 33 + offset  # 0-indexed position
    if pos < 0 or pos > len(record) - 6:
        continue
    
    test_bytes = record[pos:pos+6]
    byte_hex = ' '.join([f'{b:02X}' for b in test_bytes])
    
    # Unpack the packed decimal
    digits_str = unpack_packed_decimal(test_bytes)
    
    if len(digits_str) >= 8:
        # Take first 8 digits for date
        date_candidate = digits_str[:8]
        
        # Try different interpretations
        interpretations = []
        
        # MMDDYYYY
        try:
            mm = int(date_candidate[0:2])
            dd = int(date_candidate[2:4])
            yyyy = int(date_candidate[4:8])
            if 1 <= mm <= 12 and 1 <= dd <= 31 and 1900 <= yyyy <= 2100:
                test_date = datetime(yyyy, mm, dd)
                interpretations.append(("MMDDYYYY", test_date))
        except:
            pass
        
        # DDMMYYYY
        try:
            dd = int(date_candidate[0:2])
            mm = int(date_candidate[2:4])
            yyyy = int(date_candidate[4:8])
            if 1 <= mm <= 12 and 1 <= dd <= 31 and 1900 <= yyyy <= 2100:
                test_date = datetime(yyyy, mm, dd)
                interpretations.append(("DDMMYYYY", test_date))
        except:
            pass
        
        # YYYYMMDD
        try:
            yyyy = int(date_candidate[0:4])
            mm = int(date_candidate[4:6])
            dd = int(date_candidate[6:8])
            if 1 <= mm <= 12 and 1 <= dd <= 31 and 1900 <= yyyy <= 2100:
                test_date = datetime(yyyy, mm, dd)
                interpretations.append(("YYYYMMDD", test_date))
        except:
            pass
        
        # SAS date (days since 1960)
        try:
            sas_days = int(digits_str)
            sas_epoch = datetime(1960, 1, 1)
            test_date = sas_epoch + timedelta(days=sas_days)
            if 1900 <= test_date.year <= 2100:
                interpretations.append(("SAS days", test_date))
        except:
            pass
        
        # Check if any interpretation matches expected date
        for fmt, test_date in interpretations:
            if test_date.date() == expected_date.date():
                print(f"  ✓ Position {34+offset}: {byte_hex}")
                print(f"    Digits: {digits_str}")
                print(f"    Date: {test_date.strftime('%Y-%m-%d')} ({fmt})")
                found_date = test_date
                found_position = 34 + offset
                break
        
        if found_date:
            break

if not found_date:
    print("\nNo exact match found. Listing all potential dates:")
    print("-" * 60)
    
    for offset in range(-10, 11):
        pos = 33 + offset
        if pos < 0 or pos > len(record) - 6:
            continue
        
        test_bytes = record[pos:pos+6]
        byte_hex = ' '.join([f'{b:02X}' for b in test_bytes])
        digits_str = unpack_packed_decimal(test_bytes)
        
        if len(digits_str) >= 8:
            date_candidate = digits_str[:8]
            
            # Try MMDDYYYY
            try:
                mm = int(date_candidate[0:2])
                dd = int(date_candidate[2:4])
                yyyy = int(date_candidate[4:8])
                if 1 <= mm <= 12 and 1 <= dd <= 31 and 2000 <= yyyy <= 2030:
                    test_date = datetime(yyyy, mm, dd)
                    print(f"  Pos {34+offset:2d}: {byte_hex} -> {digits_str} -> {test_date.strftime('%Y-%m-%d')} (MMDDYYYY)")
            except:
                pass

# If we found a date, use it
if found_date:
    reptdate = found_date
    print(f"\n✓ Found matching date at position {found_position}")
else:
    print(f"\n✗ Could not find the expected date {expected_date.strftime('%Y-%m-%d')}")
    print("Please check the file layout for the correct TBDATE position")
    import sys
    sys.exit(1)

# Set environment variables
print("\nSetting environment variables...")
reptday = f"{reptdate.day:02d}"
reptdt_sas = (reptdate - datetime(1960, 1, 1)).days

os.environ['REPTDAY'] = reptday
os.environ['REPTDT'] = str(reptdt_sas)

print(f"  REPTDAY: {reptday}")
print(f"  REPTDT:  {reptdt_sas}")

# Save REPTDATE
print("\nSaving REPTDATE...")
try:
    df_reptdate = pl.DataFrame({
        'REPTDATE': [reptdate],
        'REPTDT_SAS': [reptdt_sas]
    })
    
    df_reptdate.write_parquet(f'{OUTPUT_DIR}REPTDATE.parquet')
    print(f"  ✓ REPTDATE.parquet saved")

except Exception as e:
    print(f"  ✗ ERROR: Cannot save REPTDATE: {e}")
    import sys
    sys.exit(1)

print(f"\n{'='*60}")
print(f"✓ EIBMDPDE Complete!")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")

import sys
sys.exit(0)
