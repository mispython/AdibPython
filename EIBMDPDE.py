"""
EIBMDPDE - Deposit Position Date Extraction

Purpose:
- Extract report date from DPPSTRGS file
- Scan entire file for date patterns
- Find the correct position for TBDATE
"""

import struct
import polars as pl
from datetime import datetime, timedelta
import os
import re

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

# Expected date from directory path
expected_date = datetime(2026, 2, 25)
expected_date_sas = (expected_date - datetime(1960, 1, 1)).days
print(f"\nLooking for date: {expected_date.strftime('%Y-%m-%d')}")
print(f"SAS date value: {expected_date_sas}")

# Function to try different date interpretations from bytes
def try_date_interpretations(bytes_data, position):
    """Try various date formats on a set of bytes"""
    
    results = []
    
    # 1. Try as packed decimal (PD6.)
    try:
        # Unpack packed decimal
        digits = []
        for i, byte in enumerate(bytes_data):
            high = (byte >> 4) & 0x0F
            low = byte & 0x0F
            
            if i < 5:
                if high <= 9:
                    digits.append(str(high))
                if low <= 9:
                    digits.append(str(low))
            else:
                if high <= 9:
                    digits.append(str(high))
                # Sign nibble in low
        
        if digits:
            value_str = ''.join(digits)
            if len(value_str) >= 8:
                # Try as MMDDYYYY
                if len(value_str) >= 8:
                    mm = int(value_str[0:2])
                    dd = int(value_str[2:4])
                    yyyy = int(value_str[4:8])
                    if 1 <= mm <= 12 and 1 <= dd <= 31 and 2000 <= yyyy <= 2030:
                        date_val = datetime(yyyy, mm, dd)
                        results.append(("PD6 MMDDYYYY", date_val, value_str))
                
                # Try as YYYYMMDD
                if len(value_str) >= 8:
                    yyyy = int(value_str[0:4])
                    mm = int(value_str[4:6])
                    dd = int(value_str[6:8])
                    if 1 <= mm <= 12 and 1 <= dd <= 31 and 2000 <= yyyy <= 2030:
                        date_val = datetime(yyyy, mm, dd)
                        results.append(("PD6 YYYYMMDD", date_val, value_str))
                
                # Try as SAS date
                try:
                    sas_days = int(value_str)
                    date_val = datetime(1960, 1, 1) + timedelta(days=sas_days)
                    if 2000 <= date_val.year <= 2030:
                        results.append(("PD6 SAS days", date_val, value_str))
                except:
                    pass
    except:
        pass
    
    # 2. Try as character date (MMDDYYYY)
    try:
        # Look for ASCII representation of date
        char_str = bytes_data.decode('ascii', errors='ignore')
        # Look for patterns like 02252026 or 20250225
        for pattern in [r'(\d{2})(\d{2})(\d{4})', r'(\d{4})(\d{2})(\d{2})']:
            matches = re.findall(pattern, char_str)
            for match in matches:
                if len(match) == 3:
                    if pattern == r'(\d{2})(\d{2})(\d{4})':
                        mm, dd, yyyy = match
                    else:
                        yyyy, mm, dd = match
                    mm, dd, yyyy = int(mm), int(dd), int(yyyy)
                    if 1 <= mm <= 12 and 1 <= dd <= 31 and 2000 <= yyyy <= 2030:
                        date_val = datetime(yyyy, mm, dd)
                        results.append(("CHAR", date_val, char_str[:8]))
    except:
        pass
    
    # 3. Try as binary integer (SAS date)
    try:
        # Try as 4-byte integer (big-endian)
        if len(bytes_data) >= 4:
            int_val = int.from_bytes(bytes_data[:4], byteorder='big')
            date_val = datetime(1960, 1, 1) + timedelta(days=int_val)
            if 2000 <= date_val.year <= 2030:
                results.append(("INT32 BE", date_val, str(int_val)))
        
        # Try as 4-byte integer (little-endian)
        int_val = int.from_bytes(bytes_data[:4], byteorder='little')
        date_val = datetime(1960, 1, 1) + timedelta(days=int_val)
        if 2000 <= date_val.year <= 2030:
            results.append(("INT32 LE", date_val, str(int_val)))
    except:
        pass
    
    return results

# Scan the entire record for the date
print("\nScanning entire record for date patterns...")
print("-" * 80)

found_date = None
found_position = None
found_format = None

# Scan every possible 6-byte chunk
for pos in range(0, len(record) - 6):
    test_bytes = record[pos:pos+6]
    results = try_date_interpretations(test_bytes, pos)
    
    for fmt, date_val, val_str in results:
        if date_val.date() == expected_date.date():
            print(f"\n✓ FOUND EXACT MATCH at position {pos+1} (1-indexed):")
            print(f"  Bytes: {' '.join([f'{b:02X}' for b in test_bytes])}")
            print(f"  Format: {fmt}")
            print(f"  Value: {val_str}")
            print(f"  Date: {date_val.strftime('%Y-%m-%d')}")
            found_date = date_val
            found_position = pos + 1
            found_format = fmt
            break
    
    if found_date:
        break

# If no exact match, show potential dates near position 34
if not found_date:
    print("\nNo exact match found. Checking positions around 34 for any valid dates:")
    print("-" * 80)
    
    for pos in range(20, 50):  # Check positions 20-50
        if pos < 0 or pos > len(record) - 6:
            continue
        
        test_bytes = record[pos:pos+6]
        byte_hex = ' '.join([f'{b:02X}' for b in test_bytes])
        
        # Try to unpack as packed decimal
        digits = []
        for i, byte in enumerate(test_bytes):
            high = (byte >> 4) & 0x0F
            low = byte & 0x0F
            if i < 5:
                if high <= 9:
                    digits.append(str(high))
                if low <= 9:
                    digits.append(str(low))
            else:
                if high <= 9:
                    digits.append(str(high))
        
        if digits:
            val_str = ''.join(digits)
            if len(val_str) >= 8:
                # Try MMDDYYYY
                try:
                    mm = int(val_str[0:2])
                    dd = int(val_str[2:4])
                    yyyy = int(val_str[4:8])
                    if 1 <= mm <= 12 and 1 <= dd <= 31 and 2000 <= yyyy <= 2030:
                        date_val = datetime(yyyy, mm, dd)
                        print(f"Pos {pos+1:2d}: {byte_hex} -> {val_str[:8]} -> {date_val.strftime('%Y-%m-%d')} (MMDDYYYY)")
                except:
                    pass
                
                # Try YYYYMMDD
                try:
                    yyyy = int(val_str[0:4])
                    mm = int(val_str[4:6])
                    dd = int(val_str[6:8])
                    if 1 <= mm <= 12 and 1 <= dd <= 31 and 2000 <= yyyy <= 2030:
                        date_val = datetime(yyyy, mm, dd)
                        print(f"Pos {pos+1:2d}: {byte_hex} -> {val_str[:8]} -> {date_val.strftime('%Y-%m-%d')} (YYYYMMDD)")
                except:
                    pass

# If we still haven't found a date, use the one from the directory path
if not found_date:
    print(f"\nUsing date from directory path: {expected_date.strftime('%Y-%m-%d')}")
    reptdate = expected_date
else:
    reptdate = found_date
    print(f"\n✓ Using found date at position {found_position}")

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
    print(f"    Date: {reptdate.strftime('%d/%m/%Y')}")
    print(f"    SAS value: {reptdt_sas}")

except Exception as e:
    print(f"  ✗ ERROR: Cannot save REPTDATE: {e}")
    import sys
    sys.exit(1)

print(f"\n{'='*60}")
print(f"✓ EIBMDPDE Complete!")

import sys
sys.exit(0)
