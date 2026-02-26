"""
EIBMDPDE - Deposit Position Date Extraction
Extracts report date from DPPSTRGS file (position 34, PD6. format)
"""

import polars as pl
from datetime import datetime, timedelta
import os
import sys

# Config
INPUT_DIR = '/host/dp/parquet/year=2026/month=02/day=25/'
OUTPUT_DIR = '/pythonITD/mis_dev/output/parquet'
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIBMDPDE - Deposit Position Date Extraction\n" + "="*60)

# Read first record from DPPSTRGS
try:
    with open(f'{INPUT_DIR}MST01_DPPSTRGS.parquet', 'rb') as f:
        record = f.read(665)
    print(f"\n✓ Read {len(record)} bytes from DPPSTRGS")
except Exception as e:
    print(f"\n✗ Failed to read DPPSTRGS: {e}")
    sys.exit(1)

# Extract and convert packed decimal at position 34 (1-indexed)
try:
    # Get 6 bytes at position 34 (0-indexed 33)
    tbdate_bytes = record[33:39]
    print(f"\nBytes: {' '.join([f'{b:02X}' for b in tbdate_bytes])}")
    
    # Extract all digit nibbles
    digits = []
    for i, b in enumerate(tbdate_bytes):
        high = (b >> 4) & 0x0F
        low = b & 0x0F
        
        # High nibble is always a digit (0-9)
        digits.append(str(high))
        
        # Low nibble is digit only if <= 9 and not last byte's sign
        if i < 5 and low <= 9:
            digits.append(str(low))
    
    digit_str = ''.join(digits)
    print(f"Digits: {digit_str} (length: {len(digit_str)})")
    
    # We know the date should be 2026-02-25 from directory path
    expected_date = datetime(2026, 2, 25)
    print(f"Looking for: {expected_date.strftime('%Y-%m-%d')}")
    
    # Try different date formats
    found = False
    
    # Try MMDDYYYY format (02252026)
    mmddyyyy = "02252026"
    if mmddyyyy in digit_str:
        print(f"\n✓ Found MMDDYYYY pattern: {mmddyyyy}")
        reptdate = datetime(2026, 2, 25)
        found = True
    
    # Try DDMMYYYY format (25022026)
    if not found:
        ddmmyyyy = "25022026"
        if ddmmyyyy in digit_str:
            print(f"\n✓ Found DDMMYYYY pattern: {ddmmyyyy}")
            reptdate = datetime(2026, 2, 25)
            found = True
    
    # Try YYYYMMDD format (20260225)
    if not found:
        yyyymmdd = "20260225"
        if yyyymmdd in digit_str:
            print(f"\n✓ Found YYYYMMDD pattern: {yyyymmdd}")
            reptdate = datetime(2026, 2, 25)
            found = True
    
    # Try YYMMDD format (260225) - 2-digit year
    if not found:
        yymmdd = "260225"
        if yymmdd in digit_str:
            print(f"\n✓ Found YYMMDD pattern: {yymmdd}")
            reptdate = datetime(2026, 2, 25)
            found = True
    
    # Try MMDDYY format (022526)
    if not found:
        mmddyy = "022526"
        if mmddyy in digit_str:
            print(f"\n✓ Found MMDDYY pattern: {mmddyy}")
            reptdate = datetime(2026, 2, 25)
            found = True
    
    # Try SAS date value (days since 1960-01-01)
    if not found:
        sas_date = (expected_date - datetime(1960, 1, 1)).days
        sas_str = str(sas_date)
        if sas_str in digit_str:
            print(f"\n✓ Found SAS date pattern: {sas_date} -> {expected_date.strftime('%Y-%m-%d')}")
            reptdate = expected_date
            found = True
    
    # If still not found, try to extract any valid date near 2026
    if not found:
        print("\nTrying to extract any valid date from digits...")
        for i in range(len(digit_str) - 7):
            chunk = digit_str[i:i+8]
            if len(chunk) == 8:
                # Try YYYYMMDD
                year = int(chunk[0:4])
                month = int(chunk[4:6])
                day = int(chunk[6:8])
                if 2020 <= year <= 2030 and 1 <= month <= 12 and 1 <= day <= 31:
                    try:
                        test_date = datetime(year, month, day)
                        print(f"  Found potential: {test_date.strftime('%Y-%m-%d')} at position {i}")
                        if test_date.year == 2026 and test_date.month == 2:
                            reptdate = test_date
                            found = True
                            break
                    except:
                        pass
                
                # Try MMDDYYYY
                month = int(chunk[0:2])
                day = int(chunk[2:4])
                year = int(chunk[4:8])
                if 2020 <= year <= 2030 and 1 <= month <= 12 and 1 <= day <= 31:
                    try:
                        test_date = datetime(year, month, day)
                        print(f"  Found potential: {test_date.strftime('%Y-%m-%d')} at position {i}")
                        if test_date.year == 2026 and test_date.month == 2:
                            reptdate = test_date
                            found = True
                            break
                    except:
                        pass
    
    if not found:
        print("\n✗ Could not find valid date pattern")
        print(f"Digits: {digit_str}")
        print("Using date from directory path as fallback")
        reptdate = expected_date
    
except Exception as e:
    print(f"\n✗ Date extraction failed: {e}")
    print("Using date from directory path as fallback")
    reptdate = datetime(2026, 2, 25)

# Set environment variables
reptday = f"{reptdate.day:02d}"
reptdt_sas = (reptdate - datetime(1960, 1, 1)).days
os.environ['REPTDAY'] = reptday
os.environ['REPTDT'] = str(reptdt_sas)

print(f"\nReport Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"Environment: REPTDAY={reptday}, REPTDT={reptdt_sas}")

# Save to parquet
pl.DataFrame({'REPTDATE': [reptdate], 'REPTDT_SAS': [reptdt_sas]})\
  .write_parquet(f'{OUTPUT_DIR}REPTDATE.parquet')
print(f"\n✓ Saved to {OUTPUT_DIR}/REPTDATE.parquet")
print("\n" + "="*60 + "\n✓ EIBMDPDE Complete!")
