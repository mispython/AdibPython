"""
EIBMDPDE - Deposit Position Date Extraction
Extracts report date from DPPSTRGS file (position 34, PD6. format)
"""

import polars as pl
from datetime import datetime
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
    
    # Method 1: Try different date format (YYYYMMDD)
    # Extract all digit nibbles (ignore sign nibbles)
    digits = []
    for i, b in enumerate(tbdate_bytes):
        high = (b >> 4) & 0x0F
        low = b & 0x0F
        
        # High nibble is always a digit
        if high <= 9:
            digits.append(str(high))
        
        # Low nibble is digit only if <= 9 and not last byte
        if i < 5 and low <= 9:
            digits.append(str(low))
    
    digit_str = ''.join(digits)
    print(f"Digits: {digit_str}")
    
    # Try to find a valid date pattern
    found = False
    
    # Try as YYYYMMDD (looking for 2026)
    if len(digit_str) >= 8:
        # Look for 2026 in the digits
        for i in range(len(digit_str) - 7):
            year_candidate = digit_str[i:i+4]
            if year_candidate == "2026":
                month = int(digit_str[i+4:i+6])
                day = int(digit_str[i+6:i+8])
                if 1 <= month <= 12 and 1 <= day <= 31:
                    reptdate = datetime(2026, month, day)
                    print(f"\n✓ Found YYYYMMDD pattern: 2026-{month:02d}-{day:02d}")
                    found = True
                    break
    
    # Try as MMDDYYYY
    if not found and len(digit_str) >= 8:
        for i in range(len(digit_str) - 7):
            year_candidate = digit_str[i+4:i+8]
            if year_candidate == "2026":
                month = int(digit_str[i:i+2])
                day = int(digit_str[i+2:i+4])
                if 1 <= month <= 12 and 1 <= day <= 31:
                    reptdate = datetime(2026, month, day)
                    print(f"\n✓ Found MMDDYYYY pattern: {month:02d}/{day:02d}/2026")
                    found = True
                    break
    
    # Try as SAS date (days since 1960)
    if not found:
        # Convert all digits to a number
        try:
            sas_days = int(digit_str)
            sas_epoch = datetime(1960, 1, 1)
            test_date = sas_epoch + timedelta(days=sas_days)
            if test_date.year == 2026:
                reptdate = test_date
                print(f"\n✓ Found SAS date pattern: {reptdate.strftime('%Y-%m-%d')}")
                found = True
        except:
            pass
    
    if not found:
        print("\n✗ Could not find valid date pattern")
        sys.exit(1)
    
except Exception as e:
    print(f"\n✗ Date extraction failed: {e}")
    sys.exit(1)

# Set environment variables
reptday = f"{reptdate.day:02d}"
reptdt_sas = (reptdate - datetime(1960, 1, 1)).days
os.environ['REPTDAY'] = reptday
os.environ['REPTDT'] = str(reptdt_sas)

print(f"\nEnvironment: REPTDAY={reptday}, REPTDT={reptdt_sas}")

# Save to parquet
pl.DataFrame({'REPTDATE': [reptdate], 'REPTDT_SAS': [reptdt_sas]})\
  .write_parquet(f'{OUTPUT_DIR}REPTDATE.parquet')
print(f"\n✓ Saved to {OUTPUT_DIR}/REPTDATE.parquet")
print("\n" + "="*60 + "\n✓ EIBMDPDE Complete!")
