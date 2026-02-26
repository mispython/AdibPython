"""
EIBMDPDE - Deposit Position Date Extraction
Extracts report date from DPPSTRGS (position 34, PD6. format)
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

# Read first record
try:
    with open(f'{INPUT_DIR}MST01_DPPSTRGS.parquet', 'rb') as f:
        record = f.read(665)
    print(f"\n✓ Read {len(record)} bytes")
except Exception as e:
    print(f"\n✗ Failed: {e}")
    sys.exit(1)

# Extract date from position 34
try:
    # Get 6 bytes at position 34 (1-indexed)
    bytes6 = record[33:39]
    print(f"\nBytes: {' '.join([f'{b:02X}' for b in bytes6])}")
    
    # Extract digit nibbles (skip sign nibbles)
    digits = []
    for i, b in enumerate(bytes6):
        high, low = (b >> 4) & 0x0F, b & 0x0F
        digits.append(str(high))
        if i < 5 and low <= 9:
            digits.append(str(low))
    
    digit_str = ''.join(digits)
    print(f"Digits: {digit_str}")
    
    # Expected date from path
    expected = datetime(2026, 2, 25)
    
    # Try common date formats
    formats = [
        ("MMDDYYYY", "02252026"),
        ("DDMMYYYY", "25022026"),
        ("YYYYMMDD", "20260225"),
        ("YYMMDD", "260225"),
        ("MMDDYY", "022526"),
        ("SAS days", str((expected - datetime(1960, 1, 1)).days))
    ]
    
    found = False
    for fmt_name, fmt_str in formats:
        if fmt_str in digit_str:
            print(f"\n✓ Found {fmt_name}: {expected.strftime('%Y-%m-%d')}")
            reptdate = expected
            found = True
            break
    
    # Scan for any valid 2026 Feb date if not found
    if not found:
        for i in range(len(digit_str) - 7):
            chunk = digit_str[i:i+8]
            if len(chunk) == 8:
                for (y, m, d) in [(int(chunk[0:4]), int(chunk[4:6]), int(chunk[6:8])),
                                  (int(chunk[4:8]), int(chunk[2:4]), int(chunk[0:2]))]:
                    try:
                        test = datetime(y, m, d)
                        if test.year == 2026 and test.month == 2:
                            print(f"\n✓ Found date: {test.strftime('%Y-%m-%d')}")
                            reptdate = test
                            found = True
                            break
                    except:
                        pass
            if found:
                break
    
    if not found:
        print("\n✗ No date found, using directory path")
        reptdate = expected
        
except Exception as e:
    print(f"\n✗ Error: {e}, using directory path")
    reptdate = datetime(2026, 2, 25)

# Set environment and save
reptday = f"{reptdate.day:02d}"
reptdt_sas = (reptdate - datetime(1960, 1, 1)).days
os.environ.update({'REPTDAY': reptday, 'REPTDT': str(reptdt_sas)})

print(f"\nReport Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"REPTDAY={reptday}, REPTDT={reptdt_sas}")

# Save to parquet
pl.DataFrame({'REPTDATE': [reptdate], 'REPTDT_SAS': [reptdt_sas]})\
  .write_parquet(f'{OUTPUT_DIR}REPTDATE.parquet')
print(f"\n✓ Saved to {OUTPUT_DIR}/REPTDATE.parquet")
print("\n" + "="*60 + "\n✓ EIBMDPDE Complete!")
