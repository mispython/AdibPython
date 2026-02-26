"""
EIBMDPDE - Deposit Position Date Extraction
Extracts report date from DPPSTRGS (position 34, PD6. format)
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
    # Get 6 bytes at position 34 (1-indexed = 33 0-indexed)
    bytes6 = record[33:39]
    print(f"\nBytes: {' '.join([f'{b:02X}' for b in bytes6])}")
    
    # Unpack PD6. to string of digits (SAS method)
    # Each byte has 2 digits except last byte's low nibble is sign
    digits = []
    for i, b in enumerate(bytes6):
        high = (b >> 4) & 0x0F
        low = b & 0x0F
        
        # High nibble is always a digit
        digits.append(str(high))
        
        # Low nibble is digit for first 5 bytes
        if i < 5:
            digits.append(str(low))
        else:
            print(f"  Sign nibble: {low} (0x{low:X})")
    
    # Join digits to get the packed decimal value
    value_str = ''.join(digits)
    print(f"All digits: {value_str} (length: {len(value_str)})")
    
    # Take first 11 digits for Z11. format (PD6. gives 11 digits)
    if len(value_str) > 11:
        value_str = value_str[:11]
    
    # Z11. format (zero-padded to 11 digits)
    tbdate_z11 = value_str.zfill(11)
    print(f"Z11.: {tbdate_z11}")
    
    # Take first 8 chars as MMDDYYYY
    date_str = tbdate_z11[:8]
    print(f"Date string: {date_str}")
    
    # Parse MMDDYYYY
    month = int(date_str[0:2])
    day = int(date_str[2:4])
    year = int(date_str[4:8])
    
    print(f"Parsed: Month={month}, Day={day}, Year={year}")
    
    # Validate
    if not (1 <= month <= 12):
        raise ValueError(f"Month must be 1-12, got {month}")
    if not (1 <= day <= 31):
        raise ValueError(f"Day must be 1-31, got {day}")
    
    reptdate = datetime(year, month, day)
    print(f"\n✓ Report date: {reptdate.strftime('%d/%m/%Y')}")
    
except Exception as e:
    print(f"\n✗ Date extraction failed: {e}")
    sys.exit(1)

# Set environment variables
reptday = f"{reptdate.day:02d}"
reptdt_sas = (reptdate - datetime(1960, 1, 1)).days
os.environ['REPTDAY'] = reptday
os.environ['REPTDT'] = str(reptdt_sas)

print(f"\nREPTDAY={reptday}, REPTDT={reptdt_sas}")

# Save to parquet
pl.DataFrame({'REPTDATE': [reptdate], 'REPTDT_SAS': [reptdt_sas]})\
  .write_parquet(f'{OUTPUT_DIR}REPTDATE.parquet')
print(f"\n✓ Saved to {OUTPUT_DIR}/REPTDATE.parquet")
print("\n" + "="*60 + "\n✓ EIBMDPDE Complete!")
