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
    
    # Unpack PD6. to integer (SAS method)
    # Each byte has 2 digits except last byte's low nibble is sign
    value = 0
    for i, b in enumerate(tbdate_bytes):
        high, low = (b >> 4) & 0x0F, b & 0x0F
        
        if i < 5:  # First 5 bytes: both nibbles are digits
            value = value * 100 + high * 10 + low
        else:      # Last byte: high nibble digit, low nibble sign (ignored)
            value = value * 10 + high
    
    # Z11. format: zero-pad to 11 digits
    tbdate_z11 = f"{value:011d}"
    
    # Take first 8 chars as MMDDYYYY
    date_str = tbdate_z11[:8]
    reptdate = datetime(int(date_str[4:8]), int(date_str[0:2]), int(date_str[2:4]))
    
    print(f"\n✓ Report date: {reptdate.strftime('%d/%m/%Y')}")
    
except Exception as e:
    print(f"\n✗ Date extraction failed: {e}")
    print(f"   Bytes: {' '.join([f'{b:02X}' for b in tbdate_bytes])}")
    sys.exit(1)

# Set environment variables (SAS compatibility)
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
