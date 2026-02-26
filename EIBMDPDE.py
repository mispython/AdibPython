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
    # Get 6 bytes at position 34 (1-indexed)
    bytes6 = record[33:39]
    
    # Unpack PD6. to integer (SAS method)
    value = 0
    for i, b in enumerate(bytes6):
        high, low = (b >> 4) & 0x0F, b & 0x0F
        if i < 5:
            value = value * 100 + high * 10 + low
        else:
            value = value * 10 + high
    
    # Z11. format → first 8 chars → MMDDYYYY
    date_str = f"{value:011d}"[:8]
    reptdate = datetime(int(date_str[4:8]), int(date_str[0:2]), int(date_str[2:4]))
    
    print(f"\n✓ Report date: {reptdate.strftime('%d/%m/%Y')}")
    
except Exception as e:
    print(f"\n✗ Date extraction failed: {e}")
    sys.exit(1)

# Set environment variables
reptday = f"{reptdate.day:02d}"
reptdt_sas = (reptdate - datetime(1960, 1, 1)).days
os.environ.update({'REPTDAY': reptday, 'REPTDT': str(reptdt_sas)})

# Save to parquet
pl.DataFrame({'REPTDATE': [reptdate], 'REPTDT_SAS': [reptdt_sas]})\
  .write_parquet(f'{OUTPUT_DIR}REPTDATE.parquet')

print(f"\nREPTDAY={reptday}, REPTDT={reptdt_sas}")
print(f"\n✓ Saved to {OUTPUT_DIR}/REPTDATE.parquet")
print("\n" + "="*60 + "\n✓ EIBMDPDE Complete!")
