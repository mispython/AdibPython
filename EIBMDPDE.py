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
print("\nExtracting TBDATE (packed decimal)...")
try:
    # Position 34 in SAS (1-indexed) = position 33 in Python (0-indexed)
    tbdate_bytes = record[33:39]  # 6 bytes for PD6.
    
    print(f"  Raw bytes: {' '.join([f'{b:02X}' for b in tbdate_bytes])}")
    
    # Try a completely different approach - maybe it's a normal integer
    # Convert bytes to integer (big-endian)
    int_value = int.from_bytes(tbdate_bytes, byteorder='big')
    print(f"  As big-endian integer: {int_value}")
    
    # Try as packed decimal using a proper conversion
    # In mainframe packed decimal, each byte (except last) has 2 digits
    # Last byte has 1 digit + sign (0xC = positive, 0xD = negative)
    
    digits = []
    for i, byte in enumerate(tbdate_bytes):
        high = (byte >> 4) & 0x0F
        low = byte & 0x0F
        
        if i < 5:  # First 5 bytes
            if high <= 9:
                digits.append(str(high))
            else:
                print(f"  Warning: High nibble {high} at byte {i} is not a digit")
            
            if low <= 9:
                digits.append(str(low))
            else:
                print(f"  Warning: Low nibble {low} at byte {i} is not a digit (sign nibble in middle?)")
        else:  # Last byte
            if high <= 9:
                digits.append(str(high))
            # low nibble is sign
            if low == 0x0C or low == 0x0F:
                print(f"  Positive sign (0x{low:X})")
            elif low == 0x0D:
                print(f"  Negative sign (0x{low:X})")
            else:
                print(f"  Unknown sign (0x{low:X})")
    
    if digits:
        value_str = ''.join(digits)
        print(f"  Extracted digits: {value_str} (length: {len(value_str)})")
        
        # For PD6., we expect 11 digits (5 bytes * 2 + 1 byte * 1)
        if len(value_str) > 11:
            print(f"  Truncating to 11 digits for Z11. format")
            value_str = value_str[:11]
        
        tbdate_value = int(value_str)
        print(f"  TBDATE value: {tbdate_value}")
        
        # Z11. format
        tbdate_z11 = f"{tbdate_value:011d}"
        print(f"  Z11.: {tbdate_z11}")
        
        # Take first 8 chars
        date_str = tbdate_z11[:8]
        print(f"  Date string: {date_str}")
        
        # Try different date formats
        found = False
        
        # Try as MMDDYYYY
        try:
            mm = int(date_str[0:2])
            dd = int(date_str[2:4])
            yyyy = int(date_str[4:8])
            if 1 <= mm <= 12 and 1 <= dd <= 31 and 1900 <= yyyy <= 2100:
                reptdate = datetime(yyyy, mm, dd)
                print(f"  ✓ Valid as MMDDYYYY: {reptdate.strftime('%Y-%m-%d')}")
                found = True
        except:
            pass
        
        # Try as DDMMYYYY
        if not found:
            try:
                dd = int(date_str[0:2])
                mm = int(date_str[2:4])
                yyyy = int(date_str[4:8])
                if 1 <= mm <= 12 and 1 <= dd <= 31 and 1900 <= yyyy <= 2100:
                    reptdate = datetime(yyyy, mm, dd)
                    print(f"  ✓ Valid as DDMMYYYY: {reptdate.strftime('%Y-%m-%d')}")
                    found = True
            except:
                pass
        
        # Try as YYYYMMDD
        if not found:
            try:
                yyyy = int(date_str[0:4])
                mm = int(date_str[4:6])
                dd = int(date_str[6:8])
                if 1 <= mm <= 12 and 1 <= dd <= 31 and 1900 <= yyyy <= 2100:
                    reptdate = datetime(yyyy, mm, dd)
                    print(f"  ✓ Valid as YYYYMMDD: {reptdate.strftime('%Y-%m-%d')}")
                    found = True
            except:
                pass
        
        # Try as SAS date (days since 1960)
        if not found:
            try:
                sas_days = tbdate_value
                sas_epoch = datetime(1960, 1, 1)
                test_date = sas_epoch + timedelta(days=sas_days)
                if 1900 <= test_date.year <= 2100:
                    reptdate = test_date
                    print(f"  ✓ Valid as SAS date (days since 1960): {reptdate.strftime('%Y-%m-%d')}")
                    found = True
            except:
                pass
        
        if not found:
            print(f"  ✗ ERROR: Could not find valid date format")
            print(f"  Raw bytes: {' '.join([f'{b:02X}' for b in tbdate_bytes])}")
            print(f"  This might mean position 34 is not the date field")
            
            # Try nearby positions as a last resort
            print("\n  Checking nearby positions...")
            for offset in range(-5, 6):
                if offset == 0:
                    continue
                pos = 33 + offset
                if 0 <= pos <= len(record) - 6:
                    test_bytes = record[pos:pos+6]
                    print(f"    Position {34+offset}: {' '.join([f'{b:02X}' for b in test_bytes])}")
            
            import sys
            sys.exit(1)

except Exception as e:
    print(f"  ✗ ERROR: Cannot extract/parse TBDATE: {e}")
    print(f"  Raw bytes: {' '.join([f'{b:02X}' for b in tbdate_bytes])}")
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
