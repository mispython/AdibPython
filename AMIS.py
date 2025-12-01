import polars as pl
import os
import saspy
from datetime import datetime, timedelta
from pathlib import Path

# =============================================================================
# INITIALIZATION
# =============================================================================
sas = saspy.SASsession() if saspy else None
REPTDATE = datetime.today() - timedelta(days=1)

print(f"PROCESSING DATE: {REPTDATE:%Y-%m-%d}")
print(f"CURRENT DIRECTORY: {os.getcwd()}")
print(f"USER: {os.getenv('USER')}")

# =============================================================================
# PATH CONFIGURATION - WITH DEBUGGING
# =============================================================================
BASE_PATH = "/host/mis/parquet/crm"
CURRENT_MONTH_PATH = f"{BASE_PATH}/year={REPTDATE.year}/month={REPTDATE.month:02d}"

print(f"\nPATH CHECK:")
print(f"BASE_PATH: {BASE_PATH}")
print(f"CURRENT_MONTH_PATH: {CURRENT_MONTH_PATH}")

# Check if base path exists
print(f"\nDIRECTORY CHECK:")
print(f"BASE_PATH exists: {os.path.exists(BASE_PATH)}")
print(f"CURRENT_MONTH_PATH exists: {os.path.exists(CURRENT_MONTH_PATH)}")

if not os.path.exists(CURRENT_MONTH_PATH):
    print(f"Creating directory: {CURRENT_MONTH_PATH}")
    os.makedirs(CURRENT_MONTH_PATH, exist_ok=True)
    print(f"Created successfully: {os.path.exists(CURRENT_MONTH_PATH)}")

# Check permissions
print(f"\nPERMISSIONS CHECK:")
if os.path.exists(CURRENT_MONTH_PATH):
    print(f"Can read: {os.access(CURRENT_MONTH_PATH, os.R_OK)}")
    print(f"Can write: {os.access(CURRENT_MONTH_PATH, os.W_OK)}")
    print(f"Can execute: {os.access(CURRENT_MONTH_PATH, os.X_OK)}")

# List existing files
print(f"\nEXISTING FILES in {CURRENT_MONTH_PATH}:")
if os.path.exists(CURRENT_MONTH_PATH):
    files = os.listdir(CURRENT_MONTH_PATH)
    for file in sorted(files):
        filepath = os.path.join(CURRENT_MONTH_PATH, file)
        if os.path.isfile(filepath):
            size = os.path.getsize(filepath)
            modified = datetime.fromtimestamp(os.path.getmtime(filepath))
            print(f"  {file} ({size:,} bytes, modified: {modified:%Y-%m-%d %H:%M:%S})")
else:
    print("  Directory does not exist")

# =============================================================================
# CREATE TEST DATA AND WRITE
# =============================================================================
print(f"\n" + "=" * 80)
print("TESTING FILE WRITE")
print("=" * 80)

# Create test DataFrames
test_channel = pl.DataFrame({
    "CHANNEL": ["ATM", "EBK", "OTC"],
    "TOLPROMPT": [100, 200, 300],
    "TOLUPDATE": [10, 20, 30],
    "MONTH": ["NOV25", "NOV25", "NOV25"]
})

test_update = pl.DataFrame({
    "DESC": ["TOTAL PROMPT BASE", "TOTAL UPDATED"],
    "ATM": [1000, 2000],
    "EBK": [500, 600],
    "OTC": [300, 400],
    "TOTAL": [1800, 3000],
    "DATE": ["30/11/2025", "30/11/2025"]
})

test_otc = pl.DataFrame({
    "BRANCHNO": list(range(2, 10)),
    "TOLPROMPT": [10, 20, 30, 40, 50, 60, 70, 80],
    "TOLUPDATE": [1, 2, 3, 4, 5, 6, 7, 8]
})

# Define output paths
channel_path = f"{CURRENT_MONTH_PATH}/CHANNEL_SUM.parquet"
update_path = f"{CURRENT_MONTH_PATH}/CHANNEL_UPDATE.parquet"
otc_path = f"{CURRENT_MONTH_PATH}/OTC_DETAIL.parquet"

print(f"\nWRITING TEST FILES:")
print(f"1. {channel_path}")
print(f"2. {update_path}")
print(f"3. {otc_path}")

# Write files
try:
    # Write CHANNEL_SUM
    test_channel.write_parquet(channel_path)
    print(f"✓ CHANNEL_SUM written: {channel_path}")
    print(f"  File size: {os.path.getsize(channel_path):,} bytes")
except Exception as e:
    print(f"✗ CHANNEL_SUM write failed: {e}")

try:
    # Write CHANNEL_UPDATE
    test_update.write_parquet(update_path)
    print(f"✓ CHANNEL_UPDATE written: {update_path}")
    print(f"  File size: {os.path.getsize(update_path):,} bytes")
except Exception as e:
    print(f"✗ CHANNEL_UPDATE write failed: {e}")

try:
    # Write OTC_DETAIL
    test_otc.write_parquet(otc_path)
    print(f"✓ OTC_DETAIL written: {otc_path}")
    print(f"  File size: {os.path.getsize(otc_path):,} bytes")
except Exception as e:
    print(f"✗ OTC_DETAIL write failed: {e}")

# =============================================================================
# VERIFY FILES WERE CREATED
# =============================================================================
print(f"\n" + "=" * 80)
print("VERIFYING FILE CREATION")
print("=" * 80)

print(f"\nCHECKING FILES in {CURRENT_MONTH_PATH}:")
if os.path.exists(CURRENT_MONTH_PATH):
    files = os.listdir(CURRENT_MONTH_PATH)
    parquet_files = [f for f in files if f.endswith('.parquet')]
    
    if parquet_files:
        print(f"Found {len(parquet_files)} Parquet files:")
        for file in sorted(parquet_files):
            filepath = os.path.join(CURRENT_MONTH_PATH, file)
            if os.path.isfile(filepath):
                size = os.path.getsize(filepath)
                created = datetime.fromtimestamp(os.path.getctime(filepath))
                print(f"  ✓ {file} ({size:,} bytes, created: {created:%H:%M:%S})")
    else:
        print("  No Parquet files found!")
    
    # Also check for SAS files
    sas_files = [f for f in files if f.endswith('.sas7bdat')]
    if sas_files:
        print(f"\nFound {len(sas_files)} SAS files:")
        for file in sorted(sas_files):
            filepath = os.path.join(CURRENT_MONTH_PATH, file)
            size = os.path.getsize(filepath)
            print(f"  ⚡ {file} ({size:,} bytes)")
else:
    print(f"  Directory does not exist!")

# =============================================================================
# CHECK PARENT DIRECTORY TOO
# =============================================================================
print(f"\n" + "=" * 80)
print("CHECKING PARENT DIRECTORIES")
print("=" * 80)

# Check month directory
month_dir = f"{BASE_PATH}/year={REPTDATE.year}"
print(f"\nFiles in {month_dir}:")
if os.path.exists(month_dir):
    months = os.listdir(month_dir)
    for month in sorted(months):
        month_path = os.path.join(month_dir, month)
        if os.path.isdir(month_path):
            files = os.listdir(month_path)
            parquet_count = len([f for f in files if f.endswith('.parquet')])
            sas_count = len([f for f in files if f.endswith('.sas7bdat')])
            print(f"  {month}: {parquet_count} Parquet, {sas_count} SAS files")

# =============================================================================
# FINAL SUMMARY
# =============================================================================
print(f"\n" + "=" * 80)
print("DIAGNOSTIC SUMMARY")
print("=" * 80)

print(f"\nISSUES TO CHECK:")
print(f"1. File permissions on {CURRENT_MONTH_PATH}")
print(f"2. Disk space on /host/mis/parquet/crm/")
print(f"3. SAS overwriting Parquet files during transfer")
print(f"4. Wrong path being used")

print(f"\nQUICK FIXES:")
print(f"1. Run: ls -la {CURRENT_MONTH_PATH}/")
print(f"2. Run: df -h /host/mis/parquet/crm/")
print(f"3. Check if SAS library path conflicts with Parquet path")
print(f"4. Add delay between Parquet write and SAS transfer")

print(f"\n" + "=" * 80)
