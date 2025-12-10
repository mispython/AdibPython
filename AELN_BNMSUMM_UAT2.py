# EIBWSDBM_SDBMS_DATE_VALIDATOR.py (CORRECTED VERSION)

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from pathlib import Path
from datetime import datetime, timedelta
import subprocess
import sys

# Configuration
base_path = Path("/stgsrcsys/host/uat")
SDBM_DATA_PATH = base_path / "OUTPUT"

# Connect to DuckDB
conn = duckdb.connect()
print("=" * 60)
print("EIBWSDBM - SDBMS File Processor")
print("=" * 60)

# Step 1: Process REPTDATE (equivalent to DATA REPTDATE)
reptdate = datetime.today().date() - timedelta(days=1)
rept_day = reptdate.day

if 1 <= rept_day <= 8:
    nowk = '1'
elif 9 <= rept_day <= 15:
    nowk = '2'
elif 16 <= rept_day <= 22:
    nowk = '3'
else:
    nowk = '4'

reptyear = reptdate.strftime('%y')
reptmon = reptdate.strftime('%m')
reptday = reptdate.strftime('%d')
rdate = reptdate.strftime('%d%m%y')  # 6-digit format: DDMMYY

# Create Hive-partitioned output path
OUTPUT_DATA_PATH = Path(f"{SDBM_DATA_PATH}/year={reptdate.year}/month={reptdate.month:02d}/day={reptdate.day:02d}")
OUTPUT_DATA_PATH.mkdir(parents=True, exist_ok=True)

print(f"Report Date: {reptdate} (Week {nowk})")
print(f"Expected SDBMS date (DDMMYY): {rdate}")
print(f"Output Path: {OUTPUT_DATA_PATH}")

# Step 2: Read SDBMS file date (CORRECTED FOR DDMMYYYY FORMAT)
sdbms_file = base_path / "SDBMS.txt"
sdbms_date = None

if sdbms_file.exists():
    try:
        sdbms_query = f"""
        SELECT SUBSTR(line, 1, 8) as SDBMS_DATE_STR
        FROM read_csv('{sdbms_file}', 
                      header=false, 
                      columns={{'line': 'VARCHAR'}},
                      delim='\\t',
                      all_varchar=true)
        WHERE LENGTH(TRIM(line)) >= 8
        LIMIT 1
        """
        sdbms_df = conn.execute(sdbms_query).arrow()
        
        if len(sdbms_df) > 0:
            sdbms_date_full = sdbms_df['SDBMS_DATE_STR'][0].as_py().strip()
            
            # Validate it's a date (8 digits)
            if len(sdbms_date_full) == 8 and sdbms_date_full.isdigit():
                # Convert DDMMYYYY to DDMMYY for comparison
                sdbms_date = sdbms_date_full[:6]
                print(f"✓ SDBMS file date found: {sdbms_date_full} (comparing as {sdbms_date})")
            else:
                print(f"✗ Invalid date format in SDBMS file: '{sdbms_date_full}'")
                sdbms_date = None
        else:
            print("⚠ No data found in SDBMS file")
            sdbms_date = None
    except Exception as e:
        print(f"✗ Error reading SDBMS file: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
else:
    print(f"✗ SDBMS file not found: {sdbms_file}")
    exit(1)

print("-" * 60)

# Step 3: Process logic (equivalent to %MACRO PROCESS)
if sdbms_date and sdbms_date == rdate:
    print("✓ SDBMS file date matches expected date")
    print("→ Executing EIBWSDB1...")
    print("-" * 60)
    
    # Execute the subprogram
    eibwsdb1_script = base_path / "EIBWSDB1.py"
    if eibwsdb1_script.exists():
        result = subprocess.run(
            [sys.executable, str(eibwsdb1_script)], 
            capture_output=True, 
            text=True
        )
        
        if result.stdout:
            print(result.stdout)
        
        if result.returncode == 0:
            print("-" * 60)
            print("✓ EIBWSDB1 executed successfully")
        else:
            print("-" * 60)
            print(f"✗ EIBWSDB1 failed with return code {result.returncode}")
            if result.stderr:
                print(f"Error: {result.stderr}")
            exit(result.returncode)
    else:
        print(f"⚠ EIBWSDB1 script not found: {eibwsdb1_script}")
        exit(1)
else:
    print("✗ Date mismatch detected!")
    print(f"   Expected: {rdate}")
    print(f"   Found:    {sdbms_date if sdbms_date else 'NONE'}")
    print(f"\nSDBMS FILE IS NOT DATED {rdate}")
    print("THE JOB IS NOT DONE !!")
    print("=" * 60)
    exit(77)

print("=" * 60)
print("✓ Processing completed successfully")
print("=" * 60)
conn.close()
