# EIBWSDBM_SDBMS_DATE_VALIDATOR.py

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from pathlib import Path
from datetime import datetime, timedelta
import subprocess
import sys

# Configuration
base_path = Path(".")
output_path = base_path / "OUTPUT"
output_path.mkdir(exist_ok=True)

# Connect to DuckDB
conn = duckdb.connect()

print("EIBWSDBM - SDBMS File Processor")

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
rdate = reptdate.strftime('%d%m%y')

# Save REPTDATE
reptdate_df = pa.table({'REPTDATE': pa.array([reptdate], pa.date32())})
pq.write_table(reptdate_df, output_path / "REPTDATE.parquet")
csv.write_csv(reptdate_df, output_path / "REPTDATE.csv")

# Step 2: Read SDBMS file date (equivalent to DATA _NULL_)
sdbms_file = base_path / "SDBMS.txt"
if sdbms_file.exists():
    sdbms_query = f"""
    SELECT CAST(SUBSTR(line, 1, 8) as DATE) as SDBMS_DATE
    FROM read_csv('{sdbms_file}', header=false, columns={{'line': 'VARCHAR'}})
    WHERE LENGTH(line) >= 8
    LIMIT 1
    """
    sdbms_df = conn.execute(sdbms_query).arrow()
    sdbms_date = sdbms_df['SDBMS_DATE'][0].as_py().strftime('%d%m%y') if len(sdbms_df) > 0 else None
else:
    sdbms_date = None

# Step 3: Process logic (equivalent to %MACRO PROCESS)
if sdbms_date == rdate:
    print("SDBMS file date matches - executing EIBWSDB1...")
    # Execute the subprogram
    eibwsdb1_script = base_path / "EIBWSDB1.py"
    if eibwsdb1_script.exists():
        result = subprocess.run([sys.executable, str(eibwsdb1_script)], capture_output=True, text=True)
        if result.returncode == 0:
            print("✓ EIBWSDB1 executed successfully")
        else:
            print(f"✗ EIBWSDB1 failed: {result.stderr}")
            exit(result.returncode)
    else:
        print(f"⚠ EIBWSDB1 script not found: {eibwsdb1_script}")
else:
    if sdbms_date != rdate:
        print(f"SDBMS FILE IS NOT DATED {rdate} - it is dated {sdbms_date}")
    print("THE JOB IS NOT DONE !!")
    exit(77)

print("Processing completed successfully")
conn.close()
