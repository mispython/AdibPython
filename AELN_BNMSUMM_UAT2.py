# EIBWSDB1.py - SDBMS Data Processor

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime, timedelta

# Configuration
base_path = Path("/stgsrcsys/host/uat")
SDBM_DATA_PATH = Path("/host/mis/parquet")

# Remove these legacy paths - outputs go to SDBM_DATA_PATH now
# STATUS_PATH and R1STAT_PATH are no longer needed

# Connect to DuckDB
conn = duckdb.connect()

print("=" * 60)
print("EIBWSDB1 - SDBMS Data Processor")
print("=" * 60)

# Calculate REPTDATE (same as parent program)
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

# Create Hive-partitioned output path
OUTPUT_DATA_PATH = Path(f"{SDBM_DATA_PATH}/year={reptdate.year}/month={reptdate.month:02d}/day={reptdate.day:02d}")
OUTPUT_DATA_PATH.mkdir(parents=True, exist_ok=True)

print(f"Report Date: {reptdate} (Week {nowk})")
print(f"Output Path: {OUTPUT_DATA_PATH}")
print("-" * 60)

# Step 1: Read and process SDBMS file (equivalent to DATA SDBMS)
sdbms_file = base_path / "SDBMS.txt"

if not sdbms_file.exists():
    print(f"✗ SDBMS file not found: {sdbms_file}")
    exit(1)

try:
    # Read SDBMS file as text starting from line 2 (FIRSTOBS=2)
    query = f"""
    SELECT 
        SUBSTR(content, 1, 3) as BRANCH,
        TRIM(SUBSTR(content, 5, 50)) as NAME,
        SUBSTR(content, 56, 20) as IC,
        SUBSTR(content, 77, 2) as NATIONALITY,
        SUBSTR(content, 79, 10) as BOXNO,
        SUBSTR(content, 90, 1) as HIRERTY,
        TRIM(SUBSTR(content, 92, 250)) as ADDRESS,
        SUBSTR(content, 343, 1) as ACCTHOLDER,
        SUBSTR(content, 345, 20) as ACCTNO,
        SUBSTR(content, 366, 20) as PRIPHONE,
        SUBSTR(content, 387, 20) as MOBILENO,
        SUBSTR(content, 408, 2) as OPENDD,
        SUBSTR(content, 410, 2) as OPENMM,
        SUBSTR(content, 412, 4) as OPENYY,
        SUBSTR(content, 417, 8) as RENTALDATE_STR,
        SUBSTR(content, 426, 8) as LASTRENTPAY_STR,
        CAST(SUBSTR(content, 437, 2) as INTEGER) as MTHOVERDUE1,
        CAST(SUBSTR(content, 439, 2) as INTEGER) as MTHOVERDUE2,
        CAST(SUBSTR(content, 441, 2) as INTEGER) as MTHOVERDUE3,
        CAST(SUBSTR(content, 443, 2) as INTEGER) as TOTALOVERDUE
    FROM (
        SELECT content, ROW_NUMBER() OVER () as line_num
        FROM read_text('{sdbms_file}')
    )
    WHERE line_num > 1  -- Skip first line (header with date)
      AND LENGTH(TRIM(content)) > 0
    """
    
    sdbms_df = conn.execute(query).arrow()
    print(f"✓ Read {len(sdbms_df)} records from SDBMS.txt")
    
    # Convert to pandas for date processing
    import pandas as pd
    sdbms_pd = sdbms_df.to_pandas()
    
    # Create OPENDT (equivalent to MDY function in SAS)
    def create_opendt(row):
        try:
            if pd.notna(row['OPENDD']) and pd.notna(row['OPENMM']) and pd.notna(row['OPENYY']):
                opendd = int(row['OPENDD'])
                openmm = int(row['OPENMM'])
                openyy = int(row['OPENYY'])
                return datetime(openyy, openmm, opendd).date()
        except:
            return None
        return None
    
    sdbms_pd['OPENDT'] = sdbms_pd.apply(create_opendt, axis=1)
    
    # Parse RENTALDATE and LASTRENTPAY (DDMMYYYY format)
    def parse_ddmmyyyy(date_str):
        try:
            if pd.notna(date_str) and len(str(date_str).strip()) == 8:
                date_str = str(date_str).strip()
                day = int(date_str[0:2])
                month = int(date_str[2:4])
                year = int(date_str[4:8])
                return datetime(year, month, day).date()
        except:
            return None
        return None
    
    sdbms_pd['RENTALDATE'] = sdbms_pd['RENTALDATE_STR'].apply(parse_ddmmyyyy)
    sdbms_pd['LASTRENTPAY'] = sdbms_pd['LASTRENTPAY_STR'].apply(parse_ddmmyyyy)
    
    # Drop temporary columns
    sdbms_pd = sdbms_pd.drop(columns=['OPENDD', 'OPENMM', 'OPENYY', 'RENTALDATE_STR', 'LASTRENTPAY_STR'])
    
    # Convert back to Arrow
    sdbms_table = pa.Table.from_pandas(sdbms_pd)
    
    print(f"✓ Processed dates and created OPENDT field")
    
    # Display sample (equivalent to PROC PRINT)
    print("\nSample Records (first 5 rows):")
    print("-" * 60)
    print(sdbms_pd.head().to_string())
    print("-" * 60)
    
except Exception as e:
    print(f"✗ Error processing SDBMS file: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

# Step 2: Create STATUS dataset (without NAME, ADDRESS, PRIPHONE, MOBILENO)
try:
    status_columns = [col for col in sdbms_pd.columns 
                     if col not in ['NAME', 'ADDRESS', 'PRIPHONE', 'MOBILENO']]
    status_df = sdbms_pd[status_columns].copy()
    
    # Save to STATUS path
    status_filename = "SDB.parquet"
    status_file_path = OUTPUT_DATA_PATH / status_filename
    
    status_table = pa.Table.from_pandas(status_df)
    pq.write_table(status_table, status_file_path)
    
    print(f"\n✓ STATUS dataset saved: {status_filename}")
    print(f"  Location: {status_file_path}")
    print(f"  Records: {len(status_df)}")
    print(f"  Columns: {len(status_df.columns)}")
    
except Exception as e:
    print(f"✗ Error creating STATUS dataset: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

# Step 3: Create R1STAT dataset (only IC, NAME, ADDRESS, PRIPHONE, MOBILENO)
try:
    r1stat_columns = ['IC', 'NAME', 'ADDRESS', 'PRIPHONE', 'MOBILENO']
    r1stat_df = sdbms_pd[r1stat_columns].copy()
    
    # Save to R1STAT path
    r1stat_filename = "R1SDB.parquet"
    r1stat_file_path = OUTPUT_DATA_PATH / r1stat_filename
    
    r1stat_table = pa.Table.from_pandas(r1stat_df)
    pq.write_table(r1stat_table, r1stat_file_path)
    
    print(f"\n✓ R1STAT dataset saved: {r1stat_filename}")
    print(f"  Location: {r1stat_file_path}")
    print(f"  Records: {len(r1stat_df)}")
    print(f"  Columns: {len(r1stat_df.columns)}")
    
except Exception as e:
    print(f"✗ Error creating R1STAT dataset: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

print("\n" + "=" * 60)
print("✓ EIBWSDB1 processing completed successfully")
print("=" * 60)

conn.close()
