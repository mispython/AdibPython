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
    # Use Python to read the file directly (more reliable for fixed-width)
    with open(sdbms_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    print(f"Total lines in file: {len(lines)}")
    print(f"First line (header): {lines[0][:50]}...")
    if len(lines) > 1:
        print(f"Second line (first data): {lines[1][:50]}...")
    print("-" * 60)
    
    # Skip first line (header) and process data lines
    data_lines = [line for line in lines[1:] if len(line.strip()) > 0]
    print(f"Data lines to process: {len(data_lines)}")
    
    if len(data_lines) == 0:
        print("⚠ No data lines found in SDBMS.txt")
        sdbms_pd = pd.DataFrame()
    else:
        # Parse each line using fixed-width positions
        import pandas as pd
        records = []
        
        for line in data_lines:
            try:
                record = {
                    'BRANCH': line[0:3] if len(line) > 3 else '',
                    'NAME': line[4:54].strip() if len(line) > 54 else '',
                    'IC': line[55:75] if len(line) > 75 else '',
                    'NATIONALITY': line[76:78] if len(line) > 78 else '',
                    'BOXNO': line[78:88] if len(line) > 88 else '',
                    'HIRERTY': line[89:90] if len(line) > 90 else '',
                    'ADDRESS': line[91:341].strip() if len(line) > 341 else '',
                    'ACCTHOLDER': line[342:343] if len(line) > 343 else '',
                    'ACCTNO': line[344:364] if len(line) > 364 else '',
                    'PRIPHONE': line[365:385] if len(line) > 385 else '',
                    'MOBILENO': line[386:406] if len(line) > 406 else '',
                    'OPENDD': line[407:409] if len(line) > 409 else '',
                    'OPENMM': line[409:411] if len(line) > 411 else '',
                    'OPENYY': line[411:415] if len(line) > 415 else '',
                    'RENTALDATE_STR': line[416:424] if len(line) > 424 else '',
                    'LASTRENTPAY_STR': line[425:433] if len(line) > 433 else '',
                    'MTHOVERDUE1': line[436:438] if len(line) > 438 else '0',
                    'MTHOVERDUE2': line[438:440] if len(line) > 440 else '0',
                    'MTHOVERDUE3': line[440:442] if len(line) > 442 else '0',
                    'TOTALOVERDUE': line[442:444] if len(line) > 444 else '0'
                }
                records.append(record)
            except Exception as e:
                print(f"⚠ Error parsing line: {e}")
                continue
        
        sdbms_pd = pd.DataFrame(records)
        print(f"✓ Read {len(sdbms_pd)} records from SDBMS.txt")
    
    # Convert to pandas for date processing
    import pandas as pd
    
    # Create OPENDT (equivalent to MDY function in SAS)
    if len(sdbms_pd) > 0:
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
        
        # Convert numeric fields
        for col in ['MTHOVERDUE1', 'MTHOVERDUE2', 'MTHOVERDUE3', 'TOTALOVERDUE']:
            sdbms_pd[col] = pd.to_numeric(sdbms_pd[col], errors='coerce').fillna(0).astype(int)
        
        # Drop temporary columns
        sdbms_pd = sdbms_pd.drop(columns=['OPENDD', 'OPENMM', 'OPENYY', 'RENTALDATE_STR', 'LASTRENTPAY_STR'])
        
        print(f"✓ Processed dates and created OPENDT field")
        
        # Display sample (equivalent to PROC PRINT)
        print("\nSample Records (first 5 rows):")
        print("-" * 60)
        print(sdbms_pd.head().to_string())
        print("-" * 60)
    else:
        print("⚠ No data to process")
    
    # Convert back to Arrow
    sdbms_table = pa.Table.from_pandas(sdbms_pd)
    
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
