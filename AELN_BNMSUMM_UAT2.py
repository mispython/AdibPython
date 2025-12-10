09122025                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
002 DAVID JAMES                                        00003                MY 00087      P BLOCK C 3-12F JALAN KENANGA SRI SARAWAK FLATS 55200 KL                                                                                                                                                                                                     N                      0321111111           0182154113           08042004 08042009 00000000 0 0 1 1 Hired                                                    
002 DAVID JAMES                                        00003                MY 01968      P STARHILL DAAMAN SARA                                                                                                                                                                                                                                       N                      0321111111           0182154113           31012025 31012027 00000000 0 0 0 0 Hired                                                    
002 DAVID JAMES                                        00003                MY 05017      P 3 PERSIARAN 12 47000 PUCHONG SELANGOR                                                                                                                                                                                                                      N                      0321111111           0182154113           10032020 31082024 00000000 0 0 1 1 Hired                                                    
002 DAVID JAMES                                        00003                MY 50001      P NO 2, JLN LAZAT 2 TMN BANDARAYA 58200 KUALA LUMPUR                                                                                                                                                                                                         N 4993220706           0321111111           0182154113           31082016 20092024 00000000 0 0 1 1 Hired                                                    
704 NG ZHI YONG                                        000505012022         MY 20004      P NO 1 JLN DUA TMN TIGA 43000 KAJANG SELANGOR                                                                                                     



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
base_path = Path("/stgsrcsys/host/uat")
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






*;
DATA SDBMS(DROP=OPENDD OPENMM OPENYY);
  INFILE SDBMS FIRSTOBS=2;
  INPUT @001 BRANCH       $3.
        @005 NAME        $50.
        @056 IC          $20.
        @077 NATIONALITY  $2.
        @079 BOXNO       $10.     /* SDB BOX NO.     */
        @090 HIRERTY      $1.     /* HIRER TYPE      */
        @092 ADDRESS    $250.
        @343 ACCTHOLDER   $1.
        @345 ACCTNO      $20.
        @366 PRIPHONE    $20.
        @387 MOBILENO    $20.
        @408 OPENDD       $2.
        @410 OPENMM       $2.
        @412 OPENYY       $4.
        @419 RENTALDATE   DDMMYY8. /* RENTALDUEDATE     */
        @428 LASTRENTPAY  DDMMYY8. /* LASTRENTALPAYMENT */
        @437 MTHOVERDUE1   2.      /* MTHOVERDUE(3-<4)  */
        @439 MTHOVERDUE2   2.      /* MTHOVERDUE(4-<8)  */
        @441 MTHOVERDUE3   2.      /* MTHOVERDUE(>8)    */
        @443 TOTALOVERDUE  2.
        ;
  OPENDT = MDY(OPENMM,OPENDD,OPENYY);
RUN;
PROC PRINT DATA=SDBMS;RUN;
*;
DATA STATUS.SDB&REPTMON&NOWK&REPTYEAR   (DROP=NAME ADDRESS PRIPHONE
                                              MOBILENO)
     R1STAT.R1SDB&REPTMON&NOWK&REPTYEAR (KEEP=IC NAME ADDRESS PRIPHONE
                                              MOBILENO);
     SET SDBMS;
RUN;
