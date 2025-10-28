# LOAN_ARREARS_CONTROLLER.PY
import duckdb
import polars as pl
from pathlib import Path
from datetime import datetime
import sys
import subprocess
import shutil

# DATA BNM.REPTDATE;
CURRENT_DATE = datetime.today()
CURRENT_DAY = CURRENT_DATE.day

REPORTING_DATE = None
if 10 <= CURRENT_DAY <= 14:
    REPORT_DATE_STRING = f"10{CURRENT_DATE.month:02d}{CURRENT_DATE.year}"
    REPORTING_DATE = datetime.strptime(REPORT_DATE_STRING, "%d%m%Y")
elif 15 <= CURRENT_DAY <= 19:
    REPORT_DATE_STRING = f"15{CURRENT_DATE.month:02d}{CURRENT_DATE.year}"
    REPORTING_DATE = datetime.strptime(REPORT_DATE_STRING, "%d%m%Y")
elif 20 <= CURRENT_DAY <= 25:
    REPORT_DATE_STRING = f"20{CURRENT_DATE.month:02d}{CURRENT_DATE.year}"
    REPORTING_DATE = datetime.strptime(REPORT_DATE_STRING, "%d%m%Y")
else:
    REPORTING_DATE = CURRENT_DATE

REPORT_WEEK = REPORTING_DATE.day
WEEK_NUMBER = f"{REPORT_WEEK:02d}"
FORMATTED_REPORT_DATE = REPORTING_DATE.strftime("%d%m%y")
REPORT_YEAR = REPORTING_DATE.strftime("%Y")
REPORT_MONTH = f"{REPORTING_DATE.month:02d}"
REPORT_DAY = f"{REPORTING_DATE.day:02d}"

# CREATE BNM.REPTDATE DATASET
REPORTING_DATE_DF = pl.DataFrame({"REPTDATE": [REPORTING_DATE]})
Path("REPORTING_METADATA").mkdir(exist_ok=True)
REPORTING_DATE_DF.write_parquet("REPORTING_METADATA/REPORTING_DATE_REFERENCE.PARQUET")

# DATA _NULL_;
LOAN_EXTRACT_DATE_DF = duckdb.connect().execute("SELECT REPTDATE FROM read_parquet('LOAN_DATA_SOURCE/LOAN_EXTRACT_DATE_REFERENCE.PARQUET') LIMIT 1").pl()
LOAN_EXTRACT_DATE = LOAN_EXTRACT_DATE_DF["REPTDATE"][0]
FORMATTED_LOAN_DATE = LOAN_EXTRACT_DATE.strftime("%d%m%y")

# %MACRO PROCES1;
if FORMATTED_LOAN_DATE == FORMATTED_REPORT_DATE:
    # %INC PGM(LNCCD001);
    print("EXECUTING LNCCD001.PY")
    LNCCD001_RESULT = subprocess.run([sys.executable, "lnccd001.py"], capture_output=True, text=True)
    print(LNCCD001_RESULT.stdout)
    if LNCCD001_RESULT.stderr:
        print("ERROR:", LNCCD001_RESULT.stderr)
    
    # PROC DATASETS LIB=WORK KILL NOLIST; RUN;
    print("CLEANING WORK LIBRARY AFTER LNCCD001.PY")
    WORK_DIR = Path(".")
    for item in WORK_DIR.iterdir():
        if item.is_file() and item.suffix in ['.parquet', '.csv', '.txt']:
            if item.name not in ['REPORTING_METADATA/REPORTING_DATE_REFERENCE.PARQUET', 'LOAN_DATA_SOURCE/LOAN_EXTRACT_DATE_REFERENCE.PARQUET'] and not item.name.startswith('LNCCD'):
                item.unlink()
        elif item.is_dir() and item.name == 'WORK':
            shutil.rmtree(item)
    
    # /**  LOANS IN ARREARS REPORT - THE 10 & 20 OF THE MTH VERSION **/
    # %INC PGM(LNCCD002);
    print("EXECUTING LNCCD002.PY")
    LNCCD002_RESULT = subprocess.run([sys.executable, "lnccd002.py"], capture_output=True, text=True)
    print(LNCCD002_RESULT.stdout)
    if LNCCD002_RESULT.stderr:
        print("ERROR:", LNCCD002_RESULT.stderr)
    
    # PROC DATASETS LIB=WORK KILL NOLIST; RUN;
    print("CLEANING WORK LIBRARY AFTER LNCCD002.PY")
    for item in WORK_DIR.iterdir():
        if item.is_file() and item.suffix in ['.parquet', '.csv', '.txt']:
            if item.name not in ['REPORTING_METADATA/REPORTING_DATE_REFERENCE.PARQUET', 'LOAN_DATA_SOURCE/LOAN_EXTRACT_DATE_REFERENCE.PARQUET'] and not item.name.startswith('LNCCD'):
                item.unlink()
        elif item.is_dir() and item.name == 'WORK':
            shutil.rmtree(item)
    
    # /**  LOANS IN ARREARS CLASSIFIED AS NPL - THE 10 & 20 VERSION  **/
    # %INC PGM(LNCCD004);
    print("EXECUTING LNCCD004.PY")
    LNCCD004_RESULT = subprocess.run([sys.executable, "lnccd004.py"], capture_output=True, text=True)
    print(LNCCD004_RESULT.stdout)
    if LNCCD004_RESULT.stderr:
        print("ERROR:", LNCCD004_RESULT.stderr)
    
    # PROC DATASETS LIB=WORK KILL NOLIST; RUN;
    print("CLEANING WORK LIBRARY AFTER LNCCD004.PY")
    for item in WORK_DIR.iterdir():
        if item.is_file() and item.suffix in ['.parquet', '.csv', '.txt']:
            if item.name not in ['REPORTING_METADATA/REPORTING_DATE_REFERENCE.PARQUET', 'LOAN_DATA_SOURCE/LOAN_EXTRACT_DATE_REFERENCE.PARQUET'] and not item.name.startswith('LNCCD'):
                item.unlink()
        elif item.is_dir() and item.name == 'WORK':
            shutil.rmtree(item)
    
    # /**  LOANS IN ARREARS CLASSIFIED AS NPL - BY BRH GROUPING  **/
    # /*  %INC PGM(LNCCD005);      DISCONTINUE THIS REPORT REFER TO EMAIL
    print("LNCCD005.PY DISCONTINUED AS PER EMAIL INSTRUCTION")
    
else:
    # %PUT THE LOAN EXTRACTION IS NOT DATED &RDATE;
    print(f"THE LOAN EXTRACTION IS NOT DATED {FORMATTED_REPORT_DATE}")
    # %PUT THIS JOB IS NOT RUN !!;
    print("THIS JOB IS NOT RUN !!")
    # %DO;
    #    DATA A;
    #       ABORT 77;
    # %END;
    sys.exit(77)

print("LOAN_ARREARS_CONTROLLER.PY COMPLETED SUCCESSFULLY")
