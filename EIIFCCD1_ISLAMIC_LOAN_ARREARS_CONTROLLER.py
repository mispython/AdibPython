# EIIFCCD1.PY
import duckdb
import polars as pl
from pathlib import Path
from datetime import datetime
import sys
import subprocess
import shutil

# DATA BNM.REPTDATE;
TODAY = datetime.today()
DAY_TODAY = TODAY.day

REPTDATE = None
if 10 <= DAY_TODAY <= 14:
    REPTDATE_STR = f"10{TODAY.month:02d}{TODAY.year}"
    REPTDATE = datetime.strptime(REPTDATE_STR, "%d%m%Y")
elif 15 <= DAY_TODAY <= 19:
    REPTDATE_STR = f"15{TODAY.month:02d}{TODAY.year}" 
    REPTDATE = datetime.strptime(REPTDATE_STR, "%d%m%Y")
elif 20 <= DAY_TODAY <= 25:
    REPTDATE_STR = f"20{TODAY.month:02d}{TODAY.year}"
    REPTDATE = datetime.strptime(REPTDATE_STR, "%d%m%Y")
else:
    REPTDATE = TODAY

WK = REPTDATE.day
NOWK = f"{WK:02d}"
RDATE = REPTDATE.strftime("%d%m%y")
REPTYEAR = REPTDATE.strftime("%Y")
REPTMON = f"{REPTDATE.month:02d}"
REPTDAY = f"{REPTDATE.day:02d}"

# CREATE BNM.REPTDATE DATASET
REPTDATE_DF = pl.DataFrame({"REPTDATE": [REPTDATE]})
Path("BNM").mkdir(exist_ok=True)
REPTDATE_DF.write_parquet("BNM/REPTDATE.parquet")

# DATA _NULL_;
LOAN_REPTDATE_DF = duckdb.connect().execute("SELECT REPTDATE FROM read_parquet('LOAN/REPTDATE.parquet') LIMIT 1").pl()
LOAN_REPTDATE = LOAN_REPTDATE_DF["REPTDATE"][0]
LOAN = LOAN_REPTDATE.strftime("%d%m%y")

# %MACRO PROCES1;
if LOAN == RDATE:
    # %INC PGM(LNCCD001);
    print("EXECUTING LNCCD001")
    LNCCD001_RESULT = subprocess.run([sys.executable, "LNCCD001.py"], capture_output=True, text=True)
    print(LNCCD001_RESULT.stdout)
    if LNCCD001_RESULT.stderr:
        print("ERROR:", LNCCD001_RESULT.stderr)
    
    # PROC DATASETS LIB=WORK KILL NOLIST; RUN;
    print("CLEANING WORK LIBRARY AFTER LNCCD001")
    WORK_DIR = Path(".")
    for item in WORK_DIR.iterdir():
        if item.is_file() and item.suffix in ['.parquet', '.csv', '.txt']:
            if item.name not in ['BNM/REPTDATE.parquet', 'LOAN/REPTDATE.parquet'] and not item.name.startswith('LNCCD'):
                item.unlink()
        elif item.is_dir() and item.name == 'WORK':
            shutil.rmtree(item)
    
    # /**  LOANS IN ARREARS REPORT - THE 10 & 20 OF THE MTH VERSION **/
    # %INC PGM(LNCCDI02);
    print("EXECUTING LNCCDI02") 
    LNCCDI02_RESULT = subprocess.run([sys.executable, "LNCCDI02.py"], capture_output=True, text=True)
    print(LNCCDI02_RESULT.stdout)
    if LNCCDI02_RESULT.stderr:
        print("ERROR:", LNCCDI02_RESULT.stderr)
    
    # PROC DATASETS LIB=WORK KILL NOLIST; RUN;
    print("CLEANING WORK LIBRARY AFTER LNCCDI02")
    for item in WORK_DIR.iterdir():
        if item.is_file() and item.suffix in ['.parquet', '.csv', '.txt']:
            if item.name not in ['BNM/REPTDATE.parquet', 'LOAN/REPTDATE.parquet'] and not item.name.startswith('LNCCD'):
                item.unlink()
        elif item.is_dir() and item.name == 'WORK':
            shutil.rmtree(item)
    
    # /**  LOANS IN ARREARS CLASSIFIED AS NPL - THE 10 & 20 VERSION  **/
    # %INC PGM(LNCCDI04);
    print("EXECUTING LNCCDI04")
    LNCCDI04_RESULT = subprocess.run([sys.executable, "LNCCDI04.py"], capture_output=True, text=True)
    print(LNCCDI04_RESULT.stdout)
    if LNCCDI04_RESULT.stderr:
        print("ERROR:", LNCCDI04_RESULT.stderr)
    
    # PROC DATASETS LIB=WORK KILL NOLIST; RUN;
    print("CLEANING WORK LIBRARY AFTER LNCCDI04")
    for item in WORK_DIR.iterdir():
        if item.is_file() and item.suffix in ['.parquet', '.csv', '.txt']:
            if item.name not in ['BNM/REPTDATE.parquet', 'LOAN/REPTDATE.parquet'] and not item.name.startswith('LNCCD'):
                item.unlink()
        elif item.is_dir() and item.name == 'WORK':
            shutil.rmtree(item)
    
    # /**  LOANS IN ARREARS CLASSIFIED AS NPL - BY BRH GROUPING  **/
    # /*  %INC PGM(LNCCD005);      DISCONTINUE THIS REPORT REFER TO EMAIL
    #     PROC DATASETS LIB=WORK KILL NOLIST; RUN;
    #     PROC DATASETS LIB=BNM NOLIST;
    #       DELETE LOANTEMP;
    #     RUN;  */
    print("LNCCD005 DISCONTINUED AS PER EMAIL INSTRUCTION")
    
else:
    # %PUT THE LOAN EXTRACTION IS NOT DATED &RDATE;
    print(f"THE LOAN EXTRACTION IS NOT DATED {RDATE}")
    # %PUT THIS JOB IS NOT RUN !!;
    print("THIS JOB IS NOT RUN !!")
    # %DO;
    #    DATA A;
    #       ABORT 77;
    # %END;
    sys.exit(77)

print("EIIFCCD1 COMPLETED SUCCESSFULLY")
