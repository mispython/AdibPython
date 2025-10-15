#!/usr/bin/env python3
"""
EIIMCC2B - LOAN REPORT ORCHESTRATOR
1:1 CONVERSION FROM SAS TO PYTHON
ORCHESTRATES EXECUTION OF MULTIPLE LOAN REPORTS
- VALIDATES REPORT DATE
- EXECUTES SUB-PROGRAMS: LNCCDBI2, LNCCDBI3, LNCCDBI4
- CLEANS UP WORK DATASETS BETWEEN RUNS
"""

import duckdb
from pathlib import Path
from datetime import datetime
import subprocess
import sys

# INITIALIZE PATHS
INPUT_DIR = Path("input")
LOAN_DIR = INPUT_DIR / "loan"
PGM_DIR = Path("programs")  # WHERE SUB-PROGRAMS ARE STORED

# CONNECT TO DUCKDB
con = duckdb.connect(":memory:")

print("="*80)
print("EIIMCC2B - LOAN REPORT ORCHESTRATOR")
print("="*80)

# ============================================================================
# READ REPORT DATE AND CALCULATE WEEK
# ============================================================================
REPTDATE_FILE = LOAN_DIR / "REPTDATE.parquet"

if not REPTDATE_FILE.exists():
    print(f"ERROR: REPTDATE FILE NOT FOUND: {REPTDATE_FILE}")
    print("THIS JOB IS NOT RUN !!")
    sys.exit(1)

result = con.execute(f"""
    SELECT REPTDATE 
    FROM read_parquet('{REPTDATE_FILE}')
    LIMIT 1
""").fetchone()

REPTDATE = result[0]

# HANDLE DIFFERENT DATE FORMATS
if isinstance(REPTDATE, (int, float)):
    REPTDATE = datetime.strptime(str(int(REPTDATE)), '%Y%m%d').date()
elif isinstance(REPTDATE, str):
    REPTDATE = datetime.strptime(REPTDATE, '%Y-%m-%d').date()

# DETERMINE WEEK NUMBER
DAY_OF_MONTH = REPTDATE.day

if 1 <= DAY_OF_MONTH <= 8:
    WK = '1'
elif 9 <= DAY_OF_MONTH <= 15:
    WK = '2'
elif 16 <= DAY_OF_MONTH <= 22:
    WK = '3'
else:  # 23-31
    WK = '4'

# EXTRACT DATE COMPONENTS
NOWK = WK
RDATE = REPTDATE.strftime('%d%m%Y')  # DDMMYY8 FORMAT
REPTYEAR = str(REPTDATE.year)  # YEAR4 FORMAT (FULL YEAR)
REPTMON = str(REPTDATE.month).zfill(2)
REPTDAY = str(REPTDATE.day).zfill(2)

# SET LOAN DATE (FOR COMPARISON)
LOAN = RDATE

print(f"\nREPORT DATE: {REPTDATE}")
print(f"RDATE: {RDATE}")
print(f"LOAN: {LOAN}")
print(f"WEEK: {NOWK}")
print(f"YEAR: {REPTYEAR}")
print(f"MONTH: {REPTMON}")
print(f"DAY: {REPTDAY}")

# ============================================================================
# MACRO PROCESS - VALIDATE AND EXECUTE SUB-PROGRAMS
# ============================================================================
print("\n" + "="*80)
print("VALIDATING LOAN EXTRACTION DATE")
print("="*80)

if LOAN == RDATE:
    print(f"✓ VALIDATION PASSED: LOAN DATE ({LOAN}) MATCHES REPORT DATE ({RDATE})")
    print("\nPROCEEDING WITH REPORT EXECUTION...")
    
    # ========================================================================
    # EXECUTE SUB-PROGRAM 1: LNCCDBI2
    # LOANS IN ARREARS REPORT - MONTH-END VERSION
    # ========================================================================
    print("\n" + "-"*80)
    print("EXECUTING: LNCCDBI2 - LOANS IN ARREARS REPORT (MONTH-END VERSION)")
    print("-"*80)
    
    LNCCDBI2_SCRIPT = PGM_DIR / "LNCCDBI2_LOANS_IN_ARREARS.py"
    
    if LNCCDBI2_SCRIPT.exists():
        try:
            result = subprocess.run(
                [sys.executable, str(LNCCDBI2_SCRIPT)],
                capture_output=True,
                text=True,
                check=True
            )
            print(result.stdout)
            print("✓ LNCCDBI2 COMPLETED SUCCESSFULLY")
        except subprocess.CalledProcessError as e:
            print(f"✗ ERROR EXECUTING LNCCDBI2:")
            print(e.stderr)
            print("ABORTING ORCHESTRATION")
            sys.exit(1)
    else:
        print(f"WARNING: {LNCCDBI2_SCRIPT} NOT FOUND - SKIPPING")
    
    # CLEANUP WORK DATASETS
    print("\nCLEANING UP WORK DATASETS...")
    
    # ========================================================================
    # EXECUTE SUB-PROGRAM 2: LNCCDBI3
    # LOANS IN NPL REPORT - BY CAC
    # ========================================================================
    print("\n" + "-"*80)
    print("EXECUTING: LNCCDBI3 - LOANS IN NPL REPORT (BY CAC)")
    print("-"*80)
    
    LNCCDBI3_SCRIPT = PGM_DIR / "LNCCDBI3_NPL_BY_CAC.py"
    
    if LNCCDBI3_SCRIPT.exists():
        try:
            result = subprocess.run(
                [sys.executable, str(LNCCDBI3_SCRIPT)],
                capture_output=True,
                text=True,
                check=True
            )
            print(result.stdout)
            print("✓ LNCCDBI3 COMPLETED SUCCESSFULLY")
        except subprocess.CalledProcessError as e:
            print(f"✗ ERROR EXECUTING LNCCDBI3:")
            print(e.stderr)
            print("ABORTING ORCHESTRATION")
            sys.exit(1)
    else:
        print(f"WARNING: {LNCCDBI3_SCRIPT} NOT FOUND - SKIPPING")
    
    # CLEANUP WORK DATASETS
    print("\nCLEANING UP WORK DATASETS...")
    
    # ========================================================================
    # EXECUTE SUB-PROGRAM 3: LNCCDBI4
    # LOANS IN NPL REPORT - 10TH & 20TH OF MONTH VERSION
    # ========================================================================
    print("\n" + "-"*80)
    print("EXECUTING: LNCCDBI4 - LOANS IN NPL REPORT (10TH & 20TH VERSION)")
    print("-"*80)
    
    LNCCDBI4_SCRIPT = PGM_DIR / "LNCCDBI4_NPL_10_20.py"
    
    if LNCCDBI4_SCRIPT.exists():
        try:
            result = subprocess.run(
                [sys.executable, str(LNCCDBI4_SCRIPT)],
                capture_output=True,
                text=True,
                check=True
            )
            print(result.stdout)
            print("✓ LNCCDBI4 COMPLETED SUCCESSFULLY")
        except subprocess.CalledProcessError as e:
            print(f"✗ ERROR EXECUTING LNCCDBI4:")
            print(e.stderr)
            print("ABORTING ORCHESTRATION")
            sys.exit(1)
    else:
        print(f"WARNING: {LNCCDBI4_SCRIPT} NOT FOUND - SKIPPING")
    
    # CLEANUP WORK DATASETS
    print("\nCLEANING UP WORK DATASETS...")
    
    # ========================================================================
    # SUB-PROGRAM 4: LNCCDBI5 (COMMENTED OUT IN ORIGINAL)
    # LOANS CLASSIFY AS NPL REPORT
    # ========================================================================
    # print("\n" + "-"*80)
    # print("EXECUTING: LNCCDBI5 - LOANS CLASSIFY AS NPL REPORT")
    # print("-"*80)
    # print("NOTE: LNCCDBI5 IS CURRENTLY DISABLED IN ORIGINAL SAS CODE")
    
    # ========================================================================
    # FINAL SUMMARY
    # ========================================================================
    print("\n" + "="*80)
    print("ORCHESTRATION COMPLETED SUCCESSFULLY")
    print("="*80)
    print("\nREPORTS EXECUTED:")
    print("  1. LNCCDBI2 - LOANS IN ARREARS REPORT (MONTH-END)")
    print("  2. LNCCDBI3 - LOANS IN NPL REPORT (BY CAC)")
    print("  3. LNCCDBI4 - LOANS IN NPL REPORT (10TH & 20TH)")
    print("  4. LNCCDBI5 - LOANS CLASSIFY AS NPL (DISABLED)")
    print("\nALL REPORTS PROCESSED FOR DATE: " + RDATE)
    print("="*80)

else:
    # ========================================================================
    # VALIDATION FAILED - ABORT EXECUTION
    # ========================================================================
    print(f"✗ VALIDATION FAILED!")
    print(f"   LOAN EXTRACTION DATE: {LOAN}")
    print(f"   EXPECTED DATE: {RDATE}")
    print("\nTHE LOAN EXTRACTION IS NOT DATED " + RDATE)
    print("THIS JOB IS NOT RUN !!")
    print("\nABORTING WITH EXIT CODE 77")
    print("="*80)
    sys.exit(77)

# CLOSE CONNECTION
con.close()
