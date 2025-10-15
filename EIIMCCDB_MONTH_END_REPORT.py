#!/usr/bin/env python3
"""
EIIMCCDB - MONTH-END LOAN REPORT ORCHESTRATOR
1:1 CONVERSION FROM SAS TO PYTHON
CALCULATES LAST DAY OF PREVIOUS MONTH AND EXECUTES LNCCDB06 REPORT
NOTE: DATE VALIDATION IS COMMENTED OUT IN ORIGINAL (ALWAYS RUNS)
"""

import duckdb
from pathlib import Path
from datetime import datetime, timedelta
import subprocess
import sys
import calendar

# INITIALIZE PATHS
INPUT_DIR = Path("input")
LOAN_DIR = INPUT_DIR / "loan"
PGM_DIR = Path("programs")  # WHERE SUB-PROGRAMS ARE STORED

# CONNECT TO DUCKDB
con = duckdb.connect(":memory:")

print("="*80)
print("EIIMCCDB - MONTH-END LOAN REPORT ORCHESTRATOR")
print("="*80)

# ============================================================================
# CALCULATE LAST DAY OF PREVIOUS MONTH
# REPTDATE = LAST DAY OF PREVIOUS MONTH
# ============================================================================
TODAY = datetime.today().date()

# GET FIRST DAY OF CURRENT MONTH
FIRST_DAY_THIS_MONTH = TODAY.replace(day=1)

# SUBTRACT 1 DAY TO GET LAST DAY OF PREVIOUS MONTH
REPTDATE = FIRST_DAY_THIS_MONTH - timedelta(days=1)

# EXTRACT DATE COMPONENTS
RDATE = REPTDATE.strftime('%d%m%Y')  # DDMMYY8 FORMAT
REPTYEAR = str(REPTDATE.year)  # YEAR4 FORMAT (FULL YEAR)
REPTMON = str(REPTDATE.month).zfill(2)
REPTDAY = str(REPTDATE.day).zfill(2)

print(f"\nTODAY: {TODAY}")
print(f"CALCULATED REPORT DATE (LAST DAY OF PREVIOUS MONTH): {REPTDATE}")
print(f"RDATE: {RDATE}")
print(f"YEAR: {REPTYEAR}")
print(f"MONTH: {REPTMON}")
print(f"DAY: {REPTDAY}")

# ============================================================================
# READ LOAN EXTRACTION DATE (FOR COMPARISON - CURRENTLY NOT USED)
# ============================================================================
REPTDATE_FILE = LOAN_DIR / "REPTDATE.parquet"

if REPTDATE_FILE.exists():
    result = con.execute(f"""
        SELECT REPTDATE 
        FROM read_parquet('{REPTDATE_FILE}')
        LIMIT 1
    """).fetchone()
    
    LOAN_DATE = result[0]
    
    # HANDLE DIFFERENT DATE FORMATS
    if isinstance(LOAN_DATE, (int, float)):
        LOAN_DATE = datetime.strptime(str(int(LOAN_DATE)), '%Y%m%d').date()
    elif isinstance(LOAN_DATE, str):
        LOAN_DATE = datetime.strptime(LOAN_DATE, '%Y-%m-%d').date()
    
    LOAN = LOAN_DATE.strftime('%d%m%Y')
    
    print(f"\nLOAN EXTRACTION DATE: {LOAN_DATE}")
    print(f"LOAN: {LOAN}")
else:
    print(f"\nWARNING: REPTDATE FILE NOT FOUND: {REPTDATE_FILE}")
    LOAN = "UNKNOWN"

# ============================================================================
# MACRO PROCESS - EXECUTE SUB-PROGRAM
# NOTE: DATE VALIDATION IS COMMENTED OUT IN ORIGINAL CODE
#       THE REPORT ALWAYS RUNS REGARDLESS OF LOAN EXTRACTION DATE
# ============================================================================
print("\n" + "="*80)
print("EXECUTING MONTH-END REPORT")
print("="*80)
print("\nNOTE: DATE VALIDATION IS DISABLED (COMMENTED OUT IN ORIGINAL)")
print("      REPORT WILL RUN FOR CALCULATED DATE: " + RDATE)

# ========================================================================
# COMMENTED OUT VALIDATION (FROM ORIGINAL SAS CODE)
# ========================================================================
# if LOAN == RDATE:
#     print(f"✓ VALIDATION PASSED: LOAN DATE ({LOAN}) MATCHES REPORT DATE ({RDATE})")
# else:
#     print(f"✗ VALIDATION FAILED!")
#     print(f"   LOAN EXTRACTION DATE: {LOAN}")
#     print(f"   EXPECTED DATE: {RDATE}")
#     print("\nTHE LOAN EXTRACTION IS NOT DATED " + RDATE)
#     print("THIS JOB IS NOT RUN !!")
#     print("\nABORTING WITH EXIT CODE 77")
#     sys.exit(77)

# ========================================================================
# EXECUTE SUB-PROGRAM: LNCCDB06
# MONTH-END LOAN REPORT
# ========================================================================
print("\n" + "-"*80)
print("EXECUTING: LNCCDB06 - MONTH-END LOAN REPORT")
print("-"*80)

LNCCDB06_SCRIPT = PGM_DIR / "LNCCDB06_MONTH_END_REPORT.py"

if LNCCDB06_SCRIPT.exists():
    try:
        # PASS DATE PARAMETERS TO SUB-PROGRAM
        result = subprocess.run(
            [
                sys.executable, 
                str(LNCCDB06_SCRIPT),
                "--rdate", RDATE,
                "--reptyear", REPTYEAR,
                "--reptmon", REPTMON,
                "--reptday", REPTDAY
            ],
            capture_output=True,
            text=True,
            check=True
        )
        print(result.stdout)
        print("✓ LNCCDB06 COMPLETED SUCCESSFULLY")
        
        # CLEANUP WORK DATASETS
        print("\nCLEANING UP WORK DATASETS...")
        
    except subprocess.CalledProcessError as e:
        print(f"✗ ERROR EXECUTING LNCCDB06:")
        print(e.stderr)
        print("ABORTING ORCHESTRATION")
        sys.exit(1)
        
else:
    print(f"WARNING: {LNCCDB06_SCRIPT} NOT FOUND")
    print("\nALTERNATIVE: CREATE LNCCDB06_MONTH_END_REPORT.py IN programs/ DIRECTORY")
    print("\nEXPECTED SCRIPT LOCATION:")
    print(f"  {LNCCDB06_SCRIPT}")
    print("\nPARAMETERS THAT WOULD BE PASSED:")
    print(f"  --rdate {RDATE}")
    print(f"  --reptyear {REPTYEAR}")
    print(f"  --reptmon {REPTMON}")
    print(f"  --reptday {REPTDAY}")

# ========================================================================
# FINAL SUMMARY
# ========================================================================
print("\n" + "="*80)
print("ORCHESTRATION COMPLETED")
print("="*80)
print("\nREPORT EXECUTED:")
print("  LNCCDB06 - MONTH-END LOAN REPORT")
print(f"\nREPORT DATE: {REPTDATE} (LAST DAY OF PREVIOUS MONTH)")
print(f"FORMATTED DATE: {RDATE}")
print("="*80)

# ============================================================================
# ADDITIONAL INFORMATION
# ============================================================================
print("\n" + "="*80)
print("IMPORTANT NOTES")
print("="*80)
print("\n1. DATE CALCULATION:")
print("   - REPTDATE = LAST DAY OF PREVIOUS MONTH")
print("   - CALCULATED AS: 1ST OF CURRENT MONTH - 1 DAY")
print(f"   - EXAMPLE: IF TODAY IS {TODAY}, REPTDATE IS {REPTDATE}")

print("\n2. DATE VALIDATION:")
print("   - ORIGINAL SAS CODE HAS VALIDATION COMMENTED OUT")
print("   - REPORT ALWAYS RUNS REGARDLESS OF LOAN EXTRACTION DATE")
print("   - TO ENABLE VALIDATION, UNCOMMENT LINES 62-71")

print("\n3. LOAN EXTRACTION DATE:")
if LOAN != "UNKNOWN":
    print(f"   - LOAN FILE DATE: {LOAN}")
    print(f"   - EXPECTED DATE: {RDATE}")
    if LOAN == RDATE:
        print("   - STATUS: ✓ DATES MATCH")
    else:
        print("   - STATUS: ⚠ DATES DO NOT MATCH (BUT IGNORED)")
else:
    print("   - STATUS: NOT AVAILABLE")

print("\n4. SUB-PROGRAM REQUIREMENTS:")
print("   - LNCCDB06_MONTH_END_REPORT.py MUST EXIST IN programs/ DIRECTORY")
print("   - SHOULD ACCEPT COMMAND-LINE PARAMETERS: --rdate, --reptyear, --reptmon, --reptday")

print("="*80)

# CLOSE CONNECTION
con.close()
