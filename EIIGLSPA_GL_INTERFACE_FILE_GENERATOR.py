#!/usr/bin/env python3
"""
EIIGLSPA - GL INTERFACE FILE GENERATOR FOR SPECIAL PROVISIONS
1:1 CONVERSION FROM SAS TO PYTHON
GENERATES GL INTERFACE FILE (INTFSPV2) FOR SPECIAL PROVISION ADJUSTMENTS
PROCESSES HPD CONVENTIONAL AND AITAB LOAN PROVISIONS
"""

import duckdb
from pathlib import Path
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq

# INITIALIZE PATHS
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
LOAN_DIR = INPUT_DIR / "loan"
NPL_DIR = INPUT_DIR / "npl"

# CREATE DIRECTORIES
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# CONNECT TO DUCKDB
con = duckdb.connect(":memory:")

print("="*80)
print("EIIGLSPA - GL INTERFACE FILE GENERATOR")
print("="*80)

# ============================================================================
# DATA _NULL_ - CALCULATE DATES AND TIMES
# ============================================================================
REPTDATE_FILE = LOAN_DIR / "REPTDATE.parquet"

if not REPTDATE_FILE.exists():
    print(f"ERROR: REPTDATE FILE NOT FOUND: {REPTDATE_FILE}")
    exit(1)

result = con.execute(f"""
    SELECT REPTDATE 
    FROM read_parquet('{REPTDATE_FILE}')
    LIMIT 1
""").fetchone()

REPTDATE = result[0]

if isinstance(REPTDATE, (int, float)):
    REPTDATE = datetime.strptime(str(int(REPTDATE)), '%Y%m%d').date()
elif isinstance(REPTDATE, str):
    REPTDATE = datetime.strptime(REPTDATE, '%Y-%m-%d').date()

# CALCULATE PREVIOUS MONTH
MM1 = REPTDATE.month - 1
if MM1 == 0:
    MM1 = 12

# CURRENT DATE AND TIME
DT = datetime.today()
TM = datetime.today()

# FORMAT DATE/TIME COMPONENTS
DATENOW = f"{DT.year:04d}{DT.month:02d}{DT.day:02d}"
TIMENOW = f"{TM.hour:02d}{TM.minute:02d}{TM.second:02d}"

# MACRO VARIABLES
REPTMON = str(REPTDATE.month).zfill(2)
PREVMON = str(MM1).zfill(2)
RDATE = REPTDATE.strftime('%d/%m/%Y')
REPTDAT = REPTDATE
NOWK = '4'
RPTDTE = DATENOW
RPTTIME = TIMENOW
CURDAY = str(DT.day).zfill(2)
CURMTH = str(DT.month).zfill(2)
CURYR = str(DT.year).zfill(4)
RPTYR = str(REPTDATE.year).zfill(4)
RPTDY = str(REPTDATE.day).zfill(2)

print(f"\nREPORT DATE: {REPTDATE}")
print(f"REPORT MONTH: {REPTMON}")
print(f"PREVIOUS MONTH: {PREVMON}")
print(f"CURRENT DATE: {CURYR}-{CURMTH}-{CURDAY}")
print(f"REPORT TIME: {RPTTIME}")

# ============================================================================
# PROC SORT AND MERGE NPL DATA
# ============================================================================
print("\nPROCESSING NPL DATA...")

IIS_FILE = NPL_DIR / "IIS.parquet"
SP2_FILE = NPL_DIR / "SP2.parquet"
LNNOTE_FILE = LOAN_DIR / "LNNOTE.parquet"

# REMOVE DUPLICATES FROM NPL.IIS
con.execute(f"""
    CREATE OR REPLACE TABLE NPLIIS AS
    SELECT DISTINCT *
    FROM read_parquet('{IIS_FILE}')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCTNO, NOTENO ORDER BY ACCTNO) = 1
    ORDER BY ACCTNO, NOTENO
""")

# REMOVE DUPLICATES FROM NPL.SP2
con.execute(f"""
    CREATE OR REPLACE TABLE NPLSP2 AS
    SELECT DISTINCT *
    FROM read_parquet('{SP2_FILE}')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCTNO, NOTENO ORDER BY ACCTNO) = 1
    ORDER BY ACCTNO, NOTENO
""")

# SORT LNNOTE
con.execute(f"""
    CREATE OR REPLACE TABLE LNNOTE AS
    SELECT 
        ACCTNO,
        NOTENO,
        COSTCTR
    FROM read_parquet('{LNNOTE_FILE}')
    ORDER BY ACCTNO, NOTENO
""")

# MERGE NPLSP2 WITH LNNOTE
con.execute("""
    CREATE OR REPLACE TABLE NPLIIS_MERGED AS
    SELECT 
        s.*,
        CASE 
            WHEN l.COSTCTR = 1146 THEN 146
            ELSE l.COSTCTR
        END AS COSTCTR
    FROM NPLSP2 s
    LEFT JOIN LNNOTE l ON s.ACCTNO = l.ACCTNO AND s.NOTENO = l.NOTENO
    ORDER BY COSTCTR
""")

# ============================================================================
# CREATE GL PROC DATA
# ============================================================================
print("GENERATING GL ENTRIES...")

con.execute("""
    CREATE OR REPLACE TABLE GLPROC_RAW AS
    SELECT 
        COSTCTR,
        LOANTYP,
        SPPL,
        RECOVER,
        CASE 
            WHEN SUBSTR(LOANTYP, 1, 8) = 'HPD CONV' AND SPPL IS NOT NULL AND SPPL != 0 THEN 'SPHPDPJ'
            WHEN SUBSTR(LOANTYP, 1, 8) = 'HPD CONV' AND RECOVER IS NOT NULL AND RECOVER != 0 THEN 'SPHPDWK'
            WHEN SUBSTR(LOANTYP, 1, 8) != 'HPD CONV' AND SPPL IS NOT NULL AND SPPL != 0 THEN 'SPAITPJ'
            WHEN SUBSTR(LOANTYP, 1, 8) != 'HPD CONV' AND RECOVER IS NOT NULL AND RECOVER != 0 THEN 'SPAITWK'
        END AS ARID,
        CASE 
            WHEN SUBSTR(LOANTYP, 1, 8) = 'HPD CONV' AND SPPL IS NOT NULL AND SPPL != 0 THEN 'PROVISION FOR SP (HPD)(J)'
            WHEN SUBSTR(LOANTYP, 1, 8) = 'HPD CONV' AND RECOVER IS NOT NULL AND RECOVER != 0 THEN 'SP WRITTEN BACK (HPD)(K)'
            WHEN SUBSTR(LOANTYP, 1, 8) != 'HPD CONV' AND SPPL IS NOT NULL AND SPPL != 0 THEN 'PROVISION OF SP (AITAB) (J)'
            WHEN SUBSTR(LOANTYP, 1, 8) != 'HPD CONV' AND RECOVER IS NOT NULL AND RECOVER != 0 THEN 'SP WRITTEN BACK (AITAB) (K)'
        END AS JDESC,
        CASE 
            WHEN SPPL IS NOT NULL AND SPPL != 0 THEN SPPL
            WHEN RECOVER IS NOT NULL AND RECOVER != 0 THEN RECOVER
        END AS AVALUE
    FROM NPLIIS_MERGED
    WHERE (SPPL IS NOT NULL AND SPPL != 0) 
        OR (RECOVER IS NOT NULL AND RECOVER != 0)
""")

# AGGREGATE BY COSTCTR, ARID, JDESC
con.execute("""
    CREATE OR REPLACE TABLE GLPROC AS
    SELECT 
        COSTCTR,
        ARID,
        JDESC,
        SUM(AVALUE) AS AVALUE
    FROM GLPROC_RAW
    WHERE ARID IS NOT NULL
    GROUP BY COSTCTR, ARID, JDESC
    ORDER BY COSTCTR, ARID
""")

GLPROC_COUNT = con.execute("SELECT COUNT(*) FROM GLPROC").fetchone()[0]
print(f"GL PROC RECORDS: {GLPROC_COUNT}")

# ============================================================================
# GENERATE GL INTERFACE FILE
# ============================================================================
print("\nGENERATING GL INTERFACE FILE...")

GLFILE2 = OUTPUT_DIR / f"INTFSPV2_{RPTYR}{REPTMON}{RPTDY}.txt"

# GET DATA
GLPROC_DATA = con.execute("""
    SELECT 
        COSTCTR,
        ARID,
        JDESC,
        AVALUE
    FROM GLPROC
    ORDER BY COSTCTR, ARID
""").fetchall()

TRN = len(GLPROC_DATA)

# INITIALIZE COUNTERS
CNT = 0
AMTTOTC = 0.0  # TOTAL CREDIT
AMTTOTD = 0.0  # TOTAL DEBIT

with open(GLFILE2, 'w') as f:
    # WRITE HEADER RECORD (TYPE 0)
    f.write(f"INTFSPV20{' '*8}{RPTYR}{REPTMON}{RPTDY}{RPTDTE}{RPTTIME}\n")
    
    # PROCESS EACH GL ENTRY
    for i, (COSTCTR, ARID, JDESC, AVALUE) in enumerate(GLPROC_DATA):
        # ROUND VALUE
        AVALUE = round(AVALUE, 2)
        
        # DETERMINE SIGN
        if AVALUE < 0:
            AVALUE = abs(AVALUE)
            ASIGN = '-'
            AMTTOTC += AVALUE
        else:
            ASIGN = '+'
            AMTTOTD += AVALUE
        
        CNT += 1
        
        # FORMAT AMOUNT (17.2 -> 16 CHARS WITHOUT DECIMAL POINT)
        TEMPAMT1 = f"{AVALUE:017.2f}"
        BRHAMTO = TEMPAMT1.replace('.', '')
        
        # DETERMINE DATE BASED ON ARID
        if ARID[5:8] == 'COR':
            USE_YR = CURYR
            USE_MTH = CURMTH
            USE_DAY = CURDAY
        else:
            USE_YR = RPTYR
            USE_MTH = REPTMON
            USE_DAY = RPTDY
        
        # WRITE DETAIL RECORD (TYPE 1)
        f.write(f"INTFSPV21{ARID:<8}PIBB{COSTCTR:04d}{' '*45}MYR{' '*2}{USE_YR}{USE_MTH}{USE_DAY}{ASIGN}{BRHAMTO}{JDESC:<60}{ARID:<8}SPV2{' '*77}+0000000000000000+000000000000000{' '*12}{USE_YR}{USE_MTH}{USE_DAY}{' '*6}+0000000000000000\n")
    
    # FORMAT TOTALS
    TEMPAMT1 = f"{AMTTOTC:017.2f}"
    AMTTOTCO = TEMPAMT1.replace('.', '')
    TEMPAMT2 = f"{AMTTOTD:017.2f}"
    AMTTOTDO = TEMPAMT2.replace('.', '')
    
    # WRITE TRAILER RECORD (TYPE 9)
    f.write(f"INTFSPV29{' '*9}{RPTYR}{REPTMON}{RPTDY}{RPTDTE}{RPTTIME}+{AMTTOTDO}-{AMTTOTCO}+0000000000000000-0000000000000000{CNT:09d}\n")

print(f"SAVED: {GLFILE2}")

# ============================================================================
# SUMMARY REPORT
# ============================================================================
print("\n" + "="*80)
print("PROCESSING SUMMARY")
print("="*80)
print(f"REPORT DATE: {RDATE}")
print(f"TOTAL GL ENTRIES: {CNT}")
print(f"TOTAL DEBIT: RM {AMTTOTD:,.2f}")
print(f"TOTAL CREDIT: RM {AMTTOTC:,.2f}")
print("="*80)

if CNT > 0:
    print("\nGL ENTRY BREAKDOWN:")
    summary = con.execute("""
        SELECT 
            ARID,
            JDESC,
            COUNT(*) AS COUNT,
            SUM(AVALUE) AS TOTAL
        FROM GLPROC
        GROUP BY ARID, JDESC
        ORDER BY ARID
    """).df()
    print(summary.to_string(index=False))

# ============================================================================
# GL INTERFACE FILE FORMAT DOCUMENTATION
# ============================================================================
print("\n" + "="*80)
print("GL INTERFACE FILE FORMAT (INTFSPV2)")
print("="*80)
print("\nRECORD TYPE 0 (HEADER):")
print("  Position 1-8: 'INTFSPV2'")
print("  Position 9: '0' (Header indicator)")
print("  Position 18-21: Year")
print("  Position 22-23: Month")
print("  Position 24-25: Day")
print("  Position 26-33: Date (YYYYMMDD)")
print("  Position 34-39: Time (HHMMSS)")

print("\nRECORD TYPE 1 (DETAIL):")
print("  Position 1-8: 'INTFSPV2'")
print("  Position 9: '1' (Detail indicator)")
print("  Position 10-17: ARID (Account ID)")
print("  Position 18-21: 'PIBB' (Entity code)")
print("  Position 22-25: Cost Center")
print("  Position 71-73: 'MYR' (Currency)")
print("  Position 77-83: Transaction date (YYYYMMDD)")
print("  Position 85: Sign (+/-)")
print("  Position 86-101: Amount (16 digits)")
print("  Position 102-161: Description")
print("  Position 162-169: Reference")
print("  Position 172-175: 'SPV2' (Batch ID)")

print("\nRECORD TYPE 9 (TRAILER):")
print("  Position 1-8: 'INTFSPV2'")
print("  Position 9: '9' (Trailer indicator)")
print("  Position 18-39: Date/Time")
print("  Position 40: '+' ")
print("  Position 41-56: Total Debit")
print("  Position 57: '-'")
print("  Position 58-73: Total Credit")
print("  Position 108-116: Record count")

print("\nARID CODES:")
print("  SPHPDPJ: Provision for SP (HPD) - Debit")
print("  SPHPDWK: SP Written Back (HPD) - Credit")
print("  SPAITPJ: Provision of SP (AITAB) - Debit")
print("  SPAITWK: SP Written Back (AITAB) - Credit")
print("="*80)

# CLOSE CONNECTION
con.close()
print("\nPROCESSING COMPLETE!")
