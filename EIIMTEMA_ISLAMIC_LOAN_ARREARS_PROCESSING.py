#!/usr/bin/env python3
"""
EIIMTEMA - LOAN ARREARS PROCESSING
1:1 CONVERSION FROM SAS TO PYTHON
PROCESSES LOAN DATA WITH ARREARS CLASSIFICATION AND CAP CALCULATIONS
MERGES LOAN NOTES, COMMITMENTS, AND PREVIOUS MONTH CAP DATA
"""

import duckdb
from pathlib import Path
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
import subprocess
import sys

# INITIALIZE PATHS
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
LOAN_DIR = INPUT_DIR / "loan"
LN_DIR = INPUT_DIR / "ln"
ICAP_DIR = INPUT_DIR / "icap"
BNM_DIR = OUTPUT_DIR / "bnm"
PGM_DIR = Path("programs")

# CREATE DIRECTORIES
BNM_DIR.mkdir(parents=True, exist_ok=True)

# CONNECT TO DUCKDB
con = duckdb.connect(":memory:")

print("="*80)
print("EIIMTEMA - LOAN ARREARS PROCESSING")
print("="*80)

# ============================================================================
# DATA BNM.REPTDATE - CALCULATE REPORT DATE AND WEEK
# ============================================================================
TODAY = datetime.today().date()

# GET FIRST DAY OF CURRENT MONTH
FIRST_DAY_THIS_MONTH = TODAY.replace(day=1)

# SUBTRACT 1 DAY TO GET LAST DAY OF PREVIOUS MONTH
REPTDATE = FIRST_DAY_THIS_MONTH - timedelta(days=1)

# DETERMINE WEEK NUMBER BASED ON DAY OF MONTH
DAY_OF_MONTH = REPTDATE.day

if 8 <= DAY_OF_MONTH <= 14:
    NOWK = '1'
elif 15 <= DAY_OF_MONTH <= 21:
    NOWK = '2'
elif 22 <= DAY_OF_MONTH <= 27:
    NOWK = '3'
else:
    NOWK = '4'

# CALCULATE PREVIOUS MONTH END DATE
FIRST_DAY_REPTDATE_MONTH = REPTDATE.replace(day=1)
PREVDATE = FIRST_DAY_REPTDATE_MONTH - timedelta(days=1)

# EXTRACT DATE COMPONENTS
PREVMON = str(PREVDATE.month).zfill(2)
PREVYEAR = str(PREVDATE.year)[-2:]  # YEAR2 FORMAT
RDATE = REPTDATE.strftime('%d%m%Y')  # DDMMYY8 FORMAT
REPTYEAR = str(REPTDATE.year)  # YEAR4 FORMAT
REPTMON = str(REPTDATE.month).zfill(2)
REPTDAY = str(REPTDATE.day).zfill(2)

print(f"\nTODAY: {TODAY}")
print(f"REPORT DATE (LAST DAY OF PREV MONTH): {REPTDATE}")
print(f"PREVIOUS MONTH END: {PREVDATE}")
print(f"RDATE: {RDATE}")
print(f"REPTYEAR: {REPTYEAR}")
print(f"REPTMON: {REPTMON}")
print(f"REPTDAY: {REPTDAY}")
print(f"WEEK: {NOWK}")
print(f"PREVMON: {PREVMON}")
print(f"PREVYEAR: {PREVYEAR}")

# SAVE REPTDATE FOR REFERENCE
con.execute(f"""
    CREATE OR REPLACE TABLE REPTDATE AS
    SELECT DATE '{REPTDATE}' AS REPTDATE
""")

REPTDATE_OUTPUT = BNM_DIR / "REPTDATE.parquet"
pq.write_table(con.execute("SELECT * FROM REPTDATE").arrow(), REPTDATE_OUTPUT)

# ============================================================================
# READ LOAN EXTRACTION DATE FOR VALIDATION
# ============================================================================
LOAN_REPTDATE_FILE = LOAN_DIR / "REPTDATE.parquet"

if not LOAN_REPTDATE_FILE.exists():
    print(f"\nERROR: LOAN REPTDATE FILE NOT FOUND: {LOAN_REPTDATE_FILE}")
    print("THIS JOB IS NOT RUN !!")
    sys.exit(1)

result = con.execute(f"""
    SELECT REPTDATE 
    FROM read_parquet('{LOAN_REPTDATE_FILE}')
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

# ============================================================================
# VALIDATE LOAN DATE MATCHES REPORT DATE
# ============================================================================
print("\n" + "="*80)
print("VALIDATING LOAN EXTRACTION DATE")
print("="*80)

if LOAN != RDATE:
    print(f"✗ VALIDATION FAILED!")
    print(f"   LOAN EXTRACTION DATE: {LOAN}")
    print(f"   EXPECTED DATE: {RDATE}")
    print("\nTHE LOAN EXTRACTION IS NOT DATED " + RDATE)
    print("THIS JOB IS NOT RUN !!")
    print("\nABORTING WITH EXIT CODE 77")
    sys.exit(77)

print(f"✓ VALIDATION PASSED: LOAN DATE ({LOAN}) MATCHES REPORT DATE ({RDATE})")

# ============================================================================
# EXECUTE PBBLNFMT (LOAN FORMAT PROGRAM)
# ============================================================================
print("\n" + "-"*80)
print("EXECUTING: PBBLNFMT - LOAN FORMAT PROGRAM")
print("-"*80)

PBBLNFMT_SCRIPT = PGM_DIR / "PBBLNFMT.py"

if PBBLNFMT_SCRIPT.exists():
    try:
        result = subprocess.run(
            [sys.executable, str(PBBLNFMT_SCRIPT)],
            capture_output=True,
            text=True,
            check=True
        )
        print(result.stdout)
        print("✓ PBBLNFMT COMPLETED SUCCESSFULLY")
    except subprocess.CalledProcessError as e:
        print(f"✗ ERROR EXECUTING PBBLNFMT:")
        print(e.stderr)
        sys.exit(1)
else:
    print(f"WARNING: {PBBLNFMT_SCRIPT} NOT FOUND - SKIPPING")

# ============================================================================
# PROC SORT AND MERGE LNNOTE WITH LNCOMM
# ============================================================================
print("\nPROCESSING LOAN NOTES AND COMMITMENTS...")

LNNOTE_FILE = LOAN_DIR / "LNNOTE.parquet"
LNCOMM_FILE = LOAN_DIR / "LNCOMM.parquet"

con.execute(f"""
    CREATE OR REPLACE TABLE LNNOTE_SORTED AS
    SELECT *
    FROM read_parquet('{LNNOTE_FILE}')
    ORDER BY ACCTNO, COMMNO
""")

con.execute(f"""
    CREATE OR REPLACE TABLE LNCOMM_SORTED AS
    SELECT DISTINCT *
    FROM read_parquet('{LNCOMM_FILE}')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCTNO, COMMNO ORDER BY ACCTNO) = 1
    ORDER BY ACCTNO, COMMNO
""")

con.execute("""
    CREATE OR REPLACE TABLE LNNOTE AS
    SELECT n.*
    FROM LNNOTE_SORTED n
    LEFT JOIN LNCOMM_SORTED c ON n.ACCTNO = c.ACCTNO AND n.COMMNO = c.COMMNO
    WHERE n.PAIDIND IS NULL OR n.PAIDIND != 'P'
    ORDER BY n.ACCTNO, n.NOTENO
""")

print(f"LNNOTE RECORDS: {con.execute('SELECT COUNT(*) FROM LNNOTE').fetchone()[0]:,}")

# ============================================================================
# MERGE WITH PREVIOUS MONTH CAP DATA
# ============================================================================
print("MERGING WITH PREVIOUS MONTH CAP DATA...")

ICAP_FILE = ICAP_DIR / f"ICAP{PREVMON}{PREVYEAR}.parquet"

if ICAP_FILE.exists():
    con.execute(f"""
        CREATE OR REPLACE TABLE ICAP_PREV AS
        SELECT 
            ACCTNO,
            NOTENO,
            CAP
        FROM read_parquet('{ICAP_FILE}')
        ORDER BY ACCTNO, NOTENO
    """)
    
    con.execute("""
        CREATE OR REPLACE TABLE LNNOTE_WITH_CAP AS
        SELECT 
            COALESCE(i.CAP, 0) AS CAP,
            n.*
        FROM LNNOTE n
        LEFT JOIN ICAP_PREV i ON n.ACCTNO = i.ACCTNO AND n.NOTENO = i.NOTENO
    """)
    
    con.execute("DROP TABLE LNNOTE")
    con.execute("ALTER TABLE LNNOTE_WITH_CAP RENAME TO LNNOTE")
else:
    print(f"WARNING: ICAP FILE NOT FOUND: {ICAP_FILE}")
    print("PROCEEDING WITHOUT CAP DATA")

# ============================================================================
# COMBINE LOAN FILES
# ============================================================================
print("COMBINING LOAN FILES...")

LOAN_FILE = LN_DIR / f"LOAN{REPTMON}{NOWK}.parquet"
LNWOF_FILE = LN_DIR / f"LNWOF{REPTMON}{NOWK}.parquet"
LNWOD_FILE = LN_DIR / f"LNWOD{REPTMON}{NOWK}.parquet"

# BUILD UNION QUERY
UNION_PARTS = []
if LOAN_FILE.exists():
    UNION_PARTS.append(f"SELECT * FROM read_parquet('{LOAN_FILE}')")
if LNWOF_FILE.exists():
    UNION_PARTS.append(f"SELECT * FROM read_parquet('{LNWOF_FILE}')")
if LNWOD_FILE.exists():
    UNION_PARTS.append(f"SELECT * FROM read_parquet('{LNWOD_FILE}')")

if not UNION_PARTS:
    print(f"ERROR: NO LOAN FILES FOUND FOR {REPTMON}{NOWK}")
    sys.exit(1)

UNION_QUERY = " UNION ALL ".join(UNION_PARTS)

con.execute(f"""
    CREATE OR REPLACE TABLE SASLN AS
    {UNION_QUERY}
    ORDER BY ACCTNO, NOTENO
""")

print(f"SASLN RECORDS: {con.execute('SELECT COUNT(*) FROM SASLN').fetchone()[0]:,}")

# ============================================================================
# MERGE LNNOTE WITH SASLN AND CALCULATE ARREARS
# ============================================================================
print("\nCALCULATING ARREARS CLASSIFICATION...")

# CONVERT RDATE TO DATE VALUE FOR CALCULATIONS
THISDATE_SQL = f"DATE '{REPTDATE}'"
CHECK_DATE_SQL = "DATE '1998-01-01'"

con.execute(f"""
    CREATE OR REPLACE TABLE LOANTEMP AS
    SELECT 
        COALESCE(s.BRANCH, s.NTBRCH) AS BRANCH,
        n.ACCTNO,
        n.NAME,
        n.NOTENO,
        s.PRODUCT,
        s.BALANCE,
        s.BLDATE,
        s.ORGBAL,
        s.CURBAL,
        s.PAYAMT,
        s.BORSTAT,
        s.LASTTRAN,
        s.MATUREDT,
        s.ISSDTE,
        s.FEEDUE,
        s.BILPAY,
        s.LSTTRNAM,
        s.LSTTRNCD,
        s.USER5,
        s.DELQCD,
        s.BILTOT,
        s.BILDUE,
        n.CAP,
        s.OLDNOTEDAYARR,
        s.DAYARR_MO,
        s.PAIDIND,
        s.REVERSED,
        n.CENSUS,
        n.COLLDESC,
        {THISDATE_SQL} AS THISDATE,
        CASE 
            WHEN s.ISSDTE >= {CHECK_DATE_SQL} THEN 1
            ELSE 0
        END AS CHECKDT,
        CASE 
            WHEN s.LASTTRAN IS NOT NULL AND s.LASTTRAN != 0
            THEN TRY_CAST(SUBSTR(LPAD(CAST(s.LASTTRAN AS VARCHAR), 11, '0'), 1, 8) AS DATE)
            ELSE NULL
        END AS LASTRAN,
        CASE 
            WHEN s.MATUREDT IS NOT NULL AND s.MATUREDT != 0
            THEN TRY_CAST(SUBSTR(LPAD(CAST(s.MATUREDT AS VARCHAR), 11, '0'), 1, 8) AS DATE)
            ELSE NULL
        END AS MATURDT,
        CASE 
            WHEN s.ORGBAL > 0 AND s.CURBAL > 0 AND s.PAYAMT > 0
            THEN ROUND((s.ORGBAL - s.CURBAL) / s.PAYAMT, 0.01)
            ELSE 0
        END AS NOISTLPD,
        CASE 
            WHEN s.BLDATE > 0 THEN {THISDATE_SQL} - s.BLDATE
            ELSE 0
        END AS DAYDIFF_BASE
    FROM LNNOTE n
    INNER JOIN SASLN s ON n.ACCTNO = s.ACCTNO AND n.NOTENO = s.NOTENO
    WHERE n.PAIDIND IS NULL OR n.PAIDIND != 'P'
        AND (s.REVERSED IS NULL OR s.REVERSED != 'Y')
""")

# ADJUST DAYDIFF FOR OLD NOTES
con.execute("""
    CREATE OR REPLACE TABLE LOANTEMP2 AS
    SELECT 
        *,
        CASE 
            WHEN OLDNOTEDAYARR > 0 AND NOTENO >= 98000 AND NOTENO <= 98999 THEN
                GREATEST(0, DAYDIFF_BASE) + OLDNOTEDAYARR
            WHEN DAYARR_MO IS NOT NULL THEN DAYARR_MO
            ELSE DAYDIFF_BASE
        END AS DAYDIFF
    FROM LOANTEMP
""")

# CALCULATE ARREAR AND ARREAR2 CLASSIFICATIONS
con.execute("""
    CREATE OR REPLACE TABLE BNM_LOANTEMP AS
    SELECT 
        BRANCH,
        ACCTNO,
        NAME,
        NOTENO,
        PRODUCT,
        BALANCE,
        BLDATE,
        THISDATE,
        DAYDIFF,
        CASE 
            WHEN BORSTAT = 'F' THEN NULL
            ELSE NOISTLPD
        END AS NOISTLPD,
        LASTRAN,
        MATURDT,
        LSTTRNAM,
        LSTTRNCD,
        USER5,
        CAP,
        PAYAMT,
        ORGBAL,
        CURBAL,
        DELQCD,
        BILTOT,
        BILDUE,
        CHECKDT,
        CENSUS,
        COLLDESC,
        ISSDTE,
        FEEDUE,
        BORSTAT,
        BILPAY,
        CASE 
            WHEN DAYDIFF < 31 THEN 1
            WHEN DAYDIFF >= 31 AND DAYDIFF < 60 THEN 2
            WHEN DAYDIFF >= 60 AND DAYDIFF < 90 THEN 3
            WHEN DAYDIFF >= 90 AND DAYDIFF < 122 THEN 4
            WHEN DAYDIFF >= 122 AND DAYDIFF < 152 THEN 5
            WHEN DAYDIFF >= 152 AND DAYDIFF < 183 THEN 6
            WHEN DAYDIFF >= 183 AND DAYDIFF < 214 THEN 7
            WHEN DAYDIFF >= 214 AND DAYDIFF < 244 THEN 8
            WHEN DAYDIFF >= 244 AND DAYDIFF < 274 THEN 9
            WHEN DAYDIFF >= 274 AND DAYDIFF < 365 THEN 10
            WHEN DAYDIFF >= 365 AND DAYDIFF < 548 THEN 11
            WHEN DAYDIFF >= 548 AND DAYDIFF < 730 THEN 12
            WHEN DAYDIFF >= 730 AND DAYDIFF < 1095 THEN 13
            WHEN DAYDIFF >= 1095 THEN 14
        END AS ARREAR2,
        CASE 
            WHEN DAYDIFF < 31 THEN 1
            WHEN DAYDIFF >= 31 AND DAYDIFF < 60 THEN 2
            WHEN DAYDIFF >= 60 AND DAYDIFF < 90 THEN 3
            WHEN DAYDIFF >= 90 AND DAYDIFF < 122 THEN 4
            WHEN DAYDIFF >= 122 AND DAYDIFF < 152 THEN 5
            WHEN DAYDIFF >= 152 AND DAYDIFF < 183 THEN 6
            WHEN DAYDIFF >= 183 AND DAYDIFF < 214 THEN 7
            WHEN DAYDIFF >= 214 AND DAYDIFF < 244 THEN 8
            WHEN DAYDIFF >= 244 AND DAYDIFF < 274 THEN 9
            WHEN DAYDIFF >= 274 AND DAYDIFF < 304 THEN 10
            WHEN DAYDIFF >= 304 AND DAYDIFF < 334 THEN 11
            WHEN DAYDIFF >= 334 AND DAYDIFF < 365 THEN 12
            WHEN DAYDIFF >= 365 AND DAYDIFF < 548 THEN 13
            WHEN DAYDIFF >= 548 AND DAYDIFF < 730 THEN 14
            WHEN DAYDIFF >= 730 AND DAYDIFF < 1095 THEN 15
            WHEN DAYDIFF >= 1095 THEN 16
        END AS ARREAR,
        'N/A' AS LOANSTAT
    FROM LOANTEMP2
    ORDER BY BRANCH, ARREAR
""")

FINAL_COUNT = con.execute("SELECT COUNT(*) FROM BNM_LOANTEMP").fetchone()[0]
print(f"FINAL LOANTEMP RECORDS: {FINAL_COUNT:,}")

# ============================================================================
# SAVE OUTPUT
# ============================================================================
print("\nSAVING OUTPUT...")

LOANTEMP_OUTPUT = BNM_DIR / "LOANTEMP.parquet"
LOANTEMP_CSV = BNM_DIR / "LOANTEMP.csv"

LOANTEMP_DATA = con.execute("SELECT * FROM BNM_LOANTEMP").arrow()
pq.write_table(LOANTEMP_DATA, LOANTEMP_OUTPUT)
csv.write_csv(LOANTEMP_DATA, LOANTEMP_CSV)

print(f"SAVED: {LOANTEMP_OUTPUT}")
print(f"SAVED: {LOANTEMP_CSV}")

# ============================================================================
# SUMMARY REPORT
# ============================================================================
print("\n" + "="*80)
print("PROCESSING SUMMARY")
print("="*80)
print(f"REPORT DATE: {REPTDATE}")
print(f"WEEK: {NOWK}")
print(f"PREVIOUS MONTH: {PREVMON}/{PREVYEAR}")
print(f"FINAL RECORDS: {FINAL_COUNT:,}")
print("="*80)

if FINAL_COUNT > 0:
    summary = con.execute("""
        SELECT 
            ARREAR,
            COUNT(*) AS COUNT,
            SUM(BALANCE) AS TOTAL_BALANCE
        FROM BNM_LOANTEMP
        GROUP BY ARREAR
        ORDER BY ARREAR
    """).df()
    
    print("\nARREARS CLASSIFICATION:")
    print(summary.to_string(index=False))

# CLOSE CONNECTION
con.close()
print("\nPROCESSING COMPLETE!")
