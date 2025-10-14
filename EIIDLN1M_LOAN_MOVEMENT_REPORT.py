#!/usr/bin/env python3
"""
EIIDLN1M - DAILY LOAN MOVEMENT REPORT
1:1 CONVERSION FROM SAS TO PYTHON
REPORTS DAILY MOVEMENTS IN LOANS/OD ACCOUNTS >= RM1 MILLION
COMPARES CURRENT DAY VS PREVIOUS DAY BALANCES
"""

import duckdb
from pathlib import Path
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv

# INITIALIZE PATHS
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
LOAN_DIR = INPUT_DIR / "loan"
LOANX_DIR = INPUT_DIR / "loanx"
CISDP_DIR = INPUT_DIR / "cisdp"
CISLN_DIR = INPUT_DIR / "cisln"

# CREATE DIRECTORIES
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# CONNECT TO DUCKDB
con = duckdb.connect(":memory:")

print("PROCESSING EIIDLN1M - DAILY LOAN MOVEMENT REPORT")

# ============================================================================
# DATA REPTDATE - READ REPORT DATE AND CALCULATE PREVIOUS DATE
# ============================================================================
REPTDATE_FILE = LOAN_DIR / "REPTDATE.parquet"

if not REPTDATE_FILE.exists():
    print(f"ERROR: REPTDATE FILE NOT FOUND: {REPTDATE_FILE}")
    con.close()
    exit(1)

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

# CALCULATE PREVIOUS DATE
REPTPDAT = REPTDATE - timedelta(days=1)

# EXTRACT DATE COMPONENTS
RDATE = REPTDATE.strftime('%d%m%Y')
REPTDAY = str(REPTDATE.day).zfill(2)
REPTPDAY = str(REPTPDAT.day).zfill(2)
REPTMON = str(REPTDATE.month).zfill(2)
REPTPMON = str(REPTPDAT.month).zfill(2)
REPTYEAR = str(REPTDATE.year)[-2:]
REPTPYEA = str(REPTPDAT.year)[-2:]

print(f"REPORT DATE: {REPTDATE}")
print(f"PREVIOUS DATE: {REPTPDAT}")
print(f"CURRENT: {REPTMON}/{REPTDAY}/{REPTYEAR}")
print(f"PREVIOUS: {REPTPMON}/{REPTPDAY}/{REPTPYEA}")

# ============================================================================
# DATA CISNM - CUSTOMER NAME FROM CIS FILES
# ============================================================================
print("\nLOADING CUSTOMER NAMES...")

DEPOSIT_FILE = CISDP_DIR / "DEPOSIT.parquet"
LOAN_CIS_FILE = CISLN_DIR / "LOAN.parquet"

con.execute(f"""
    CREATE OR REPLACE TABLE CISNM_RAW AS
    SELECT 
        ACCTNO,
        CUSTNAM1 AS CUSTNAME,
        SECCUST
    FROM read_parquet('{DEPOSIT_FILE}')
    
    UNION ALL
    
    SELECT 
        ACCTNO,
        CUSTNAM1 AS CUSTNAME,
        SECCUST
    FROM read_parquet('{LOAN_CIS_FILE}')
""")

# SORT AND REMOVE DUPLICATES (NODUPKEY)
con.execute("""
    CREATE OR REPLACE TABLE CISNM AS
    SELECT DISTINCT 
        ACCTNO,
        FIRST_VALUE(CUSTNAME) OVER (PARTITION BY ACCTNO ORDER BY SECCUST, CUSTNAME) AS CUSTNAME,
        FIRST_VALUE(SECCUST) OVER (PARTITION BY ACCTNO ORDER BY SECCUST, CUSTNAME) AS SECCUST
    FROM CISNM_RAW
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCTNO ORDER BY SECCUST, CUSTNAME) = 1
    ORDER BY ACCTNO
""")

print(f"CUSTOMER RECORDS: {con.execute('SELECT COUNT(*) FROM CISNM').fetchone()[0]:,}")

# ============================================================================
# DATA SASC - CURRENT DAY LOAN DATA
# ============================================================================
print("\nPROCESSING CURRENT DAY LOAN DATA...")

LOAN_CURRENT_FILE = LOAN_DIR / f"LOAN{REPTMON}{REPTDAY}.parquet"

if not LOAN_CURRENT_FILE.exists():
    print(f"ERROR: CURRENT LOAN FILE NOT FOUND: {LOAN_CURRENT_FILE}")
    con.close()
    exit(1)

con.execute(f"""
    CREATE OR REPLACE TABLE SASC_RAW AS
    SELECT 
        ACCTNO,
        BRANCH,
        APPRLIMT,
        APPRLIM2,
        BALANCE,
        ACCTYPE,
        PRODUCT,
        'C' AS TDI,
        CASE 
            WHEN ACCTYPE = 'OD' THEN 'OD'
            WHEN ACCTYPE = 'LN' AND PRODUCT IN (302,350,364,365,506,902,903,910,925,951) THEN 'RC'
            WHEN ACCTYPE = 'LN' AND PRODUCT IN (128,130,131,132,380,381,700,705,720,725) THEN 'HP'
            WHEN ACCTYPE = 'LN' THEN 'TL'
            ELSE NULL
        END AS CATG
    FROM read_parquet('{LOAN_CURRENT_FILE}')
    WHERE ACCTYPE IN ('OD', 'LN')
        AND NOT (ACCTYPE = 'OD' AND PRODUCT IN (107, 173))
        AND CASE 
            WHEN ACCTYPE = 'OD' THEN 'OD'
            WHEN ACCTYPE = 'LN' AND PRODUCT IN (302,350,364,365,506,902,903,910,925,951) THEN 'RC'
            WHEN ACCTYPE = 'LN' AND PRODUCT IN (128,130,131,132,380,381,700,705,720,725) THEN 'HP'
            WHEN ACCTYPE = 'LN' THEN 'TL'
        END IS NOT NULL
    ORDER BY BRANCH, ACCTNO
""")

# AGGREGATE BY BRANCH AND ACCTNO
con.execute("""
    CREATE OR REPLACE TABLE SASC AS
    SELECT 
        BRANCH,
        ACCTNO,
        TDI,
        CATG,
        SUM(BALANCE) AS ACCBAL,
        SUM(APPRLIMT) AS LIMTBAL,
        MAX(APPRLIM2) AS APPRLIM2
    FROM SASC_RAW
    GROUP BY BRANCH, ACCTNO, TDI, CATG
    ORDER BY BRANCH, ACCTNO
""")

print(f"CURRENT DAY RECORDS: {con.execute('SELECT COUNT(*) FROM SASC').fetchone()[0]:,}")

# ============================================================================
# DATA SASP - PREVIOUS DAY LOAN DATA
# ============================================================================
print("PROCESSING PREVIOUS DAY LOAN DATA...")

LOAN_PREVIOUS_FILE = LOANX_DIR / f"LOAN{REPTPMON}{REPTPDAY}.parquet"

if not LOAN_PREVIOUS_FILE.exists():
    print(f"WARNING: PREVIOUS LOAN FILE NOT FOUND: {LOAN_PREVIOUS_FILE}")
    print("ASSUMING ALL BALANCES ARE NEW")
    con.execute("""
        CREATE OR REPLACE TABLE SASP AS
        SELECT 
            BRANCH,
            ACCTNO,
            'P' AS TDI,
            CATG,
            0.0 AS PACCBAL,
            0 AS PLIMBAL
        FROM SASC
        WHERE 1=0
    """)
else:
    con.execute(f"""
        CREATE OR REPLACE TABLE SASP_RAW AS
        SELECT 
            ACCTNO,
            BRANCH,
            APPRLIMT,
            APPRLIM2,
            BALANCE,
            ACCTYPE,
            PRODUCT,
            'P' AS TDI,
            CASE 
                WHEN ACCTYPE = 'OD' THEN 'OD'
                WHEN ACCTYPE = 'LN' AND PRODUCT IN (302,350,364,365,506,902,903,910,925,951) THEN 'RC'
                WHEN ACCTYPE = 'LN' AND PRODUCT IN (128,130,131,132,380,381,700,705,720,725) THEN 'HP'
                WHEN ACCTYPE = 'LN' THEN 'TL'
                ELSE NULL
            END AS CATG
        FROM read_parquet('{LOAN_PREVIOUS_FILE}')
        WHERE ACCTYPE IN ('OD', 'LN')
            AND NOT (ACCTYPE = 'OD' AND PRODUCT IN (107, 173))
            AND CASE 
                WHEN ACCTYPE = 'OD' THEN 'OD'
                WHEN ACCTYPE = 'LN' AND PRODUCT IN (302,350,364,365,506,902,903,910,925,951) THEN 'RC'
                WHEN ACCTYPE = 'LN' AND PRODUCT IN (128,130,131,132,380,381,700,705,720,725) THEN 'HP'
                WHEN ACCTYPE = 'LN' THEN 'TL'
            END IS NOT NULL
        ORDER BY BRANCH, ACCTNO
    """)
    
    con.execute("""
        CREATE OR REPLACE TABLE SASP AS
        SELECT 
            BRANCH,
            ACCTNO,
            TDI,
            CATG,
            SUM(BALANCE) AS PACCBAL,
            SUM(APPRLIMT) AS PLIMBAL
        FROM SASP_RAW
        GROUP BY BRANCH, ACCTNO, TDI, CATG
        ORDER BY BRANCH, ACCTNO
    """)
    
    print(f"PREVIOUS DAY RECORDS: {con.execute('SELECT COUNT(*) FROM SASP').fetchone()[0]:,}")

# ============================================================================
# MERGE CURRENT AND PREVIOUS DATA
# ============================================================================
print("\nMERGING CURRENT AND PREVIOUS DATA...")

con.execute("""
    CREATE OR REPLACE TABLE SASC_MERGED AS
    SELECT 
        COALESCE(c.BRANCH, p.BRANCH) AS BRANCH,
        COALESCE(c.ACCTNO, p.ACCTNO) AS ACCTNO,
        COALESCE(c.TDI, p.TDI) AS TDI,
        COALESCE(c.CATG, p.CATG) AS CATG,
        COALESCE(c.ACCBAL, 0.0) AS ACCBAL,
        COALESCE(p.PACCBAL, 0.0) AS PACCBAL,
        COALESCE(c.LIMTBAL, COALESCE(p.PLIMBAL, 0)) AS LIMTBAL,
        COALESCE(c.ACCBAL, 0.0) - COALESCE(p.PACCBAL, 0.0) AS MOVEAMTS,
        ABS(COALESCE(c.ACCBAL, 0.0) - COALESCE(p.PACCBAL, 0.0)) AS MOVEAMT
    FROM SASC c
    FULL OUTER JOIN SASP p ON c.BRANCH = p.BRANCH AND c.ACCTNO = p.ACCTNO
    WHERE ABS(COALESCE(c.ACCBAL, 0.0) - COALESCE(p.PACCBAL, 0.0)) >= 1000000
""")

FILTERED_COUNT = con.execute("SELECT COUNT(*) FROM SASC_MERGED").fetchone()[0]
print(f"ACCOUNTS WITH MOVEMENT >= RM1,000,000: {FILTERED_COUNT:,}")

# ============================================================================
# LOAD BRANCH DATA
# ============================================================================
print("\nLOADING BRANCH MASTER...")

BRANCHF_FILE = INPUT_DIR / "branchfile.parquet"

con.execute(f"""
    CREATE OR REPLACE TABLE BRANCH AS
    SELECT 
        BRANCH,
        BANK,
        ABBREV AS BRNAME
    FROM read_parquet('{BRANCHF_FILE}')
    ORDER BY BRANCH
""")

# ============================================================================
# MERGE WITH BRANCH AND CUSTOMER DATA
# ============================================================================
con.execute("""
    CREATE OR REPLACE TABLE SASC_FINAL AS
    SELECT 
        s.BRANCH,
        b.BRNAME,
        s.ACCTNO,
        c.CUSTNAME,
        s.CATG,
        CASE 
            WHEN s.CATG = 'OD' THEN 'OVERDRAFT       '
            WHEN s.CATG = 'TL' THEN 'TERM LOAN       '
            WHEN s.CATG = 'HP' THEN 'HIRE PURCHASE   '
            WHEN s.CATG = 'RC' THEN 'REVOLVING CREDIT'
            ELSE '                '
        END AS CATEGORY,
        s.LIMTBAL,
        s.ACCBAL,
        s.PACCBAL,
        s.MOVEAMTS
    FROM SASC_MERGED s
    LEFT JOIN BRANCH b ON s.BRANCH = b.BRANCH
    LEFT JOIN CISNM c ON s.ACCTNO = c.ACCTNO
    WHERE s.CATG IS NOT NULL
    ORDER BY CATEGORY, BRANCH, ACCTNO
""")

# ============================================================================
# GENERATE REPORT OUTPUT
# ============================================================================
print("\nGENERATING REPORT...")

REPORT_DATA = con.execute("SELECT * FROM SASC_FINAL ORDER BY CATEGORY, BRANCH, ACCTNO").arrow()

# SAVE TO FILES
OUTPUT_PARQUET = OUTPUT_DIR / f"EIIDLN1M_{REPTYEAR}{REPTMON}{REPTDAY}.parquet"
OUTPUT_CSV = OUTPUT_DIR / f"EIIDLN1M_{REPTYEAR}{REPTMON}{REPTDAY}.csv"
OUTPUT_TXT = OUTPUT_DIR / f"EIIDLN1M_{REPTYEAR}{REPTMON}{REPTDAY}.txt"

pq.write_table(REPORT_DATA, OUTPUT_PARQUET)
csv.write_csv(REPORT_DATA, OUTPUT_CSV)

print(f"SAVED: {OUTPUT_PARQUET}")
print(f"SAVED: {OUTPUT_CSV}")

# ============================================================================
# GENERATE FORMATTED TEXT REPORT
# ============================================================================
with open(OUTPUT_TXT, 'w') as f:
    f.write("PUBLIC ISLAMIC BANK BERHAD - RETAIL BANKING DIVISION\n")
    f.write("REPORT TITLE : EIIDLN1M\n")
    f.write(f"DAILY MOVEMENT IN BANK'S LOANS/OD ACCOUNTS @ {RDATE}\n")
    f.write("NET INCREASED/(DECREASED) OF RM1 MILLION & ABOVE PER CUSTOMER\n")
    f.write("*\n\n")
    
    current_category = None
    
    for row in REPORT_DATA.to_pylist():
        if row['CATEGORY'] != current_category:
            if current_category is not None:
                f.write("\f")  # PAGE BREAK
            
            current_category = row['CATEGORY']
            f.write(f"{current_category}\n")
            f.write("-" * 131 + "\n")
            f.write("BRH  BRH\n")
            f.write("CODE ABBR CUSTOMER NAME                              ACCOUNT NO.   APPROVE LIMIT  CURRENT BALANCE  PREVIOUS BAL    NET (INC/DEC)\n")
            f.write("-" * 131 + "\n")
        
        branch = str(row['BRANCH']).zfill(3) if row['BRANCH'] else '   '
        brname = (row['BRNAME'] or '   ')[:3]
        custname = (row['CUSTNAME'] or '')[:40].ljust(40)
        acctno = str(row['ACCTNO']) if row['ACCTNO'] else ''
        limtbal = f"{row['LIMTBAL']:,}" if row['LIMTBAL'] else '0'
        accbal = f"{row['ACCBAL']:,.2f}" if row['ACCBAL'] is not None else '0.00'
        paccbal = f"{row['PACCBAL']:,.2f}" if row['PACCBAL'] is not None else '0.00'
        moveamts = f"{row['MOVEAMTS']:,.2f}" if row['MOVEAMTS'] is not None else '0.00'
        
        f.write(f"{branch}  {brname}  {custname}  {acctno:>10}  {limtbal:>15}  {accbal:>15}  {paccbal:>15}  {moveamts:>15}\n")

print(f"SAVED: {OUTPUT_TXT}")

# ============================================================================
# SUMMARY REPORT
# ============================================================================
print("\n" + "="*80)
print("PROCESSING SUMMARY")
print("="*80)
print(f"REPORT DATE      : {REPTDATE}")
print(f"PREVIOUS DATE    : {REPTPDAT}")
print(f"MOVEMENTS >= RM1M: {FILTERED_COUNT:,}")
print("="*80)

if FILTERED_COUNT > 0:
    summary = con.execute("""
        SELECT 
            CATEGORY,
            COUNT(*) AS COUNT,
            SUM(MOVEAMTS) AS TOTAL_MOVEMENT,
            SUM(ACCBAL) AS TOTAL_CURRENT,
            SUM(PACCBAL) AS TOTAL_PREVIOUS
        FROM SASC_FINAL
        GROUP BY CATEGORY
        ORDER BY CATEGORY
    """).df()
    
    print("\nMOVEMENT BY CATEGORY:")
    print(summary.to_string(index=False))

# CLOSE CONNECTION
con.close()
print("\nPROCESSING COMPLETE!")
