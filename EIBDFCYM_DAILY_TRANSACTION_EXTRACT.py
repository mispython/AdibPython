#!/usr/bin/env python3
"""
EIBDFCYM - DAILY TRANSACTION EXTRACT
1:1 CONVERSION FROM SAS TO PYTHON
EXTRACTS SPECIFIC TRANSACTION CODES FROM WEEKLY CRM FILES
FILTERS BY DATE AND ACCOUNT NUMBER RANGE
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
CRM_DIR = INPUT_DIR / "crm"
DAILY_DIR = OUTPUT_DIR / "daily"

# CREATE DIRECTORIES
DAILY_DIR.mkdir(parents=True, exist_ok=True)

# CONNECT TO DUCKDB
con = duckdb.connect(":memory:")

print("PROCESSING EIBDFCYM - DAILY TRANSACTION EXTRACT")

# ============================================================================
# DATA REPTDATE - CALCULATE REPORT DATE AND WEEK NUMBER
# ============================================================================
REPTDATE = datetime.today().date() - timedelta(days=1)
DATE = REPTDATE  # FOR COMPARISON IN FILTER

# DETERMINE WEEK NUMBER BASED ON DAY OF MONTH
DAY_OF_MONTH = REPTDATE.day

if 1 <= DAY_OF_MONTH <= 8:
    WK = '01'
    WK1 = '1'
elif 9 <= DAY_OF_MONTH <= 15:
    WK = '02'
    WK1 = '2'
elif 16 <= DAY_OF_MONTH <= 22:
    WK = '03'
    WK1 = '3'
else:  # 23-31
    WK = '04'
    WK1 = '4'

# EXTRACT DATE COMPONENTS
MM = REPTDATE.month
REPTDAY = str(REPTDATE.day).zfill(2)
REPTMON = str(MM).zfill(2)
REPTYEAR = str(REPTDATE.year)[-2:]  # LAST 2 DIGITS (YEAR2 FORMAT)
RDATE = REPTDATE.strftime('%d%m%Y')  # DDMMYY8 FORMAT (8 DIGITS)

# MACRO VARIABLES (FOR REFERENCE)
NOWK = WK
NOWK1 = WK1
REPTDT = REPTDATE

print(f"REPORT DATE: {REPTDATE}")
print(f"REPTDAY: {REPTDAY}")
print(f"REPTMON: {REPTMON}")
print(f"REPTYEAR: {REPTYEAR}")
print(f"WEEK: {WK} (WK1: {WK1})")
print(f"RDATE: {RDATE}")

# ============================================================================
# DATA DAILY.DPBTRAN_DAILY - EXTRACT TRANSACTIONS FROM WEEKLY FILE
# INPUT: CRM.DPBTRAN{YEAR}{MONTH}{WEEK}
# OUTPUT: DAILY.DPBTRAN_DAILY{YEAR}{MONTH}{DAY}
# ============================================================================
print(f"\nPROCESSING DAILY TRANSACTION EXTRACT...")

# CONSTRUCT INPUT FILE NAME
INPUT_FILENAME = f"DPBTRAN{REPTYEAR}{REPTMON}{NOWK}.parquet"
INPUT_FILE = CRM_DIR / INPUT_FILENAME

# CONSTRUCT OUTPUT FILE NAME
OUTPUT_FILENAME = f"DPBTRAN_DAILY{REPTYEAR}{REPTMON}{REPTDAY}"
OUTPUT_PARQUET = DAILY_DIR / f"{OUTPUT_FILENAME}.parquet"
OUTPUT_CSV = DAILY_DIR / f"{OUTPUT_FILENAME}.csv"

# CHECK IF INPUT FILE EXISTS
if not INPUT_FILE.exists():
    print(f"ERROR: INPUT FILE NOT FOUND: {INPUT_FILE}")
    print("PROCESSING ABORTED")
    con.close()
    exit(1)

print(f"INPUT FILE: {INPUT_FILE}")

# ============================================================================
# EXTRACT AND FILTER TRANSACTIONS
# FILTER CRITERIA:
#   - TRANCODE IN (297, 770, 970, 971, 972)
#   - REPTDATE = DATE (YESTERDAY)
#   - ACCTNO BETWEEN 1590000000 AND 1799999999
# ============================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE DPBTRAN_DAILY AS
    SELECT 
        ACCTNO,
        TRANCODE,
        TRANAMT,
        CHQNO,
        CDNO,
        REPTDATE,
        DATE '{DATE}' AS DATE
    FROM read_parquet('{INPUT_FILE}')
    WHERE TRANCODE IN (297, 770, 970, 971, 972)
        AND REPTDATE = DATE '{DATE}'
        AND ACCTNO >= 1590000000 
        AND ACCTNO <= 1799999999
""")

TOTAL_RECORDS = con.execute("SELECT COUNT(*) FROM DPBTRAN_DAILY").fetchone()[0]
print(f"RECORDS EXTRACTED: {TOTAL_RECORDS}")

if TOTAL_RECORDS == 0:
    print("WARNING: NO RECORDS MATCH FILTER CRITERIA")

# ============================================================================
# SAVE OUTPUT
# ============================================================================
OUTPUT_DATA = con.execute("SELECT * FROM DPBTRAN_DAILY").arrow()
pq.write_table(OUTPUT_DATA, OUTPUT_PARQUET)
csv.write_csv(OUTPUT_DATA, OUTPUT_CSV)

print(f"\nSAVED: {OUTPUT_PARQUET}")
print(f"SAVED: {OUTPUT_CSV}")

# ============================================================================
# SUMMARY REPORT
# ============================================================================
print("\n" + "="*80)
print("PROCESSING SUMMARY")
print("="*80)
print(f"INPUT FILE       : {INPUT_FILENAME}")
print(f"OUTPUT FILE      : {OUTPUT_FILENAME}")
print(f"REPORT DATE      : {REPTDATE}")
print(f"WEEK NUMBER      : {WK}")
print(f"TOTAL RECORDS    : {TOTAL_RECORDS:,}")
print("="*80)

print("\nFILTER CRITERIA:")
print("  - TRANCODE IN (297, 770, 970, 971, 972)")
print(f"  - REPTDATE = {DATE}")
print("  - ACCTNO BETWEEN 1590000000 AND 1799999999")

# ============================================================================
# TRANSACTION ANALYSIS
# ============================================================================
if TOTAL_RECORDS > 0:
    print("\n" + "="*80)
    print("TRANSACTION ANALYSIS")
    print("="*80)
    
    # SUMMARY BY TRANSACTION CODE
    trancode_summary = con.execute("""
        SELECT 
            TRANCODE,
            COUNT(*) AS COUNT,
            SUM(TRANAMT) AS TOTAL_AMOUNT,
            AVG(TRANAMT) AS AVG_AMOUNT,
            MIN(TRANAMT) AS MIN_AMOUNT,
            MAX(TRANAMT) AS MAX_AMOUNT
        FROM DPBTRAN_DAILY
        GROUP BY TRANCODE
        ORDER BY TRANCODE
    """).df()
    
    print("\nBY TRANSACTION CODE:")
    print(trancode_summary.to_string(index=False))
    
    # ACCOUNT NUMBER RANGE DISTRIBUTION
    acct_dist = con.execute("""
        SELECT 
            CASE 
                WHEN ACCTNO >= 1590000000 AND ACCTNO < 1600000000 THEN '159X'
                WHEN ACCTNO >= 1600000000 AND ACCTNO < 1700000000 THEN '16XX'
                WHEN ACCTNO >= 1700000000 AND ACCTNO < 1800000000 THEN '17XX'
                ELSE 'OTHER'
            END AS ACCT_RANGE,
            COUNT(*) AS COUNT,
            SUM(TRANAMT) AS TOTAL_AMOUNT
        FROM DPBTRAN_DAILY
        GROUP BY ACCT_RANGE
        ORDER BY ACCT_RANGE
    """).df()
    
    print("\nBY ACCOUNT RANGE:")
    print(acct_dist.to_string(index=False))
    
    # SAMPLE RECORDS
    print("\nSAMPLE RECORDS (FIRST 10):")
    sample = con.execute("""
        SELECT ACCTNO, TRANCODE, TRANAMT, CHQNO, CDNO, REPTDATE
        FROM DPBTRAN_DAILY
        LIMIT 10
    """).df()
    print(sample.to_string(index=False))

# ============================================================================
# TRANSACTION CODE DEFINITIONS (FOR REFERENCE)
# ============================================================================
print("\n" + "="*80)
print("TRANSACTION CODE DEFINITIONS")
print("="*80)
print("TRANCODE 297 : (ADD DEFINITION)")
print("TRANCODE 770 : (ADD DEFINITION)")
print("TRANCODE 970 : (ADD DEFINITION)")
print("TRANCODE 971 : (ADD DEFINITION)")
print("TRANCODE 972 : (ADD DEFINITION)")
print("="*80)

print("\nACCOUNT NUMBER RANGE:")
print("  1590000000 - 1799999999")
print("  (SPECIFIC PRODUCT LINE OR ACCOUNT CATEGORY)")

# CLOSE CONNECTION
con.close()
print("\nPROCESSING COMPLETE!")
