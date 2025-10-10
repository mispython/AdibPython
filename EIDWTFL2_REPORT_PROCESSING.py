#!/usr/bin/env python3
"""
EIDWTFL2 - REPORT FILE PROCESSING
1:1 CONVERSION FROM SAS TO PYTHON
PROCESSES DPRPTF FILE INTO STG_DP_DPRPTFL2 STAGING DATASET
EXTRACTS TRANSACTION SUMMARY DATA (REPTNO=210, FMTCODE=1)
"""

import duckdb
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv

# INITIALIZE PATHS
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
STG_DIR = OUTPUT_DIR / "stg"

# CREATE DIRECTORIES
STG_DIR.mkdir(parents=True, exist_ok=True)

# INPUT FILES
DPRPTF_FILE = INPUT_DIR / "dprptf.parquet"

# OUTPUT FILES
OUTPUT_FILE = STG_DIR / "STG_DP_DPRPTFL2.parquet"
OUTPUT_CSV = STG_DIR / "STG_DP_DPRPTFL2.csv"

# CONNECT TO DUCKDB
con = duckdb.connect(":memory:")

print("PROCESSING EIDWFTL2 - REPORT FILE PROCESSING")

# ============================================================================
# DATA STG_DP_DPRPTFL2 - TRANSACTION SUMMARY REPORT
# FILTER: REPTNO = 210 AND FMTCODE = 1
# ============================================================================
print("\nPROCESSING STG_DP_DPRPTFL2...")

con.execute(f"""
    CREATE OR REPLACE TABLE STG_DP_DPRPTFL2 AS
    SELECT 
        REPTNO,
        FMTCODE,
        BRANCH,
        AMOUNT,
        TRANCODE,
        CASHIN,
        CASHOUT,
        CHECKIN,
        TRANNAME
    FROM read_parquet('{DPRPTF_FILE}')
    WHERE REPTNO = 210 
        AND FMTCODE = 1
""")

TOTAL_RECORDS = con.execute("SELECT COUNT(*) FROM STG_DP_DPRPTFL2").fetchone()[0]
print(f"STG_DP_DPRPTFL2 RECORDS CREATED: {TOTAL_RECORDS}")

# SAVE STG_DP_DPRPTFL2
OUTPUT_DATA = con.execute("SELECT * FROM STG_DP_DPRPTFL2").arrow()
pq.write_table(OUTPUT_DATA, OUTPUT_FILE)
csv.write_csv(OUTPUT_DATA, OUTPUT_CSV)
print(f"SAVED: {OUTPUT_FILE}")
print(f"SAVED: {OUTPUT_CSV}")

# ============================================================================
# SUMMARY REPORT
# ============================================================================
print("\n" + "="*80)
print("PROCESSING SUMMARY")
print("="*80)
print(f"TOTAL RECORDS PROCESSED: {TOTAL_RECORDS}")
print(f"FILTER CRITERIA: REPTNO=210, FMTCODE=1")
print("="*80)

# ============================================================================
# AGGREGATE STATISTICS
# ============================================================================
if TOTAL_RECORDS > 0:
    print("\n" + "="*80)
    print("TRANSACTION SUMMARY STATISTICS")
    print("="*80)
    
    summary = con.execute("""
        SELECT 
            COUNT(DISTINCT BRANCH) AS TOTAL_BRANCHES,
            COUNT(DISTINCT TRANCODE) AS TOTAL_TRANCODES,
            SUM(AMOUNT) AS TOTAL_AMOUNT,
            SUM(CASHIN) AS TOTAL_CASHIN,
            SUM(CASHOUT) AS TOTAL_CASHOUT,
            SUM(CHECKIN) AS TOTAL_CHECKIN
        FROM STG_DP_DPRPTFL2
    """).fetchone()
    
    print(f"TOTAL BRANCHES       : {summary[0]:,}")
    print(f"TOTAL TRANSACTION CODES: {summary[1]:,}")
    print(f"TOTAL AMOUNT         : {summary[2]:,.2f}")
    print(f"TOTAL CASH IN        : {summary[3]:,.2f}")
    print(f"TOTAL CASH OUT       : {summary[4]:,.2f}")
    print(f"TOTAL CHECK IN       : {summary[5]:,.2f}")
    print("="*80)
    
    # BREAKDOWN BY BRANCH
    print("\nTOP 10 BRANCHES BY TOTAL AMOUNT:")
    branch_summary = con.execute("""
        SELECT 
            BRANCH,
            COUNT(*) AS TRANS_COUNT,
            SUM(AMOUNT) AS TOTAL_AMOUNT,
            SUM(CASHIN) AS TOTAL_CASHIN,
            SUM(CASHOUT) AS TOTAL_CASHOUT,
            SUM(CHECKIN) AS TOTAL_CHECKIN
        FROM STG_DP_DPRPTFL2
        GROUP BY BRANCH
        ORDER BY TOTAL_AMOUNT DESC
        LIMIT 10
    """).df()
    print(branch_summary.to_string(index=False))
    
    # BREAKDOWN BY TRANSACTION CODE
    print("\nTOP 10 TRANSACTION CODES BY COUNT:")
    trancode_summary = con.execute("""
        SELECT 
            TRANCODE,
            TRANNAME,
            COUNT(*) AS TRANS_COUNT,
            SUM(AMOUNT) AS TOTAL_AMOUNT
        FROM STG_DP_DPRPTFL2
        GROUP BY TRANCODE, TRANNAME
        ORDER BY TRANS_COUNT DESC
        LIMIT 10
    """).df()
    print(trancode_summary.to_string(index=False))

# ============================================================================
# FIELD DEFINITIONS
# ============================================================================
print("\n" + "="*80)
print("FIELD DEFINITIONS")
print("="*80)
print("REPTNO    : REPORT NUMBER (210 = TRANSACTION SUMMARY)")
print("FMTCODE   : FORMAT CODE (1 = STANDARD FORMAT)")
print("BRANCH    : BRANCH CODE")
print("AMOUNT    : TOTAL TRANSACTION AMOUNT")
print("TRANCODE  : TRANSACTION CODE (4 CHARACTERS)")
print("CASHIN    : CASH RECEIVED AMOUNT")
print("CASHOUT   : CASH DISBURSED AMOUNT")
print("CHECKIN   : CHECK RECEIVED AMOUNT")
print("TRANNAME  : TRANSACTION NAME/DESCRIPTION (20 CHARACTERS)")
print("="*80)

# ============================================================================
# SAMPLE RECORDS
# ============================================================================
if TOTAL_RECORDS > 0:
    print("\nSAMPLE RECORDS (FIRST 10):")
    sample = con.execute("""
        SELECT BRANCH, TRANCODE, TRANNAME, AMOUNT, CASHIN, CASHOUT, CHECKIN
        FROM STG_DP_DPRPTFL2
        LIMIT 10
    """).df()
    print(sample.to_string(index=False))

# CLOSE CONNECTION
con.close()
print("\nPROCESSING COMPLETE!")
