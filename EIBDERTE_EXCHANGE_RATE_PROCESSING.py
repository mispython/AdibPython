#!/usr/bin/env python3
"""
EIBDERTE - EXCHANGE RATE PROCESSING
PROCESSES FOREIGN CURRENCY EXCHANGE RATES WITH TENURE-BASED ADJUSTMENTS
APPENDS TO HISTORICAL RATE DATA
"""

import duckdb
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from datetime import datetime

# INITIALIZE PATHS
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
RATE_DIR = OUTPUT_DIR / "rate"

# CREATE DIRECTORIES
RATE_DIR.mkdir(parents=True, exist_ok=True)

# INPUT FILES
ERATE_FILE = INPUT_DIR / "erate.parquet"

# OUTPUT FILES
ERTE_BASE_FILE = RATE_DIR / "ERTE.parquet"
EDATE_FILE = RATE_DIR / "EDATE.parquet"

# CONNECT TO DUCKDB
con = duckdb.connect(":memory:")

print("PROCESSING EIBDERTE - EXCHANGE RATE PROCESSING")

# ============================================================================
# DATA ERTE EDATE - READ AND PROCESS EXCHANGE RATE FILE
# ============================================================================

# READ ERATE FILE WITH POSITIONAL INPUT
con.execute(f"""
    CREATE OR REPLACE TABLE ERATE_RAW AS
    SELECT 
        FCIND,
        REIND,
        EFDATE,
        EYY,
        EMM,
        EDD,
        RATE1,
        RATE2,
        RATE3,
        CURCODE
    FROM read_parquet('{ERATE_FILE}')
    WHERE FCIND = 'FC'
""")

# CALCULATE EDTE (EFFECTIVE DATE) FROM YEAR, MONTH, DAY
con.execute("""
    CREATE OR REPLACE TABLE ERATE_PROCESSED AS
    SELECT 
        FCIND,
        REIND,
        EFDATE,
        EYY,
        EMM,
        EDD,
        RATE1,
        RATE2,
        RATE3,
        CURCODE,
        MAKE_DATE(EYY, EMM, EDD) AS EDTE
    FROM ERATE_RAW
""")

print(f"ERATE RECORDS READ: {con.execute('SELECT COUNT(*) FROM ERATE_PROCESSED').fetchone()[0]}")

# ============================================================================
# PROCESS REIND='01' (01-MONTH TENURE)
# ============================================================================
con.execute("""
    CREATE OR REPLACE TABLE ERTE_01 AS
    SELECT 
        EFDATE,
        EDTE,
        RATE1 + 1.0 AS RATE1,
        CASE 
            WHEN RATE2 = 0 THEN RATE1 + 1.0
            ELSE RATE2 + 1.0
        END AS RATE2,
        RATE3,
        CURCODE,
        '01-MONTH' AS TENURE
    FROM ERATE_PROCESSED
    WHERE REIND = '01'
""")

# OUTPUT EDATE FOR AUD CURRENCY ONLY
con.execute("""
    CREATE OR REPLACE TABLE EDATE_01 AS
    SELECT DISTINCT EDTE
    FROM ERTE_01
    WHERE CURCODE = 'AUD'
""")

RECORDS_01 = con.execute("SELECT COUNT(*) FROM ERTE_01").fetchone()[0]
print(f"REIND='01' (01-MONTH) RECORDS PROCESSED: {RECORDS_01}")

# ============================================================================
# PROCESS REIND='03' (03-MONTH TENURE)
# ============================================================================
con.execute("""
    CREATE OR REPLACE TABLE ERTE_03 AS
    SELECT 
        EFDATE,
        EDTE,
        RATE1 + 0.5 AS RATE1,
        CASE 
            WHEN RATE2 = 0 THEN RATE1 + 0.5
            ELSE RATE2 + 0.5
        END AS RATE2,
        RATE3,
        CURCODE,
        '03-MONTH' AS TENURE
    FROM ERATE_PROCESSED
    WHERE REIND = '03'
""")

RECORDS_03 = con.execute("SELECT COUNT(*) FROM ERTE_03").fetchone()[0]
print(f"REIND='03' (03-MONTH) RECORDS PROCESSED: {RECORDS_03}")

# ============================================================================
# PROCESS REIND='06' (06-MONTH TENURE)
# ============================================================================
con.execute("""
    CREATE OR REPLACE TABLE ERTE_06 AS
    SELECT 
        EFDATE,
        EDTE,
        RATE1 + 0.3 AS RATE1,
        CASE 
            WHEN RATE2 = 0 THEN RATE1 + 0.3
            ELSE RATE2 + 0.3
        END AS RATE2,
        RATE3,
        CURCODE,
        '06-MONTH' AS TENURE
    FROM ERATE_PROCESSED
    WHERE REIND = '06'
""")

RECORDS_06 = con.execute("SELECT COUNT(*) FROM ERTE_06").fetchone()[0]
print(f"REIND='06' (06-MONTH) RECORDS PROCESSED: {RECORDS_06}")

# ============================================================================
# PROCESS REIND='12' (12-MONTH TENURE)
# ============================================================================
con.execute("""
    CREATE OR REPLACE TABLE ERTE_12 AS
    SELECT 
        EFDATE,
        EDTE,
        RATE1 + 0.1 AS RATE1,
        CASE 
            WHEN RATE2 = 0 THEN RATE1 + 0.1
            ELSE RATE2 + 0.1
        END AS RATE2,
        RATE3,
        CURCODE,
        '12-MONTH' AS TENURE
    FROM ERATE_PROCESSED
    WHERE REIND = '12'
""")

RECORDS_12 = con.execute("SELECT COUNT(*) FROM ERTE_12").fetchone()[0]
print(f"REIND='12' (12-MONTH) RECORDS PROCESSED: {RECORDS_12}")

# ============================================================================
# COMBINE ALL ERTE RECORDS
# ============================================================================
con.execute("""
    CREATE OR REPLACE TABLE ERTE_NEW AS
    SELECT * FROM ERTE_01
    UNION ALL
    SELECT * FROM ERTE_03
    UNION ALL
    SELECT * FROM ERTE_06
    UNION ALL
    SELECT * FROM ERTE_12
""")

TOTAL_ERTE = con.execute("SELECT COUNT(*) FROM ERTE_NEW").fetchone()[0]
print(f"\nTOTAL ERTE RECORDS CREATED: {TOTAL_ERTE}")

# ============================================================================
# PROC APPEND - APPEND TO BASE ERTE FILE
# ============================================================================
print("\nAPPENDING TO RATE.ERTE...")

if ERTE_BASE_FILE.exists():
    # LOAD EXISTING BASE FILE
    con.execute(f"""
        CREATE OR REPLACE TABLE ERTE_BASE AS
        SELECT * FROM read_parquet('{ERTE_BASE_FILE}')
    """)
    
    # APPEND NEW RECORDS
    con.execute("""
        CREATE OR REPLACE TABLE ERTE_COMBINED AS
        SELECT * FROM ERTE_BASE
        UNION ALL
        SELECT * FROM ERTE_NEW
    """)
    
    BASE_RECORDS = con.execute("SELECT COUNT(*) FROM ERTE_BASE").fetchone()[0]
    print(f"EXISTING ERTE RECORDS: {BASE_RECORDS}")
    print(f"NEW RECORDS APPENDED: {TOTAL_ERTE}")
    
    # SAVE COMBINED DATA
    ERTE_COMBINED = con.execute("SELECT * FROM ERTE_COMBINED").arrow()
    pq.write_table(ERTE_COMBINED, ERTE_BASE_FILE)
    csv.write_csv(ERTE_COMBINED, RATE_DIR / "ERTE.csv")
    
    TOTAL_COMBINED = len(ERTE_COMBINED)
    print(f"TOTAL ERTE RECORDS: {TOTAL_COMBINED}")
else:
    # CREATE NEW BASE FILE
    print("NO EXISTING ERTE FILE - CREATING NEW")
    ERTE_DATA = con.execute("SELECT * FROM ERTE_NEW").arrow()
    pq.write_table(ERTE_DATA, ERTE_BASE_FILE)
    csv.write_csv(ERTE_DATA, RATE_DIR / "ERTE.csv")
    print(f"CREATED RATE.ERTE WITH {TOTAL_ERTE} RECORDS")

# ============================================================================
# PROC SORT - SORT EDATE AND REMOVE DUPLICATES
# ============================================================================
print("\nPROCESSING EDATE (AUD CURRENCY DATES)...")

# COMBINE ALL EDATE RECORDS (ONLY FROM REIND='01')
EDATE_NEW = con.execute("SELECT * FROM EDATE_01").arrow()
EDATE_NEW_COUNT = len(EDATE_NEW)

if EDATE_FILE.exists():
    # LOAD EXISTING EDATE FILE
    con.execute(f"""
        CREATE OR REPLACE TABLE EDATE_BASE AS
        SELECT EDTE FROM read_parquet('{EDATE_FILE}')
    """)
    
    # COMBINE WITH NEW DATES
    con.execute("""
        CREATE OR REPLACE TABLE EDATE_COMBINED AS
        SELECT EDTE FROM EDATE_BASE
        UNION
        SELECT EDTE FROM EDATE_01
    """)
    
    # SORT AND REMOVE DUPLICATES (NODUPKEY)
    con.execute("""
        CREATE OR REPLACE TABLE EDATE_FINAL AS
        SELECT DISTINCT EDTE
        FROM EDATE_COMBINED
        ORDER BY EDTE
    """)
    
    BASE_DATES = con.execute("SELECT COUNT(*) FROM EDATE_BASE").fetchone()[0]
    print(f"EXISTING EDATE RECORDS: {BASE_DATES}")
    print(f"NEW AUD DATES: {EDATE_NEW_COUNT}")
    
    # SAVE SORTED UNIQUE DATES
    EDATE_FINAL = con.execute("SELECT * FROM EDATE_FINAL").arrow()
    pq.write_table(EDATE_FINAL, EDATE_FILE)
    csv.write_csv(EDATE_FINAL, RATE_DIR / "EDATE.csv")
    
    TOTAL_DATES = len(EDATE_FINAL)
    print(f"TOTAL UNIQUE EDATE RECORDS: {TOTAL_DATES}")
else:
    # CREATE NEW EDATE FILE
    con.execute("""
        CREATE OR REPLACE TABLE EDATE_FINAL AS
        SELECT DISTINCT EDTE
        FROM EDATE_01
        ORDER BY EDTE
    """)
    
    EDATE_DATA = con.execute("SELECT * FROM EDATE_FINAL").arrow()
    pq.write_table(EDATE_DATA, EDATE_FILE)
    csv.write_csv(EDATE_DATA, RATE_DIR / "EDATE.csv")
    print(f"CREATED RATE.EDATE WITH {len(EDATE_DATA)} UNIQUE DATES")

# ============================================================================
# SUMMARY REPORT
# ============================================================================
print("\n" + "="*80)
print("PROCESSING SUMMARY")
print("="*80)
print(f"{'TENURE':<15} {'RATE ADJUSTMENT':<20} {'RECORDS':<10}")
print("-"*80)
print(f"{'01-MONTH':<15} {'RATE1 +1.0, RATE2 +1.0':<20} {RECORDS_01:<10}")
print(f"{'03-MONTH':<15} {'RATE1 +0.5, RATE2 +0.5':<20} {RECORDS_03:<10}")
print(f"{'06-MONTH':<15} {'RATE1 +0.3, RATE2 +0.3':<20} {RECORDS_06:<10}")
print(f"{'12-MONTH':<15} {'RATE1 +0.1, RATE2 +0.1':<20} {RECORDS_12:<10}")
print("-"*80)
print(f"{'TOTAL':<15} {'':<20} {TOTAL_ERTE:<10}")
print("="*80)

print("\nOUTPUT FILES:")
print(f"  - RATE.ERTE:  {ERTE_BASE_FILE}")
print(f"  - RATE.EDATE: {EDATE_FILE}")
print(f"\nEDATE CONTAINS UNIQUE DATES FOR AUD CURRENCY (REIND='01' ONLY)")

print("\nRATE ADJUSTMENT LOGIC:")
print("  - IF RATE2 = 0, THEN RATE2 = ADJUSTED RATE1")
print("  - OTHERWISE, RATE2 = RATE2 + ADJUSTMENT")
print("  - RATE3 REMAINS UNCHANGED")

# CLOSE CONNECTION
con.close()
print("\nPROCESSING COMPLETE!")
