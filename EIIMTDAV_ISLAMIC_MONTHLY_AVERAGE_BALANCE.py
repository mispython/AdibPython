#!/usr/bin/env python3
"""
EIIMTDAV - ISLAMIC MONTHLY AVERAGE BALANCE CALCULATION
PROCESSES DAILY ACCOUNT DATA (SAVING, UMA, FD, CURRENT) 
AND CALCULATES MONTH-TO-DATE AVERAGE BALANCES BY ACCOUNT
NOTE: ISLAMIC VERSION EXCLUDES VOSTRO ACCOUNTS FROM CURCA
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
DP_DIR = INPUT_DIR / "dp"
MIS_DIR = OUTPUT_DIR / "mis"

# CREATE DIRECTORIES
MIS_DIR.mkdir(parents=True, exist_ok=True)

# INPUT FILES
REPTDATE_FILE = DP_DIR / "REPTDATE.parquet"
SAVING_FILE = DP_DIR / "SAVING.parquet"
UMA_FILE = DP_DIR / "UMA.parquet"
FD_FILE = DP_DIR / "FD.parquet"
CURRENT_FILE = DP_DIR / "CURRENT.parquet"

# CONNECT TO DUCKDB
con = duckdb.connect(":memory:")

print("PROCESSING EIIMTDAV - ISLAMIC MONTHLY AVERAGE BALANCE CALCULATION")

# ============================================================================
# DATA REPTDATE - READ REPORT DATE AND CREATE MACRO VARIABLES
# ============================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE REPTDATE AS
    SELECT REPTDATE
    FROM read_parquet('{REPTDATE_FILE}')
""")

result = con.execute("SELECT REPTDATE FROM REPTDATE").fetchone()
REPTDATE = result[0]

# EXTRACT DATE COMPONENTS FOR MACRO VARIABLES
if isinstance(REPTDATE, (int, float)):
    # IF REPTDATE IS NUMERIC, CONVERT TO DATE
    REPTDATE = datetime.strptime(str(int(REPTDATE)), '%Y%m%d').date()
elif isinstance(REPTDATE, str):
    # IF STRING, PARSE IT
    REPTDATE = datetime.strptime(REPTDATE, '%Y-%m-%d').date()

REPTMON = str(REPTDATE.month).zfill(2)
REPTDAY = str(REPTDATE.day).zfill(2)
RDATE = REPTDATE

print(f"REPORT DATE: {REPTDATE}")
print(f"REPORT MONTH: {REPTMON}")
print(f"REPORT DAY: {REPTDAY}")

# ============================================================================
# DATA CURSA - COMBINE SAVING AND UMA DATASETS
# ============================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE CURSA AS
    SELECT 
        BRANCH,
        ACCTNO,
        CURBAL,
        OPENIND,
        PRODUCT,
        CURCODE,
        DATE '{RDATE}' AS REPTDATE
    FROM read_parquet('{SAVING_FILE}')
    
    UNION ALL
    
    SELECT 
        BRANCH,
        ACCTNO,
        CURBAL,
        OPENIND,
        PRODUCT,
        CURCODE,
        DATE '{RDATE}' AS REPTDATE
    FROM read_parquet('{UMA_FILE}')
""")

print(f"CURSA RECORDS CREATED: {con.execute('SELECT COUNT(*) FROM CURSA').fetchone()[0]}")

# ============================================================================
# DATA CURFD - PROCESS FD DATASET
# ============================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE CURFD AS
    SELECT 
        BRANCH,
        ACCTNO,
        CURBAL,
        FORBAL,
        OPENIND,
        PRODUCT,
        CURCODE,
        DATE '{RDATE}' AS REPTDATE
    FROM read_parquet('{FD_FILE}')
""")

print(f"CURFD RECORDS CREATED: {con.execute('SELECT COUNT(*) FROM CURFD').fetchone()[0]}")

# ============================================================================
# DATA CURCA - PROCESS CURRENT DATASET ONLY (NO VOSTRO FOR ISLAMIC)
# ============================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE CURCA AS
    SELECT 
        BRANCH,
        ACCTNO,
        CURBAL,
        FORBAL,
        OPENIND,
        PRODUCT,
        CURCODE,
        DATE '{RDATE}' AS REPTDATE
    FROM read_parquet('{CURRENT_FILE}')
""")

print(f"CURCA RECORDS CREATED: {con.execute('SELECT COUNT(*) FROM CURCA').fetchone()[0]}")

# ============================================================================
# MACRO APPEND - APPEND OR REPLACE MONTHLY DATA
# LOGIC: IF DAY 1 OF MONTH, CREATE NEW FILE; OTHERWISE APPEND TO EXISTING
# ============================================================================
print(f"\nPROCESSING MONTHLY APPEND LOGIC FOR DAY {REPTDAY}...")

SA_FILE = MIS_DIR / f"SA{REPTMON}.parquet"
FD_FILE_OUT = MIS_DIR / f"FD{REPTMON}.parquet"
CA_FILE = MIS_DIR / f"CA{REPTMON}.parquet"

if REPTDAY == "01":
    # FIRST DAY OF MONTH - CREATE NEW MONTHLY FILES
    print("DAY 01 DETECTED - CREATING NEW MONTHLY FILES")
    
    # MIS.SA{REPTMON}
    SA_DATA = con.execute("SELECT * FROM CURSA").arrow()
    pq.write_table(SA_DATA, SA_FILE)
    
    # MIS.FD{REPTMON}
    FD_DATA = con.execute("SELECT * FROM CURFD").arrow()
    pq.write_table(FD_DATA, FD_FILE_OUT)
    
    # MIS.CA{REPTMON}
    CA_DATA = con.execute("SELECT * FROM CURCA").arrow()
    pq.write_table(CA_DATA, CA_FILE)
    
else:
    # NOT FIRST DAY - APPEND TO EXISTING MONTHLY FILES AFTER DELETING CURRENT DATE RECORDS
    print(f"DAY {REPTDAY} DETECTED - APPENDING TO EXISTING MONTHLY FILES")
    
    # PROCESS SA
    if SA_FILE.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE MIS_SA_EXISTING AS
            SELECT *
            FROM read_parquet('{SA_FILE}')
            WHERE REPTDATE != DATE '{RDATE}'
        """)
        
        con.execute("""
            CREATE OR REPLACE TABLE MIS_SA_COMBINED AS
            SELECT * FROM CURSA
            UNION ALL
            SELECT * FROM MIS_SA_EXISTING
        """)
        
        SA_DATA = con.execute("SELECT * FROM MIS_SA_COMBINED").arrow()
        pq.write_table(SA_DATA, SA_FILE)
    else:
        SA_DATA = con.execute("SELECT * FROM CURSA").arrow()
        pq.write_table(SA_DATA, SA_FILE)
    
    # PROCESS FD
    if FD_FILE_OUT.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE MIS_FD_EXISTING AS
            SELECT *
            FROM read_parquet('{FD_FILE_OUT}')
            WHERE REPTDATE != DATE '{RDATE}'
        """)
        
        con.execute("""
            CREATE OR REPLACE TABLE MIS_FD_COMBINED AS
            SELECT * FROM CURFD
            UNION ALL
            SELECT * FROM MIS_FD_EXISTING
        """)
        
        FD_DATA = con.execute("SELECT * FROM MIS_FD_COMBINED").arrow()
        pq.write_table(FD_DATA, FD_FILE_OUT)
    else:
        FD_DATA = con.execute("SELECT * FROM CURFD").arrow()
        pq.write_table(FD_DATA, FD_FILE_OUT)
    
    # PROCESS CA
    if CA_FILE.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE MIS_CA_EXISTING AS
            SELECT *
            FROM read_parquet('{CA_FILE}')
            WHERE REPTDATE != DATE '{RDATE}'
        """)
        
        con.execute("""
            CREATE OR REPLACE TABLE MIS_CA_COMBINED AS
            SELECT * FROM CURCA
            UNION ALL
            SELECT * FROM MIS_CA_EXISTING
        """)
        
        CA_DATA = con.execute("SELECT * FROM MIS_CA_COMBINED").arrow()
        pq.write_table(CA_DATA, CA_FILE)
    else:
        CA_DATA = con.execute("SELECT * FROM CURCA").arrow()
        pq.write_table(CA_DATA, CA_FILE)

print(f"MONTHLY FILES UPDATED: SA{REPTMON}, FD{REPTMON}, CA{REPTMON}")

# ============================================================================
# PROC SORT AND PROC SUMMARY FOR SA - CALCULATE MTD AVERAGE BALANCES
# ============================================================================
print(f"\nCALCULATING SAVING/UMA AVERAGE BALANCES...")

con.execute(f"""
    CREATE OR REPLACE TABLE CURSA_SORTED AS
    SELECT *
    FROM read_parquet('{SA_FILE}')
    ORDER BY ACCTNO
""")

con.execute(f"""
    CREATE OR REPLACE TABLE SAVG_SUMMARY AS
    SELECT 
        ACCTNO,
        SUM(CURBAL) AS MTDAVBAL_MIS
    FROM CURSA_SORTED
    WHERE REPTDATE <= DATE '{RDATE}'
    GROUP BY ACCTNO
    ORDER BY ACCTNO
""")

# CALCULATE AVERAGE BY DIVIDING BY NUMBER OF DAYS
con.execute(f"""
    CREATE OR REPLACE TABLE MIS_SAVG AS
    SELECT 
        ACCTNO,
        MTDAVBAL_MIS / {int(REPTDAY)} AS MTDAVBAL_MIS
    FROM SAVG_SUMMARY
""")

# SAVE MIS.SAVG{REPTMON}
SAVG_DATA = con.execute("SELECT * FROM MIS_SAVG").arrow()
SAVG_FILE = MIS_DIR / f"SAVG{REPTMON}.parquet"
pq.write_table(SAVG_DATA, SAVG_FILE)
csv.write_csv(SAVG_DATA, MIS_DIR / f"SAVG{REPTMON}.csv")
print(f"SAVED: MIS.SAVG{REPTMON} ({len(SAVG_DATA)} ACCOUNTS)")

# ============================================================================
# PROC SORT AND PROC SUMMARY FOR FD - CALCULATE MTD AVERAGE BALANCES
# ============================================================================
print(f"CALCULATING FD AVERAGE BALANCES...")

con.execute(f"""
    CREATE OR REPLACE TABLE CURFD_SORTED AS
    SELECT *
    FROM read_parquet('{FD_FILE_OUT}')
    ORDER BY ACCTNO
""")

con.execute(f"""
    CREATE OR REPLACE TABLE FAVG_SUMMARY AS
    SELECT 
        ACCTNO,
        SUM(CURBAL) AS MTDAVBAL_MIS,
        SUM(FORBAL) AS MTDAVFORBAL_MIS
    FROM CURFD_SORTED
    WHERE REPTDATE <= DATE '{RDATE}'
    GROUP BY ACCTNO
    ORDER BY ACCTNO
""")

# CALCULATE AVERAGE BY DIVIDING BY NUMBER OF DAYS
con.execute(f"""
    CREATE OR REPLACE TABLE MIS_FAVG AS
    SELECT 
        ACCTNO,
        MTDAVBAL_MIS / {int(REPTDAY)} AS MTDAVBAL_MIS,
        MTDAVFORBAL_MIS / {int(REPTDAY)} AS MTDAVFORBAL_MIS
    FROM FAVG_SUMMARY
""")

# SAVE MIS.FAVG{REPTMON}
FAVG_DATA = con.execute("SELECT * FROM MIS_FAVG").arrow()
FAVG_FILE = MIS_DIR / f"FAVG{REPTMON}.parquet"
pq.write_table(FAVG_DATA, FAVG_FILE)
csv.write_csv(FAVG_DATA, MIS_DIR / f"FAVG{REPTMON}.csv")
print(f"SAVED: MIS.FAVG{REPTMON} ({len(FAVG_DATA)} ACCOUNTS)")

# ============================================================================
# PROC SORT AND PROC SUMMARY FOR CA - CALCULATE MTD AVERAGE BALANCES
# ============================================================================
print(f"CALCULATING CURRENT AVERAGE BALANCES...")

con.execute(f"""
    CREATE OR REPLACE TABLE CURCA_SORTED AS
    SELECT *
    FROM read_parquet('{CA_FILE}')
    ORDER BY ACCTNO
""")

con.execute(f"""
    CREATE OR REPLACE TABLE CAVG_SUMMARY AS
    SELECT 
        ACCTNO,
        SUM(CURBAL) AS MTDAVBAL_MIS,
        SUM(FORBAL) AS MTDAVFORBAL_MIS
    FROM CURCA_SORTED
    WHERE REPTDATE <= DATE '{RDATE}'
    GROUP BY ACCTNO
    ORDER BY ACCTNO
""")

# CALCULATE AVERAGE BY DIVIDING BY NUMBER OF DAYS
con.execute(f"""
    CREATE OR REPLACE TABLE MIS_CAVG AS
    SELECT 
        ACCTNO,
        MTDAVBAL_MIS / {int(REPTDAY)} AS MTDAVBAL_MIS,
        MTDAVFORBAL_MIS / {int(REPTDAY)} AS MTDAVFORBAL_MIS
    FROM CAVG_SUMMARY
""")

# SAVE MIS.CAVG{REPTMON}
CAVG_DATA = con.execute("SELECT * FROM MIS_CAVG").arrow()
CAVG_FILE = MIS_DIR / f"CAVG{REPTMON}.parquet"
pq.write_table(CAVG_DATA, CAVG_FILE)
csv.write_csv(CAVG_DATA, MIS_DIR / f"CAVG{REPTMON}.csv")
print(f"SAVED: MIS.CAVG{REPTMON} ({len(CAVG_DATA)} ACCOUNTS)")

# ============================================================================
# SUMMARY REPORT
# ============================================================================
print("\n" + "="*80)
print("PROCESSING SUMMARY")
print("="*80)
print(f"REPORT DATE: {RDATE}")
print(f"REPORT MONTH: {REPTMON}")
print(f"REPORT DAY: {REPTDAY}")
print(f"DAYS IN CALCULATION: {REPTDAY}")
print("="*80)
print(f"{'DATASET':<20} {'ACCOUNTS':<15} {'OUTPUT FILE':<30}")
print("-"*80)
print(f"{'SAVING/UMA (SA)':<20} {len(SAVG_DATA):<15} {'SAVG{}.parquet'.format(REPTMON):<30}")
print(f"{'FIXED DEPOSIT (FD)':<20} {len(FAVG_DATA):<15} {'FAVG{}.parquet'.format(REPTMON):<30}")
print(f"{'CURRENT (CA)':<20} {len(CAVG_DATA):<15} {'CAVG{}.parquet'.format(REPTMON):<30}")
print("="*80)

print("\nMONTHLY ACCUMULATION FILES:")
print(f"  - MIS.SA{REPTMON}.parquet (DAILY RECORDS)")
print(f"  - MIS.FD{REPTMON}.parquet (DAILY RECORDS)")
print(f"  - MIS.CA{REPTMON}.parquet (DAILY RECORDS)")

print("\nAVERAGE BALANCE FILES:")
print(f"  - MIS.SAVG{REPTMON}.parquet (ACCOUNT AVERAGES)")
print(f"  - MIS.FAVG{REPTMON}.parquet (ACCOUNT AVERAGES)")
print(f"  - MIS.CAVG{REPTMON}.parquet (ACCOUNT AVERAGES)")

print("\nKEY DIFFERENCE FROM EIDMTDAV:")
print("  - CURCA INCLUDES ONLY CURRENT ACCOUNTS (NO VOSTRO)")
print("  - THIS IS THE ISLAMIC BANKING VERSION")

# CLOSE CONNECTION
con.close()
print("\nPROCESSING COMPLETE!
