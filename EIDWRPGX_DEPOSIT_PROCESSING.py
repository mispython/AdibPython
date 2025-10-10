#!/usr/bin/env python3
"""
EIDWRPGX - DEPOSIT DATA PROCESSING
1:1 CONVERSION FROM SAS TO PYTHON
PROCESSES DEPOSIT FILE INTO TWO STAGING DATASETS:
  - STG_DP_DPDARPGS: DEPOSIT ACCOUNT RECORDS (REPTNO=4001, FMTCODE 1,2)
  - STG_DP_DPDARPG2: AGENT CODE CHANGES (REPTNO=2030, FMTCODE 1, KEYWRD='AGCODE')
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
DEPOSIT_FILE = INPUT_DIR / "deposit.parquet"

# OUTPUT FILES
DPDARPGS_FILE = STG_DIR / "STG_DP_DPDARPGS.parquet"
DPDARPGS_CSV = STG_DIR / "STG_DP_DPDARPGS.csv"
DPDARPG2_FILE = STG_DIR / "STG_DP_DPDARPG2.parquet"
DPDARPG2_CSV = STG_DIR / "STG_DP_DPDARPG2.csv"

# CONNECT TO DUCKDB
con = duckdb.connect(":memory:")

print("PROCESSING EIDWRPGX - DEPOSIT DATA PROCESSING")

# ============================================================================
# DATA STG_DP_DPDARPGS - DEPOSIT ACCOUNT RECORDS
# FILTER: BANKNO = 33 AND REPTNO = 4001 AND FMTCODE IN (1,2)
# ============================================================================
print("\nPROCESSING STG_DP_DPDARPGS...")

con.execute(f"""
    CREATE OR REPLACE TABLE STG_DP_DPDARPGS AS
    SELECT 
        BANKNO,
        REPTNO,
        FMTCODE,
        BRANCH,
        ACCTNO,
        STATEC,
        PURPOSE,
        CUSTCD,
        DEPODTE,
        NAME,
        CDNO,
        OPENIND,
        CURBAL,
        ORIGAMT,
        ORGDATE,
        MATDATE,
        RATE,              -- CURRENT-INT-RATE
        ACCTTYPE,
        TERM,
        MATID,
        INTPLAN,
        PAYMENT,
        RENEWAL,
        INTPDYTD,          -- INT-PD-YTD
        INTPAY,            -- ACC-INT-CTD
        INTDATE,           -- LAST-INT-DT
        LASTACTV,
        INTFREQ,
        INTFREQID,
        PENDINT,
        CURCODE,
        LMATDATE,
        PRORATIO,          -- PROFIT RATIO
        FDHOLD,
        COSTCTR,
        COLLNO,
        INTTFRACCT,
        PRN_DISP_OPT,
        PRN_RENEW,
        PRN_TFR_ACCT
    FROM read_parquet('{DEPOSIT_FILE}')
    WHERE BANKNO = 33 
        AND REPTNO = 4001 
        AND FMTCODE IN (1, 2)
""")

DPDARPGS_COUNT = con.execute("SELECT COUNT(*) FROM STG_DP_DPDARPGS").fetchone()[0]
print(f"STG_DP_DPDARPGS RECORDS CREATED: {DPDARPGS_COUNT}")

# SAVE STG_DP_DPDARPGS
DPDARPGS_DATA = con.execute("SELECT * FROM STG_DP_DPDARPGS").arrow()
pq.write_table(DPDARPGS_DATA, DPDARPGS_FILE)
csv.write_csv(DPDARPGS_DATA, DPDARPGS_CSV)
print(f"SAVED: {DPDARPGS_FILE}")
print(f"SAVED: {DPDARPGS_CSV}")

# ============================================================================
# DATA STG_DP_DPDARPG2 - AGENT CODE CHANGES
# FILTER: REPTNO = 2030 AND FMTCODE = 1 AND KEYWRD = 'AGCODE  '
# ============================================================================
print("\nPROCESSING STG_DP_DPDARPG2...")

con.execute(f"""
    CREATE OR REPLACE TABLE STG_DP_DPDARPG2 AS
    SELECT 
        BANKNO,
        REPTNO,
        FMTCODE,
        BRANCH,
        ACCTNO,
        PREVCHG,
        NEWCHG,
        LASTCHG,
        USERID
    FROM read_parquet('{DEPOSIT_FILE}')
    WHERE REPTNO = 2030 
        AND FMTCODE = 1
        AND KEYWRD = 'AGCODE  '
""")

DPDARPG2_COUNT = con.execute("SELECT COUNT(*) FROM STG_DP_DPDARPG2").fetchone()[0]
print(f"STG_DP_DPDARPG2 RECORDS CREATED: {DPDARPG2_COUNT}")

# SAVE STG_DP_DPDARPG2
DPDARPG2_DATA = con.execute("SELECT * FROM STG_DP_DPDARPG2").arrow()
pq.write_table(DPDARPG2_DATA, DPDARPG2_FILE)
csv.write_csv(DPDARPG2_DATA, DPDARPG2_CSV)
print(f"SAVED: {DPDARPG2_FILE}")
print(f"SAVED: {DPDARPG2_CSV}")

# ============================================================================
# SUMMARY REPORT
# ============================================================================
print("\n" + "="*80)
print("PROCESSING SUMMARY")
print("="*80)
print(f"{'DATASET':<25} {'FILTER CRITERIA':<35} {'RECORDS':<10}")
print("-"*80)
print(f"{'STG_DP_DPDARPGS':<25} {'BANKNO=33, REPTNO=4001, FMT 1,2':<35} {DPDARPGS_COUNT:<10}")
print(f"{'STG_DP_DPDARPG2':<25} {'REPTNO=2030, FMT 1, AGCODE':<35} {DPDARPG2_COUNT:<10}")
print("="*80)

print("\n" + "="*80)
print("DATASET DESCRIPTIONS")
print("="*80)
print("\nSTG_DP_DPDARPGS (DEPOSIT ACCOUNTS):")
print("  - DEPOSIT ACCOUNT MASTER RECORDS")
print("  - INCLUDES: BALANCES, RATES, MATURITY DATES, INTEREST INFO")
print("  - KEY FIELDS: BRANCH, ACCTNO, CURBAL, RATE, MATDATE")
print("  - FILTER: BANKNO=33, REPTNO=4001, FMTCODE IN (1,2)")
print("\nSTG_DP_DPDARPG2 (AGENT CODE CHANGES):")
print("  - AGENT CODE CHANGE AUDIT TRAIL")
print("  - INCLUDES: PREVIOUS/NEW AGENT CODES, CHANGE DATE, USER")
print("  - KEY FIELDS: ACCTNO, PREVCHG, NEWCHG, LASTCHG, USERID")
print("  - FILTER: REPTNO=2030, FMTCODE=1, KEYWRD='AGCODE  '")
print("="*80)

# ============================================================================
# FIELD DEFINITIONS (FOR REFERENCE)
# ============================================================================
print("\n" + "="*80)
print("KEY FIELD DEFINITIONS - STG_DP_DPDARPGS")
print("="*80)
print("INTPDYTD  : INTEREST PAID YEAR-TO-DATE")
print("INTPAY    : ACCUMULATED INTEREST CURRENT-TO-DATE")
print("INTDATE   : LAST INTEREST DATE")
print("PRORATIO  : PROFIT RATIO")
print("RATE      : CURRENT INTEREST RATE")
print("FDHOLD    : FD HOLD FLAG")
print("MATID     : MATURITY INDICATOR")
print("="*80)

# DISPLAY SAMPLE RECORDS IF DATA EXISTS
if DPDARPGS_COUNT > 0:
    print("\nSAMPLE RECORDS - STG_DP_DPDARPGS (FIRST 5):")
    sample_dpdarpgs = con.execute("""
        SELECT BRANCH, ACCTNO, NAME, CURBAL, RATE, MATDATE, CURCODE
        FROM STG_DP_DPDARPGS
        LIMIT 5
    """).df()
    print(sample_dpdarpgs.to_string(index=False))

if DPDARPG2_COUNT > 0:
    print("\nSAMPLE RECORDS - STG_DP_DPDARPG2 (FIRST 5):")
    sample_dpdarpg2 = con.execute("""
        SELECT BRANCH, ACCTNO, PREVCHG, NEWCHG, LASTCHG, USERID
        FROM STG_DP_DPDARPG2
        LIMIT 5
    """).df()
    print(sample_dpdarpg2.to_string(index=False))

# CLOSE CONNECTION
con.close()
print("\nPROCESSING COMPLETE!
