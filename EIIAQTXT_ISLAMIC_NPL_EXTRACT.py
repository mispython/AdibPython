#!/usr/bin/env python3
"""
EIIAQTXT - AQ NPL EXTRACT TO TEXT FILE
1:1 CONVERSION FROM SAS TO PYTHON
EXTRACTS HPD LOAN NPL DATA FROM AQ, SP2, AND IIS FILES
GENERATES FIXED-WIDTH TEXT FILE FOR REPORTING
"""

import duckdb
from pathlib import Path
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import sys

# INITIALIZE PATHS
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
LOAN_DIR = INPUT_DIR / "loan"
NPL6_DIR = INPUT_DIR / "npl6"

# CREATE DIRECTORIES
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# CONNECT TO DUCKDB
con = duckdb.connect(":memory:")

print("="*80)
print("EIIAQTXT - AQ NPL EXTRACT")
print("="*80)

# ============================================================================
# READ LOAN REPTDATE
# ============================================================================
LOAN_REPTDATE_FILE = LOAN_DIR / "REPTDATE.parquet"

if not LOAN_REPTDATE_FILE.exists():
    print(f"ERROR: LOAN REPTDATE FILE NOT FOUND: {LOAN_REPTDATE_FILE}")
    sys.exit(1)

result = con.execute(f"""
    SELECT REPTDATE 
    FROM read_parquet('{LOAN_REPTDATE_FILE}')
    LIMIT 1
""").fetchone()

REPTDATE = result[0]

if isinstance(REPTDATE, (int, float)):
    REPTDATE = datetime.strptime(str(int(REPTDATE)), '%Y%m%d').date()
elif isinstance(REPTDATE, str):
    REPTDATE = datetime.strptime(REPTDATE, '%Y-%m-%d').date()

# EXTRACT DATE COMPONENTS
MM = str(REPTDATE.month).zfill(2)
YY = str(REPTDATE.year)
DD = str(REPTDATE.day).zfill(2)
RDATE = REPTDATE.strftime('%d%m%Y')

print(f"\nLOAN REPORT DATE: {REPTDATE}")
print(f"RDATE: {RDATE}")

# ============================================================================
# READ NPL6 REPTDATE
# ============================================================================
NPL6_REPTDATE_FILE = NPL6_DIR / "REPTDATE.parquet"

if not NPL6_REPTDATE_FILE.exists():
    print(f"ERROR: NPL6 REPTDATE FILE NOT FOUND: {NPL6_REPTDATE_FILE}")
    sys.exit(1)

result = con.execute(f"""
    SELECT REPTDATE 
    FROM read_parquet('{NPL6_REPTDATE_FILE}')
    LIMIT 1
""").fetchone()

NPLDATE_DT = result[0]

if isinstance(NPLDATE_DT, (int, float)):
    NPLDATE_DT = datetime.strptime(str(int(NPLDATE_DT)), '%Y%m%d').date()
elif isinstance(NPLDATE_DT, str):
    NPLDATE_DT = datetime.strptime(NPLDATE_DT, '%Y-%m-%d').date()

NPLDATE = NPLDATE_DT.strftime('%d%m%Y')

print(f"NPL6 REPORT DATE: {NPLDATE_DT}")
print(f"NPLDATE: {NPLDATE}")

# ============================================================================
# VALIDATE DATES MATCH
# ============================================================================
print("\n" + "="*80)
print("VALIDATING DATES")
print("="*80)

if NPLDATE != RDATE:
    print(f"✗ VALIDATION FAILED!")
    print(f"   NPL DATE: {NPLDATE}")
    print(f"   EXPECTED DATE: {RDATE}")
    print(f"\nSAP.PIBB.NPL.HP.SASDATA IS NOT DATED {RDATE}")
    print("ABORTING WITH EXIT CODE 77")
    sys.exit(77)

print(f"✓ VALIDATION PASSED: NPL DATE ({NPLDATE}) MATCHES LOAN DATE ({RDATE})")

# ============================================================================
# EXTRACT FROM AQ, SP2 & IIS
# ============================================================================
print("\nPROCESSING NPL DATA...")

AQ_FILE = NPL6_DIR / "AQ.parquet"
SP2_FILE = NPL6_DIR / "SP2.parquet"
IIS_FILE = NPL6_DIR / "IIS.parquet"

# SORT AND EXTRACT AQ
con.execute(f"""
    CREATE OR REPLACE TABLE AQ AS
    SELECT *
    FROM read_parquet('{AQ_FILE}')
    ORDER BY ACCTNO, NOTENO
""")

# SORT AND EXTRACT SP2 (KEEP SPECIFIC COLUMNS)
con.execute(f"""
    CREATE OR REPLACE TABLE SP2 AS
    SELECT 
        ACCTNO,
        NOTENO,
        SP,
        SPPL,
        MARKETVL
    FROM read_parquet('{SP2_FILE}')
    ORDER BY ACCTNO, NOTENO
""")

# SORT AND EXTRACT IIS (KEEP SPECIFIC COLUMNS)
con.execute(f"""
    CREATE OR REPLACE TABLE IIS AS
    SELECT 
        ACCTNO,
        NOTENO,
        TOTIIS
    FROM read_parquet('{IIS_FILE}')
    ORDER BY ACCTNO, NOTENO
""")

# ============================================================================
# MERGE AND PROCESS AQ DATA
# ============================================================================
print("MERGING AQ, SP2, AND IIS DATA...")

con.execute(f"""
    CREATE OR REPLACE TABLE AQ_MERGED AS
    SELECT 
        a.*,
        s.SP,
        s.SPPL,
        s.MARKETVL,
        i.TOTIIS,
        CASE 
            WHEN SUBSTR(a.RISK, 1, 1) = 'S' THEN
                CASE 
                    WHEN a.RISK = 'SUBSTANDARD-1' THEN 'S1'
                    ELSE 'S2'
                END
            ELSE SUBSTR(a.RISK, 1, 1)
        END AS RISKCD,
        s.SP AS SPAMT,
        s.SPPL AS SPPLAMT,
        CASE 
            WHEN SUBSTR(a.LOANTYP, 1, 9) = 'HPD AITAB' THEN 'AITAB'
            ELSE 'HPD'
        END AS LOANDESC,
        DATE '{REPTDATE}' AS RPTDATE
    FROM AQ a
    LEFT JOIN SP2 s ON a.ACCTNO = s.ACCTNO AND a.NOTENO = s.NOTENO
    LEFT JOIN IIS i ON a.ACCTNO = i.ACCTNO AND a.NOTENO = i.NOTENO
    WHERE SUBSTR(a.LOANTYP, 1, 3) = 'HPD'
    ORDER BY a.BRANCH, LOANDESC, RISKCD, RPTDATE
""")

MERGED_COUNT = con.execute("SELECT COUNT(*) FROM AQ_MERGED").fetchone()[0]
print(f"AQ MERGED RECORDS: {MERGED_COUNT:,}")

# ============================================================================
# AGGREGATE BY BRANCH, LOANDESC, RISKCD, RPTDATE
# ============================================================================
print("AGGREGATING DATA...")

con.execute("""
    CREATE OR REPLACE TABLE AQ_SUMMARY AS
    SELECT 
        BRANCH,
        LOANDESC,
        RISKCD,
        RPTDATE,
        COUNT(*) AS NOACCT,
        SUM(NETBALP) AS NETBALP,
        SUM(NEWNPL) AS NEWNPL,
        SUM(RECOVER) AS RECOVER,
        SUM(PL) AS PL,
        SUM(NPLW) AS NPLW,
        SUM(NPL) AS NPL,
        SUM(TOTIIS) AS TOTIIS,
        SUM(SPAMT) AS SPAMT,
        SUM(SPPLAMT) AS SPPLAMT,
        SUM(MARKETVL) AS MARKETVL,
        SUM(ADJUST) AS ADJUST
    FROM AQ_MERGED
    GROUP BY BRANCH, LOANDESC, RISKCD, RPTDATE
    ORDER BY BRANCH, LOANDESC, RISKCD, RPTDATE
""")

SUMMARY_COUNT = con.execute("SELECT COUNT(*) FROM AQ_SUMMARY").fetchone()[0]
print(f"AQ SUMMARY RECORDS: {SUMMARY_COUNT:,}")

# ============================================================================
# GENERATE FIXED-WIDTH TEXT FILE
# ============================================================================
print("\nGENERATING TEXT FILE...")

AQTXT_FILE = OUTPUT_DIR / f"AQTXT_{YY}{MM}{DD}.txt"

# GET DATA
AQ_DATA = con.execute("""
    SELECT 
        RPTDATE,
        BRANCH,
        LOANDESC,
        RISKCD,
        NETBALP,
        NEWNPL,
        RECOVER,
        PL,
        NPLW,
        NPL,
        TOTIIS,
        SPAMT,
        SPPLAMT,
        MARKETVL,
        ADJUST
    FROM AQ_SUMMARY
    ORDER BY BRANCH, LOANDESC, RISKCD, RPTDATE
""").fetchall()

# WRITE FIXED-WIDTH FILE
with open(AQTXT_FILE, 'w') as f:
    for row in AQ_DATA:
        (RPTDATE, BRANCH, LOANDESC, RISKCD, NETBALP, NEWNPL, RECOVER, 
         PL, NPLW, NPL, TOTIIS, SPAMT, SPPLAMT, MARKETVL, ADJUST) = row
        
        # FORMAT DATE AS DD/MM/YYYY (DDMMYY10)
        DATE_STR = RPTDATE.strftime('%d/%m/%Y')
        
        # FORMAT NUMBERS AS 13.2 (11 digits + decimal + 2 decimals)
        def fmt_num(val):
            if val is None:
                val = 0.0
            return f"{val:13.2f}"
        
        # WRITE LINE WITH FIXED POSITIONS
        line = (
            f"{DATE_STR:>12}"                    # @001-012 RPTDATE
            f" {BRANCH or '':>7}"                # @014-020 BRANCH
            f"  {LOANDESC or '':>5}"             # @023-027 LOANDESC
            f" {RISKCD or '':>2}"                # @029-030 RISKCD
            f"{fmt_num(NETBALP)}"                # @031-043 NETBALP
            f"{fmt_num(NEWNPL)}"                 # @044-056 NEWNPL
            f"{fmt_num(RECOVER)}"                # @057-069 RECOVER
            f"{fmt_num(PL)}"                     # @070-082 PL
            f"{fmt_num(NPLW)}"                   # @083-095 NPLW
            f"{fmt_num(NPL)}"                    # @096-108 NPL
            f"{fmt_num(TOTIIS)}"                 # @109-121 TOTIIS
            f"{fmt_num(SPAMT)}"                  # @122-134 SPAMT
            f"{fmt_num(SPPLAMT)}"                # @135-147 SPPLAMT
            f"{fmt_num(MARKETVL)}"               # @148-160 MARKETVL
            f"{fmt_num(ADJUST)}\n"               # @161-173 ADJUST
        )
        f.write(line)

print(f"SAVED: {AQTXT_FILE}")

# ============================================================================
# SUMMARY REPORT
# ============================================================================
print("\n" + "="*80)
print("PROCESSING SUMMARY")
print("="*80)
print(f"REPORT DATE: {REPTDATE}")
print(f"TOTAL SUMMARY RECORDS: {SUMMARY_COUNT:,}")
print("="*80)

if SUMMARY_COUNT > 0:
    print("\nBREAKDOWN BY LOAN TYPE:")
    loandesc_summary = con.execute("""
        SELECT 
            LOANDESC,
            COUNT(*) AS COUNT,
            SUM(NOACCT) AS TOTAL_ACCOUNTS,
            SUM(NPL) AS TOTAL_NPL,
            SUM(TOTIIS) AS TOTAL_IIS
        FROM AQ_SUMMARY
        GROUP BY LOANDESC
        ORDER BY LOANDESC
    """).df()
    print(loandesc_summary.to_string(index=False))
    
    print("\nBREAKDOWN BY RISK CODE:")
    risk_summary = con.execute("""
        SELECT 
            RISKCD,
            COUNT(*) AS COUNT,
            SUM(NOACCT) AS TOTAL_ACCOUNTS,
            SUM(NPL) AS TOTAL_NPL
        FROM AQ_SUMMARY
        GROUP BY RISKCD
        ORDER BY RISKCD
    """).df()
    print(risk_summary.to_string(index=False))

# ============================================================================
# FILE FORMAT DOCUMENTATION
# ============================================================================
print("\n" + "="*80)
print("TEXT FILE FORMAT")
print("="*80)
print("\nFIXED-WIDTH POSITIONS:")
print("  @001-012: RPTDATE (DD/MM/YYYY)")
print("  @014-020: BRANCH (7 chars)")
print("  @023-027: LOANDESC (5 chars - AITAB or HPD)")
print("  @029-030: RISKCD (2 chars - D, L, S1, S2)")
print("  @031-043: NETBALP (13.2 format)")
print("  @044-056: NEWNPL (13.2 format)")
print("  @057-069: RECOVER (13.2 format)")
print("  @070-082: PL (13.2 format)")
print("  @083-095: NPLW (13.2 format)")
print("  @096-108: NPL (13.2 format)")
print("  @109-121: TOTIIS (13.2 format)")
print("  @122-134: SPAMT (13.2 format)")
print("  @135-147: SPPLAMT (13.2 format)")
print("  @148-160: MARKETVL (13.2 format)")
print("  @161-173: ADJUST (13.2 format)")

print("\nRISK CODES:")
print("  D:  Doubtful")
print("  L:  Loss")
print("  S1: Substandard-1")
print("  S2: Substandard-2 (or other substandard)")

print("\nLOAN TYPES:")
print("  HPD:   HP Direct Conventional")
print("  AITAB: HP Direct AITAB")
print("="*80)

# CLOSE CONNECTION
con.close()
print("\nPROCESSING COMPLETE!")
