#!/usr/bin/env python3
"""
EIMIR202 - NPL HIRE PURCHASE DIRECT REPORT
1:1 CONVERSION FROM SAS TO PYTHON
REPORTS OUTSTANDING LOANS CLASSIFIED AS NPL FOR HP DIRECT PRODUCTS
ISSUED FROM 1 JAN 1998, CATEGORIZED BY PRODUCT TYPE AND ARREARS BUCKET
"""

import duckdb
from pathlib import Path
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq

# INITIALIZE PATHS
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
BNM_DIR = INPUT_DIR / "bnm"

# CREATE DIRECTORIES
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# CONNECT TO DUCKDB
con = duckdb.connect(":memory:")

print("="*80)
print("EIMIR202 - NPL HIRE PURCHASE DIRECT REPORT")
print("="*80)

# ============================================================================
# READ REPORT DATE
# ============================================================================
REPTDATE_FILE = BNM_DIR / "REPTDATE.parquet"

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

RDATE = REPTDATE.strftime('%d%m%Y')
REPTYEAR = str(REPTDATE.year)
REPTMON = str(REPTDATE.month).zfill(2)
REPTDAY = str(REPTDATE.day).zfill(2)

print(f"REPORT DATE: {REPTDATE}")
print(f"RDATE: {RDATE}")

# ============================================================================
# READ BRANCH DATA
# ============================================================================
BRHFILE = INPUT_DIR / "brhfile.parquet"

con.execute(f"""
    CREATE OR REPLACE TABLE BRHDATA AS
    SELECT 
        BRANCH,
        BRHCODE
    FROM read_parquet('{BRHFILE}')
""")

# ============================================================================
# PROCESS LOAN DATA - CREATE CATEGORIES
# ============================================================================
print("\nPROCESSING LOAN DATA...")

LOANTEMP_FILE = BNM_DIR / "LOANTEMP.parquet"

con.execute(f"""
    CREATE OR REPLACE TABLE LOAN1_BASE AS
    SELECT 
        *,
        CASE 
            WHEN PRODUCT IN (380, 381, 700, 705) AND CHECKDT = 1 THEN 'A'
            WHEN PRODUCT IN (380, 381) AND CHECKDT = 1 THEN 'B'
            WHEN PRODUCT IN (128, 130) AND CHECKDT = 1 THEN 'C'
            WHEN PRODUCT IN (128, 130, 380, 381, 700, 705) AND CHECKDT = 1 THEN 'D'
        END AS CAT,
        CASE 
            WHEN PRODUCT IN (380, 381, 700, 705) AND CHECKDT = 1 THEN '(HPD-C)'
            WHEN PRODUCT IN (380, 381) AND CHECKDT = 1 THEN '(HP 380/381)'
            WHEN PRODUCT IN (128, 130) AND CHECKDT = 1 THEN '(AITAB)'
            WHEN PRODUCT IN (128, 130, 380, 381, 700, 705) AND CHECKDT = 1 THEN '(-HPD-)'
        END AS TYPE
    FROM read_parquet('{LOANTEMP_FILE}')
    WHERE (ARREAR > 6 OR BORSTAT IN ('R', 'I', 'F'))
        AND BALANCE > 0
""")

# EXPAND RECORDS FOR EACH CATEGORY (MIMICS MULTIPLE OUTPUT STATEMENTS)
con.execute("""
    CREATE OR REPLACE TABLE LOAN1 AS
    SELECT * FROM LOAN1_BASE WHERE CAT = 'A'
    UNION ALL
    SELECT * FROM LOAN1_BASE WHERE CAT = 'B'
    UNION ALL
    SELECT * FROM LOAN1_BASE WHERE CAT = 'C'
    UNION ALL
    SELECT * FROM LOAN1_BASE WHERE CAT = 'D'
    ORDER BY CAT, BRANCH
""")

LOAN_COUNT = con.execute("SELECT COUNT(*) FROM LOAN1").fetchone()[0]
print(f"LOAN1 RECORDS: {LOAN_COUNT:,}")

# MERGE WITH BRANCH DATA
con.execute("""
    CREATE OR REPLACE TABLE LOAN1_FINAL AS
    SELECT 
        l.*,
        b.BRHCODE
    FROM LOAN1 l
    LEFT JOIN BRHDATA b ON l.BRANCH = b.BRANCH
    ORDER BY l.CAT, l.BRANCH
""")

# ============================================================================
# AGGREGATE BY CATEGORY AND BRANCH WITH ARREARS BUCKETS
# ============================================================================
print("AGGREGATING BY ARREARS BUCKETS...")

# CREATE SUMMARY BY CAT, BRANCH, AND ARREARS
con.execute("""
    CREATE OR REPLACE TABLE BRANCH_SUMMARY AS
    SELECT 
        CAT,
        TYPE,
        BRANCH,
        BRHCODE,
        ARREAR,
        COUNT(*) AS NOACC,
        SUM(BALANCE) AS BRHAMT
    FROM LOAN1_FINAL
    GROUP BY CAT, TYPE, BRANCH, BRHCODE, ARREAR
    ORDER BY CAT, BRANCH, ARREAR
""")

# PIVOT TO CREATE 17 ARREARS COLUMNS
con.execute("""
    CREATE OR REPLACE TABLE BRANCH_PIVOT AS
    SELECT 
        CAT,
        TYPE,
        BRANCH,
        BRHCODE,
        SUM(CASE WHEN ARREAR = 1 THEN NOACC ELSE 0 END) AS NOACC1,
        SUM(CASE WHEN ARREAR = 1 THEN BRHAMT ELSE 0 END) AS BRHAMT1,
        SUM(CASE WHEN ARREAR = 2 THEN NOACC ELSE 0 END) AS NOACC2,
        SUM(CASE WHEN ARREAR = 2 THEN BRHAMT ELSE 0 END) AS BRHAMT2,
        SUM(CASE WHEN ARREAR = 3 THEN NOACC ELSE 0 END) AS NOACC3,
        SUM(CASE WHEN ARREAR = 3 THEN BRHAMT ELSE 0 END) AS BRHAMT3,
        SUM(CASE WHEN ARREAR = 4 THEN NOACC ELSE 0 END) AS NOACC4,
        SUM(CASE WHEN ARREAR = 4 THEN BRHAMT ELSE 0 END) AS BRHAMT4,
        SUM(CASE WHEN ARREAR = 5 THEN NOACC ELSE 0 END) AS NOACC5,
        SUM(CASE WHEN ARREAR = 5 THEN BRHAMT ELSE 0 END) AS BRHAMT5,
        SUM(CASE WHEN ARREAR = 6 THEN NOACC ELSE 0 END) AS NOACC6,
        SUM(CASE WHEN ARREAR = 6 THEN BRHAMT ELSE 0 END) AS BRHAMT6,
        SUM(CASE WHEN ARREAR = 7 THEN NOACC ELSE 0 END) AS NOACC7,
        SUM(CASE WHEN ARREAR = 7 THEN BRHAMT ELSE 0 END) AS BRHAMT7,
        SUM(CASE WHEN ARREAR = 8 THEN NOACC ELSE 0 END) AS NOACC8,
        SUM(CASE WHEN ARREAR = 8 THEN BRHAMT ELSE 0 END) AS BRHAMT8,
        SUM(CASE WHEN ARREAR = 9 THEN NOACC ELSE 0 END) AS NOACC9,
        SUM(CASE WHEN ARREAR = 9 THEN BRHAMT ELSE 0 END) AS BRHAMT9,
        SUM(CASE WHEN ARREAR = 10 THEN NOACC ELSE 0 END) AS NOACC10,
        SUM(CASE WHEN ARREAR = 10 THEN BRHAMT ELSE 0 END) AS BRHAMT10,
        SUM(CASE WHEN ARREAR = 11 THEN NOACC ELSE 0 END) AS NOACC11,
        SUM(CASE WHEN ARREAR = 11 THEN BRHAMT ELSE 0 END) AS BRHAMT11,
        SUM(CASE WHEN ARREAR = 12 THEN NOACC ELSE 0 END) AS NOACC12,
        SUM(CASE WHEN ARREAR = 12 THEN BRHAMT ELSE 0 END) AS BRHAMT12,
        SUM(CASE WHEN ARREAR = 13 THEN NOACC ELSE 0 END) AS NOACC13,
        SUM(CASE WHEN ARREAR = 13 THEN BRHAMT ELSE 0 END) AS BRHAMT13,
        SUM(CASE WHEN ARREAR = 14 THEN NOACC ELSE 0 END) AS NOACC14,
        SUM(CASE WHEN ARREAR = 14 THEN BRHAMT ELSE 0 END) AS BRHAMT14,
        SUM(CASE WHEN ARREAR = 15 THEN NOACC ELSE 0 END) AS NOACC15,
        SUM(CASE WHEN ARREAR = 15 THEN BRHAMT ELSE 0 END) AS BRHAMT15,
        SUM(CASE WHEN ARREAR = 16 THEN NOACC ELSE 0 END) AS NOACC16,
        SUM(CASE WHEN ARREAR = 16 THEN BRHAMT ELSE 0 END) AS BRHAMT16,
        SUM(CASE WHEN ARREAR = 17 THEN NOACC ELSE 0 END) AS NOACC17,
        SUM(CASE WHEN ARREAR = 17 THEN BRHAMT ELSE 0 END) AS BRHAMT17
    FROM BRANCH_SUMMARY
    GROUP BY CAT, TYPE, BRANCH, BRHCODE
    ORDER BY CAT, BRANCH
""")

# ============================================================================
# CALCULATE SUBTOTALS AND TOTALS
# ============================================================================
con.execute("""
    CREATE OR REPLACE TABLE REPORT_DATA AS
    SELECT 
        *,
        -- SUBTOTAL >= 3 MTH (ARREARS 4-17)
        (NOACC4 + NOACC5 + NOACC6 + NOACC7 + NOACC8 + NOACC9 + NOACC10 +
         NOACC11 + NOACC12 + NOACC13 + NOACC14 + NOACC15 + NOACC16 + NOACC17) AS SUBACC,
        (BRHAMT4 + BRHAMT5 + BRHAMT6 + BRHAMT7 + BRHAMT8 + BRHAMT9 + BRHAMT10 +
         BRHAMT11 + BRHAMT12 + BRHAMT13 + BRHAMT14 + BRHAMT15 + BRHAMT16 + BRHAMT17) AS SUBBRH,
        -- SUBTOTAL >= 6 MTH (ARREARS 7-17)
        (NOACC7 + NOACC8 + NOACC9 + NOACC10 + NOACC11 + NOACC12 + 
         NOACC13 + NOACC14 + NOACC15 + NOACC16 + NOACC17) AS SUBAC2,
        (BRHAMT7 + BRHAMT8 + BRHAMT9 + BRHAMT10 + BRHAMT11 + BRHAMT12 + 
         BRHAMT13 + BRHAMT14 + BRHAMT15 + BRHAMT16 + BRHAMT17) AS SUBBR2,
        -- TOTAL (ALL ARREARS)
        (NOACC1 + NOACC2 + NOACC3 + NOACC4 + NOACC5 + NOACC6 + NOACC7 + NOACC8 + NOACC9 +
         NOACC10 + NOACC11 + NOACC12 + NOACC13 + NOACC14 + NOACC15 + NOACC16 + NOACC17) AS SOTACC,
        (BRHAMT1 + BRHAMT2 + BRHAMT3 + BRHAMT4 + BRHAMT5 + BRHAMT6 + BRHAMT7 + BRHAMT8 + BRHAMT9 +
         BRHAMT10 + BRHAMT11 + BRHAMT12 + BRHAMT13 + BRHAMT14 + BRHAMT15 + BRHAMT16 + BRHAMT17) AS TOTBRH
    FROM BRANCH_PIVOT
""")

# ============================================================================
# GENERATE FORMATTED TEXT REPORT
# ============================================================================
print("\nGENERATING FORMATTED REPORT...")

OUTPUT_TXT = OUTPUT_DIR / f"EIMAR202_{REPTYEAR}{REPTMON}{REPTDAY}.txt"

# GET DATA BY CATEGORY
CATEGORIES = con.execute("SELECT DISTINCT CAT, TYPE FROM REPORT_DATA ORDER BY CAT").fetchall()

with open(OUTPUT_TXT, 'w') as f:
    for cat_idx, (CAT, TYPE) in enumerate(CATEGORIES):
        # PAGE HEADER
        PAGE_NUM = cat_idx + 1
        f.write(f"PROGRAM-ID : EIMAR202{' '*26}P U B L I C   I S L A M I C  B A N K   B E R H A D{' '*9}PAGE NO.: {PAGE_NUM}\n")
        f.write(f"{' '*15}OUTSTANDING LOANS CLASSIFIED AS NPL ISSUED FROM 1 JAN 98{' '*8}{TYPE:<13}{' '*2}{RDATE}\n")
        f.write(" \n")
        f.write("BRH     NO         < 1 MTH              NO     1 TO < 2 MTH              NO     2 TO < 3 MTH              NO      3 TO < 4 MTH              NO      4 TO < 5 MTH\n")
        f.write("        NO    5 TO < 6 MTH              NO     6 TO < 7 MTH              NO     7 TO < 8 MTH              NO      8 TO < 9 MTH              NO     9 TO < 10 MTH\n")
        f.write("        NO  10 TO < 11 MTH              NO   11 TO < 12 MTH              NO   12 TO < 18 MTH              NO    18 TO < 24 MTH              NO    24 TO < 36 MTH\n")
        f.write("        NO        > 36 MTH              NO          DEFICIT             NO   SUBTOTAL >=3MTH              NO   SUBTOTAL >=6MTH              NO             TOTAL\n")
        f.write("-"*40 + "-"*40 + "-"*40 + "-"*10 + "\n")
        
        # GET BRANCH DATA FOR THIS CATEGORY
        branches = con.execute(f"""
            SELECT * FROM REPORT_DATA 
            WHERE CAT = '{CAT}'
            ORDER BY BRANCH
        """).fetchall()
        
        # CATEGORY TOTALS
        TOTALS = [0] * 34  # 17 NOACC + 17 BRHAMT
        
        for branch_row in branches:
            BRANCH = str(branch_row[2]).zfill(3)
            BRHCODE = branch_row[3] or '   '
            
            # LINE 1
            f.write(f"{BRANCH:<3} {branch_row[4]:>7,} {branch_row[5]:>16,.2f} {branch_row[6]:>7,} {branch_row[7]:>15,.2f} {branch_row[8]:>7,} {branch_row[9]:>15,.2f} {branch_row[10]:>8,} {branch_row[11]:>17,.2f} {branch_row[12]:>8,} {branch_row[13]:>17,.2f}\n")
            
            # LINE 2
            f.write(f"{BRHCODE:<3} {branch_row[14]:>7,} {branch_row[15]:>16,.2f} {branch_row[16]:>7,} {branch_row[17]:>15,.2f} {branch_row[18]:>7,} {branch_row[19]:>15,.2f} {branch_row[20]:>8,} {branch_row[21]:>17,.2f} {branch_row[22]:>8,} {branch_row[23]:>17,.2f}\n")
            
            # LINE 3
            f.write(f"    {branch_row[24]:>7,} {branch_row[25]:>16,.2f} {branch_row[26]:>7,} {branch_row[27]:>15,.2f} {branch_row[28]:>7,} {branch_row[29]:>15,.2f} {branch_row[30]:>8,} {branch_row[31]:>17,.2f} {branch_row[32]:>8,} {branch_row[33]:>17,.2f}\n")
            
            # LINE 4 (with subtotals)
            SUBACC = branch_row[38]
            SUBBRH = branch_row[39]
            SUBAC2 = branch_row[40]
            SUBBR2 = branch_row[41]
            SOTACC = branch_row[42]
            TOTBRH = branch_row[43]
            
            f.write(f"    {branch_row[34]:>7,} {branch_row[35]:>16,.2f} {branch_row[36]:>7,} {branch_row[37]:>15,.2f} {SUBACC:>7,} {SUBBRH:>15,.2f} {SUBAC2:>8,} {SUBBR2:>17,.2f} {SOTACC:>8,} {TOTBRH:>17,.2f}\n")
            
            # ACCUMULATE TOTALS
            for i in range(4, 38):
                TOTALS[i-4] += branch_row[i]
            TOTALS[34] += SUBACC
            TOTALS[35] += SUBBRH
            TOTALS[36] += SUBAC2
            TOTALS[37] += SUBBR2
            TOTALS[38] += SOTACC
            TOTALS[39] += TOTBRH
        
        # CATEGORY TOTALS
        f.write("-"*40 + "-"*40 + "-"*40 + "-"*10 + "\n")
        f.write(f"TOT {TOTALS[0]:>7,} {TOTALS[1]:>16,.2f} {TOTALS[2]:>7,} {TOTALS[3]:>15,.2f} {TOTALS[4]:>7,} {TOTALS[5]:>15,.2f} {TOTALS[6]:>8,} {TOTALS[7]:>17,.2f} {TOTALS[8]:>8,} {TOTALS[9]:>17,.2f}\n")
        f.write(f"    {TOTALS[10]:>7,} {TOTALS[11]:>16,.2f} {TOTALS[12]:>7,} {TOTALS[13]:>15,.2f} {TOTALS[14]:>7,} {TOTALS[15]:>15,.2f} {TOTALS[16]:>8,} {TOTALS[17]:>17,.2f} {TOTALS[18]:>8,} {TOTALS[19]:>17,.2f}\n")
        f.write(f"    {TOTALS[20]:>7,} {TOTALS[21]:>16,.2f} {TOTALS[22]:>7,} {TOTALS[23]:>15,.2f} {TOTALS[24]:>7,} {TOTALS[25]:>15,.2f} {TOTALS[26]:>8,} {TOTALS[27]:>17,.2f} {TOTALS[28]:>8,} {TOTALS[29]:>17,.2f}\n")
        f.write(f"    {TOTALS[30]:>7,} {TOTALS[31]:>16,.2f} {TOTALS[32]:>7,} {TOTALS[33]:>15,.2f} {TOTALS[34]:>7,} {TOTALS[35]:>15,.2f} {TOTALS[36]:>8,} {TOTALS[37]:>17,.2f} {TOTALS[38]:>8,} {TOTALS[39]:>17,.2f}\n")
        f.write("-"*40 + "-"*40 + "-"*40 + "-"*10 + "\n")
        f.write("\n")
        
        if cat_idx < len(CATEGORIES) - 1:
            f.write("\f")  # PAGE BREAK

print(f"SAVED: {OUTPUT_TXT}")

# ============================================================================
# SAVE DATA FILES
# ============================================================================
OUTPUT_PARQUET = OUTPUT_DIR / f"EIMAR202_{REPTYEAR}{REPTMON}{REPTDAY}.parquet"
pq.write_table(con.execute("SELECT * FROM REPORT_DATA").arrow(), OUTPUT_PARQUET)
print(f"SAVED: {OUTPUT_PARQUET}")

print("\n" + "="*80)
print("REPORT COMPLETE")
print("="*80)

con.close()
