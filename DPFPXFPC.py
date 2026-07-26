#!/usr/bin/env python3
"""
EIMIR202 - NPL HIRE PURCHASE DIRECT REPORT
1:1 CONVERSION FROM SAS TO PYTHON
REPORTS OUTSTANDING LOANS CLASSIFIED AS NPL FOR HP DIRECT PRODUCTS
ISSUED FROM 1 JAN 1998, CATEGORIZED BY PRODUCT TYPE AND ARREARS BUCKET
"""

import duckdb
from pathlib import Path
from datetime import datetime, timedelta
import pyreadstat
import numpy as np

# INITIALIZE PATHS
INPUT_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMIR202")
OUTPUT_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMIR202")

# CREATE DIRECTORIES
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# CONNECT TO DUCKDB
con = duckdb.connect(":memory:")

print("="*80)
print("EIMIR202 - NPL HIRE PURCHASE DIRECT REPORT")
print("="*80)

# ============================================================================
# SET REPORT DATE (YESTERDAY)
# ============================================================================
REPTDATE = datetime.now().date() - timedelta(days=1)
RDATE = REPTDATE.strftime('%d%m%Y')
REPTYEAR = str(REPTDATE.year)
REPTMON = str(REPTDATE.month).zfill(2)
REPTDAY = str(REPTDATE.day).zfill(2)

print(f"REPORT DATE: {REPTDATE}")
print(f"RDATE: {RDATE}")

# ============================================================================
# READ BRANCH DATA (LKP_BRANCH - FLAT FILE)
# ============================================================================
BRANCH_FILE = INPUT_DIR / "LKP_BRANCH"

if not BRANCH_FILE.exists():
    print(f"ERROR: BRANCH FILE NOT FOUND: {BRANCH_FILE}")
    exit(1)

# Read flat file - assuming space or tab delimited
with open(BRANCH_FILE, 'r') as f:
    lines = f.readlines()

# Parse flat file - adjust based on your actual format
branch_data = []
for line in lines:
    line = line.strip()
    if line:
        parts = line.split()
        if len(parts) >= 2:
            branch_data.append((str(parts[0]).strip(), str(parts[1]).strip()))

# Create branch table with VARCHAR type for BRANCH
con.execute("""
    CREATE OR REPLACE TABLE BRHDATA (
        BRANCH VARCHAR,
        BRHCODE VARCHAR
    )
""")

for branch, brhcode in branch_data:
    con.execute(f"INSERT INTO BRHDATA VALUES ('{branch}', '{brhcode}')")

branch_count = con.execute("SELECT COUNT(*) FROM BRHDATA").fetchone()[0]
print(f"BRANCH RECORDS: {branch_count:,}")

# ============================================================================
# READ LOAN DATA (LOANTEMP.sas7bdat)
# ============================================================================
LOANTEMP_FILE = INPUT_DIR / "LOANTEMP.sas7bdat"

if not LOANTEMP_FILE.exists():
    print(f"ERROR: LOANTEMP FILE NOT FOUND: {LOANTEMP_FILE}")
    exit(1)

print("\nREADING LOANTEMP.SAS7BDAT...")
df, meta = pyreadstat.read_sas7bdat(str(LOANTEMP_FILE))

# Register DataFrame as DuckDB table
con.execute("CREATE OR REPLACE TABLE LOANTEMP AS SELECT * FROM df")

print(f"LOANTEMP RECORDS: {con.execute('SELECT COUNT(*) FROM LOANTEMP').fetchone()[0]:,}")

# ============================================================================
# PROCESS LOAN DATA - CREATE CATEGORIES
# ============================================================================
print("\nPROCESSING LOAN DATA...")

con.execute("""
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
    FROM LOANTEMP
    WHERE (ARREAR > 6 OR BORSTAT IN ('R', 'I', 'F'))
        AND BALANCE > 0
""")

# EXPAND RECORDS FOR EACH CATEGORY
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
    LEFT JOIN BRHDATA b ON CAST(l.BRANCH AS VARCHAR) = CAST(b.BRANCH AS VARCHAR)
    ORDER BY l.CAT, l.BRANCH
""")

unmatched = con.execute("""
    SELECT COUNT(*) FROM LOAN1_FINAL WHERE BRHCODE IS NULL
""").fetchone()[0]

if unmatched > 0:
    print(f"WARNING: {unmatched:,} records have no matching BRHCODE")

# ============================================================================
# AGGREGATE BY CATEGORY AND BRANCH WITH ARREARS BUCKETS
# ============================================================================
print("AGGREGATING BY ARREARS BUCKETS...")

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
# GENERATE FORMATTED TEXT REPORT
# ============================================================================
print("\nGENERATING FORMATTED REPORT...")

OUTPUT_TXT = OUTPUT_DIR / f"EIMAR202_{REPTYEAR}{REPTMON}{REPTDAY}.txt"

# Define formatting functions to match SAS output
def fmt_no(val):
    return f"{int(val):>7,}" if val > 0 else f"{0:>7}"

def fmt_amt(val):
    return f"{val:>14,.2f}" if val > 0 else f"{0:>14,.2f}"

def fmt_line1(branch, data):
    # branch + NO1 + AMT1 + NO2 + AMT2 + NO3 + AMT3 + NO4 + AMT4 + NO5 + AMT5
    return (f"{str(branch):<3} " +
            f"{fmt_no(data[0])} {fmt_amt(data[1])} " +
            f"{fmt_no(data[2])} {fmt_amt(data[3])} " +
            f"{fmt_no(data[4])} {fmt_amt(data[5])} " +
            f"{fmt_no(data[6])} {fmt_amt(data[7])} " +
            f"{fmt_no(data[8])} {fmt_amt(data[9])}")

def fmt_line2(brhcode, data):
    # BRHCODE + NO6 + AMT6 + NO7 + AMT7 + NO8 + AMT8 + NO9 + AMT9 + NO10 + AMT10
    return (f"{str(brhcode):<3} " +
            f"{fmt_no(data[0])} {fmt_amt(data[1])} " +
            f"{fmt_no(data[2])} {fmt_amt(data[3])} " +
            f"{fmt_no(data[4])} {fmt_amt(data[5])} " +
            f"{fmt_no(data[6])} {fmt_amt(data[7])} " +
            f"{fmt_no(data[8])} {fmt_amt(data[9])}")

def fmt_line3(data):
    # NO11 + AMT11 + NO12 + AMT12 + NO13 + AMT13 + NO14 + AMT14 + NO15 + AMT15
    return ("    " +
            f"{fmt_no(data[0])} {fmt_amt(data[1])} " +
            f"{fmt_no(data[2])} {fmt_amt(data[3])} " +
            f"{fmt_no(data[4])} {fmt_amt(data[5])} " +
            f"{fmt_no(data[6])} {fmt_amt(data[7])} " +
            f"{fmt_no(data[8])} {fmt_amt(data[9])}")

def fmt_line4(data):
    # NO16 + AMT16 + NO17 + AMT17 + SUBACC + SUBBRH + SUBAC2 + SUBBR2 + SOTACC + TOTBRH
    return ("    " +
            f"{fmt_no(data[0])} {fmt_amt(data[1])} " +
            f"{fmt_no(data[2])} {fmt_amt(data[3])} " +
            f"{fmt_no(data[4])} {fmt_amt(data[5])} " +
            f"{fmt_no(data[6])} {fmt_amt(data[7])} " +
            f"{fmt_no(data[8])} {fmt_amt(data[9])}")

# GET DATA BY CATEGORY
CATEGORIES = con.execute("SELECT DISTINCT CAT, TYPE FROM REPORT_DATA ORDER BY CAT").fetchall()

with open(OUTPUT_TXT, 'w') as f:
    pagecnt = 0
    
    for cat_idx, (CAT, TYPE) in enumerate(CATEGORIES):
        # Initialize category-level totals
        totamt = np.zeros(17)
        totacc = np.zeros(17)
        
        first_branch_in_category = True
        
        # Get branches for this category
        branches = con.execute(f"""
            SELECT * FROM REPORT_DATA 
            WHERE CAT = '{CAT}'
            ORDER BY BRANCH
        """).fetchall()
        
        for branch_row in branches:
            brhamt = np.zeros(17)
            noacc = np.zeros(17)
            
            # Populate arrays from pivot data
            for i in range(17):
                noacc[i] = branch_row[4 + i*2]  # NOACC columns start at index 4
                brhamt[i] = branch_row[5 + i*2]  # BRHAMT columns start at index 5
            
            # Calculate subtotals
            subbrh = np.sum(brhamt[3:])
            subbr2 = subbrh - brhamt[3] - brhamt[4] - brhamt[5]
            subacc = np.sum(noacc[3:])
            subac2 = subacc - noacc[3] - noacc[4] - noacc[5]
            totbrh = subbrh + brhamt[0] + brhamt[1] + brhamt[2]
            sotacc = subacc + noacc[0] + noacc[1] + noacc[2]
            
            # Update category totals
            totamt += brhamt
            totacc += noacc
            
            # Get branch info
            BRANCH = str(int(float(branch_row[2]))).zfill(3)
            BRHCODE = str(branch_row[3] or '').strip()
            if not BRHCODE:
                BRHCODE = '   '
            
            # Print page header if first branch in category
            if first_branch_in_category:
                pagecnt += 1
                f.write(f"PROGRAM-ID : EIMAR202                     P U B L I C   I S L A M I C   B A N K   B E R H A D                        PAGE NO.: {pagecnt:>2}\n")
                f.write(f"                                   OUTSTANDING LOANS CLASSIFIED AS NPL ISSUED FROM 1 JAN 1998        {TYPE:<13}       {RDATE}\n")
                f.write("\n")
                f.write("BRH     NO          < 1 MTH       NO     1 TO < 2 MTH       NO     2 TO < 3 MTH        NO      3 TO < 4 MTH        NO      4 TO < 5 MTH\n")
                f.write("        NO     5 TO < 6 MTH       NO     6 TO < 7 MTH       NO     7 TO < 8 MTH        NO      8 TO < 9 MTH        NO     9 TO < 10 MTH\n")
                f.write("        NO   10 TO < 11 MTH       NO   11 TO < 12 MTH       NO   12 TO < 18 MTH        NO    18 TO < 24 MTH        NO    24 TO < 36 MTH\n")
                f.write("        NO         > 36 MTH       NO          DEFICIT       NO   SUBTOTAL >=3MTH       NO   SUBTOTAL >=6MTH        NO             TOTAL\n")
                f.write("-" * 134 + "\n")
                first_branch_in_category = False
            
            # Line 1: Branch number + columns 1-5
            f.write(fmt_line1(BRANCH, [noacc[0], brhamt[0], noacc[1], brhamt[1], noacc[2], brhamt[2], noacc[3], brhamt[3], noacc[4], brhamt[4]]) + "\n")
            
            # Line 2: BRHCODE + columns 6-10
            f.write(fmt_line2(BRHCODE, [noacc[5], brhamt[5], noacc[6], brhamt[6], noacc[7], brhamt[7], noacc[8], brhamt[8], noacc[9], brhamt[9]]) + "\n")
            
            # Line 3: Columns 11-15
            f.write(fmt_line3([noacc[10], brhamt[10], noacc[11], brhamt[11], noacc[12], brhamt[12], noacc[13], brhamt[13], noacc[14], brhamt[14]]) + "\n")
            
            # Line 4: Columns 16-17 + subtotals
            f.write(fmt_line4([noacc[15], brhamt[15], noacc[16], brhamt[16], subacc, subbrh, subac2, subbr2, sotacc, totbrh]) + "\n")
        
        # Calculate grand totals for category
        sgtotbrh = np.sum(totamt[3:])
        sgtotbr2 = sgtotbrh - totamt[3] - totamt[4] - totamt[5]
        sgtotacc = np.sum(totacc[3:])
        sgtotac2 = sgtotacc - totacc[3] - totacc[4] - totacc[5]
        gtotbrh = sgtotbrh + totamt[0] + totamt[1] + totamt[2]
        gtotacc = sgtotacc + totacc[0] + totacc[1] + totacc[2]
        
        # Print category totals
        f.write("-" * 134 + "\n")
        f.write(fmt_line1("TOT", [totacc[0], totamt[0], totacc[1], totamt[1], totacc[2], totamt[2], totacc[3], totamt[3], totacc[4], totamt[4]]) + "\n")
        f.write(fmt_line2("", [totacc[5], totamt[5], totacc[6], totamt[6], totacc[7], totamt[7], totacc[8], totamt[8], totacc[9], totamt[9]]) + "\n")
        f.write(fmt_line3([totacc[10], totamt[10], totacc[11], totamt[11], totacc[12], totamt[12], totacc[13], totamt[13], totacc[14], totamt[14]]) + "\n")
        f.write(fmt_line4([totacc[15], totamt[15], totacc[16], totamt[16], sgtotacc, sgtotbrh, sgtotac2, sgtotbr2, gtotacc, gtotbrh]) + "\n")
        f.write("-" * 134 + "\n")
        f.write("\n")
        
        if cat_idx < len(CATEGORIES) - 1:
            f.write("\f")  # PAGE BREAK

print(f"SAVED: {OUTPUT_TXT}")

print("\n" + "="*80)
print("REPORT COMPLETE")
print("="*80)

con.close()
