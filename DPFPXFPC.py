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
# READ BRANCH DATA (BRHFILE - FLAT FILE)
# ============================================================================
# SAS: INPUT @2 BRANCH 3. @6 BRHCODE $3.;
# Branch is at position 2-4 (3 chars), BRHCODE at position 6-8 (3 chars)
BRANCH_FILE = INPUT_DIR / "BRHFILE"

if not BRANCH_FILE.exists():
    print(f"ERROR: BRANCH FILE NOT FOUND: {BRANCH_FILE}")
    exit(1)

with open(BRANCH_FILE, 'r') as f:
    lines = f.readlines()

branch_data = []
for line in lines:
    line = line.strip()
    if len(line) >= 8:
        # Extract branch from position 2-4 (0-index: position 1-3)
        branch = line[1:4].strip()
        # Extract brhcode from position 6-8 (0-index: position 5-7)
        brhcode = line[5:8].strip()
        if branch:
            branch_data.append((branch, brhcode))

# Create branch table
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
# PROCESS LOAN DATA - CREATE CATEGORIES (Matches SAS exactly)
# ============================================================================
print("\nPROCESSING LOAN DATA...")

con.execute("""
    CREATE OR REPLACE TABLE LOAN1 AS
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

print(f"LOAN1 RECORDS: {con.execute('SELECT COUNT(*) FROM LOAN1').fetchone()[0]:,}")

# MERGE WITH BRANCH DATA (Matches SAS: MERGE LOAN1 BRHDATA; BY BRANCH;)
con.execute("""
    CREATE OR REPLACE TABLE LOAN1_FINAL AS
    SELECT 
        l.*,
        b.BRHCODE
    FROM LOAN1 l
    LEFT JOIN BRHDATA b ON CAST(l.BRANCH AS VARCHAR) = CAST(b.BRANCH AS VARCHAR)
""")

# ============================================================================
# PROCESS BY CATEGORY AND BRANCH (Matches SAS DATA TRY step)
# ============================================================================
print("\nGENERATING REPORT...")

OUTPUT_TXT = OUTPUT_DIR / f"EIMAR202_{REPTYEAR}{REPTMON}{REPTDAY}.txt"

def format_branch(branch_val):
    """Format branch as Z3. (3 digits with leading zeros)"""
    try:
        return f"{int(float(branch_val)):03d}"
    except:
        return str(branch_val).zfill(3)

# SAS column positions (0-indexed in Python)
# @1=0, @5=4, @13=12, @30=29, @38=37, @54=53, @62=61, @78=77, @87=86, @105=104, @114=113

def put_at(pos, text):
    """Position text at exact SAS column position (1-indexed)"""
    return f"{' ' * (pos - 1)}{text}"

with open(OUTPUT_TXT, 'w') as f:
    pagecnt = 0
    
    # Get distinct categories
    categories = con.execute("""
        SELECT DISTINCT CAT, TYPE 
        FROM LOAN1_FINAL 
        WHERE CAT IS NOT NULL 
        ORDER BY CAT
    """).fetchall()
    
    for cat_idx, (CAT, TYPE) in enumerate(categories):
        # Initialize arrays
        totamt = np.zeros(17)
        totacc = np.zeros(17)
        
        # Get branches for this category
        branches = con.execute(f"""
            SELECT 
                BRANCH,
                BRHCODE,
                ARREAR,
                BALANCE
            FROM LOAN1_FINAL
            WHERE CAT = '{CAT}'
            ORDER BY BRANCH
        """).fetchall()
        
        if not branches:
            continue
        
        current_branch = None
        brhamt = np.zeros(17)
        noacc = np.zeros(17)
        first_branch_in_category = True
        first_branch_printed = False
        
        for row in branches:
            branch_val = row[0]
            brhcode = row[1] or '   '
            arrears = int(row[2])
            balance = float(row[3]) if row[3] else 0
            
            # New branch
            if current_branch != branch_val:
                # If we have data from previous branch, print it
                if current_branch is not None:
                    # Calculate subtotals
                    subbrh = np.sum(brhamt[3:17])
                    subbr2 = subbrh - brhamt[3] - brhamt[4] - brhamt[5]
                    subacc = np.sum(noacc[3:17])
                    subac2 = subacc - noacc[3] - noacc[4] - noacc[5]
                    totbrh = subbrh + brhamt[0] + brhamt[1] + brhamt[2]
                    sotacc = subacc + noacc[0] + noacc[1] + noacc[2]
                    
                    # Update totals
                    totamt += brhamt
                    totacc += noacc
                    
                    # Print branch data
                    if not first_branch_printed:
                        # Page header - using exact SAS positioning
                        pagecnt += 1
                        # Line 1: PROGRAM-ID
                        f.write(f"{' ' * 0}PROGRAM-ID : EIMAR202")
                        f.write(f"{' ' * 31}P U B L I C   I S L A M I C  B A N K   B E R H A D")
                        f.write(f"{' ' * 9}PAGE NO.: {pagecnt:>2}\n")
                        # Line 2: Title
                        f.write(f"{' ' * 31}OUTSTANDING LOANS CLASSIFIED AS NPL ISSUED FROM 1 JAN 98")
                        f.write(f"{' ' * 7}{TYPE:<13}")
                        f.write(f"{' ' * 3}{RDATE}\n")
                        f.write(" \n")
                        # Line 3-6: Column headers
                        f.write("BRH     NO         < 1 MTH")
                        f.write("         NO     1 TO < 2 MTH")
                        f.write("         NO     2 TO < 3 MTH")
                        f.write("        NO      3 TO < 4 MTH")
                        f.write("        NO      4 TO < 5 MTH\n")
                        f.write("        NO    5 TO < 6 MTH")
                        f.write("         NO     6 TO < 7 MTH")
                        f.write("         NO     7 TO < 8 MTH")
                        f.write("        NO      8 TO < 9 MTH")
                        f.write("        NO     9 TO < 10 MTH\n")
                        f.write("        NO  10 TO < 11 MTH")
                        f.write("         NO   11 TO < 12 MTH")
                        f.write("         NO   12 TO < 18 MTH")
                        f.write("        NO    18 TO < 24 MTH")
                        f.write("        NO    24 TO < 36 MTH\n")
                        f.write("        NO        > 36 MTH")
                        f.write("         NO          DEFICIT")
                        f.write("        NO   SUBTOTAL >=3MTH")
                        f.write("        NO   SUBTOTAL >=6MTH")
                        f.write("        NO             TOTAL\n")
                        f.write("-" * 40 + "-" * 40 + "-" * 40 + "-" * 10 + "\n")
                        first_branch_printed = True
                    
                    # Print 4 lines per branch - using exact SAS column positions
                    branch_fmt = format_branch(current_branch)
                    
                    # LINE 1: @1 BRANCH, @5 NOACC1, @13 BRHAMT1, @30 NOACC2, @38 BRHAMT2, 
                    # @54 NOACC3, @62 BRHAMT3, @78 NOACC4, @87 BRHAMT4, @105 NOACC5, @114 BRHAMT5
                    line1 = (f"{branch_fmt:<3}" +
                             f"{int(noacc[0]):>7,}" + f"{brhamt[0]:>16,.2f}" +
                             f"{int(noacc[1]):>8,}" + f"{brhamt[1]:>15,.2f}" +
                             f"{int(noacc[2]):>8,}" + f"{brhamt[2]:>15,.2f}" +
                             f"{int(noacc[3]):>9,}" + f"{brhamt[3]:>17,.2f}" +
                             f"{int(noacc[4]):>9,}" + f"{brhamt[4]:>17,.2f}")
                    f.write(line1 + "\n")
                    
                    # LINE 2: @1 BRHCODE, @5 NOACC6, @13 BRHAMT6, @30 NOACC7, @38 BRHAMT7,
                    # @54 NOACC8, @62 BRHAMT8, @78 NOACC9, @87 BRHAMT9, @105 NOACC10, @114 BRHAMT10
                    line2 = (f"{brhcode:<3}" +
                             f"{int(noacc[5]):>7,}" + f"{brhamt[5]:>16,.2f}" +
                             f"{int(noacc[6]):>8,}" + f"{brhamt[6]:>15,.2f}" +
                             f"{int(noacc[7]):>8,}" + f"{brhamt[7]:>15,.2f}" +
                             f"{int(noacc[8]):>9,}" + f"{brhamt[8]:>17,.2f}" +
                             f"{int(noacc[9]):>9,}" + f"{brhamt[9]:>17,.2f}")
                    f.write(line2 + "\n")
                    
                    # LINE 3: @5 NOACC11, @13 BRHAMT11, @30 NOACC12, @38 BRHAMT12,
                    # @54 NOACC13, @62 BRHAMT13, @78 NOACC14, @87 BRHAMT14, @105 NOACC15, @114 BRHAMT15
                    line3 = ("    " +
                             f"{int(noacc[10]):>7,}" + f"{brhamt[10]:>16,.2f}" +
                             f"{int(noacc[11]):>8,}" + f"{brhamt[11]:>15,.2f}" +
                             f"{int(noacc[12]):>8,}" + f"{brhamt[12]:>15,.2f}" +
                             f"{int(noacc[13]):>9,}" + f"{brhamt[13]:>17,.2f}" +
                             f"{int(noacc[14]):>9,}" + f"{brhamt[14]:>17,.2f}")
                    f.write(line3 + "\n")
                    
                    # LINE 4: @5 NOACC16, @13 BRHAMT16, @30 NOACC17, @38 BRHAMT17,
                    # @54 SUBACC, @62 SUBBRH, @78 SUBAC2, @87 SUBBR2, @105 SOTACC, @114 TOTBRH
                    line4 = ("    " +
                             f"{int(noacc[15]):>7,}" + f"{brhamt[15]:>16,.2f}" +
                             f"{int(noacc[16]):>8,}" + f"{brhamt[16]:>15,.2f}" +
                             f"{int(subacc):>8,}" + f"{subbrh:>15,.2f}" +
                             f"{int(subac2):>9,}" + f"{subbr2:>17,.2f}" +
                             f"{int(sotacc):>9,}" + f"{totbrh:>17,.2f}")
                    f.write(line4 + "\n")
                
                # Reset for new branch
                current_branch = branch_val
                brhamt = np.zeros(17)
                noacc = np.zeros(17)
            
            # Accumulate for current branch
            if balance > 0:
                idx = arrears - 1
                if 0 <= idx < 17:
                    brhamt[idx] += balance
                    noacc[idx] += 1
        
        # Print last branch
        if current_branch is not None:
            subbrh = np.sum(brhamt[3:17])
            subbr2 = subbrh - brhamt[3] - brhamt[4] - brhamt[5]
            subacc = np.sum(noacc[3:17])
            subac2 = subacc - noacc[3] - noacc[4] - noacc[5]
            totbrh = subbrh + brhamt[0] + brhamt[1] + brhamt[2]
            sotacc = subacc + noacc[0] + noacc[1] + noacc[2]
            
            totamt += brhamt
            totacc += noacc
            
            if not first_branch_printed:
                pagecnt += 1
                f.write(f"{' ' * 0}PROGRAM-ID : EIMAR202")
                f.write(f"{' ' * 31}P U B L I C   I S L A M I C  B A N K   B E R H A D")
                f.write(f"{' ' * 9}PAGE NO.: {pagecnt:>2}\n")
                f.write(f"{' ' * 31}OUTSTANDING LOANS CLASSIFIED AS NPL ISSUED FROM 1 JAN 98")
                f.write(f"{' ' * 7}{TYPE:<13}")
                f.write(f"{' ' * 3}{RDATE}\n")
                f.write(" \n")
                f.write("BRH     NO         < 1 MTH")
                f.write("         NO     1 TO < 2 MTH")
                f.write("         NO     2 TO < 3 MTH")
                f.write("        NO      3 TO < 4 MTH")
                f.write("        NO      4 TO < 5 MTH\n")
                f.write("        NO    5 TO < 6 MTH")
                f.write("         NO     6 TO < 7 MTH")
                f.write("         NO     7 TO < 8 MTH")
                f.write("        NO      8 TO < 9 MTH")
                f.write("        NO     9 TO < 10 MTH\n")
                f.write("        NO  10 TO < 11 MTH")
                f.write("         NO   11 TO < 12 MTH")
                f.write("         NO   12 TO < 18 MTH")
                f.write("        NO    18 TO < 24 MTH")
                f.write("        NO    24 TO < 36 MTH\n")
                f.write("        NO        > 36 MTH")
                f.write("         NO          DEFICIT")
                f.write("        NO   SUBTOTAL >=3MTH")
                f.write("        NO   SUBTOTAL >=6MTH")
                f.write("        NO             TOTAL\n")
                f.write("-" * 40 + "-" * 40 + "-" * 40 + "-" * 10 + "\n")
                first_branch_printed = True
            
            branch_fmt = format_branch(current_branch)
            brhcode = branches[-1][1] or '   '
            
            # LINE 1
            line1 = (f"{branch_fmt:<3}" +
                     f"{int(noacc[0]):>7,}" + f"{brhamt[0]:>16,.2f}" +
                     f"{int(noacc[1]):>8,}" + f"{brhamt[1]:>15,.2f}" +
                     f"{int(noacc[2]):>8,}" + f"{brhamt[2]:>15,.2f}" +
                     f"{int(noacc[3]):>9,}" + f"{brhamt[3]:>17,.2f}" +
                     f"{int(noacc[4]):>9,}" + f"{brhamt[4]:>17,.2f}")
            f.write(line1 + "\n")
            
            # LINE 2
            line2 = (f"{brhcode:<3}" +
                     f"{int(noacc[5]):>7,}" + f"{brhamt[5]:>16,.2f}" +
                     f"{int(noacc[6]):>8,}" + f"{brhamt[6]:>15,.2f}" +
                     f"{int(noacc[7]):>8,}" + f"{brhamt[7]:>15,.2f}" +
                     f"{int(noacc[8]):>9,}" + f"{brhamt[8]:>17,.2f}" +
                     f"{int(noacc[9]):>9,}" + f"{brhamt[9]:>17,.2f}")
            f.write(line2 + "\n")
            
            # LINE 3
            line3 = ("    " +
                     f"{int(noacc[10]):>7,}" + f"{brhamt[10]:>16,.2f}" +
                     f"{int(noacc[11]):>8,}" + f"{brhamt[11]:>15,.2f}" +
                     f"{int(noacc[12]):>8,}" + f"{brhamt[12]:>15,.2f}" +
                     f"{int(noacc[13]):>9,}" + f"{brhamt[13]:>17,.2f}" +
                     f"{int(noacc[14]):>9,}" + f"{brhamt[14]:>17,.2f}")
            f.write(line3 + "\n")
            
            # LINE 4
            line4 = ("    " +
                     f"{int(noacc[15]):>7,}" + f"{brhamt[15]:>16,.2f}" +
                     f"{int(noacc[16]):>8,}" + f"{brhamt[16]:>15,.2f}" +
                     f"{int(subacc):>8,}" + f"{subbrh:>15,.2f}" +
                     f"{int(subac2):>9,}" + f"{subbr2:>17,.2f}" +
                     f"{int(sotacc):>9,}" + f"{totbrh:>17,.2f}")
            f.write(line4 + "\n")
        
        # Print category totals (matches SAS LAST.CAT logic)
        sgtotbrh = np.sum(totamt[3:17])
        sgtotbr2 = sgtotbrh - totamt[3] - totamt[4] - totamt[5]
        sgtotacc = np.sum(totacc[3:17])
        sgtotac2 = sgtotacc - totacc[3] - totacc[4] - totacc[5]
        gtotbrh = sgtotbrh + totamt[0] + totamt[1] + totamt[2]
        gtotacc = sgtotacc + totacc[0] + totacc[1] + totacc[2]
        
        f.write("-" * 40 + "-" * 40 + "-" * 40 + "-" * 10 + "\n")
        
        # TOT Line 1
        line1 = ("TOT" +
                 f"{int(totacc[0]):>7,}" + f"{totamt[0]:>16,.2f}" +
                 f"{int(totacc[1]):>8,}" + f"{totamt[1]:>15,.2f}" +
                 f"{int(totacc[2]):>8,}" + f"{totamt[2]:>15,.2f}" +
                 f"{int(totacc[3]):>9,}" + f"{totamt[3]:>17,.2f}" +
                 f"{int(totacc[4]):>9,}" + f"{totamt[4]:>17,.2f}")
        f.write(line1 + "\n")
        
        # TOT Line 2
        line2 = ("    " +
                 f"{int(totacc[5]):>7,}" + f"{totamt[5]:>16,.2f}" +
                 f"{int(totacc[6]):>8,}" + f"{totamt[6]:>15,.2f}" +
                 f"{int(totacc[7]):>8,}" + f"{totamt[7]:>15,.2f}" +
                 f"{int(totacc[8]):>9,}" + f"{totamt[8]:>17,.2f}" +
                 f"{int(totacc[9]):>9,}" + f"{totamt[9]:>17,.2f}")
        f.write(line2 + "\n")
        
        # TOT Line 3
        line3 = ("    " +
                 f"{int(totacc[10]):>7,}" + f"{totamt[10]:>16,.2f}" +
                 f"{int(totacc[11]):>8,}" + f"{totamt[11]:>15,.2f}" +
                 f"{int(totacc[12]):>8,}" + f"{totamt[12]:>15,.2f}" +
                 f"{int(totacc[13]):>9,}" + f"{totamt[13]:>17,.2f}" +
                 f"{int(totacc[14]):>9,}" + f"{totamt[14]:>17,.2f}")
        f.write(line3 + "\n")
        
        # TOT Line 4
        line4 = ("    " +
                 f"{int(totacc[15]):>7,}" + f"{totamt[15]:>16,.2f}" +
                 f"{int(totacc[16]):>8,}" + f"{totamt[16]:>15,.2f}" +
                 f"{int(sgtotacc):>8,}" + f"{sgtotbrh:>15,.2f}" +
                 f"{int(sgtotac2):>9,}" + f"{sgtotbr2:>17,.2f}" +
                 f"{int(gtotacc):>9,}" + f"{gtotbrh:>17,.2f}")
        f.write(line4 + "\n")
        
        f.write("-" * 40 + "-" * 40 + "-" * 40 + "-" * 10 + "\n")
        f.write("\n")
        
        if cat_idx < len(categories) - 1:
            f.write("\f")  # Form feed for page break

print(f"SAVED: {OUTPUT_TXT}")

print("\n" + "="*80)
print("REPORT COMPLETE")
print("="*80)

con.close()
