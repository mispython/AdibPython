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

# INITIALIZE PATHS
LOANTEMP_PATH = Path("/dwh/dwh_m/tempsource")
LKP_PATH = Path("/sasdata/rawdata/lookup")
OUTPUT_DIR = Path("/host_pq/mis/output/report/EIMIR")

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
# SAS: INPUT @2 BRANCH 3. @6 BRHCODE $3.;
# Position 2-4 = BRANCH (3 chars), Position 6-8 = BRHCODE (3 chars)
# ============================================================================
BRANCH_FILE = LKP_PATH / "LKP_BRANCH"

if not BRANCH_FILE.exists():
    print(f"ERROR: BRANCH FILE NOT FOUND: {BRANCH_FILE}")
    exit(1)

with open(BRANCH_FILE, 'r') as f:
    lines = f.readlines()

branch_data = []
for line in lines:
    line = line.rstrip('\n').rstrip('\r')
    if len(line) >= 8:
        # SAS @2 BRANCH 3. -> 1-indexed pos 2-4, 0-indexed 1:4
        branch = line[1:4].strip()
        # SAS @6 BRHCODE $3. -> 1-indexed pos 6-8, 0-indexed 5:8
        brhcode = line[5:8].strip()
        if branch:
            branch_data.append((branch, brhcode))

con.execute("""
    CREATE OR REPLACE TABLE BRHDATA (
        BRANCH VARCHAR,
        BRHCODE VARCHAR
    )
""")

for branch, brhcode in branch_data:
    # Escape single quotes in branch/brhcode
    b_esc = branch.replace("'", "''")
    h_esc = brhcode.replace("'", "''")
    con.execute(f"INSERT INTO BRHDATA VALUES ('{b_esc}', '{h_esc}')")

branch_count = con.execute("SELECT COUNT(*) FROM BRHDATA").fetchone()[0]
print(f"BRANCH RECORDS: {branch_count:,}")

# ============================================================================
# READ LOAN DATA (LOANTEMP.sas7bdat)
# ============================================================================
LOANTEMP_FILE = LOANTEMP_PATH / "loantemp_iccd2.sas7bdat"

if not LOANTEMP_FILE.exists():
    print(f"ERROR: LOANTEMP FILE NOT FOUND: {LOANTEMP_FILE}")
    exit(1)

print("\nREADING LOANTEMP.SAS7BDAT...")
df, meta = pyreadstat.read_sas7bdat(str(LOANTEMP_FILE))
con.execute("CREATE OR REPLACE TABLE LOANTEMP AS SELECT * FROM df")
print(f"LOANTEMP RECORDS: {con.execute('SELECT COUNT(*) FROM LOANTEMP').fetchone()[0]:,}")

# ============================================================================
# PROCESS LOAN DATA - CREATE CATEGORIES
# Mirrors SAS DATA LOAN1 step:
#   IF ARREAR > 6 OR BORSTAT = 'R' OR BORSTAT = 'I' OR BORSTAT = 'F'
#   THEN IF (product conditions) AND CHECKDT = 1 THEN OUTPUT (multiple times)
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
""")

# Union all 4 categories (mimics the 4 OUTPUT statements in SAS DATA step)
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

# Merge with BRHDATA (SAS: MERGE LOAN1 BRHDATA; BY BRANCH; IF PRESENT=1)
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
# Mirrors SAS DATA TRY step: BY CAT BRANCH, ARRAY BRHAMT{17}, NOACC{17}
# ============================================================================
print("AGGREGATING BY ARREARS BUCKETS...")

con.execute("""
    CREATE OR REPLACE TABLE BRANCH_SUMMARY AS
    SELECT 
        CAT, TYPE, BRANCH, BRHCODE, ARREAR,
        COUNT(*) AS NOACC,
        SUM(BALANCE) AS BRHAMT
    FROM LOAN1_FINAL
    WHERE BALANCE > 0
    GROUP BY CAT, TYPE, BRANCH, BRHCODE, ARREAR
    ORDER BY CAT, BRANCH, ARREAR
""")

con.execute("""
    CREATE OR REPLACE TABLE BRANCH_PIVOT AS
    SELECT 
        CAT, TYPE, BRANCH, BRHCODE,
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
# Mirrors SAS: SUBBRH = SUM(BRHAMT4..17), SUBBR2 = SUBBRH - 4 - 5 - 6,
#              SUBACC = SUM(NOACC4..17),  SUBAC2 = SUBACC - 4 - 5 - 6,
#              TOTBRH = SUM(SUBBRH, BRHAMT1-3), SOTACC = SUM(SUBACC, NOACC1-3)
# ============================================================================
con.execute("""
    CREATE OR REPLACE TABLE REPORT_DATA AS
    SELECT 
        *,
        (NOACC4 + NOACC5 + NOACC6 + NOACC7 + NOACC8 + NOACC9 + NOACC10 +
         NOACC11 + NOACC12 + NOACC13 + NOACC14 + NOACC15 + NOACC16 + NOACC17) AS SUBACC,
        (BRHAMT4 + BRHAMT5 + BRHAMT6 + BRHAMT7 + BRHAMT8 + BRHAMT9 + BRHAMT10 +
         BRHAMT11 + BRHAMT12 + BRHAMT13 + BRHAMT14 + BRHAMT15 + BRHAMT16 + BRHAMT17) AS SUBBRH,
        (NOACC7 + NOACC8 + NOACC9 + NOACC10 + NOACC11 + NOACC12 + 
         NOACC13 + NOACC14 + NOACC15 + NOACC16 + NOACC17) AS SUBAC2,
        (BRHAMT7 + BRHAMT8 + BRHAMT9 + BRHAMT10 + BRHAMT11 + BRHAMT12 + 
         BRHAMT13 + BRHAMT14 + BRHAMT15 + BRHAMT16 + BRHAMT17) AS SUBBR2,
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

OUTPUT_TXT = OUTPUT_DIR / f"CCDTXT3.txt"

CATEGORIES = con.execute("SELECT DISTINCT CAT, TYPE FROM REPORT_DATA ORDER BY CAT").fetchall()

# ----------------------------------------------------------------------------
# SAS COLUMN MAP (1-indexed positions from the PUT statements):
#   @1   -> BRANCH / BRHCODE / TOT   (width 3)
#   @5   -> NOACC   COMMA7.0  -> width 7,  0 decimals
#   @13  -> BRHAMT  COMMA16.2 -> width 16, 2 decimals
#   @30  -> NOACC   COMMA7.0  -> width 7,  0 decimals
#   @38  -> BRHAMT  COMMA15.2 -> width 15, 2 decimals
#   @54  -> NOACC   COMMA7.0  -> width 7,  0 decimals
#   @62  -> BRHAMT  COMMA15.2 -> width 15, 2 decimals
#   @78  -> NOACC   COMMA8.0  -> width 8,  0 decimals
#   @87  -> BRHAMT  COMMA17.2 -> width 17, 2 decimals
#   @105 -> NOACC   COMMA8.0  -> width 8,  0 decimals
#   @114 -> BRHAMT  COMMA17.2 -> width 17, 2 decimals
# Separator: @1'-'*40, @41'-'*40, @81'-'*40, @121'-'*10 => total width 130
# ----------------------------------------------------------------------------
LINE_WIDTH = 130

FIELD_COLS    = [5,  13, 30, 38, 54, 62, 78, 87, 105, 114]
FIELD_WIDTHS  = [7,  16,  7, 15,  7, 15,  8, 17,   8,  17]
FIELD_DECIMALS = [0,  2,  0,  2,  0,  2,  0,  2,   0,   2]


def fmt_num(val, width, decimals):
    """Format number like SAS COMMA w.d — right-justified, comma-separated.
    If it doesn't fit, SAS prints * for the field. We mimic that."""
    if val is None:
        val = 0
    try:
        if decimals == 0:
            s = f"{int(round(float(val))):,}"
        else:
            s = f"{float(val):,.2f}"
    except (ValueError, TypeError):
        s = "0"
    # SAS overflow behavior: if the formatted value is wider than the field,
    # the field is filled with asterisks
    if len(s) > width:
        return "*" * width
    return s.rjust(width)


def build_line(lead_text, values):
    """Build one fixed-column report line mirroring SAS PUT @col value FORMAT."""
    buf = [' '] * LINE_WIDTH
    if lead_text:
        for i, ch in enumerate(lead_text):
            if i < LINE_WIDTH:
                buf[i] = ch
    for col, width, dec, val in zip(FIELD_COLS, FIELD_WIDTHS, FIELD_DECIMALS, values):
        text = fmt_num(val, width, dec)
        start = col - 1
        end = start + len(text)
        if end > len(buf):
            buf.extend([' '] * (end - len(buf)))
        buf[start:end] = list(text)
    return ''.join(buf).rstrip()


def text_line(segments, width=LINE_WIDTH):
    """Place literal text segments at fixed 1-indexed columns (SAS @N 'literal')."""
    buf = [' '] * width
    for col, text in segments:
        start = col - 1
        end = start + len(text)
        if end > len(buf):
            buf.extend([' '] * (end - len(buf)))
        buf[start:end] = list(text)
    return ''.join(buf).rstrip()


with open(OUTPUT_TXT, 'w') as f:
    for cat_idx, (CAT, TYPE) in enumerate(CATEGORIES):
        PAGE_NUM = cat_idx + 1

        # ---- SAS NEWPAGE header ----
        f.write(f"PROGRAM-ID : EIMAR202                     P U B L I C   I S L A M I C   B A N K   B E R H A D                        PAGE NO.: {PAGE_NUM}\n")
        f.write(f"                                   OUTSTANDING LOANS CLASSIFIED AS NPL ISSUED FROM 1 JAN 1998        {TYPE:<13}       {RDATE}\n")
        f.write("\n")
        f.write(text_line([(1, "BRH     NO         < 1 MTH"), (34, "NO     1 TO < 2 MTH"),
                            (59, "NO     2 TO < 3 MTH"), (84, "NO      3 TO < 4 MTH"),
                            (111, "NO      4 TO < 5 MTH")]) + "\n")
        f.write(text_line([(1, "        NO    5 TO < 6 MTH"), (34, "NO     6 TO < 7 MTH"),
                            (59, "NO     7 TO < 8 MTH"), (84, "NO      8 TO < 9 MTH"),
                            (111, "NO     9 TO < 10 MTH")]) + "\n")
        f.write(text_line([(1, "        NO  10 TO < 11 MTH"), (34, "NO   11 TO < 12 MTH"),
                            (59, "NO   12 TO < 18 MTH"), (84, "NO    18 TO < 24 MTH"),
                            (111, "NO    24 TO < 36 MTH")]) + "\n")
        f.write(text_line([(1, "        NO        > 36 MTH"), (34, "NO          DEFICIT"),
                            (59, "NO   SUBTOTAL >=3MTH"), (84, "NO   SUBTOTAL >=6MTH"),
                            (111, "NO             TOTAL")]) + "\n")
        # SAS separator: @1'-'*40, @41'-'*40, @81'-'*40, @121'-'*10
        f.write(text_line([(1, "-"*40), (41, "-"*40), (81, "-"*40), (121, "-"*10)]) + "\n")

        branches = con.execute(f"""
            SELECT * FROM REPORT_DATA 
            WHERE CAT = '{CAT}'
            ORDER BY BRANCH
        """).fetchall()

        TOTALS = [0] * 40

        for branch_row in branches:
            BRANCH = str(int(float(branch_row[2]))).zfill(3)
            BRHCODE = str(branch_row[3] or '').strip()
            if not BRHCODE:
                BRHCODE = '   '

            SUBACC = branch_row[38]
            SUBBRH = branch_row[39]
            SUBAC2 = branch_row[40]
            SUBBR2 = branch_row[41]
            SOTACC = branch_row[42]
            TOTBRH = branch_row[43]

            # LINE 1: BRANCH + <1MTH, 1-2MTH, 2-3MTH, 3-4MTH, 4-5MTH
            f.write(build_line(BRANCH, [branch_row[4],  branch_row[5],
                                         branch_row[6],  branch_row[7],
                                         branch_row[8],  branch_row[9],
                                         branch_row[10], branch_row[11],
                                         branch_row[12], branch_row[13]]) + "\n")

            # LINE 2: BRHCODE + 5-6MTH, 6-7MTH, 7-8MTH, 8-9MTH, 9-10MTH
            f.write(build_line(BRHCODE, [branch_row[14], branch_row[15],
                                          branch_row[16], branch_row[17],
                                          branch_row[18], branch_row[19],
                                          branch_row[20], branch_row[21],
                                          branch_row[22], branch_row[23]]) + "\n")

            # LINE 3: 10-11MTH, 11-12MTH, 12-18MTH, 18-24MTH, 24-36MTH
            f.write(build_line('   ', [branch_row[24], branch_row[25],
                                        branch_row[26], branch_row[27],
                                        branch_row[28], branch_row[29],
                                        branch_row[30], branch_row[31],
                                        branch_row[32], branch_row[33]]) + "\n")

            # LINE 4: >36MTH, DEFICIT, SUBTOTAL>=3MTH, SUBTOTAL>=6MTH, TOTAL
            f.write(build_line('   ', [branch_row[34], branch_row[35],
                                        branch_row[36], branch_row[37],
                                        SUBACC, SUBBRH,
                                        SUBAC2, SUBBR2,
                                        SOTACC, TOTBRH]) + "\n")

            for i in range(4, 38):
                TOTALS[i - 4] += branch_row[i]
            TOTALS[34] += SUBACC
            TOTALS[35] += SUBBRH
            TOTALS[36] += SUBAC2
            TOTALS[37] += SUBBR2
            TOTALS[38] += SOTACC
            TOTALS[39] += TOTBRH

        # ---- SAS LAST.CAT block: category totals ----
        # NOTE: SAS computes SGTOTBRH etc. using TOTAMT1..17 / TOTACC1..17
        # TOTALS layout: [0..16] = TOTACC1..17, [17..33] = TOTAMT1..17,
        #                [34] = SGTOTACC, [35] = SGTOTBRH,
        #                [36] = SGTOTAC2, [37] = SGTOTBR2,
        #                [38] = GTOTACC,  [39] = GTOTBRH
        # The per-branch loop above accumulates the raw NOACC/BRHAMT into
        # positions 0..33 (interleaved as NOACC1,BRHAMT1,NOACC2,...). We need
        # to re-derive the SAS-ordered sums here.
        #
        # Actually, to stay faithful: rebuild from raw arrays.
        TOTACC_arr = [0]*17
        TOTAMT_arr = [0]*17
        # TOTALS was accumulated interleaved NOACC/BRHAMT — re-derive arrays:
        for i in range(17):
            TOTACC_arr[i] = TOTALS[i*2]
            TOTAMT_arr[i] = TOTALS[i*2 + 1]

        sgtotbrh = sum(TOTAMT_arr[3:17])
        sgtotbr2 = sgtotbrh - TOTAMT_arr[3] - TOTAMT_arr[4] - TOTAMT_arr[5]
        sgtotacc = sum(TOTACC_arr[3:17])
        sgtotac2 = sgtotacc - TOTACC_arr[3] - TOTACC_arr[4] - TOTACC_arr[5]
        gtotbrh  = sgtotbrh + TOTAMT_arr[0] + TOTAMT_arr[1] + TOTAMT_arr[2]
        gtotacc  = sgtotacc + TOTACC_arr[0] + TOTACC_arr[1] + TOTACC_arr[2]

        # SAS separator
        f.write(text_line([(1, "-"*40), (41, "-"*40), (81, "-"*40), (121, "-"*10)]) + "\n")

        # TOT line 1 (SAS order: TOTACC1,TOTAMT1,TOTACC2,TOTAMT2,...,TOTACC5,TOTAMT5)
        f.write(build_line("TOT", [
            TOTACC_arr[0], TOTAMT_arr[0],
            TOTACC_arr[1], TOTAMT_arr[1],
            TOTACC_arr[2], TOTAMT_arr[2],
            TOTACC_arr[3], TOTAMT_arr[3],
            TOTACC_arr[4], TOTAMT_arr[4],
        ]) + "\n")

        # TOT line 2
        f.write(build_line('   ', [
            TOTACC_arr[5], TOTAMT_arr[5],
            TOTACC_arr[6], TOTAMT_arr[6],
            TOTACC_arr[7], TOTAMT_arr[7],
            TOTACC_arr[8], TOTAMT_arr[8],
            TOTACC_arr[9], TOTAMT_arr[9],
        ]) + "\n")

        # TOT line 3
        f.write(build_line('   ', [
            TOTACC_arr[10], TOTAMT_arr[10],
            TOTACC_arr[11], TOTAMT_arr[11],
            TOTACC_arr[12], TOTAMT_arr[12],
            TOTACC_arr[13], TOTAMT_arr[13],
            TOTACC_arr[14], TOTAMT_arr[14],
        ]) + "\n")

        # TOT line 4 (with grand subtotals and grand totals)
        f.write(build_line('   ', [
            TOTACC_arr[15], TOTAMT_arr[15],
            TOTACC_arr[16], TOTAMT_arr[16],
            sgtotacc, sgtotbrh,
            sgtotac2, sgtotbr2,
            gtotacc, gtotbrh,
        ]) + "\n")

        # SAS separator
        f.write(text_line([(1, "-"*40), (41, "-"*40), (81, "-"*40), (121, "-"*10)]) + "\n")
        f.write("\n")

        # SAS resets PAGECNT=0 after LAST.CAT but HEADER=NEWPAGE handles
        # the page break at FIRST.CAT of the next group. We insert a form-feed
        # between categories to mimic a physical page break.
        if cat_idx < len(CATEGORIES) - 1:
            f.write("\f")

print(f"SAVED: {OUTPUT_TXT}")

print("\n" + "="*80)
print("REPORT COMPLETE")
print("="*80)

con.close()
