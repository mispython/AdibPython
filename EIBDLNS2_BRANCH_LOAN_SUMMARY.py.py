#!/usr/bin/env python3
"""
EIBDLNS2 - Branch Daily Outstanding Loan Summary Report
1:1 conversion from SAS to Python
Generates branch summary on daily outstanding amounts for BAE Personal loans
"""

import duckdb
from datetime import timedelta
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv

# Initialize paths
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
MIS_DIR = OUTPUT_DIR / "mis"
MIS_DIR.mkdir(parents=True, exist_ok=True)

# Input files
DATEFILE = INPUT_DIR / "datefile.parquet"
FEEFILE = INPUT_DIR / "feefile.parquet"
ACCTFILE = INPUT_DIR / "acctfile.parquet"
BRANCHF = INPUT_DIR / "branchfile.parquet"

# Connect to DuckDB
con = duckdb.connect(":memory:")

print("REPORT ID : EIBDLNSA")

# ============================================================================
# DATA REPTDATE - Extract and calculate report dates
# ============================================================================
result = con.execute(f"""
    SELECT extdate
    FROM read_parquet('{DATEFILE}')
    LIMIT 1
""").fetchone()

EXTDATE = result[0]
# Convert EXTDATE (format: YYMMDDXXXXX) to date by taking first 8 chars
extdate_str = str(EXTDATE).zfill(11)[:8]
REPTDATE = con.execute(f"SELECT CAST('{extdate_str}' AS DATE)").fetchone()[0]

PREVDATE = REPTDATE - timedelta(days=1)
DLETDATE = REPTDATE - timedelta(days=3)
REPTDAY = REPTDATE.day
PREVDAY = PREVDATE.day
DLETDAY = DLETDATE.day

# Determine year for report
if REPTDATE.month == 1 and REPTDATE.day == 1:
    YY = REPTDATE.year - 1
else:
    YY = REPTDATE.year

# Create macro variables (stored as Python variables)
REPTYEAR = str(REPTDATE.year)
PREVYEAR = str(YY).zfill(4)
REPTMON = str(REPTDATE.month).zfill(2)
REPTDAY_STR = str(REPTDAY).zfill(2)
PREVDAY_STR = str(PREVDAY).zfill(2)
DLETDAY_STR = str(DLETDAY).zfill(2)
RDATE = REPTDATE.strftime('%d%m%Y')
REPTDATE_INT = int(REPTDATE.strftime('%y%m%d'))

print(f"Report Date: {REPTDATE.strftime('%d/%m/%Y')}")

# ============================================================================
# DATA FEPLAN - Read and filter fee file
# ============================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE feplan AS
    SELECT 
        acctno,
        noteno,
        loantype,
        feepln,
        feeamta,
        feeamtc,
        feeamtb
    FROM read_parquet('{FEEFILE}')
    WHERE acctno < 3000000000 
        AND loantype IN (135, 136)
        AND feepln = 'PA'
""")

# ============================================================================
# PROC SUMMARY - Aggregate fee data by account and note
# ============================================================================
con.execute("""
    CREATE OR REPLACE TABLE feepo AS
    SELECT 
        acctno,
        noteno,
        SUM(feeamta) as feeamta,
        SUM(feeamtb) as feeamtb,
        SUM(feeamtc) as feeamtc
    FROM feplan
    GROUP BY acctno, noteno
""")

# ============================================================================
# PROC SORT - Sort by account and note
# ============================================================================
con.execute("""
    CREATE OR REPLACE TABLE feepo_sorted AS
    SELECT * FROM feepo
    ORDER BY acctno, noteno
""")
con.execute("DROP TABLE feepo")
con.execute("ALTER TABLE feepo_sorted RENAME TO feepo")

# ============================================================================
# DATA LOAN - Read and filter account file
# ============================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE loan_raw AS
    SELECT 
        acctno,
        name,
        bankno,
        accbrch,
        noteno,
        reversed,
        loantype,
        ntbrch,
        pendbrh,
        lasttran,
        curbal,
        intamt,
        paidind,
        ntint,
        intearn,
        accrual,
        intearn2,
        intearn3,
        intearn4,
        feeamt,
        feeamt2,
        feeamt4,
        nfeeamt5,
        nfeeamt6,
        nfeeamt7,
        feeamt8,
        feeamt9,
        feeamt13,
        feeamt10,
        feeamt11,
        feeamt12,
        feeamt14,
        feeamt15,
        feeamt16,
        CASE 
            WHEN pendbrh != 0 THEN pendbrh
            WHEN ntbrch != 0 THEN ntbrch
            ELSE accbrch
        END as branch
    FROM read_parquet('{ACCTFILE}')
    WHERE loantype IN (135, 136)
        AND (
            ((reversed IS NULL OR reversed != 'Y') 
             AND noteno IS NOT NULL 
             AND (paidind IS NULL OR paidind != 'P'))
            OR 
            (paidind = 'P' AND lasttran = {REPTDATE_INT})
        )
""")

# ============================================================================
# DATA LOAN - Merge with fee data and calculate balance
# ============================================================================
con.execute("""
    CREATE OR REPLACE TABLE loan AS
    SELECT 
        l.acctno,
        l.name,
        l.bankno,
        l.accbrch,
        l.noteno,
        l.reversed,
        l.loantype,
        l.ntbrch,
        l.pendbrh,
        l.lasttran,
        l.curbal,
        l.intamt,
        l.paidind,
        l.ntint,
        l.intearn,
        l.accrual,
        l.intearn2,
        l.intearn3,
        l.intearn4,
        l.feeamt as feeamt_orig,
        l.feeamt2,
        l.feeamt4,
        l.nfeeamt5,
        l.nfeeamt6,
        l.nfeeamt7,
        l.feeamt8,
        l.feeamt9,
        l.feeamt13,
        l.feeamt10,
        l.feeamt11,
        l.feeamt12,
        l.feeamt14,
        l.feeamt15,
        l.feeamt16,
        l.branch,
        COALESCE(f.feeamta, 0) as feeamta,
        COALESCE(f.feeamtb, 0) as feeamtb,
        COALESCE(f.feeamtc, 0) as feeamtc,
        CASE 
            WHEN l.acctno > 8000000000 AND l.loantype IN (720, 725)
            THEN l.feeamt + COALESCE(f.feeamta, 0) + COALESCE(f.feeamtc, 0)
            ELSE l.feeamt + COALESCE(f.feeamta, 0)
        END as feeamt,
        CASE 
            WHEN l.ntint = 'A' 
            THEN l.curbal + l.intearn + (-1 * l.intamt) + 
                 CASE 
                     WHEN l.acctno > 8000000000 AND l.loantype IN (720, 725)
                     THEN l.feeamt + COALESCE(f.feeamta, 0) + COALESCE(f.feeamtc, 0)
                     ELSE l.feeamt + COALESCE(f.feeamta, 0)
                 END
            ELSE l.curbal + l.accrual + 
                 CASE 
                     WHEN l.acctno > 8000000000 AND l.loantype IN (720, 725)
                     THEN l.feeamt + COALESCE(f.feeamta, 0) + COALESCE(f.feeamtc, 0)
                     ELSE l.feeamt + COALESCE(f.feeamta, 0)
                 END
        END as balance
    FROM loan_raw l
    LEFT JOIN feepo f ON l.acctno = f.acctno AND l.noteno = f.noteno
""")

# ============================================================================
# PROC SUMMARY - Aggregate by branch
# ============================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE loan_summary AS
    SELECT 
        branch,
        DATE '{REPTDATE}' as reptdate,
        {EXTDATE} as extdate,
        COUNT(*) as noacct,
        SUM(balance) as brlnamt
    FROM loan
    GROUP BY branch, reptdate, extdate
""")

# ============================================================================
# PROC SORT - Sort by branch
# ============================================================================
con.execute("""
    CREATE OR REPLACE TABLE loan_summary_sorted AS
    SELECT * FROM loan_summary
    ORDER BY branch
""")
con.execute("DROP TABLE loan_summary")
con.execute("ALTER TABLE loan_summary_sorted RENAME TO loan_summary")

# ============================================================================
# DATA MIS.LOAN&REPTDAY - Save current day summary
# ============================================================================
current_loan = con.execute("SELECT * FROM loan_summary").arrow()
pq.write_table(current_loan, MIS_DIR / f"LOAN{REPTDAY_STR}.parquet")

# ============================================================================
# DATA PREVLN - Load previous day data
# ============================================================================
prev_file = MIS_DIR / f"LOAN{PREVDAY_STR}.parquet"
if prev_file.exists():
    con.execute(f"""
        CREATE OR REPLACE TABLE prevln AS
        SELECT 
            branch,
            brlnamt as pbrlnamt
        FROM read_parquet('{prev_file}')
    """)
else:
    # If previous day file doesn't exist, create empty table with zero values
    con.execute("""
        CREATE OR REPLACE TABLE prevln AS
        SELECT 
            branch,
            CAST(0.0 AS DOUBLE) as pbrlnamt
        FROM loan_summary
        WHERE 1=0
    """)

# ============================================================================
# PROC SORT - Sort previous data by branch
# ============================================================================
con.execute("""
    CREATE OR REPLACE TABLE prevln_sorted AS
    SELECT * FROM prevln
    ORDER BY branch
""")
con.execute("DROP TABLE prevln")
con.execute("ALTER TABLE prevln_sorted RENAME TO prevln")

# ============================================================================
# DATA LOANS - Merge current and previous data
# ============================================================================
con.execute("""
    CREATE OR REPLACE TABLE loans AS
    SELECT 
        COALESCE(l.branch, p.branch) as branch,
        l.reptdate,
        l.extdate,
        l.noacct,
        l.brlnamt,
        COALESCE(p.pbrlnamt, 0) as pbrlnamt
    FROM loan_summary l
    FULL OUTER JOIN prevln p ON l.branch = p.branch
""")

# ============================================================================
# DATA BRANCH - Read branch file
# ============================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE branch AS
    SELECT 
        bank,
        branch,
        abbrev,
        brchname
    FROM read_parquet('{BRANCHF}')
""")

# ============================================================================
# PROC SORT - Sort branch by branch code
# ============================================================================
con.execute("""
    CREATE OR REPLACE TABLE branch_sorted AS
    SELECT * FROM branch
    ORDER BY branch
""")
con.execute("DROP TABLE branch")
con.execute("ALTER TABLE branch_sorted RENAME TO branch")

# ============================================================================
# DATA MLOAN - Merge loans with branch data and calculate variance
# ============================================================================
con.execute("""
    CREATE OR REPLACE TABLE mloan AS
    SELECT 
        COALESCE(l.branch, b.branch) as branch,
        b.bank,
        b.abbrev,
        b.brchname,
        l.reptdate,
        l.extdate,
        COALESCE(l.noacct, 0) as noacct,
        l.pbrlnamt,
        l.brlnamt,
        l.brlnamt - l.pbrlnamt as varianln
    FROM loans l
    LEFT JOIN branch b ON l.branch = b.branch
""")

# ============================================================================
# PROC TABULATE - Generate final report with totals
# ============================================================================
report = con.execute("""
    SELECT 
        branch as code,
        abbrev,
        brchname as name,
        noacct as no_of_accounts,
        pbrlnamt as prev_amount,
        brlnamt as curr_amount,
        varianln as variance
    FROM mloan
    WHERE branch IS NOT NULL
    
    UNION ALL
    
    SELECT 
        999999 as code,
        'TOTAL' as abbrev,
        'TOTAL' as name,
        SUM(noacct) as no_of_accounts,
        SUM(pbrlnamt) as prev_amount,
        SUM(brlnamt) as curr_amount,
        SUM(varianln) as variance
    FROM mloan
    
    ORDER BY code
""").arrow()

# ============================================================================
# Output files
# ============================================================================
output_base = f"EIBDLNS2_Branch_Loan_Summary_{REPTDATE.strftime('%Y%m%d')}"

# Save as Parquet
parquet_file = OUTPUT_DIR / f"{output_base}.parquet"
pq.write_table(report, parquet_file)
print(f"Parquet saved: {parquet_file}")

# Save as CSV
csv_file = OUTPUT_DIR / f"{output_base}.csv"
csv.write_csv(report, csv_file)
print(f"CSV saved: {csv_file}")

# ============================================================================
# Display report (mimicking PROC TABULATE output)
# ============================================================================
print("\n" + "="*130)
print("PUBLIC BANK BERHAD")
print("BRANCH SUMMARY ON DAILY OUTSTANDING(RM)-BAE PERSONAL")
print(f"AS AT {RDATE}")
print("="*130)
print(f"{'BRANCH':<8} {'ABBREV':<8} {'NAME':<35} {'NO OF':>12} {'PREV.AMOUNT':>20} {'CURR.AMOUNT':>20} {'VARIANCE':>20}")
print(f"{'CODE':<8} {'':<8} {'':<35} {'ACCOUNTS':>12} {'(RM)':>20} {'(RM)':>20} {'(RM)':>20}")
print("-"*130)

for row in report.to_pylist():
    code = row['code']
    abbrev = row['abbrev'] or ''
    name = row['name'] or ''
    accounts = row['no_of_accounts']
    prev_amt = row['prev_amount']
    curr_amt = row['curr_amount']
    variance = row['variance']
    
    if code == 999999:
        print("-"*130)
    
    print(f"{code:<8} {abbrev:<8} {name:<35} {accounts:>12,} {prev_amt:>20,.2f} {curr_amt:>20,.2f} {variance:>20,.2f}")

print("="*130)

# Close connection
con.close()
print("\nProcessing complete!")
