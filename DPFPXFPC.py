"""
EIBDLNS2 - Branch Daily Outstanding Loan Summary Report
1:1 conversion from SAS to Python
Generates branch summary on daily outstanding amounts for BAE Personal loans
"""

import duckdb
from datetime import timedelta
from pathlib import Path
import pyarrow.parquet as pq
import saspy

# Initialize paths
OUTPUT_DIR = Path("stgsrcsys/host/holding")
MIS_DIR = Path("/host_pq/dwh/mis")

# Input files (macro vars resolved in Python)
DATEFILE = Path("/host_pq/dwh/input/LOAN/DATEFILE")
FEEFILE = Path("/host_pq/dwh/input/LOAN/NFEEFILE_{reptyear}{reptmon}{reptday}")
ACCTFILE = Path("/host_pq/dwh/input/LOAN/ACCTFILE_{reptyear}{reptmon}{reptday}")
BRANCHF = Path("/sasdata/rawdata/lookup/LKP_BRANCH")

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
extdate_str = str(EXTDATE).zfill(11)[:8]
REPTDATE = con.execute(f"SELECT CAST('{extdate_str}' AS DATE)").fetchone()[0]

# SAS: PREVDATE = REPTDATE -1;  ->  timedelta(days=1)
PREVDATE = REPTDATE - timedelta(days=1)
DLETDATE = REPTDATE - timedelta(days=3)
REPTDAY = REPTDATE.day
PREVDAY = PREVDATE.day
DLETDAY = DLETDATE.day

if REPTDATE.month == 1 and REPTDATE.day == 1:
    YY = REPTDATE.year - 1
else:
    YY = REPTDATE.year

REPTYEAR = str(REPTDATE.year)
PREVYEAR = str(YY).zfill(4)
REPTMON = str(REPTDATE.month).zfill(2)
REPTDAY_STR = str(REPTDAY).zfill(2)
PREVDAY_STR = str(PREVDAY).zfill(2)
DLETDAY_STR = str(DLETDAY).zfill(2)
RDATE = REPTDATE.strftime('%d%m%Y')
REPTDATE_INT = int(REPTDATE.strftime('%y%m%d'))

print(f"Report Date: {REPTDATE.strftime('%d/%m/%Y')}")

# Resolve actual filenames with macro substitution
feefile_path = str(FEEFILE).format(
    reptyear=REPTYEAR, reptmon=REPTMON, reptday=REPTDAY_STR
)
acctfile_path = str(ACCTFILE).format(
    reptyear=REPTYEAR, reptmon=REPTMON, reptday=REPTDAY_STR
)

# ============================================================================
# DATA FEPLAN
# ============================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE feplan AS
    SELECT acctno, noteno, loantype, feepln, feeamta, feeamtc, feeamtb
    FROM read_parquet('{feefile_path}')
    WHERE acctno < 3000000000
      AND loantype IN (135, 136)
      AND feepln = 'PA'
""")

# PROC SUMMARY + SORT
con.execute("""
    CREATE OR REPLACE TABLE feepo AS
    SELECT acctno, noteno,
           SUM(feeamta) AS feeamta,
           SUM(feeamtb) AS feeamtb,
           SUM(feeamtc) AS feeamtc
    FROM feplan
    GROUP BY acctno, noteno
    ORDER BY acctno, noteno
""")

# ============================================================================
# DATA LOAN
# ============================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE loan_raw AS
    SELECT
        acctno, name, bankno, accbrch, noteno, reversed, loantype,
        ntbrch, pendbrh, lasttran, curbal, intamt, paidind, ntint,
        intearn, accrual, intearn2, intearn3, intearn4,
        feeamt, feeamt2, feeamt4, nfeeamt5, nfeeamt6, nfeeamt7,
        feeamt8, feeamt9, feeamt13, feeamt10, feeamt11, feeamt12,
        feeamt14, feeamt15, feeamt16,
        CASE
            WHEN pendbrh != 0 THEN pendbrh
            WHEN ntbrch != 0 THEN ntbrch
            ELSE accbrch
        END AS branch
    FROM read_parquet('{acctfile_path}')
    WHERE loantype IN (135, 136)
      AND (
            ((reversed IS NULL OR reversed != 'Y')
             AND noteno IS NOT NULL
             AND (paidind IS NULL OR paidind != 'P'))
            OR
            (paidind = 'P' AND lasttran = {REPTDATE_INT})
          )
""")

# MERGE LOAN with FEEPO + derive FEEAMT / BALANCE
con.execute("""
    CREATE OR REPLACE TABLE loan AS
    SELECT
        l.acctno, l.name, l.bankno, l.accbrch, l.noteno, l.reversed,
        l.loantype, l.ntbrch, l.pendbrh, l.lasttran, l.curbal, l.intamt,
        l.paidind, l.ntint, l.intearn, l.accrual, l.intearn2, l.intearn3,
        l.intearn4, l.feeamt2, l.feeamt4, l.nfeeamt5, l.nfeeamt6,
        l.nfeeamt7, l.feeamt8, l.feeamt9, l.feeamt13, l.feeamt10,
        l.feeamt11, l.feeamt12, l.feeamt14, l.feeamt15, l.feeamt16,
        l.branch,
        COALESCE(f.feeamta, 0) AS feeamta,
        COALESCE(f.feeamtb, 0) AS feeamtb,
        COALESCE(f.feeamtc, 0) AS feeamtc,
        CASE
            WHEN l.acctno > 8000000000 AND l.loantype IN (720, 725)
            THEN l.feeamt + COALESCE(f.feeamta, 0) + COALESCE(f.feeamtc, 0)
            ELSE l.feeamt + COALESCE(f.feeamta, 0)
        END AS feeamt,
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
        END AS balance
    FROM loan_raw l
    LEFT JOIN feepo f ON l.acctno = f.acctno AND l.noteno = f.noteno
""")

# ============================================================================
# PROC SUMMARY DATA=LOAN NWAY  (reptdate removed from output)
# ============================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE loan_summary AS
    SELECT
        branch,
        {EXTDATE} AS extdate,
        COUNT(*) AS noacct,
        SUM(balance) AS brlnamt
    FROM loan
    GROUP BY branch, extdate
    ORDER BY branch
""")

# DATA MIS.LOAN&REPTDAY
pq.write_table(
    con.execute("SELECT * FROM loan_summary").arrow(),
    MIS_DIR / f"LOAN{REPTDAY_STR}.parquet"
)

# ============================================================================
# DATA PREVLN  ->  current-day records from previous-day file
# ============================================================================
prev_file = MIS_DIR / f"LOAN{PREVDAY_STR}.parquet"
if prev_file.exists():
    con.execute(f"""
        CREATE OR REPLACE TABLE prevln AS
        SELECT branch, brlnamt AS pbrlnamt
        FROM read_parquet('{prev_file}')
        ORDER BY branch
    """)
else:
    con.execute("""
        CREATE OR REPLACE TABLE prevln AS
        SELECT branch, CAST(0.0 AS DOUBLE) AS pbrlnamt
        FROM loan_summary WHERE 1=0
    """)

# DATA LOANS
con.execute("""
    CREATE OR REPLACE TABLE loans AS
    SELECT
        COALESCE(l.branch, p.branch) AS branch,
        l.extdate,
        l.noacct,
        l.brlnamt,
        COALESCE(p.pbrlnamt, 0) AS pbrlnamt
    FROM loan_summary l
    FULL OUTER JOIN prevln p ON l.branch = p.branch
""")

# ============================================================================
# DATA BRANCH
# ============================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE branch AS
    SELECT bank, branch, abbrev, brchname
    FROM read_parquet('{BRANCHF}')
    ORDER BY branch
""")

# DATA MLOAN
con.execute("""
    CREATE OR REPLACE TABLE mloan AS
    SELECT
        COALESCE(l.branch, b.branch) AS branch,
        b.bank, b.abbrev, b.brchname,
        l.extdate,
        COALESCE(l.noacct, 0) AS noacct,
        l.pbrlnamt, l.brlnamt,
        l.brlnamt - l.pbrlnamt AS varianln
    FROM loans l
    LEFT JOIN branch b ON l.branch = b.branch
""")

# ============================================================================
# PROC TABULATE equivalent (no reptdate)
# ============================================================================
report = con.execute("""
    SELECT branch AS code, abbrev, brchname AS name,
           noacct AS no_of_accounts,
           pbrlnamt AS prev_amount,
           brlnamt  AS curr_amount,
           varianln AS variance
    FROM mloan
    WHERE branch IS NOT NULL
    UNION ALL
    SELECT 999999, 'TOTAL', 'TOTAL',
           SUM(noacct), SUM(pbrlnamt), SUM(brlnamt), SUM(varianln)
    FROM mloan
    ORDER BY code
""").arrow()

# ============================================================================
# Output: Parquet + SAS7BDAT via saspy
# ============================================================================
output_base = f"EIBDLNS2_Branch_Loan_Summary_{REPTDATE.strftime('%Y%m%d')}"

parquet_file = OUTPUT_DIR / f"{output_base}.parquet"
pq.write_table(report, parquet_file)
print(f"Parquet saved: {parquet_file}")

# --- SAS7BDAT via saspy ---
sas = saspy.SASsession(cfgname='default')   # adjust cfgname to your SAS profile

sas.df2sd(report.to_pandas(), table='EIBDLNS2', libref='WORK')

# Write out to disk as .sas7bdat
sas.submit(f"""
    libname out "{OUTPUT_DIR}";
    data out.{output_base};
        set WORK.EIBDLNS2;
    run;
""")
print(f"SAS7BDAT saved: {OUTPUT_DIR / (output_base + '.sas7bdat')}")

sas.endsas()

# ============================================================================
# Display report (mimics PROC TABULATE output)
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
    print(f"{code:<8} {abbrev:<8} {name:<35} {accounts:>12,} "
          f"{prev_amt:>20,.2f} {curr_amt:>20,.2f} {variance:>20,.2f}")

print("="*130)
con.close()
print("\nProcessing complete!")
