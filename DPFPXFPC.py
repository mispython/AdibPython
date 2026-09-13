# -*- coding: utf-8 -*-
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

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
OUTPUT_DIR = Path("stgsrcsys/host/holding")
MIS_DIR    = Path("/host_pq/dwh/mis")

DATEFILE = Path("/host_pq/dwh/input/LOAN/DATEFILE")
FEEFILE  = "/host_pq/dwh/input/LOAN/NFEEFILE_{reptyear}{reptmon}{reptday}"
ACCTFILE = "/host_pq/dwh/input/LOAN/ACCTFILE_{reptyear}{reptmon}{reptday}"
BRANCHF  = Path("/sasdata/rawdata/lookup/LKP_BRANCH")

# ---------------------------------------------------------------------------
# DuckDB connection
# ---------------------------------------------------------------------------
con = duckdb.connect(":memory:")
print("REPORT ID : EIBDLNSA")

# ===========================================================================
# DATA REPTDATE - read single-line flat file
#   SAS: INPUT @01 EXTDATE 11.;
#        REPTDATE = INPUT(SUBSTR(PUT(EXTDATE, Z11.), 1, 8), MMDDYY8.);
# ===========================================================================
with open(DATEFILE, "r") as f:
    first_line = f.readline()

# Keep as a zero-padded string; do NOT cast to int first
EXTDATE_str = first_line[0:11].strip().zfill(11)   # e.g. "09122026000"
extdate_str = EXTDATE_str[:8]                      # "09122026" -> MMDDYYYY
EXTDATE     = int(EXTDATE_str)                     # numeric copy if needed

# MMDDYY8. means MMDDYYYY (8 chars), so use %m%d%Y (capital Y)
REPTDATE = con.execute(
    f"SELECT CAST(strptime('{extdate_str}', '%m%d%Y') AS DATE)"
).fetchone()[0]

# SAS: PREVDATE = REPTDATE - 1;
PREVDATE = REPTDATE - timedelta(days=1)
DLETDATE = REPTDATE - timedelta(days=3)
REPTDAY  = REPTDATE.day
PREVDAY  = PREVDATE.day
DLETDAY  = DLETDATE.day

if REPTDATE.month == 1 and REPTDATE.day == 1:
    YY = REPTDATE.year - 1
else:
    YY = REPTDATE.year

REPTYEAR     = str(REPTDATE.year)
PREVYEAR     = str(YY).zfill(4)
REPTMON      = str(REPTDATE.month).zfill(2)
REPTDAY_STR  = str(REPTDAY).zfill(2)
PREVDAY_STR  = str(PREVDAY).zfill(2)
DLETDAY_STR  = str(DLETDAY).zfill(2)
RDATE        = REPTDATE.strftime("%d%m%Y")
REPTDATE_INT = int(REPTDATE.strftime("%y%m%d"))

print(f"Report Date: {REPTDATE.strftime('%d/%m/%Y')}")

feefile_path  = FEEFILE.format(reptyear=REPTYEAR, reptmon=REPTMON, reptday=REPTDAY_STR)
acctfile_path = ACCTFILE.format(reptyear=REPTYEAR, reptmon=REPTMON, reptday=REPTDAY_STR)

# ===========================================================================
# DATA FEPLAN - fixed-width read of NFEEFILE
#   @001 ACCTNO    PD6
#   @007 NOTENO    PD3
#   @010 LOANTYPE  PD2
#   @022 FEEPLN    $2
#   @035 FEEAMTA   PD8.2
#   @067 FEEAMTC   PD8.2
#   @075 FEEAMTB   PD8.2
# ===========================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE feplan AS
    SELECT
        TRY_CAST(TRIM(SUBSTR(line,  1, 6)) AS BIGINT)  AS acctno,
        TRY_CAST(TRIM(SUBSTR(line,  7, 3)) AS BIGINT)  AS noteno,
        TRY_CAST(TRIM(SUBSTR(line, 10, 2)) AS INTEGER) AS loantype,
        SUBSTR(line, 22, 2)                            AS feepln,
        TRY_CAST(TRIM(SUBSTR(line, 35, 8)) AS DOUBLE)  AS feeamta,
        TRY_CAST(TRIM(SUBSTR(line, 67, 8)) AS DOUBLE)  AS feeamtc,
        TRY_CAST(TRIM(SUBSTR(line, 75, 8)) AS DOUBLE)  AS feeamtb
    FROM read_csv(
        '{feefile_path}',
        columns = {{'line': 'VARCHAR'}},
        header = false,
        delim  = '\\x01',
        quote  = ''
    )
    WHERE TRY_CAST(TRIM(SUBSTR(line, 1, 6)) AS BIGINT) < 3000000000
      AND TRY_CAST(TRIM(SUBSTR(line,10, 2)) AS INTEGER) IN (135, 136)
      AND SUBSTR(line, 22, 2) = 'PA'
""")

# ---------------------------------------------------------------------------
# PROC SUMMARY DATA=FEPLAN NWAY
# ---------------------------------------------------------------------------
con.execute("""
    CREATE OR REPLACE TABLE feepo AS
    SELECT
        acctno,
        noteno,
        SUM(feeamta) AS feeamta,
        SUM(feeamtb) AS feeamtb,
        SUM(feeamtc) AS feeamtc
    FROM feplan
    GROUP BY acctno, noteno
    ORDER BY acctno, noteno
""")

# ===========================================================================
# DATA LOAN - fixed-width read of ACCTFILE
#   @001 ACCTNO    PD6     @007 NAME      $24
#   @063 BANKNO    PD2     @066 ACCBRCH   PD4
#   @081 NOTENO    PD3     @084 REVERSED  $1
#   @085 LOANTYPE  PD2     @087 NTBRCH    PD4
#   @091 PENDBRH   PD4     @113 LASTTRAN  PD6
#   @121 CURBAL    PD8.2   @129 INTAMT    PD8.2
#   @261 PAIDIND   $1      @296 NTINT     $1
#   @311 INTEARN   PD8.2   @319 ACCRUAL   PD8.7
#   @400 INTEARN2  PD8.2   @408 INTEARN3  PD8.2
#   @416 INTEARN4  PD8.2   @457 FEEAMT    PD8.2
#   @465 FEEAMT2   PD8.2   @554 FEEAMT4   PD8.2
#   @764 NFEEAMT5  PD8.2   @772 NFEEAMT6  PD8.2
#   @780 NFEEAMT7  PD8.2   @861 FEEAMT8   PD8.2
#   @869 FEEAMT9   PD8.2   @885 FEEAMT13  PD8.2
#   @904 FEEAMT10  PD8.2   @912 FEEAMT11  PD8.2
#   @920 FEEAMT12  PD8.2   @928 FEEAMT14  PD8.2
#   @936 FEEAMT15  PD8.2   @944 FEEAMT16  PD8.2
# ===========================================================================
con.execute(f"""
    CREATE OR REPLACE TABLE loan_raw AS
    SELECT
        TRY_CAST(TRIM(SUBSTR(line,  1, 6)) AS BIGINT)   AS acctno,
        SUBSTR(line,   7, 24)                            AS name,
        TRY_CAST(TRIM(SUBSTR(line, 63, 2)) AS INTEGER)   AS bankno,
        TRY_CAST(TRIM(SUBSTR(line, 66, 4)) AS INTEGER)   AS accbrch,
        TRY_CAST(TRIM(SUBSTR(line, 81, 3)) AS BIGINT)    AS noteno,
        SUBSTR(line,  84, 1)                             AS reversed,
        TRY_CAST(TRIM(SUBSTR(line, 85, 2)) AS INTEGER)   AS loantype,
        TRY_CAST(TRIM(SUBSTR(line, 87, 4)) AS INTEGER)   AS ntbrch,
        TRY_CAST(TRIM(SUBSTR(line, 91, 4)) AS INTEGER)   AS pendbrh,
        TRY_CAST(TRIM(SUBSTR(line,113, 6)) AS BIGINT)    AS lasttran,
        TRY_CAST(TRIM(SUBSTR(line,121, 8)) AS DOUBLE)    AS curbal,
        TRY_CAST(TRIM(SUBSTR(line,129, 8)) AS DOUBLE)    AS intamt,
        SUBSTR(line, 261, 1)                             AS paidind,
        SUBSTR(line, 296, 1)                             AS ntint,
        TRY_CAST(TRIM(SUBSTR(line,311, 8)) AS DOUBLE)    AS intearn,
        TRY_CAST(TRIM(SUBSTR(line,319, 8)) AS DOUBLE)    AS accrual,
        TRY_CAST(TRIM(SUBSTR(line,400, 8)) AS DOUBLE)    AS intearn2,
        TRY_CAST(TRIM(SUBSTR(line,408, 8)) AS DOUBLE)    AS intearn3,
        TRY_CAST(TRIM(SUBSTR(line,416, 8)) AS DOUBLE)    AS intearn4,
        TRY_CAST(TRIM(SUBSTR(line,457, 8)) AS DOUBLE)    AS feeamt,
        TRY_CAST(TRIM(SUBSTR(line,465, 8)) AS DOUBLE)    AS feeamt2,
        TRY_CAST(TRIM(SUBSTR(line,554, 8)) AS DOUBLE)    AS feeamt4,
        TRY_CAST(TRIM(SUBSTR(line,764, 8)) AS DOUBLE)    AS nfeeamt5,
        TRY_CAST(TRIM(SUBSTR(line,772, 8)) AS DOUBLE)    AS nfeeamt6,
        TRY_CAST(TRIM(SUBSTR(line,780, 8)) AS DOUBLE)    AS nfeeamt7,
        TRY_CAST(TRIM(SUBSTR(line,861, 8)) AS DOUBLE)    AS feeamt8,
        TRY_CAST(TRIM(SUBSTR(line,869, 8)) AS DOUBLE)    AS feeamt9,
        TRY_CAST(TRIM(SUBSTR(line,885, 8)) AS DOUBLE)    AS feeamt13,
        TRY_CAST(TRIM(SUBSTR(line,904, 8)) AS DOUBLE)    AS feeamt10,
        TRY_CAST(TRIM(SUBSTR(line,912, 8)) AS DOUBLE)    AS feeamt11,
        TRY_CAST(TRIM(SUBSTR(line,920, 8)) AS DOUBLE)    AS feeamt12,
        TRY_CAST(TRIM(SUBSTR(line,928, 8)) AS DOUBLE)    AS feeamt14,
        TRY_CAST(TRIM(SUBSTR(line,936, 8)) AS DOUBLE)    AS feeamt15,
        TRY_CAST(TRIM(SUBSTR(line,944, 8)) AS DOUBLE)    AS feeamt16,
        CASE
            WHEN TRY_CAST(TRIM(SUBSTR(line, 91, 4)) AS INTEGER) <> 0
                THEN TRY_CAST(TRIM(SUBSTR(line, 91, 4)) AS INTEGER)
            WHEN TRY_CAST(TRIM(SUBSTR(line, 87, 4)) AS INTEGER) <> 0
                THEN TRY_CAST(TRIM(SUBSTR(line, 87, 4)) AS INTEGER)
            ELSE TRY_CAST(TRIM(SUBSTR(line, 66, 4)) AS INTEGER)
        END AS branch
    FROM read_csv(
        '{acctfile_path}',
        columns = {{'line': 'VARCHAR'}},
        header = false,
        delim  = '\\x01',
        quote  = ''
    )
    WHERE TRY_CAST(TRIM(SUBSTR(line, 85, 2)) AS INTEGER) IN (135, 136)
      AND (
            ( (SUBSTR(line, 84, 1) IS NULL OR SUBSTR(line, 84, 1) <> 'Y')
              AND TRY_CAST(TRIM(SUBSTR(line, 81, 3)) AS BIGINT) IS NOT NULL
              AND (SUBSTR(line, 261, 1) IS NULL OR SUBSTR(line, 261, 1) <> 'P')
            )
            OR
            ( SUBSTR(line, 261, 1) = 'P'
              AND TRY_CAST(TRIM(SUBSTR(line, 113, 6)) AS BIGINT) = {REPTDATE_INT}
            )
          )
""")

# ---------------------------------------------------------------------------
# Merge LOAN + FEEPO, compute FEEAMT and BALANCE
# ---------------------------------------------------------------------------
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
    LEFT JOIN feepo f
      ON l.acctno = f.acctno AND l.noteno = f.noteno
""")

# ---------------------------------------------------------------------------
# PROC SUMMARY DATA=LOAN NWAY  (reptdate removed)
# ---------------------------------------------------------------------------
con.execute(f"""
    CREATE OR REPLACE TABLE loan_summary AS
    SELECT
        branch,
        {EXTDATE} AS extdate,
        COUNT(*)  AS noacct,
        SUM(balance) AS brlnamt
    FROM loan
    GROUP BY branch, extdate
    ORDER BY branch
""")

# ---------------------------------------------------------------------------
# DATA MIS.LOAN&REPTDAY
# ---------------------------------------------------------------------------
pq.write_table(
    con.execute("SELECT * FROM loan_summary").arrow(),
    MIS_DIR / f"LOAN{REPTDAY_STR}.parquet"
)

# ---------------------------------------------------------------------------
# DATA PREVLN
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# DATA LOANS
# ---------------------------------------------------------------------------
con.execute("""
    CREATE OR REPLACE TABLE loans AS
    SELECT
        COALESCE(l.branch, p.branch) AS branch,
        l.extdate, l.noacct, l.brlnamt,
        COALESCE(p.pbrlnamt, 0) AS pbrlnamt
    FROM loan_summary l
    FULL OUTER JOIN prevln p ON l.branch = p.branch
""")

# ---------------------------------------------------------------------------
# DATA BRANCH - fixed-width read of LKP_BRANCH
#   @001 BANK     $1
#   @002 BRANCH   3.
#   @006 ABBREV   $3
#   @012 BRCHNAME $30
# ---------------------------------------------------------------------------
con.execute(f"""
    CREATE OR REPLACE TABLE branch AS
    SELECT
        SUBSTR(line,  1,  1)                           AS bank,
        TRY_CAST(TRIM(SUBSTR(line, 2, 3)) AS INTEGER)  AS branch,
        SUBSTR(line,  6,  3)                           AS abbrev,
        SUBSTR(line, 12, 30)                           AS brchname
    FROM read_csv(
        '{BRANCHF}',
        columns = {{'line': 'VARCHAR'}},
        header = false,
        delim  = '\\x01',
        quote  = ''
    )
    ORDER BY branch
""")

# ---------------------------------------------------------------------------
# DATA MLOAN
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# PROC TABULATE equivalent - final report with TOTAL row
# ---------------------------------------------------------------------------
report = con.execute("""
    SELECT branch AS code, abbrev, brchname AS name,
           noacct   AS no_of_accounts,
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

# ===========================================================================
# Outputs
# ===========================================================================
output_base = f"EIBDLNS2_Branch_Loan_Summary_{REPTDATE.strftime('%Y%m%d')}"

# Parquet (intermediate)
parquet_file = OUTPUT_DIR / f"{output_base}.parquet"
pq.write_table(report, parquet_file)
print(f"Parquet saved: {parquet_file}")

# SAS7BDAT via saspy
sas = saspy.SASsession(cfgname="default")          # <-- adjust cfgname
sas.df2sd(report.to_pandas(), table="EIBDLNS2", libref="WORK")
sas.submit(f"""
    libname out "{OUTPUT_DIR}";
    data out.{output_base};
        set WORK.EIBDLNS2;
    run;
""")
sas.endsas()
print(f"SAS7BDAT saved: {OUTPUT_DIR / (output_base + '.sas7bdat')}")

# ===========================================================================
# Display report (mimics PROC TABULATE)
# ===========================================================================
print("\n" + "=" * 130)
print("PUBLIC BANK BERHAD")
print("BRANCH SUMMARY ON DAILY OUTSTANDING(RM)-BAE PERSONAL")
print(f"AS AT {RDATE}")
print("=" * 130)
print(f"{'BRANCH':<8} {'ABBREV':<8} {'NAME':<35} {'NO OF':>12} "
      f"{'PREV.AMOUNT':>20} {'CURR.AMOUNT':>20} {'VARIANCE':>20}")
print(f"{'CODE':<8} {'':<8} {'':<35} {'ACCOUNTS':>12} "
      f"{'(RM)':>20} {'(RM)':>20} {'(RM)':>20}")
print("-" * 130)

for row in report.to_pylist():
    code     = row["code"]
    abbrev   = row["abbrev"] or ""
    name     = row["name"] or ""
    accounts = row["no_of_accounts"]
    prev_amt = row["prev_amount"]
    curr_amt = row["curr_amount"]
    variance = row["variance"]

    if code == 999999:
        print("-" * 130)
    print(f"{code:<8} {abbrev:<8} {name:<35} {accounts:>12,} "
          f"{prev_amt:>20,.2f} {curr_amt:>20,.2f} {variance:>20,.2f}")

print("=" * 130)

con.close()
print("\nProcessing complete!")
