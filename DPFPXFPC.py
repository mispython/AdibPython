# -*- coding: utf-8 -*-
"""
EIBDLNS2 - Branch Daily Outstanding Loan Summary Report
Reads SAS7BDAT inputs (NFEEFILE, ACCTFILE) + fixed-width ASCII (LKP_BRANCH)
"""

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyreadstat
from datetime import timedelta
from pathlib import Path
import saspy

OUTPUT_DIR = Path("stgsrcsys/host/holding")
MIS_DIR    = Path("/host_pq/dwh/mis")

DATEFILE = Path("/host_pq/dwh/input/LOAN/DATEFILE")
FEEFILE  = "/host_pq/dwh/input/LOAN/NFEEFILE_{reptyear}{reptmon}{reptday}"
ACCTFILE = "/host_pq/dwh/input/LOAN/ACCTFILE_{reptyear}{reptmon}{reptday}"
BRANCHF  = Path("/sasdata/rawdata/lookup/LKP_BRANCH")

con = duckdb.connect(":memory:")
print("REPORT ID : EIBDLNSA")

# ---------------------------------------------------------------------------
# DATA REPTDATE
# ---------------------------------------------------------------------------
with open(DATEFILE, "r") as f:
    first_line = f.readline()

EXTDATE_str = first_line[0:11].strip().zfill(11)
extdate_str = EXTDATE_str[:8]
EXTDATE     = int(EXTDATE_str)

REPTDATE = con.execute(
    f"SELECT CAST(strptime('{extdate_str}', '%m%d%Y') AS DATE)"
).fetchone()[0]

PREVDATE = REPTDATE - timedelta(days=1)
DLETDATE = REPTDATE - timedelta(days=3)
REPTDAY  = REPTDATE.day
PREVDAY  = PREVDATE.day
DLETDAY  = DLETDATE.day
YY = REPTDATE.year - 1 if (REPTDATE.month == 1 and REPTDATE.day == 1) else REPTDATE.year

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

# ---------------------------------------------------------------------------
# Read SAS7BDAT inputs with pyreadstat (column-pruned)
# ---------------------------------------------------------------------------
print("Reading NFEEFILE ...")
fee_cols = ["ACCTNO","NOTENO","LOANTYPE","FEEPLN","FEEAMTA","FEEAMTC","FEEAMTB"]
df_fee, _ = pyreadstat.read_sas7bdat(feefile_path, usecols=fee_cols)
print(f"  {len(df_fee):,} rows")

print("Reading ACCTFILE ...")
acct_cols = [
    "ACCTNO","NAME","BANKNO","ACCBRCH","NOTENO","REVERSED","LOANTYPE",
    "NTBRCH","PENDBRH","LASTTRAN","CURBAL","INTAMT","PAIDIND","NTINT",
    "INTEARN","ACCRUAL","INTEARN2","INTEARN3","INTEARN4",
    "FEEAMT","FEEAMT2","FEEAMT4","NFEEAMT5","NFEEAMT6","NFEEAMT7",
    "FEEAMT8","FEEAMT9","FEEAMT13","FEEAMT10","FEEAMT11","FEEAMT12",
    "FEEAMT14","FEEAMT15","FEEAMT16",
]
df_acct, _ = pyreadstat.read_sas7bdat(acctfile_path, usecols=acct_cols)
print(f"  {len(df_acct):,} rows")

# Register as DuckDB views
con.register("feplan_src", df_fee)
con.register("loan_src",   df_acct)

# ---------------------------------------------------------------------------
# DATA FEPLAN
# ---------------------------------------------------------------------------
con.execute("""
    CREATE OR REPLACE TABLE feplan AS
    SELECT
        CAST(ACCTNO   AS BIGINT)  AS acctno,
        CAST(NOTENO   AS BIGINT)  AS noteno,
        CAST(LOANTYPE AS INTEGER) AS loantype,
        FEEPLN                    AS feepln,
        CAST(FEEAMTA  AS DOUBLE)  AS feeamta,
        CAST(FEEAMTC  AS DOUBLE)  AS feeamtc,
        CAST(FEEAMTB  AS DOUBLE)  AS feeamtb
    FROM feplan_src
    WHERE ACCTNO < 3000000000
      AND LOANTYPE IN (135, 136)
      AND FEEPLN = 'PA'
""")

con.execute("""
    CREATE OR REPLACE TABLE feepo AS
    SELECT acctno, noteno,
           SUM(feeamta) AS feeamta,
           SUM(feeamtb) AS feeamtb,
           SUM(feeamtc) AS feeamtc
    FROM feplan
    GROUP BY acctno, noteno
""")

# ---------------------------------------------------------------------------
# DATA LOAN
# ---------------------------------------------------------------------------
con.execute(f"""
    CREATE OR REPLACE TABLE loan_raw AS
    SELECT
        CAST(ACCTNO   AS BIGINT)  AS acctno,
        NAME                      AS name,
        CAST(BANKNO   AS INTEGER) AS bankno,
        CAST(ACCBRCH  AS INTEGER) AS accbrch,
        CAST(NOTENO   AS BIGINT)  AS noteno,
        REVERSED                  AS reversed,
        CAST(LOANTYPE AS INTEGER) AS loantype,
        CAST(NTBRCH   AS INTEGER) AS ntbrch,
        CAST(PENDBRH  AS INTEGER) AS pendbrh,
        CAST(LASTTRAN AS BIGINT)  AS lasttran,
        CAST(CURBAL   AS DOUBLE)  AS curbal,
        CAST(INTAMT   AS DOUBLE)  AS intamt,
        PAIDIND                   AS paidind,
        NTINT                     AS ntint,
        CAST(INTEARN  AS DOUBLE)  AS intearn,
        CAST(ACCRUAL  AS DOUBLE)  AS accrual,
        CAST(INTEARN2 AS DOUBLE)  AS intearn2,
        CAST(INTEARN3 AS DOUBLE)  AS intearn3,
        CAST(INTEARN4 AS DOUBLE)  AS intearn4,
        CAST(FEEAMT   AS DOUBLE)  AS feeamt,
        CAST(FEEAMT2  AS DOUBLE)  AS feeamt2,
        CAST(FEEAMT4  AS DOUBLE)  AS feeamt4,
        CAST(NFEEAMT5 AS DOUBLE)  AS nfeeamt5,
        CAST(NFEEAMT6 AS DOUBLE)  AS nfeeamt6,
        CAST(NFEEAMT7 AS DOUBLE)  AS nfeeamt7,
        CAST(FEEAMT8  AS DOUBLE)  AS feeamt8,
        CAST(FEEAMT9  AS DOUBLE)  AS feeamt9,
        CAST(FEEAMT13 AS DOUBLE)  AS feeamt13,
        CAST(FEEAMT10 AS DOUBLE)  AS feeamt10,
        CAST(FEEAMT11 AS DOUBLE)  AS feeamt11,
        CAST(FEEAMT12 AS DOUBLE)  AS feeamt12,
        CAST(FEEAMT14 AS DOUBLE)  AS feeamt14,
        CAST(FEEAMT15 AS DOUBLE)  AS feeamt15,
        CAST(FEEAMT16 AS DOUBLE)  AS feeamt16,
        CASE
            WHEN COALESCE(PENDBRH,0) <> 0 THEN PENDBRH
            WHEN COALESCE(NTBRCH, 0) <> 0 THEN NTBRCH
            ELSE ACCBRCH
        END AS branch
    FROM loan_src
    WHERE LOANTYPE IN (135, 136)
      AND (
            ( (REVERSED IS NULL OR REVERSED <> 'Y')
              AND NOTENO IS NOT NULL
              AND (PAIDIND IS NULL OR PAIDIND <> 'P') )
            OR
            ( PAIDIND = 'P' AND LASTTRAN = {REPTDATE_INT} )
          )
""")

con.execute("""
    CREATE OR REPLACE TABLE loan AS
    SELECT
        l.*,
        COALESCE(f.feeamta, 0) AS feeamta_x,
        COALESCE(f.feeamtb, 0) AS feeamtb_x,
        COALESCE(f.feeamtc, 0) AS feeamtc_x,
        CASE
            WHEN l.acctno > 8000000000 AND l.loantype IN (720, 725)
            THEN l.feeamt + COALESCE(f.feeamta, 0) + COALESCE(f.feeamtc, 0)
            ELSE l.feeamt + COALESCE(f.feeamta, 0)
        END AS feeamt_final,
        CASE
            WHEN l.ntint = 'A'
            THEN l.curbal + l.intearn - l.intamt
                 + CASE
                     WHEN l.acctno > 8000000000 AND l.loantype IN (720,725)
                     THEN l.feeamt + COALESCE(f.feeamta,0) + COALESCE(f.feeamtc,0)
                     ELSE l.feeamt + COALESCE(f.feeamta,0)
                   END
            ELSE l.curbal + l.accrual
                 + CASE
                     WHEN l.acctno > 8000000000 AND l.loantype IN (720,725)
                     THEN l.feeamt + COALESCE(f.feeamta,0) + COALESCE(f.feeamtc,0)
                     ELSE l.feeamt + COALESCE(f.feeamta,0)
                   END
        END AS balance
    FROM loan_raw l
    LEFT JOIN feepo f ON l.acctno = f.acctno AND l.noteno = f.noteno
""")

# ---------------------------------------------------------------------------
# PROC SUMMARY / MIS.LOAN
# ---------------------------------------------------------------------------
con.execute(f"""
    CREATE OR REPLACE TABLE loan_summary AS
    SELECT branch,
           {EXTDATE} AS extdate,
           COUNT(*)  AS noacct,
           SUM(balance) AS brlnamt
    FROM loan
    GROUP BY branch, extdate
    ORDER BY branch
""")

pq.write_table(
    con.execute("SELECT * FROM loan_summary").arrow(),
    MIS_DIR / f"LOAN{REPTDAY_STR}.parquet"
)

# ---------------------------------------------------------------------------
# PREVLN / LOANS
# ---------------------------------------------------------------------------
prev_file = MIS_DIR / f"LOAN{PREVDAY_STR}.parquet"
if prev_file.exists():
    con.execute(f"""
        CREATE OR REPLACE TABLE prevln AS
        SELECT branch, brlnamt AS pbrlnamt
        FROM read_parquet('{prev_file}')
    """)
else:
    con.execute("""
        CREATE OR REPLACE TABLE prevln AS
        SELECT branch, CAST(0.0 AS DOUBLE) AS pbrlnamt
        FROM loan_summary WHERE 1=0
    """)

con.execute("""
    CREATE OR REPLACE TABLE loans AS
    SELECT COALESCE(l.branch, p.branch) AS branch,
           l.extdate, l.noacct, l.brlnamt,
           COALESCE(p.pbrlnamt, 0) AS pbrlnamt
    FROM loan_summary l
    FULL OUTER JOIN prevln p ON l.branch = p.branch
""")

# ---------------------------------------------------------------------------
# BRANCH - read ASCII fixed-width directly in Python
# ---------------------------------------------------------------------------
branch_rows = []
with open(BRANCHF, "r", encoding="latin-1", newline="") as f:
    for line in f:
        line = line.rstrip("\r\n")
        if not line or line.startswith("NOTE:") or "The SAS System" in line:
            continue
        bank   = line[0:1]
        branch = line[1:4].strip()
        abbrev = line[5:8]
        name   = line[11:41].rstrip()
        branch_rows.append({
            "bank": bank,
            "branch": int(branch) if branch.isdigit() else None,
            "abbrev": abbrev,
            "brchname": name,
        })

df_branch = pd.DataFrame(branch_rows)
con.register("branch", df_branch)

# ---------------------------------------------------------------------------
# MLOAN
# ---------------------------------------------------------------------------
con.execute("""
    CREATE OR REPLACE TABLE mloan AS
    SELECT COALESCE(l.branch, b.branch) AS branch,
           b.bank, b.abbrev, b.brchname,
           l.extdate,
           COALESCE(l.noacct, 0) AS noacct,
           l.pbrlnamt, l.brlnamt,
           l.brlnamt - l.pbrlnamt AS varianln
    FROM loans l
    LEFT JOIN branch b ON l.branch = b.branch
""")

# ---------------------------------------------------------------------------
# Final report
# ---------------------------------------------------------------------------
report = con.execute("""
    SELECT branch AS code, abbrev, brchname AS name,
           noacct AS no_of_accounts,
           pbrlnamt AS prev_amount,
           brlnamt  AS curr_amount,
           varianln AS variance
    FROM mloan WHERE branch IS NOT NULL
    UNION ALL
    SELECT 999999, 'TOTAL', 'TOTAL',
           SUM(noacct), SUM(pbrlnamt), SUM(brlnamt), SUM(varianln)
    FROM mloan
    ORDER BY code
""").arrow()

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------
output_base = f"EIBDLNS2_Branch_Loan_Summary_{REPTDATE.strftime('%Y%m%d')}"

pq.write_table(report, OUTPUT_DIR / f"{output_base}.parquet")
print(f"Parquet saved: {OUTPUT_DIR / (output_base + '.parquet')}")

sas = saspy.SASsession(cfgname="default")
sas.df2sd(report.to_pandas(), table="EIBDLNS2", libref="WORK")
sas.submit(f"""
    libname out "{OUTPUT_DIR}";
    data out.{output_base};
        set WORK.EIBDLNS2;
    run;
""")
sas.endsas()
print(f"SAS7BDAT saved: {OUTPUT_DIR / (output_base + '.sas7bdat')}")

# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------
print("\n" + "=" * 130)
print("PUBLIC BANK BERHAD")
print("BRANCH SUMMARY ON DAILY OUTSTANDING(RM)-BAE PERSONAL")
print(f"AS AT {RDATE}")
print("=" * 130)

for row in report.to_pylist():
    code = row["code"]
    if code == 999999:
        print("-" * 130)
    print(f"{code:<8} {row['abbrev'] or '':<8} {row['name'] or '':<35} "
          f"{row['no_of_accounts']:>12,} "
          f"{row['prev_amount']:>20,.2f} "
          f"{row['curr_amount']:>20,.2f} "
          f"{row['variance']:>20,.2f}")

print("=" * 130)
con.close()
print("\nProcessing complete!")
