#!/usr/bin/env python3
"""
EIIDLOAN - Islamic Daily Loan Movement Report
Tracks daily changes in term loans, revolving credit, and HP accounts

All inputs are in FLAT FILE format, except lnnote which is SAS7BDAT.
Read using pyreadstat.

Usage:
    python3 eiidloan.py
"""

import duckdb
from pathlib import Path
from datetime import datetime, timedelta
import pyreadstat
import pandas as pd


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATEFILE_PATH = Path('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLOAN/DATEFILE')
BRANCH_PATH   = Path('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLOAN/LKP_BRANCH')
LNNOTE_PATH   = Path('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m{REPTMON}.sas7bdat')
OUTPUT_DIR    = Path('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLOAN')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()


# ---------------------------------------------------------------------------
# 1. Read DATEFILE (flat file)
# ---------------------------------------------------------------------------
with open(DATEFILE_PATH / 'DATEFILE', 'r') as f:
    extdate = int(f.readline().strip())

reptdate_str = str(extdate)[:8]                      # MMDDYYYY
reptdate     = datetime.strptime(reptdate_str, '%m%d%Y')
prevdate     = reptdate - timedelta(days=1)
dletdate     = reptdate - timedelta(days=3)

reptday = reptdate.day
prevday = prevdate.day
dletday = dletdate.day

yy        = reptdate.year - 1 if (reptdate.month == 1 and reptdate.day == 1) else reptdate.year
reptyear  = reptdate.year
prevyear  = yy
reptmon   = reptdate.month
rdate     = reptdate.strftime('%d/%m/%Y')

# SAS-compatible numeric representations
reptdate_num = int(reptdate.strftime('%y%m%d'))   # Z5. of SAS date => YYMMDD as number
prevdate_num = int(prevdate.strftime('%y%m%d'))

print(f"Islamic Daily Loan Movement - {rdate}")
print(f"  REPTDATE  : {reptdate.date()}   (num: {reptdate_num})")
print(f"  PREVDATE  : {prevdate.date()}   (num: {prevdate_num})")
print(f"  DLETDATE  : {dletdate.date()}")
print(f"  EXTDATE   : {extdate}")


# ---------------------------------------------------------------------------
# 2. Read LNNOTE (SAS7BDAT) using pyreadstat
# ---------------------------------------------------------------------------
lnnote_file = LNNOTE_PATH.parent / LNNOTE_PATH.name.format(REPTMON=f"{reptmon:02d}")
print(f"Reading LNNOTE: {lnnote_file}")

df_lnnote, meta = pyreadstat.read_sas7bdat(str(lnnote_file))
df_lnnote.columns = [c.upper() for c in df_lnnote.columns]
print(f"  Loaded {len(df_lnnote):,} rows, columns: {list(df_lnnote.columns)}")


# ---------------------------------------------------------------------------
# 3. Build LOAN dataframe (equivalent to SAS DATA LOAN)
# ---------------------------------------------------------------------------
# SAS: KEEP ACCTNO NOTENO NAME BALANCE BRANCH LOANTYPE CURBAL REPTDATE EXTDATE
#      REPTDATE=&REPTDATE; EXTDATE=&EXTDATE; BRANCH = PENDBRH;
required = ['ACCTNO', 'NOTENO', 'NAME', 'BALANCE', 'LOANTYPE', 'CURBAL', 'PENDBRH']
missing = [c for c in required if c not in df_lnnote.columns]
if missing:
    raise ValueError(f"Missing required columns in LNNOTE: {missing}")

loan = df_lnnote[required].copy()
loan = loan.rename(columns={'PENDBRH': 'BRANCH'})
loan['REPTDATE'] = reptdate_num
loan['EXTDATE']  = extdate

# Ensure numeric types
for c in ['ACCTNO', 'NOTENO', 'BALANCE', 'LOANTYPE', 'CURBAL', 'BRANCH']:
    loan[c] = pd.to_numeric(loan[c], errors='coerce')

loan['ACCTNO']   = loan['ACCTNO'].astype('Int64')
loan['NOTENO']   = loan['NOTENO'].astype('Int64')
loan['LOANTYPE'] = loan['LOANTYPE'].astype('Int64')
loan['BRANCH']   = loan['BRANCH'].astype('Int64')
loan['BALANCE']  = loan['BALANCE'].astype('float64')
loan['CURBAL']   = loan['CURBAL'].astype('float64')


# ---------------------------------------------------------------------------
# 4. Register LOAN in DuckDB and write LNDLY{reptday}.parquet
# ---------------------------------------------------------------------------
con.register('loan_df', loan)

con.execute(f"""
    CREATE TEMP TABLE loan AS
    SELECT acctno, noteno, name, balance, loantype, curbal,
           {reptdate_num} AS reptdate,
           {extdate}      AS extdate,
           branch
    FROM loan_df
""")

lndly_today = OUTPUT_DIR / f"lndly{reptday:02d}.parquet"
con.execute(f"COPY loan TO '{lndly_today}'")
print(f"  Wrote {lndly_today}")


# ---------------------------------------------------------------------------
# 5. Split into LNNOTE / REVCRE / HPLOAN
# ---------------------------------------------------------------------------
REVCRE_TYPES = (302, 350, 364, 365, 506, 902, 903, 910, 925, 951)
HPLOAN_TYPES = (128, 130, 380, 381, 700, 705)

revcre_in  = ",".join(str(x) for x in REVCRE_TYPES)
hploan_in  = ",".join(str(x) for x in HPLOAN_TYPES)

con.execute(f"""
    CREATE TEMP TABLE lnnote AS
    SELECT * FROM loan
    WHERE loantype NOT IN ({revcre_in}, {hploan_in})
""")

con.execute(f"""
    CREATE TEMP TABLE revcre AS
    SELECT * FROM loan
    WHERE loantype IN ({revcre_in})
""")

con.execute(f"""
    CREATE TEMP TABLE hploan AS
    SELECT * FROM loan
    WHERE loantype IN ({hploan_in})
""")


# ---------------------------------------------------------------------------
# 6. Branch summaries for current date
# ---------------------------------------------------------------------------
con.execute("""
    CREATE TEMP TABLE loansum AS
    SELECT branch, reptdate, extdate,
           COUNT(*)     AS noacct,
           SUM(balance) AS brlnamt
    FROM lnnote
    GROUP BY branch, reptdate, extdate
""")

con.execute("""
    CREATE TEMP TABLE revsumm AS
    SELECT branch, reptdate, extdate,
           COUNT(*)     AS revacc,
           SUM(balance) AS brrvamt
    FROM revcre
    GROUP BY branch, reptdate, extdate
""")

con.execute("""
    CREATE TEMP TABLE hpsumm AS
    SELECT branch, reptdate, extdate,
           COUNT(*)     AS hpacc,
           SUM(balance) AS brhpamt
    FROM hploan
    GROUP BY branch, reptdate, extdate
""")


# ---------------------------------------------------------------------------
# 7. Load previous day LNDLY{prevday}.parquet (from prior run output)
# ---------------------------------------------------------------------------
lndly_prev = OUTPUT_DIR / f"lndly{prevday:02d}.parquet"
print(f"Reading previous day: {lndly_prev}")

con.execute(f"""
    CREATE TEMP TABLE prev_loan AS
    SELECT acctno, noteno, name, balance, loantype, curbal,
           reptdate, extdate, branch
    FROM read_parquet('{lndly_prev}')
""")


# ---------------------------------------------------------------------------
# 8. Split previous day into PLNNOTE / PREVCRE / PHPLOAN
# ---------------------------------------------------------------------------
con.execute(f"""
    CREATE TEMP TABLE plnnote AS
    SELECT * FROM prev_loan
    WHERE loantype NOT IN ({revcre_in}, {hploan_in})
""")

con.execute(f"""
    CREATE TEMP TABLE prevcre AS
    SELECT * FROM prev_loan
    WHERE loantype IN ({revcre_in})
""")

con.execute(f"""
    CREATE TEMP TABLE phploan AS
    SELECT * FROM prev_loan
    WHERE loantype IN ({hploan_in})
""")


# ---------------------------------------------------------------------------
# 9. Previous-day branch summaries
# ---------------------------------------------------------------------------
con.execute("""
    CREATE TEMP TABLE ploansum AS
    SELECT branch, reptdate, extdate,
           COUNT(*)     AS pnoacct,
           SUM(balance) AS pbrlnamt
    FROM plnnote
    GROUP BY branch, reptdate, extdate
""")

con.execute("""
    CREATE TEMP TABLE prevsumm AS
    SELECT branch, reptdate, extdate,
           COUNT(*)     AS prevacc,
           SUM(balance) AS pbrrvamt
    FROM prevcre
    GROUP BY branch, reptdate, extdate
""")

con.execute("""
    CREATE TEMP TABLE phpsumm AS
    SELECT branch, reptdate, extdate,
           COUNT(*)     AS phpacc,
           SUM(balance) AS pbrhpamt
    FROM phploan
    GROUP BY branch, reptdate, extdate
""")


# ---------------------------------------------------------------------------
# 10. Read BRANCH lookup (flat file)
# ---------------------------------------------------------------------------
branch_file = BRANCH_PATH / 'LKP_BRANCH'
print(f"Reading branch lookup: {branch_file}")

branch_rows = []
with open(branch_file, 'r') as f:
    for line in f:
        line = line.rstrip('\n')
        if not line.strip():
            continue
        # SAS: @001 BANK $1.  @002 BRANCH 3.  @006 ABBREV $3.  @012 BRCHNAME $30.
        bank     = line[0:1].strip()
        branch_s = line[1:4].strip()
        abbrev   = line[5:8].strip()
        brchname = line[11:41].strip()
        if not branch_s:
            continue
        try:
            branch_val = int(branch_s)
        except ValueError:
            continue
        branch_rows.append({
            'branch':   branch_val,
            'abbrev':   abbrev,
            'brchname': brchname,
        })

branch_df = pd.DataFrame(branch_rows, columns=['branch', 'abbrev', 'brchname'])
print(f"  Loaded {len(branch_df)} branch records")

con.register('branch_df', branch_df)
con.execute("""
    CREATE TEMP TABLE branch AS
    SELECT branch, abbrev, brchname FROM branch_df
""")


# ---------------------------------------------------------------------------
# 11. Build MLOAN / MCRED / MHP (branch summary reports)
# ---------------------------------------------------------------------------
con.execute("""
    CREATE TEMP TABLE mloan AS
    SELECT
        l.branch,
        b.abbrev,
        b.brchname,
        COALESCE(l.brlnamt,  0)                              AS brlnamt,
        COALESCE(p.pbrlnamt, 0)                              AS pbrlnamt,
        COALESCE(l.noacct,   0)                              AS noacct,
        COALESCE(l.brlnamt,  0) - COALESCE(p.pbrlnamt, 0)    AS varianln
    FROM loansum l
    LEFT JOIN ploansum p ON l.branch = p.branch
    LEFT JOIN branch   b ON l.branch = b.branch
""")

con.execute("""
    CREATE TEMP TABLE mcred AS
    SELECT
        r.branch,
        b.abbrev,
        b.brchname,
        COALESCE(r.brrvamt,  0)                              AS brrvamt,
        COALESCE(p.pbrrvamt, 0)                              AS pbrrvamt,
        COALESCE(r.revacc,   0)                              AS revacc,
        COALESCE(r.brrvamt,  0) - COALESCE(p.pbrrvamt, 0)    AS varianrv
    FROM revsumm r
    LEFT JOIN prevsumm p ON r.branch = p.branch
    LEFT JOIN branch   b ON r.branch = b.branch
""")

con.execute("""
    CREATE TEMP TABLE mhp AS
    SELECT
        h.branch,
        b.abbrev,
        b.brchname,
        COALESCE(h.brhpamt,  0)                              AS brhpamt,
        COALESCE(p.pbrhpamt, 0)                              AS pbrhpamt,
        COALESCE(h.hpacc,    0)                              AS hpacc,
        COALESCE(h.brhpamt,  0) - COALESCE(p.pbrhpamt, 0)    AS varianhp
    FROM hpsumm h
    LEFT JOIN phpsumm p ON h.branch = p.branch
    LEFT JOIN branch  b ON h.branch = b.branch
""")

rdate_file = rdate.replace('/', '-')
con.execute(f"COPY mloan TO '{OUTPUT_DIR}/mloan_{rdate_file}.csv' (HEADER, DELIMITER ',')")
con.execute(f"COPY mcred TO '{OUTPUT_DIR}/mcred_{rdate_file}.csv' (HEADER, DELIMITER ',')")
con.execute(f"COPY mhp   TO '{OUTPUT_DIR}/mhp_{rdate_file}.csv'   (HEADER, DELIMITER ',')")

print(f"  Wrote mloan_{rdate_file}.csv")
print(f"  Wrote mcred_{rdate_file}.csv")
print(f"  Wrote mhp_{rdate_file}.csv")


# ---------------------------------------------------------------------------
# 12. Per-account daily totals (current & previous)
# ---------------------------------------------------------------------------
con.execute("""
    CREATE TEMP TABLE dloan1 AS
    SELECT acctno, reptdate, SUM(balance) AS dltotol
    FROM lnnote
    GROUP BY acctno, reptdate
""")

con.execute("""
    CREATE TEMP TABLE dcred1 AS
    SELECT acctno, reptdate, SUM(balance) AS drtotol
    FROM revcre
    GROUP BY acctno, reptdate
""")

con.execute("""
    CREATE TEMP TABLE dhp1 AS
    SELECT acctno, reptdate, SUM(balance) AS dhptotol
    FROM hploan
    GROUP BY acctno, reptdate
""")

con.execute("""
    CREATE TEMP TABLE pdloan1 AS
    SELECT acctno, reptdate, SUM(balance) AS pdltotol
    FROM plnnote
    GROUP BY acctno, reptdate
""")

con.execute("""
    CREATE TEMP TABLE pdcred1 AS
    SELECT acctno, reptdate, SUM(balance) AS pdrtotol
    FROM prevcre
    GROUP BY acctno, reptdate
""")

con.execute("""
    CREATE TEMP TABLE pdhp1 AS
    SELECT acctno, reptdate, SUM(balance) AS pdhptoto
    FROM phploan
    GROUP BY acctno, reptdate
""")


# ---------------------------------------------------------------------------
# 13. Identify accounts with movement >= RM 500,000 (FULL OUTER JOIN)
# ---------------------------------------------------------------------------
con.execute("""
    CREATE TEMP TABLE dmloan_check AS
    SELECT
        COALESCE(d.acctno, p.acctno)                            AS acctno,
        COALESCE(d.dltotol, 0)                                  AS dltotol,
        COALESCE(p.pdltotol, 0)                                 AS pdltotol
    FROM dloan1 d
    FULL OUTER JOIN pdloan1 p ON d.acctno = p.acctno
    WHERE ABS(COALESCE(d.dltotol, 0) - COALESCE(p.pdltotol, 0)) >= 500000
""")

con.execute("""
    CREATE TEMP TABLE dmcred_check AS
    SELECT
        COALESCE(d.acctno, p.acctno)                            AS acctno,
        COALESCE(d.drtotol, 0)                                  AS drtotol,
        COALESCE(p.pdrtotol, 0)                                 AS pdrtotol
    FROM dcred1 d
    FULL OUTER JOIN pdcred1 p ON d.acctno = p.acctno
    WHERE ABS(COALESCE(d.drtotol, 0) - COALESCE(p.pdrtotol, 0)) >= 500000
""")

con.execute("""
    CREATE TEMP TABLE dmhp_check AS
    SELECT
        COALESCE(d.acctno, p.acctno)                            AS acctno,
        COALESCE(d.dhptotol, 0)                                 AS dhptotol,
        COALESCE(p.pdhptoto, 0)                                 AS pdhptoto
    FROM dhp1 d
    FULL OUTER JOIN pdhp1 p ON d.acctno = p.acctno
    WHERE ABS(COALESCE(d.dhptotol, 0) - COALESCE(p.pdhptoto, 0)) >= 500000
""")


# ---------------------------------------------------------------------------
# 14. Build DMLOAN / DMCRED / DMHP with customer details
#     (DISTINCT ON acctno ... ORDER BY acctno, noteno = SAS "FIRST.ACCTNO")
# ---------------------------------------------------------------------------
con.execute("""
    CREATE TEMP TABLE dmloan AS
    SELECT DISTINCT ON (l.acctno)
        l.acctno,
        l.name,
        l.branch,
        b.abbrev,
        c.dltotol,
        c.pdltotol,
        c.dltotol - c.pdltotol AS movement
    FROM lnnote l
    JOIN dmloan_check c ON l.acctno = c.acctno
    LEFT JOIN branch  b ON l.branch = b.branch
    ORDER BY l.acctno, l.noteno
""")

con.execute("""
    CREATE TEMP TABLE dmcred AS
    SELECT DISTINCT ON (r.acctno)
        r.acctno,
        r.name,
        r.branch,
        b.abbrev,
        c.drtotol,
        c.pdrtotol,
        c.drtotol - c.pdrtotol AS movement
    FROM revcre r
    JOIN dmcred_check c ON r.acctno = c.acctno
    LEFT JOIN branch   b ON r.branch = b.branch
    ORDER BY r.acctno, r.noteno
""")

con.execute("""
    CREATE TEMP TABLE dmhp AS
    SELECT DISTINCT ON (h.acctno)
        h.acctno,
        h.name,
        h.branch,
        b.abbrev,
        c.dhptotol,
        c.pdhptoto,
        c.dhptotol - c.pdhptoto AS movement
    FROM hploan h
    JOIN dmhp_check c ON h.acctno = c.acctno
    LEFT JOIN branch  b ON h.branch = b.branch
    ORDER BY h.acctno, h.noteno
""")

con.execute(f"COPY dmloan TO '{OUTPUT_DIR}/dmloan_{rdate_file}.csv' (HEADER, DELIMITER ',')")
con.execute(f"COPY dmcred TO '{OUTPUT_DIR}/dmcred_{rdate_file}.csv' (HEADER, DELIMITER ',')")
con.execute(f"COPY dmhp   TO '{OUTPUT_DIR}/dmhp_{rdate_file}.csv'   (HEADER, DELIMITER ',')")

print(f"  Wrote dmloan_{rdate_file}.csv")
print(f"  Wrote dmcred_{rdate_file}.csv")
print(f"  Wrote dmhp_{rdate_file}.csv")


# ---------------------------------------------------------------------------
# 15. Summary
# ---------------------------------------------------------------------------
loan_mvmt = con.execute("SELECT COUNT(*) FROM dmloan").fetchone()[0]
cred_mvmt = con.execute("SELECT COUNT(*) FROM dmcred").fetchone()[0]
hp_mvmt   = con.execute("SELECT COUNT(*) FROM dmhp").fetchone()[0]

print(f"""
Islamic Daily Loan Movement Report Complete
Date: {rdate}

Branch Summaries:
  1. MLOAN - Term Loan Outstanding by Branch
  2. MCRED - Revolving Credit Outstanding by Branch
  3. MHP   - HP Outstanding by Branch

Customer Movements (>= RM 500K):
  - Term Loans       : {loan_mvmt} accounts
  - Revolving Credit : {cred_mvmt} accounts
  - HP Loans         : {hp_mvmt} accounts

Output Files:
  - mloan_{rdate_file}.csv
  - mcred_{rdate_file}.csv
  - mhp_{rdate_file}.csv
  - dmloan_{rdate_file}.csv
  - dmcred_{rdate_file}.csv
  - dmhp_{rdate_file}.csv
  - lndly{reptday:02d}.parquet
""")

con.close()
print(f"Completed: {OUTPUT_DIR}")
