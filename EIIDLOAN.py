#!/usr/bin/env python3
"""
EIIDLOAN - Islamic Daily Loan Movement Report
Tracks daily changes in term loans, revolving credit, and HP accounts
"""

import duckdb
from pathlib import Path
from datetime import datetime, timedelta

BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()

with open(INPUT_DIR / 'datefile.txt', 'r') as f:
    extdate = int(f.readline().strip())

reptdate_str = str(extdate)[:8]
reptdate = datetime.strptime(reptdate_str, '%m%d%Y')
prevdate = reptdate - timedelta(days=1)
dletdate = reptdate - timedelta(days=3)

reptday = reptdate.day
prevday = prevdate.day
dletday = dletdate.day

yy = reptdate.year - 1 if reptdate.month == 1 and reptdate.day == 1 else reptdate.year
reptyear, prevyear = reptdate.year, yy
reptmon = reptdate.month
rdate = reptdate.strftime('%d/%m/%Y')

print(f"Islamic Daily Loan Movement - {rdate}")

con.execute(f"""
    CREATE TEMP TABLE loan AS
    SELECT 
        acctno, noteno, name, balance, loantype, curbal,
        {int(reptdate.strftime('%y%m%d'))} reptdate,
        {extdate} extdate,
        pendbrh branch
    FROM read_parquet('{INPUT_DIR}/loan/lnnote.parquet')
""")

con.execute(f"COPY loan TO '{OUTPUT_DIR}/lndly{reptday:02d}.parquet'")

con.execute("""
    CREATE TEMP TABLE lnnote AS
    SELECT * FROM loan
    WHERE loantype NOT IN (302,350,364,365,506,902,903,910,925,951,128,130,380,381,700,705)
""")

con.execute("""
    CREATE TEMP TABLE revcre AS
    SELECT * FROM loan
    WHERE loantype IN (302,350,364,365,506,902,903,910,925,951)
""")

con.execute("""
    CREATE TEMP TABLE hploan AS
    SELECT * FROM loan
    WHERE loantype IN (128,130,380,381,700,705)
""")

con.execute("""
    CREATE TEMP TABLE loansum AS
    SELECT branch, reptdate, extdate, COUNT(*) noacct, SUM(balance) brlnamt
    FROM lnnote
    GROUP BY branch, reptdate, extdate
""")

con.execute("""
    CREATE TEMP TABLE revsumm AS
    SELECT branch, reptdate, extdate, COUNT(*) revacc, SUM(balance) brrvamt
    FROM revcre
    GROUP BY branch, reptdate, extdate
""")

con.execute("""
    CREATE TEMP TABLE hpsumm AS
    SELECT branch, reptdate, extdate, COUNT(*) hpacc, SUM(balance) brhpamt
    FROM hploan
    GROUP BY branch, reptdate, extdate
""")

con.execute(f"""
    CREATE TEMP TABLE prev_loan AS
    SELECT acctno, noteno, name, balance, loantype, curbal, reptdate, extdate, branch
    FROM read_parquet('{OUTPUT_DIR}/lndly{prevday:02d}.parquet')
""")

con.execute("""
    CREATE TEMP TABLE plnnote AS
    SELECT * FROM prev_loan
    WHERE loantype NOT IN (302,350,364,365,506,902,903,910,925,951,128,130,380,381,700,705)
""")

con.execute("""
    CREATE TEMP TABLE prevcre AS
    SELECT * FROM prev_loan
    WHERE loantype IN (302,350,364,365,506,902,903,910,925,951)
""")

con.execute("""
    CREATE TEMP TABLE phploan AS
    SELECT * FROM prev_loan
    WHERE loantype IN (128,130,380,381,700,705)
""")

con.execute("""
    CREATE TEMP TABLE ploansum AS
    SELECT branch, reptdate, extdate, COUNT(*) pnoacct, SUM(balance) pbrlnamt
    FROM plnnote
    GROUP BY branch, reptdate, extdate
""")

con.execute("""
    CREATE TEMP TABLE prevsumm AS
    SELECT branch, reptdate, extdate, COUNT(*) prevacc, SUM(balance) pbrrvamt
    FROM prevcre
    GROUP BY branch, reptdate, extdate
""")

con.execute("""
    CREATE TEMP TABLE phpsumm AS
    SELECT branch, reptdate, extdate, COUNT(*) phpacc, SUM(balance) pbrhpamt
    FROM phploan
    GROUP BY branch, reptdate, extdate
""")

con.execute(f"""
    CREATE TEMP TABLE branch AS
    SELECT branch, abbrev, brchname
    FROM read_parquet('{INPUT_DIR}/branchf.parquet')
""")

con.execute("""
    CREATE TEMP TABLE mloan AS
    SELECT 
        l.branch, b.abbrev, b.brchname,
        COALESCE(l.brlnamt, 0) brlnamt,
        COALESCE(p.pbrlnamt, 0) pbrlnamt,
        COALESCE(l.noacct, 0) noacct,
        COALESCE(l.brlnamt, 0) - COALESCE(p.pbrlnamt, 0) varianln
    FROM loansum l
    LEFT JOIN ploansum p ON l.branch = p.branch
    LEFT JOIN branch b ON l.branch = b.branch
""")

con.execute("""
    CREATE TEMP TABLE mcred AS
    SELECT 
        r.branch, b.abbrev, b.brchname,
        COALESCE(r.brrvamt, 0) brrvamt,
        COALESCE(p.pbrrvamt, 0) pbrrvamt,
        COALESCE(r.revacc, 0) revacc,
        COALESCE(r.brrvamt, 0) - COALESCE(p.pbrrvamt, 0) varianrv
    FROM revsumm r
    LEFT JOIN prevsumm p ON r.branch = p.branch
    LEFT JOIN branch b ON r.branch = b.branch
""")

con.execute("""
    CREATE TEMP TABLE mhp AS
    SELECT 
        h.branch, b.abbrev, b.brchname,
        COALESCE(h.brhpamt, 0) brhpamt,
        COALESCE(p.pbrhpamt, 0) pbrhpamt,
        COALESCE(h.hpacc, 0) hpacc,
        COALESCE(h.brhpamt, 0) - COALESCE(p.pbrhpamt, 0) varianhp
    FROM hpsumm h
    LEFT JOIN phpsumm p ON h.branch = p.branch
    LEFT JOIN branch b ON h.branch = b.branch
""")

con.execute(f"COPY mloan TO '{OUTPUT_DIR}/mloan_{rdate.replace('/','-')}.csv' (HEADER, DELIMITER ',')")
con.execute(f"COPY mcred TO '{OUTPUT_DIR}/mcred_{rdate.replace('/','-')}.csv' (HEADER, DELIMITER ',')")
con.execute(f"COPY mhp TO '{OUTPUT_DIR}/mhp_{rdate.replace('/','-')}.csv' (HEADER, DELIMITER ',')")

con.execute("""
    CREATE TEMP TABLE dloan1 AS
    SELECT acctno, reptdate, SUM(balance) dltotol
    FROM lnnote
    GROUP BY acctno, reptdate
""")

con.execute("""
    CREATE TEMP TABLE dcred1 AS
    SELECT acctno, reptdate, SUM(balance) drtotol
    FROM revcre
    GROUP BY acctno, reptdate
""")

con.execute("""
    CREATE TEMP TABLE dhp1 AS
    SELECT acctno, reptdate, SUM(balance) dhptotol
    FROM hploan
    GROUP BY acctno, reptdate
""")

con.execute("""
    CREATE TEMP TABLE pdloan1 AS
    SELECT acctno, reptdate, SUM(balance) pdltotol
    FROM plnnote
    GROUP BY acctno, reptdate
""")

con.execute("""
    CREATE TEMP TABLE pdcred1 AS
    SELECT acctno, reptdate, SUM(balance) pdrtotol
    FROM prevcre
    GROUP BY acctno, reptdate
""")

con.execute("""
    CREATE TEMP TABLE pdhp1 AS
    SELECT acctno, reptdate, SUM(balance) pdhptoto
    FROM phploan
    GROUP BY acctno, reptdate
""")

con.execute("""
    CREATE TEMP TABLE dmloan_check AS
    SELECT 
        COALESCE(d.acctno, p.acctno) acctno,
        COALESCE(d.dltotol, 0) dltotol,
        COALESCE(p.pdltotol, 0) pdltotol
    FROM dloan1 d
    FULL OUTER JOIN pdloan1 p ON d.acctno = p.acctno
    WHERE ABS(COALESCE(d.dltotol, 0) - COALESCE(p.pdltotol, 0)) >= 500000
""")

con.execute("""
    CREATE TEMP TABLE dmcred_check AS
    SELECT 
        COALESCE(d.acctno, p.acctno) acctno,
        COALESCE(d.drtotol, 0) drtotol,
        COALESCE(p.pdrtotol, 0) pdrtotol
    FROM dcred1 d
    FULL OUTER JOIN pdcred1 p ON d.acctno = p.acctno
    WHERE ABS(COALESCE(d.drtotol, 0) - COALESCE(p.pdrtotol, 0)) >= 500000
""")

con.execute("""
    CREATE TEMP TABLE dmhp_check AS
    SELECT 
        COALESCE(d.acctno, p.acctno) acctno,
        COALESCE(d.dhptotol, 0) dhptotol,
        COALESCE(p.pdhptoto, 0) pdhptoto
    FROM dhp1 d
    FULL OUTER JOIN pdhp1 p ON d.acctno = p.acctno
    WHERE ABS(COALESCE(d.dhptotol, 0) - COALESCE(p.pdhptoto, 0)) >= 500000
""")

con.execute("""
    CREATE TEMP TABLE dmloan AS
    SELECT DISTINCT ON (l.acctno)
        l.acctno, l.name, l.branch, b.abbrev,
        c.dltotol, c.pdltotol,
        c.dltotol - c.pdltotol movement
    FROM lnnote l
    JOIN dmloan_check c ON l.acctno = c.acctno
    LEFT JOIN branch b ON l.branch = b.branch
    ORDER BY l.acctno, l.noteno
""")

con.execute("""
    CREATE TEMP TABLE dmcred AS
    SELECT DISTINCT ON (r.acctno)
        r.acctno, r.name, r.branch, b.abbrev,
        c.drtotol, c.pdrtotol,
        c.drtotol - c.pdrtotol movement
    FROM revcre r
    JOIN dmcred_check c ON r.acctno = c.acctno
    LEFT JOIN branch b ON r.branch = b.branch
    ORDER BY r.acctno, r.noteno
""")

con.execute("""
    CREATE TEMP TABLE dmhp AS
    SELECT DISTINCT ON (h.acctno)
        h.acctno, h.name, h.branch, b.abbrev,
        c.dhptotol, c.pdhptoto,
        c.dhptotol - c.pdhptoto movement
    FROM hploan h
    JOIN dmhp_check c ON h.acctno = c.acctno
    LEFT JOIN branch b ON h.branch = b.branch
    ORDER BY h.acctno, h.noteno
""")

con.execute(f"COPY dmloan TO '{OUTPUT_DIR}/dmloan_{rdate.replace('/','-')}.csv' (HEADER, DELIMITER ',')")
con.execute(f"COPY dmcred TO '{OUTPUT_DIR}/dmcred_{rdate.replace('/','-')}.csv' (HEADER, DELIMITER ',')")
con.execute(f"COPY dmhp TO '{OUTPUT_DIR}/dmhp_{rdate.replace('/','-')}.csv' (HEADER, DELIMITER ',')")

loan_mvmt = con.execute("SELECT COUNT(*) FROM dmloan").fetchone()[0]
cred_mvmt = con.execute("SELECT COUNT(*) FROM dmcred").fetchone()[0]
hp_mvmt = con.execute("SELECT COUNT(*) FROM dmhp").fetchone()[0]

print(f"""
Islamic Daily Loan Movement Report Complete
Date: {rdate}

Branch Summaries:
1. MLOAN - Term Loan Outstanding by Branch
2. MCRED - Revolving Credit Outstanding by Branch
3. MHP - HP Outstanding by Branch

Customer Movements (>= RM 500K):
- Term Loans: {loan_mvmt} accounts
- Revolving Credit: {cred_mvmt} accounts
- HP Loans: {hp_mvmt} accounts

Output Files:
- mloan_{rdate.replace('/','-')}.csv
- mcred_{rdate.replace('/','-')}.csv
- mhp_{rdate.replace('/','-')}.csv
- dmloan_{rdate.replace('/','-')}.csv
- dmcred_{rdate.replace('/','-')}.csv
- dmhp_{rdate.replace('/','-')}.csv
- lndly{reptday:02d}.parquet
""")

con.close()
print(f"Completed: {OUTPUT_DIR}")
