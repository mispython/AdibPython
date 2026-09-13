#!/usr/bin/env python3
"""
EIIDLOAN - Islamic Daily Loan Movement Report
Tracks daily changes in term loans, revolving credit, and HP accounts
"""

import duckdb
from pathlib import Path
from datetime import datetime, timedelta


DATEFILE_PATH = Path('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLOAN/DATEFILE') 
BRANCH_PATH = Path('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLOAN/LKP_BRANCH') 
LNNOTE = Path('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m{REPTMON}.sas7bdat') 
OUTPUT_DIR = Path('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLOAN') 
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()

with open(DATEFILE_PATH / 'DATEFILE', 'r') as f:
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

*
all inputs are in FLAT FILE, for both DATEFILE and LKP_BRANCH.
only the lnnote file in sas7bdat.
use pyreadstat to read it.
remove reptdate, use datetime timedelta - 1 instead. 
*
    
below is the original sas code:


DATA REPTDATE;
     INFILE DATEFILE LRECL=80 OBS=1;
     INPUT @01  EXTDATE   11.;
     REPTDATE = INPUT(SUBSTR(PUT(EXTDATE, Z11.), 1, 8), MMDDYY8.);
     PREVDATE = REPTDATE -1;
     DLETDATE = REPTDATE -3;
     REPTDAY  = DAY(REPTDATE);
     PREVDAY  = DAY(PREVDATE);
     DLETDAY  = DAY(DLETDATE);
     IF MONTH(REPTDATE) = 1 AND DAY(REPTDATE) = 1 THEN
        YY = YEAR(REPTDATE) - 1;
     ELSE YY = YEAR(REPTDATE);
     CALL SYMPUT('REPTYEAR', PUT(REPTDATE, YEAR4.));
     CALL SYMPUT('PREVYEAR', PUT(YY,Z4.));
     CALL SYMPUT('REPTMON', PUT(MONTH(REPTDATE), Z2.));
     CALL SYMPUT('REPTDAY', PUT(DAY(REPTDATE), Z2.));
     CALL SYMPUT('RDATE', PUT(REPTDATE, DDMMYY8.));
     CALL SYMPUT('EXTDATE', PUT(EXTDATE, Z11.));
     CALL SYMPUT('REPTDATE',PUT(REPTDATE,Z5.));
     CALL SYMPUT('PREVDAY',PUT(PREVDAY,Z2.));
     CALL SYMPUT('DLETDAY',PUT(DLETDAY,Z2.));
RUN;

LIBNAME MIS    "SAP.PIBB.MIS.D&REPTYEAR" DISP=OLD;
LIBNAME MIS1   "SAP.PIBB.MIS.D&PREVYEAR" DISP=SHR;

DATA LOAN;
    SET LOAN.LNNOTE;
    KEEP ACCTNO NOTENO NAME BALANCE BRANCH
         LOANTYPE CURBAL REPTDATE EXTDATE;
    REPTDATE=&REPTDATE;
    EXTDATE= &EXTDATE;
    BRANCH = PENDBRH;
*;
PROC DATASETS LIB=MIS NOLIST;
     DELETE LNDLY&REPTDAY;
     DELETE LNDLY&DLETDAY;
RUN;
*;
DATA MIS.LNDLY&REPTDAY;
    SET LOAN;
RUN;
*;
*;
DATA LNNOTE
     HPLOAN
     REVCRE;
  SET MIS.LNDLY&REPTDAY;
  IF LOANTYPE IN (302,350,364,365,506,902,903,910,
                  925,951)  THEN OUTPUT REVCRE;
  ELSE IF LOANTYPE IN (128,130,380,381,700,705)
                       THEN OUTPUT HPLOAN;
  ELSE OUTPUT LNNOTE;
*;
PROC SUMMARY DATA=LNNOTE NWAY;
   CLASS BRANCH REPTDATE EXTDATE;
   VAR BALANCE;
   OUTPUT OUT=LOANSUM
              (RENAME=(_FREQ_=NOACCT) DROP=_TYPE_)
          SUM=BRLNAMT;
RUN;
*;
PROC SUMMARY DATA=REVCRE NWAY;
   CLASS BRANCH REPTDATE EXTDATE;
   VAR BALANCE;
   OUTPUT OUT=REVSUMM
              (RENAME=(_FREQ_=REVACC) DROP=_TYPE_)
          SUM=BRRVAMT;
RUN;
*;
PROC SUMMARY DATA=HPLOAN NWAY;
   CLASS BRANCH REPTDATE EXTDATE;
   VAR BALANCE;
   OUTPUT OUT=HPSUMM
              (RENAME=(_FREQ_=HPACC) DROP=_TYPE_)
          SUM=BRHPAMT;
RUN;
*;
DATA PLNNOTE
     PHPLOAN
     PREVCRE;
  SET MIS1.LNDLY&PREVDAY;
  IF LOANTYPE IN (302,350,364,365,506,902,903,910,
                  925,951)  THEN OUTPUT PREVCRE;
  ELSE IF LOANTYPE IN (128,130,380,381,700,705)
                       THEN OUTPUT PHPLOAN;
  ELSE OUTPUT PLNNOTE;
*;
PROC SUMMARY DATA=PLNNOTE NWAY;
   CLASS BRANCH REPTDATE EXTDATE;
   VAR BALANCE;
   OUTPUT OUT=PLOANSUM
              (RENAME=(_FREQ_=PNOACCT) DROP=_TYPE_)
          SUM=PBRLNAMT;
RUN;
*;
PROC SUMMARY DATA=PREVCRE NWAY;
   CLASS BRANCH REPTDATE EXTDATE;
   VAR BALANCE;
   OUTPUT OUT=PREVSUMM
              (RENAME=(_FREQ_=PREVACC) DROP=_TYPE_)
          SUM=PBRRVAMT;
RUN;
*;
PROC SUMMARY DATA=PHPLOAN NWAY;
   CLASS BRANCH REPTDATE EXTDATE;
   VAR BALANCE;
   OUTPUT OUT=PHPSUMM
              (RENAME=(_FREQ_=PHPACC) DROP=_TYPE_)
          SUM=PBRHPAMT;
RUN;
*;
DATA HPLOANS;
   MERGE HPSUMM PHPSUMM; BY BRANCH;
*;
DATA REVCRED;
   MERGE REVSUMM PREVSUMM; BY BRANCH;
*;
DATA LOANS;
   MERGE LOANSUM PLOANSUM; BY BRANCH;
*;
PROC SORT DATA=LOANS; BY BRANCH;
PROC SORT DATA=REVCRED; BY BRANCH;
PROC SORT DATA=HPLOANS; BY BRANCH;
*;
DATA BRANCH;
  INFILE BRANCHF;
  INPUT @001 BANK       $1.
        @002 BRANCH      3.
        @006 ABBREV     $3.
        @012 BRCHNAME  $30.;
*;
PROC SORT DATA=BRANCH; BY BRANCH;
*;
DATA MLOAN;
   MERGE LOANS(IN=A) BRANCH; BY BRANCH;
   IF A;
   VARIANLN = BRLNAMT - PBRLNAMT;
*;
DATA MCRED;
   MERGE REVCRED(IN=A) BRANCH; BY BRANCH;
   IF A;
   VARIANRV = BRRVAMT - PBRRVAMT;
*;
DATA MHP;
   MERGE HPLOANS(IN=A) BRANCH; BY BRANCH;
   IF A;
   VARIANHP = BRHPAMT - PBRHPAMT;
*;
TITLE2 'PUBLIC ISLAMIC BANK BERHAD';
TITLE3 'BRANCH SUMMARY REPORT ON DAILY TERM LOAN OUTSTANDING(RM)';
TITLE4 'AS AT ' &RDATE;
*;
PROC TABULATE DATA=MLOAN MISSING NOSEPS;
   CLASS BRANCH ABBREV BRCHNAME;
   VAR PBRLNAMT BRLNAMT NOACCT VARIANLN;
   TABLE BRANCH='CODE'*
         ABBREV='ABBREV'*
         BRCHNAME='NAME' ALL='TOTAL',
         SUM=' '*NOACCT='NO OF ACCOUNTS'*F=10.
         SUM='OUSTANDING TERM LOAN (RM)'*(PBRLNAMT='PREVIOUS AMT'
         BRLNAMT='CURRENT AMT')*F=COMMA18.2
         SUM=' '*VARIANLN='VARIANCE'*F=COMMA20.2
         /RTS=60 BOX='BRANCH';
*;
PROC TABULATE DATA=MCRED MISSING NOSEPS;
   CLASS BRANCH ABBREV BRCHNAME;
   VAR PBRRVAMT BRRVAMT REVACC VARIANRV;
   TABLE BRANCH='CODE'*
         ABBREV='ABBREV'*
         BRCHNAME='NAME' ALL='TOTAL',
         SUM=' '*REVACC='NO OF ACCOUNTS'*F=10.
         SUM='OUSTANDING REVOLVING CREDIT (RM)'*(PBRRVAMT='PREVIOUS AMT'
         BRRVAMT='CURRENT AMT')*F=COMMA18.2
         SUM=' '*VARIANRV='VARIANCE'*F=COMMA20.2
         /RTS=60 BOX='BRANCH';
*;
TITLE3 'BRANCH SUMMARY REPORT ON DAILY REVOLVING CREDIT OUTSTANDING(RM)';
*;
PROC TABULATE DATA=MHP MISSING NOSEPS;
   CLASS BRANCH ABBREV BRCHNAME;
   VAR PBRHPAMT BRHPAMT HPACC VARIANHP;
   TABLE BRANCH='CODE'*
         ABBREV='ABBREV'*
         BRCHNAME='NAME' ALL='TOTAL',
         SUM=' '*HPACC='NO OF ACCOUNTS'*F=10.
         SUM='OUSTANDING HP (RM)'*(PBRHPAMT='PREVIOUS AMT'
         BRHPAMT='CURRENT AMT')*F=COMMA18.2
         SUM=' '*VARIANHP='VARIANCE'*F=COMMA20.2
         /RTS=60 BOX='BRANCH';
*;
TITLE3 'BRANCH SUMMARY REPORT ON DAILY HP OUTSTANDING(RM)'              ;
*;
   /**********************************************************/
   /** START PROCESSING REPORT II.                          **/
   /***       CUSTOMER'S MOVEMENT ON DAILY BASIS FOR BANK   **/
   /**********************************************************/
*;
DATA DLOAN
     DHP
     DCRED;
  SET MIS.LNDLY&REPTDAY;
  IF LOANTYPE IN (302,350,364,365,506,902,903,910,
                  925,951)  THEN OUTPUT DCRED;
  ELSE IF LOANTYPE IN (128,130,380,381,700,705)
                       THEN OUTPUT DHP;
  ELSE OUTPUT DLOAN;
*;
DATA PDLOAN
     PDHP
     PDCRED;
  SET MIS1.LNDLY&PREVDAY (RENAME=(CURBAL=PCURBAL BALANCE=PBALANCE));
  IF LOANTYPE IN (302,350,364,365,506,902,903,910,
                  925,951)  THEN OUTPUT PDCRED;
  ELSE IF LOANTYPE IN (128,130,380,381,700,705)
                       THEN OUTPUT PDHP;
  ELSE OUTPUT PDLOAN;
*;
PROC SUMMARY DATA=DLOAN;
   BY    ACCTNO REPTDATE;
   VAR BALANCE;
   OUTPUT OUT=DLOAN1
              (DROP=_FREQ_ _TYPE_) SUM=DLTOTOL;
RUN;
*;
PROC SUMMARY DATA=DCRED;
   BY    ACCTNO REPTDATE;
   VAR BALANCE;
   OUTPUT OUT=DCRED1
              (DROP=_FREQ_ _TYPE_) SUM=DRTOTOL;
RUN;
*;
PROC SUMMARY DATA=DHP;
   BY    ACCTNO REPTDATE;
   VAR BALANCE;
   OUTPUT OUT=DHP1
              (DROP=_FREQ_ _TYPE_) SUM=DHPTOTOL;
RUN;
*;
PROC SUMMARY DATA=PDLOAN;
   BY    ACCTNO REPTDATE;
   VAR PBALANCE;
   OUTPUT OUT=PDLOAN1
              (DROP=_FREQ_ _TYPE_) SUM=PDLTOTOL;
RUN;
*;
PROC SUMMARY DATA=PDCRED;
   BY    ACCTNO REPTDATE;
   VAR PBALANCE;
   OUTPUT OUT=PDCRED1
              (DROP=_FREQ_ _TYPE_) SUM=PDRTOTOL;
RUN;
*;
PROC SUMMARY DATA=PDHP;
   BY    ACCTNO REPTDATE;
   VAR PBALANCE;
   OUTPUT OUT=PDHP1
              (DROP=_FREQ_ _TYPE_) SUM=PDHPTOTO;
RUN;
*;
PROC SORT DATA=DLOAN1;  BY ACCTNO;
PROC SORT DATA=DCRED1;  BY ACCTNO;
PROC SORT DATA=DHP1;    BY ACCTNO;
PROC SORT DATA=PDLOAN1; BY ACCTNO;
PROC SORT DATA=PDCRED1; BY ACCTNO;
PROC SORT DATA=PDHP1;   BY ACCTNO;
*;
DATA LOAN;
   MERGE DLOAN1 PDLOAN1; BY ACCTNO;
*;
DATA CRED;
   MERGE DCRED1 PDCRED1; BY ACCTNO;
*;
DATA HP;
   MERGE DHP1 PDHP1; BY ACCTNO;
*;
DATA DMLOAN;
   SET LOAN;
   IF DLTOTOL = .    THEN DLTOTOL = 0.00;
   IF PDLTOTOL = .   THEN PDLTOTOL = 0.00;
   IF ABS(DLTOTOL-PDLTOTOL) GE 500000   THEN OUTPUT;
*;
DATA DMCRED;
   SET CRED;
   IF DRTOTOL = .    THEN DRTOTOL = 0.00;
   IF PDRTOTOL = .   THEN PDRTOTOL = 0.00;
   IF ABS(DRTOTOL-PDRTOTOL) GE 500000   THEN OUTPUT;
*;
DATA DMHP;
   SET HP;
   IF DHPTOTOL = .    THEN DHPTOTOL = 0.00;
   IF PDHPTOTO = .   THEN PDHPTOTO = 0.00;
   IF ABS(DHPTOTOL-PDHPTOTO) GE 500000   THEN OUTPUT;
*;
DATA DMLOAN;
   MERGE DLOAN PDLOAN DMLOAN(IN=A); BY ACCTNO;
   IF A   THEN OUTPUT;

DATA DMCRED;
   MERGE DCRED PDCRED DMCRED(IN=A); BY ACCTNO;
   IF A   THEN OUTPUT;
*;
DATA DMHP;
   MERGE DHP PDHP DMHP(IN=A); BY ACCTNO;
   IF A   THEN OUTPUT;
*;
DATA DMLOAN;
  SET DMLOAN; BY ACCTNO;
  IF FIRST.ACCTNO  THEN OUTPUT;

DATA DMCRED;
  SET DMCRED; BY ACCTNO;
  IF FIRST.ACCTNO  THEN OUTPUT;
*;
DATA DMHP;
  SET DMHP; BY ACCTNO;
  IF FIRST.ACCTNO  THEN OUTPUT;
*;
PROC SORT DATA=DMLOAN; BY BRANCH;
PROC SORT DATA=DMCRED; BY BRANCH;
PROC SORT DATA=DMHP;   BY BRANCH;
*;
DATA DMLOAN;
   MERGE DMLOAN(IN=A) BRANCH; BY BRANCH;
   IF A   THEN OUTPUT;
*;
DATA DMCRED;
   MERGE DMCRED(IN=A) BRANCH; BY BRANCH;
   IF A   THEN OUTPUT;
*;
DATA DMHP;
   MERGE DMHP(IN=A) BRANCH; BY BRANCH;
   IF A   THEN OUTPUT;
*;
TITLE2 'PUBLIC ISLAMIC BANK BERHAD - RETAIL BANKING DIVISION';
TITLE3 'REPORT TITLE : EIBDLOAN';
TITLE4 "DAILY MOVEMENT IN BANK'S TERM LOAN ACCOUNTS AS AT : " &RDATE;
TITLE5 'NET INCREASED/(DECREASE) OF RM 500K ABOVE PER CUSTOMER';
*;
PROC REPORT DATA=DMLOAN NOWD HEADLINE SPLIT='*';
  COLUMN ABBREV NAME ACCTNO DLTOTOL PDLTOTOL MOVEMENT;
  DEFINE ABBREV / DISPLAY FORMAT=$6. 'BRANCH';
  DEFINE NAME    / DISPLAY FORMAT=$25. 'NAME OF CUSTOMER';
  DEFINE ACCTNO  / DISPLAY FORMAT=20. 'ACCOUNT NO';
  DEFINE DLTOTOL / ANALYSIS SUM FORMAT=COMMA18.2 'CURRENT BALANCE';
  DEFINE PDLTOTOL / ANALYSIS SUM FORMAT=COMMA18.2 'PREVIOUS BALANCE';
  DEFINE MOVEMENT/ COMPUTED FORMAT=COMMA18.2
                   'NET INCREASE/*(DECREASE)';

  COMPUTE MOVEMENT;
     MOVEMENT=SUM(DLTOTOL.SUM,(-1)*PDLTOTOL.SUM);
  ENDCOMP;

  RBREAK AFTER / PAGE DUL OL SUMMARIZE;
RUN;
*;
TITLE2 'PUBLIC ISLAMIC BANK BERHAD - RETAIL BANKING DIVISION';
TITLE3 'REPORT TITLE : EIIDLOAN';
TITLE4 "DAILY MOVEMENT IN BANK S REVOLVING CREDIT ACCOUNTS AS AT : " &RDATE;
TITLE5 'NET INCREASED/(DECREASE) OF RM 500K & ABOVE PER CUSTOMER';
*;
PROC REPORT DATA=DMCRED NOWD HEADLINE SPLIT='*';
  COLUMN ABBREV NAME ACCTNO DRTOTOL PDRTOTOL MOVEMENT;
  DEFINE ABBREV / DISPLAY  FORMAT=$6. 'BRANCH';
  DEFINE NAME    / DISPLAY FORMAT=$25. 'NAME OF CUSTOMER';
  DEFINE ACCTNO  / DISPLAY FORMAT=20.  'ACCOUNT NO';
  DEFINE DRTOTOL / ANALYSIS SUM FORMAT=COMMA18.2 'CURRENT BALANCE';
  DEFINE PDRTOTOL / ANALYSIS SUM FORMAT=COMMA18.2 'PREVIOUS BALANCE';
  DEFINE MOVEMENT/ COMPUTED FORMAT=COMMA18.2
                   'NET INCREASE/*(DECREASE)';

  COMPUTE MOVEMENT;
     MOVEMENT=SUM(DRTOTOL.SUM,(-1)*PDRTOTOL.SUM);
  ENDCOMP;

  RBREAK AFTER / PAGE DUL OL SUMMARIZE;
RUN;

TITLE2 'PUBLIC ISLAMIC BANK BERHAD - RETAIL BANKING DIVISION';
TITLE3 'REPORT TITLE : EIIDLOAN';
TITLE4 "DAILY MOVEMENT IN BANK S HP ACCOUNTS AS AT : " &RD              ATE;
TITLE5 'NET INCREASED/(DECREASE) OF RM 500K & ABOVE PER CUSTOMER';
*;
PROC REPORT DATA=DMHP NOWD HEADLINE SPLIT='*';
  COLUMN ABBREV NAME ACCTNO DHPTOTOL PDHPTOTO MOVEMENT;
  DEFINE ABBREV / DISPLAY  FORMAT=$6. 'BRANCH';
  DEFINE NAME    / DISPLAY FORMAT=$25. 'NAME OF CUSTOMER';
  DEFINE ACCTNO  / DISPLAY FORMAT=20.  'ACCOUNT NO';
  DEFINE DHPTOTOL / ANALYSIS SUM FORMAT=COMMA18.2 'CURRENT BALANCE';
  DEFINE PDHPTOTO / ANALYSIS SUM FORMAT=COMMA18.2 'PREVIOUS BALANCE';
  DEFINE MOVEMENT/ COMPUTED FORMAT=COMMA18.2
                   'NET INCREASE/*(DECREASE)';

  COMPUTE MOVEMENT;
     MOVEMENT=SUM(DHPTOTOL.SUM,(-1)*PDHPTOTO.SUM);
  ENDCOMP;

  RBREAK AFTER / PAGE DUL OL SUMMARIZE;
RUN;
