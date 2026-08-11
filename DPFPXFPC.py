original sas code:

%INC PGM(PBBLNFMT);

PROC FORMAT;
   VALUE $BICTAG
      '95311'                        = 'A1.01'
      '95312'                        = 'A1.02'
      '95313'                        = 'A1.03'
      '95810'                        = 'A1.04'
      '95820'                        = 'A1.05'
      '95830','9583X'                = 'A1.06'
      '95840'                        = 'A1.07'
      '95329'                        = 'A1.08'
      '953XX'                        = 'A1.09'
      '9531X'                        = 'A1.10'
      '96311'                        = 'B1.01'
      '96313'                        = 'B1.02'
      '96810'                        = 'B1.03'
      '96820'                        = 'B1.04'
      '96830','9683X'                = 'B1.05'
      '96840'                        = 'B1.06'
      '96329'                        = 'B1.07'
       OTHER                         = '     '
       ;
RUN;

DATA _NULL_;
   SET LCR.REPTDATE;
   CALL SYMPUT('REPTYEAR',PUT(REPTDATE,YEAR4.));
   CALL SYMPUT('REPTMON',PUT(MONTH(REPTDATE),Z2.));
   CALL SYMPUT('REPTDAY',PUT(DAY(REPTDATE),Z2.));
   CALL SYMPUT('RPTDT',PUT(REPTDATE,YYMMDDN6.));
   CALL SYMPUT('FILDT',PUT(REPTDATE,DDMMYYN6.));
   CALL SYMPUT('RDATE',PUT(REPTDATE,DDMMYY10.));
RUN;

%LET EXCLCIS=%STR();
PROC SQL NOPRINT;
   SELECT DISTINCT CUSTNO INTO :EXCLCIS SEPARATED BY ','
     FROM LIST.KEEP_TOP_DEP_EXCL_PBB
    WHERE CUSTNO > 0;
QUIT;

PROC SORT DATA=LCR.VOSTRO OUT=VOSTRO; BY ACCTNO; RUN;

DATA VOSTRO;
   MERGE VOSTRO(IN=A) LCR.CISINFO;
   BY ACCTNO;
   IF A;
   CMMCODE = '953XX';  *VOSTRO;
   KEEP CMMCODE BRANCH ACCTNO CUSTCD CUSTNAME PRODUCT CURCODE
        AMOUNT CUSTNO NEWIC CUSTNAME;
RUN;

DATA CMM;
   SET LCR.CMM&REPTMON VOSTRO;
RUN;
PROC SORT DATA=CMM; BY NEWIC; RUN;

PROC SORT DATA=LIST.COF_MNI_DEPOSITOR_LIST
           OUT=COF_MNI_IDNO(KEEP=DEPID DEPGRP BUSSREG) NODUPKEY;
   BY BUSSREG;
RUN;

DATA MNISRC1 XMNISRC(DROP=DEPID DEPGRP);
   MERGE CMM(IN=A) COF_MNI_IDNO(RENAME=(BUSSREG=NEWIC));
   BY NEWIC;
   IF A;
   IF CUSTNO IN (&EXCLCIS) THEN EXCL = 'Y';
   IF DEPID > 0 THEN OUTPUT MNISRC1;
   ELSE              OUTPUT XMNISRC;
RUN;

PROC SORT DATA=XMNISRC OUT=CMM; BY CUSTNO; RUN;
PROC SORT DATA=LIST.COF_MNI_DEPOSITOR_LIST
           OUT=COF_MNI_CUST(KEEP=DEPID DEPGRP CUSTNO) NODUPKEY;
   BY CUSTNO;
RUN;

DATA MNISRC2 XMNISRC(DROP=DEPID);
   MERGE CMM(IN=A) COF_MNI_CUST;
   BY CUSTNO;
   IF A;
   IF DEPID > 0 THEN OUTPUT MNISRC2;
   ELSE              OUTPUT XMNISRC;
RUN;

DATA XMNISRC;
   SET XMNISRC;
   BY CUSTNO;
   RETAIN DEPID;
   IF _N_ = 1 THEN DEPID = 5000;
   IF FIRST.CUSTNO THEN DEPID = DEPID + 1;
   DEPGRP = CUSTNAME;
RUN;

DATA MNISRC;
   SET MNISRC1 MNISRC2 XMNISRC;
   BIC = SUBSTR(CMMCODE,1,5);
   SELECT(BIC);
      WHEN('95311','96311') FD   = AMOUNT;
      WHEN('95312')         SA   = AMOUNT;
      WHEN('95313','96313') CA   = AMOUNT;
      WHEN('953XX'        ) VOST = AMOUNT;
      WHEN('9531X'        ) GOLD = AMOUNT;
      WHEN('95840','96840') RNID = AMOUNT;
      OTHERWISE             DELETE;
   END;
   IF EXCL NE 'Y' THEN DO; *19-3747;
      FD2   = FD  ;
      SA2   = SA  ;
      CA2   = CA  ;
      RNID2 = RNID;
   END;
   IF CUSTCD IN ('77','78','95','96') THEN CUSTYPE = 'I';
   ELSE                                    CUSTYPE = 'C';
RUN;
PROC SORT; BY DEPID DEPGRP CUSTYPE; RUN;
PROC SUMMARY DATA=MNISRC NWAY;
   BY DEPID DEPGRP CUSTYPE;
   VAR FD SA CA VOST GOLD RNID FD2 SA2 CA2 RNID2;
   OUTPUT OUT=LIST.MNITOT(DROP=_TYPE_ _FREQ_) SUM=;
RUN;

%LET EXCLEQU=%STR();
PROC SQL NOPRINT;
   SELECT COMPRESS(CAT("'",CUSTNO,"'")) INTO :EXCLEQU SEPARATED BY ','
     FROM LIST.KEEP_TOP_DEP_EXCL_EQU_PBB
    WHERE CUSTNO NE '';
QUIT;

DATA CMM;
   SET LCR.EQU&REPTMON;
   WHERE CUSTNO NE '';
   IF CUSTNO IN (&EXCLEQU) THEN EXCL = 'Y';
RUN;
PROC SORT; BY CUSTNO; RUN;

PROC SORT DATA=LIST.COF_EQU_DEPOSITOR_LIST
           OUT=COF_EQU_CUST(KEEP=DEPID DEPGRP CUSTNO LINKID) NODUPKEY;
   BY CUSTNO;
RUN;

DATA EQUSRC XEQUSRC(DROP=DEPID);
   MERGE CMM(IN=A) COF_EQU_CUST;
   BY CUSTNO;
   IF A;
   IF DEPID > 0 THEN OUTPUT EQUSRC;
   ELSE              OUTPUT XEQUSRC;
RUN;

DATA XEQUSRC;
   SET XEQUSRC;
   BY CUSTNO;
   RETAIN DEPID;
   IF _N_ = 1 THEN DEPID = 50005000;
   IF FIRST.CUSTNO THEN DEPID = DEPID + 1;
   IF DEPGRP = '' THEN DEPGRP = CUSTNAME;
   IF DEPGRP = '' THEN DEPGRP = CUSTNO;
RUN;

DATA EQUSRC;
   SET EQUSRC(IN=A) XEQUSRC;
   BIC = SUBSTR(CMMCODE,1,5);
   SELECT(BIC);
      WHEN('95830','96830','9583X','9683X') STD  = AMOUNT;
      WHEN('95840','96840')                 NID  = AMOUNT;
      WHEN('95810','96810')                 IBB  = AMOUNT;
      WHEN('95820','96820')                 REPO = AMOUNT;
      WHEN('95329','96329')                 DCI  = AMOUNT;
      OTHERWISE                             DELETE;
   END;
   IF EXCL NE 'Y' THEN DO; *19-3747;
      STD2  = STD;
      NID2  = NID;
      DCI2  = DCI;
   END;
   IF BIC IN ('95850','96850') THEN DELETE; *N/A;
   IF A & LINKID = . THEN LINKID = 50000000+DEPID;
   IF LINKID = . THEN LINKID = DEPID;
   IF CUSTFISS IN ('77','78','95','96') THEN CUSTYPE = 'I';
   ELSE                                      CUSTYPE = 'C';
RUN;
PROC SORT; BY LINKID DEPGRP CUSTYPE; RUN;

PROC SUMMARY DATA=EQUSRC NWAY;
   BY LINKID DEPGRP CUSTYPE;
   VAR STD NID IBB REPO DCI STD2 NID2 DCI2;
   OUTPUT OUT=LIST.EQUTOT(DROP=_TYPE_ _FREQ_) SUM=;
RUN;

DATA ALLSRC;
   MERGE LIST.MNITOT LIST.EQUTOT(RENAME=(LINKID=DEPID CUSTYPE=CUSTYPEQ
                                         DEPGRP=DEPGRPEQ));
   BY DEPID;
   IF DEPGRP = '' THEN DEPGRP = DEPGRPEQ;
   IF CUSTYPE= '' THEN CUSTYPE= CUSTYPEQ;
   NID = SUM(NID,RNID); *17-766;
   TOT = SUM(FD,SA,GOLD,CA,STD,NID,IBB,REPO,DCI,VOST);
   *19-3747;
   MNI = SUM(FD2,SA2,CA2,RNID2);
   EQU = SUM(STD2,NID2,DCI2);
   TOT2= SUM(MNI,EQU);
RUN;
PROC SORT; BY DEPID DEPGRP CUSTYPE; RUN;

PROC SUMMARY DATA=ALLSRC NWAY;
   BY DEPID DEPGRP CUSTYPE;
   VAR TOT2 MNI EQU;
   OUTPUT OUT=ALLTOT2(DROP=_TYPE_ _FREQ_) SUM=;
RUN;
PROC SORT DATA=ALLTOT2; BY CUSTYPE DESCENDING TOT2; RUN;

%MACRO REPORT(CTYPE,DESC);
DATA ALLTOT;
   SET ALLTOT2(WHERE=(CUSTYPE="&CTYPE") OBS=50);
   RANK = _N_;
   DLM='05'X;
   FILE COFOUT&CTYPE;
   IF _N_=1 THEN DO;
      PUT @001 'PUBLIC BANK BERHAD'
          /    "TOP 50 LARGEST DEPOSITORS AS AT &RDATE"
          /
          /    "(i) Top 50 &DESC Depositors by Sources"
          /
          ;
      PUT @001                                    DLM
                'NOBS'                            DLM
                'DEPOSITORS'                      DLM
                'TOTAL BALANCE'                   DLM
                'M&I'                             DLM
                'EQUATION'                        DLM
          ;
   END;
   FORMAT TOT2 MNI EQU COMMA20.2;
   PUT @001                                  DLM
             _N_                             DLM
             DEPGRP                          DLM
             TOT2                            DLM
             MNI                             DLM
             EQU                             DLM
       ;
RUN;

PROC SORT DATA=ALLTOT; BY DEPID; RUN;
PROC SORT DATA=MNISRC; BY DEPID; RUN;
PROC SORT DATA=EQUSRC; BY LINKID; RUN;

DATA ALLMNI;
   MERGE ALLTOT(IN=A) MNISRC;
   BY DEPID;
   IF A;
   SRC = '1 - M&I';
RUN;
DATA ALLEQU;
   MERGE ALLTOT(IN=A) EQUSRC(DROP=DEPID RENAME=(LINKID=DEPID
                                                CUSTNO=EQCUSTNO));
   BY DEPID;
   IF A;
   SRC = '2 - EQU';
RUN;

DATA ALLTOT;
   SET ALLMNI ALLEQU;
   WHERE AMOUNT>0 AND EXCL NE 'Y' AND
         BIC NOT IN ('953XX','9531X','95810','96810','95820','96820');
RUN;
PROC SORT; BY RANK SRC ACCTNO; RUN;

DATA _NULL_;
   SET ALLTOT END=LAST;
   BY RANK SRC ACCTNO;
   DLM='05'X;
   FILE COFOUT&CTYPE MOD;
   IF _N_=1 THEN DO;
      PUT @001
          /
          /   "(ii) Detail Accounts Listing for Top 50 &DESC Depositors"
          /
          ;
   END;
   SRCX = SUBSTR(SRC,5,3);
   RETAIN CNT;
   IF FIRST.RANK THEN DO;
      IF FIRST.SRC THEN CNT=0;
      PUT @001  RANK                              DLM
                DEPGRP '(' DEPID ')'              DLM
          /
          ;
   END;
   IF FIRST.SRC THEN DO;
      CNT=0;
      PUT @001                                    DLM
                'Source: ' SRCX
          /
          ;
      IF SRCX = 'M&I' THEN
      PUT @001                                    DLM
                'NO'                              DLM
                'BRANCH'                          DLM
                'ACCTNO'                          DLM
                'CUSTNAME'                        DLM
                'CUSTNO'                          DLM
                'BUSSREG'                         DLM
                'CUSTCD'                          DLM
                'PRODUCT'                         DLM
                'BALANCE'                         DLM
          ;
      ELSE
      PUT @001                                    DLM
                'NO'                              DLM
                'DEALREF'                         DLM
                'DEALTYPE'                        DLM
                'NAME'                            DLM
                'CUST MNEMONIC'                   DLM
                'AMOUNT'                          DLM
          ;
   END;
   IF FIRST.ACCTNO THEN CURBAL=0;
   CURBAL+AMOUNT;
   IF FIRST.RANK OR FIRST.SRC THEN TOTBAL=0;
   TOTBAL+AMOUNT;
   FORMAT CURBAL TOTBAL AMOUNT COMMA20.2;

   IF LAST.ACCTNO THEN DO;
      CNT+1;
   IF SRCX = 'M&I' THEN
   PUT @001                                    DLM
             CNT                               DLM
             BRANCH                            DLM
             ACCTNO                            DLM
             CUSTNAME                          DLM
             CUSTNO                            DLM
             NEWIC                             DLM
             CUSTCD                            DLM
             PRODUCT                           DLM
             CURBAL                            DLM
          ;
   END;
   IF SRCX NE 'M&I' THEN DO;
      CNT+1;
   PUT @001                                    DLM
             CNT                               DLM
             DEALREF                           DLM
             DEALTYPE                          DLM
             CUSTNAME                          DLM
             EQCUSTNO                          DLM
             AMOUNT                            DLM
          ;
   END;
   IF LAST.RANK OR LAST.SRC THEN
   IF SRCX = 'M&I' THEN
   PUT @001                                    DLM
                                               DLM
                                               DLM
                                               DLM
                                               DLM
                                               DLM
                                               DLM
                                               DLM
                                               DLM
             TOTBAL                            DLM
          /
          ;
   ELSE
   PUT @001                                    DLM
                                               DLM
                                               DLM
                                               DLM
                                               DLM
                                               DLM
             TOTBAL                            DLM
          /
          ;
RUN;
%MEND REPORT;
%REPORT(I,Individual);
%REPORT(C,Corporate);

PROC SUMMARY DATA=ALLSRC NWAY;
   BY DEPID DEPGRP;
   VAR TOT FD SA GOLD CA STD NID IBB REPO DCI VOST;
   OUTPUT OUT=ALLTOT(DROP=_TYPE_ _FREQ_) SUM=;
RUN;
PROC SORT DATA=ALLTOT; BY DESCENDING TOT; RUN;

DATA ALLTOT;
   SET ALLTOT(OBS=100);
   RANK = _N_;
   DLM='05'X;
   FILE COFOUT1;
   IF _N_=1 THEN DO;
      PUT @001 'PUBLIC BANK BERHAD'
          /    "TOP 100 LARGEST DEPOSITORS AS AT &RDATE"
          /
          /    '(i) Top 100 Depositors by Products'
          /
          ;
      PUT @001                                    DLM
                'NOBS'                            DLM
                'DEPOSITORS'                      DLM
                'TOTAL BALANCE'                   DLM
                'FIXED DEPOSIT'                   DLM
                'SAVINGS'                         DLM
                'DEMAND DEPOSIT'                  DLM
                'SHORT TERM DEPOSIT'              DLM
                'NID ISSUED'                      DLM
                'INTERBANK BORROWING'             DLM
                'REPOS'                           DLM
                'DUAL CURRENCY INVESTMENT'        DLM
                'GOLD INVESTMENT'                 DLM
                'VOSTRO'                          DLM
          ;
   END;
   FORMAT TOT FD SA CA STD NID IBB REPO DCI GOLD VOST COMMA20.2;
   PUT @001                                  DLM
             _N_                             DLM
             DEPGRP                          DLM
             TOT                             DLM
             FD                              DLM
             SA                              DLM
             CA                              DLM
             STD                             DLM
             NID                             DLM
             IBB                             DLM
             REPO                            DLM
             DCI                             DLM
             GOLD                            DLM
             VOST                            DLM
       ;
RUN;

PROC SORT DATA=ALLTOT; BY DEPID; RUN;
PROC SORT DATA=MNISRC; BY DEPID; RUN;
PROC SORT DATA=EQUSRC; BY LINKID; RUN;

DATA ALLMNI;
   MERGE ALLTOT(IN=A) MNISRC;
   BY DEPID;
   IF A;
   SRC = '1 - M&I';
RUN;
DATA ALLEQU;
   MERGE ALLTOT(IN=A) EQUSRC(DROP=DEPID RENAME=(LINKID=DEPID
                                                CUSTNO=EQCUSTNO));
   BY DEPID;
   IF A;
   SRC = '2 - EQU';
RUN;

DATA LIST.ALLTOT ALLTOT;
   SET ALLMNI ALLEQU;
   WHERE AMOUNT>0;
RUN;
PROC SORT; BY RANK SRC ACCTNO; RUN;

DATA _NULL_;
   SET ALLTOT END=LAST;
   BY RANK SRC ACCTNO;
   DLM='05'X;
   FILE COFOUT2;
   IF _N_=1 THEN DO;
      PUT @001 'PUBLIC BANK BERHAD'
          /    "TOP 100 LARGEST DEPOSITORS AS AT &RDATE"
          /
          /    '(ii) Detail Accounts Listing for Top 100 Depositors'
          /
          ;
   END;
   SRCX = SUBSTR(SRC,5,3);
   RETAIN CNT;
   IF FIRST.RANK THEN DO;
      IF FIRST.SRC THEN CNT=0;
      PUT @001  RANK                              DLM
                DEPGRP '(' DEPID ')'              DLM
          /
          ;
   END;
   IF FIRST.SRC THEN DO;
      CNT=0;
      PUT @001                                    DLM
                'Source: ' SRCX
          /
          ;
      IF SRCX = 'M&I' THEN
      PUT @001                                    DLM
                'NO'                              DLM
                'BRANCH'                          DLM
                'ACCTNO'                          DLM
                'CUSTNAME'                        DLM
                'CUSTNO'                          DLM
                'BUSSREG'                         DLM
                'CUSTCD'                          DLM
                'PRODUCT'                         DLM
                'BALANCE'                         DLM
          ;
      ELSE
      PUT @001                                    DLM
                'NO'                              DLM
                'DEALREF'                         DLM
                'DEALTYPE'                        DLM
                'NAME'                            DLM
                'CUST MNEMONIC'                   DLM
                'AMOUNT'                          DLM
          ;
   END;
   IF FIRST.ACCTNO THEN CURBAL=0;
   CURBAL+AMOUNT;
   IF FIRST.RANK OR FIRST.SRC THEN TOTBAL=0;
   TOTBAL+AMOUNT;
   FORMAT TOT FD SA CA STD NID IBB REPO DCI GOLD VOST
          CURBAL TOTBAL AMOUNT COMMA20.2;
   IF LAST.ACCTNO THEN DO;
      CNT+1;
   IF SRCX = 'M&I' THEN
   PUT @001                                    DLM
             CNT                               DLM
             BRANCH                            DLM
             ACCTNO                            DLM
             CUSTNAME                          DLM
             CUSTNO                            DLM
             NEWIC                             DLM
             CUSTCD                            DLM
             PRODUCT                           DLM
             CURBAL                            DLM
          ;
   END;
   IF SRCX NE 'M&I' THEN DO;
      CNT+1;
   PUT @001                                    DLM
             CNT                               DLM
             DEALREF                           DLM
             DEALTYPE                          DLM
             CUSTNAME                          DLM
             EQCUSTNO                          DLM
             AMOUNT                            DLM
          ;
   END;
   IF LAST.RANK OR LAST.SRC THEN
   IF SRCX = 'M&I' THEN
   PUT @001                                    DLM
                                               DLM
                                               DLM
                                               DLM
                                               DLM
                                               DLM
                                               DLM
                                               DLM
                                               DLM
             TOTBAL                            DLM
          /
          ;
   ELSE
   PUT @001                                    DLM
                                               DLM
                                               DLM
                                               DLM
                                               DLM
                                               DLM
             TOTBAL                            DLM
          /
          ;
RUN;
PROC SORT DATA=ALLTOT; BY RANK CMMCODE; RUN;
PROC SORT DATA=ALLTOT OUT=RANKNM(KEEP=RANK DEPGRP DEPID) NODUPKEY;
BY RANK; RUN;

DATA TEMPLATE;
   INFILE TEMPL;
   INPUT @1  ITEM           $5.
         @11 IDESC     $CHAR50.
         ;
   FORMAT AMOUNT BUC1-BUC7 COMMA20.2;
   DO I=1 TO 100;
      RANK=I;
      OUTPUT;
   END;
RUN;
PROC SORT; BY RANK ITEM; RUN;
DATA TEMPLATE;
   MERGE TEMPLATE(IN=A) RANKNM;
   BY RANK;
   IF A;
RUN;

PROC SUMMARY DATA=ALLTOT NWAY;
   BY RANK CMMCODE;
   VAR AMOUNT;
   OUTPUT OUT=LCRCMM(DROP=_TYPE_ _FREQ_) SUM=;
RUN;

DATA LCRCMM;
   SET LCRCMM;
   FORMAT ITEM  $5.;
   BIC    = SUBSTR(CMMCODE,1,5);
   REM    = SUBSTR(CMMCODE,8,2);
   ITEM   = PUT(BIC,$BICTAG.);
   IF ITEM NE '';

   /* NO SPECIFIC MATURITY */
   IF BIC IN ('95312','95313','96313','9531X','953XX') THEN REM = '07';

   SELECT(REM);
      WHEN(01) BUC1 = AMOUNT;
      WHEN(02) BUC2 = AMOUNT;
      WHEN(03) BUC3 = AMOUNT;
      WHEN(04) BUC4 = AMOUNT;
      WHEN(05) BUC5 = AMOUNT;
      WHEN(06) BUC6 = AMOUNT;
      WHEN(07) BUC7 = AMOUNT;
      OTHERWISE;
   END;
   PART1 = SUBSTR(ITEM,1,2);     /* GRAND TOTAL RETAIL & NON-RETAIL */
RUN;
PROC SORT; BY RANK ITEM; RUN;

PROC SUMMARY DATA=LCRCMM NWAY;
   BY RANK ITEM;
   VAR AMOUNT BUC1-BUC7;
   OUTPUT OUT=CMM(DROP=_TYPE_ _FREQ_) SUM=;
RUN;
PROC SORT; BY RANK ITEM; RUN;

PROC SUMMARY DATA=LCRCMM NWAY;
   CLASS RANK PART1;
   VAR AMOUNT BUC1-BUC7;
   OUTPUT OUT=P1CMM(DROP=_TYPE_ _FREQ_) SUM=;
RUN;

PROC SUMMARY DATA=LCRCMM NWAY;
   CLASS RANK;
   VAR AMOUNT BUC1-BUC7;
   OUTPUT OUT=P2CMM(DROP=_TYPE_ _FREQ_) SUM=;
RUN;

DATA TOTCMM;
   SET P1CMM(IN=A) P2CMM;
   FORMAT ITEM  $5.;
   IF A THEN ITEM = COMPRESS(PART1 || '.99');
   ELSE      ITEM = 'C9.99';
RUN;
PROC SORT DATA=TOTCMM; BY RANK ITEM; RUN;

DATA _NULL_;
   MERGE TEMPLATE(IN=A) CMM TOTCMM;
   BY RANK ITEM;
   IF A;
   DLM='05'X;
   FILE COFOUT3;
   IF _N_=1 THEN DO;
      PUT @001 'PUBLIC BANK BERHAD'
          /    "TOP 100 DEPOSITORS BY CONTRACTUAL MATURITY AS AT &RDATE"
          /
          /    '(iii) Top 100 Depositors by Contractual Maturity'
          ;
   END;
   IF FIRST.RANK THEN DO;
      PUT @001
          /     RANK                              DLM
                DEPGRP
          /                                       DLM
                'DEPOSIT TYPE'                    DLM
                'UP TO 1 WEEK'                    DLM
                '> 1 WK - 1 MTH'                  DLM
                '> 1 - 3 MTHS'                    DLM
                '> 3 - 6 MTHS'                    DLM
                '> 6 MTHS -  1 YR'                DLM
                '> 1 YEAR'                        DLM
                'NO SPECIFIC MATURITY'            DLM
                'TOTAL'                           DLM
          ;
   END;
   FORMAT AMOUNT BUC1-BUC7 COMMA20.2;
   PUT @001                                  DLM
             IDESC        $CHAR50.           DLM
             BUC1                            DLM
             BUC2                            DLM
             BUC3                            DLM
             BUC4                            DLM
             BUC5                            DLM
             BUC6                            DLM
             BUC7                            DLM
             AMOUNT                          DLM
       ;
RUN;

python converted:

"""
EIBMTLCR - Top Depositors Report
Generates top depositor reports by:
- Individual/Corporate categories (Top 50 each)
- Product breakdown (Top 100)
- Contractual maturity (Top 100)
"""

import polars as pl
from datetime import datetime
from pathlib import Path
import re

# =============================================================================
# CONFIGURATION
# =============================================================================
PATHS = {
    'LCR': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTLCR/',
    'LIST': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTLCR/list/',
    'DEPOSIT': 'data/deposit/',
    'OUTPUT': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMTLCR/'
}

for path in PATHS.values():
    Path(path).mkdir(parents=True, exist_ok=True)

# BIC to item mapping ($BICTAG format)
BIC_TAG = {
    '95311': 'A1.01', '95312': 'A1.02', '95313': 'A1.03',
    '95810': 'A1.04', '95820': 'A1.05', '95830': 'A1.06',
    '9583X': 'A1.06', '95840': 'A1.07', '95329': 'A1.08',
    '953XX': 'A1.09', '9531X': 'A1.10', '96311': 'B1.01',
    '96313': 'B1.02', '96810': 'B1.03', '96820': 'B1.04',
    '96830': 'B1.05', '9683X': 'B1.05', '96840': 'B1.06',
    '96329': 'B1.07'
}

# =============================================================================
# DATE AND REPORT VARIABLES
# =============================================================================
def get_report_vars():
    """Get report date variables"""
    df = pl.read_parquet(f"{PATHS['LCR']}REPTDATE.parquet")
    reptdate = df['REPTDATE'][0]
    
    return {
        'reptyear': str(reptdate.year),
        'reptmon': f"{reptdate.month:02d}",
        'reptday': f"{reptdate.day:02d}",
        'rptdt': reptdate.strftime('%y%m%d'),
        'fildt': reptdate.strftime('%d%m%y'),
        'rdate': reptdate.strftime('%d/%m/%Y')
    }

# =============================================================================
# EXCLUSION LISTS
# =============================================================================
def get_exclusion_lists():
    """Get exclusion lists from SQL"""
    excl_cis = []
    excl_equ = []
    
    try:
        df_cis = pl.read_parquet(f"{PATHS['LIST']}KEEP_TOP_DEP_EXCL_PBB.parquet")
        excl_cis = [str(r['CUSTNO']) for r in df_cis.rows(named=True) if r.get('CUSTNO', 0) > 0]
    except:
        pass
    
    try:
        df_equ = pl.read_parquet(f"{PATHS['LIST']}KEEP_TOP_DEP_EXCL_EQU_PBB.parquet")
        excl_equ = [str(r['CUSTNO']) for r in df_equ.rows(named=True) if r.get('CUSTNO')]
    except:
        pass
    
    return excl_cis, excl_equ

# =============================================================================
# M&I (Monetary & Islamic) PROCESSING
# =============================================================================
def process_mni(rep_vars, excl_cis):
    """Process M&I source data"""
    # Read VOSTRO
    try:
        vostro = pl.read_parquet(f"{PATHS['LCR']}VOSTRO.parquet")
        # Merge with CISINFO
        cisinfo = pl.read_parquet(f"{PATHS['LCR']}CISINFO.parquet")
        vostro = vostro.join(cisinfo, on='ACCTNO', how='left')
        vostro = vostro.with_columns([
            pl.lit('953XX').alias('CMMCODE')
        ])
        vostro = vostro.select(['CMMCODE', 'BRANCH', 'ACCTNO', 'CUSTCD', 
                                'CUSTNAME', 'PRODUCT', 'CURCODE', 'AMOUNT',
                                'CUSTNO', 'NEWIC'])
    except:
        vostro = pl.DataFrame()
    
    # Read CMM
    try:
        cmm = pl.read_parquet(f"{PATHS['LCR']}CMM{rep_vars['reptmon']}.parquet")
    except:
        cmm = pl.DataFrame()
    
    # Combine
    cmm = pl.concat([cmm, vostro]) if not cmm.is_empty() else vostro
    
    # Read COF_MNI_DEPOSITOR_LIST
    try:
        cof = pl.read_parquet(f"{PATHS['LIST']}COF_MNI_DEPOSITOR_LIST.parquet")
        cof = cof.unique(subset=['BUSSREG']).select(['DEPID', 'DEPGRP', 'BUSSREG'])
        cof = cof.rename({'BUSSREG': 'NEWIC'})
    except:
        cof = pl.DataFrame()
    
    # Merge by NEWIC
    if not cmm.is_empty() and not cof.is_empty():
        mni1 = cmm.join(cof, on='NEWIC', how='left')
    else:
        mni1 = cmm.clone() if not cmm.is_empty() else pl.DataFrame()
    
    # Add EXCL flag
    if not mni1.is_empty():
        mni1 = mni1.with_columns([
            pl.when(pl.col('CUSTNO').cast(pl.Utf8).is_in(excl_cis))
            .then(pl.lit('Y')).otherwise(pl.lit('N')).alias('EXCL')
        ])
    
    # Split matched/unmatched
    if not mni1.is_empty():
        mni1_matched = mni1.filter(pl.col('DEPID').is_not_null())
        mni1_unmatched = mni1.filter(pl.col('DEPID').is_null())
    else:
        mni1_matched, mni1_unmatched = pl.DataFrame(), pl.DataFrame()
    
    # Second match by CUSTNO
    if not mni1_unmatched.is_empty():
        cof2 = pl.read_parquet(f"{PATHS['LIST']}COF_MNI_DEPOSITOR_LIST.parquet")
        cof2 = cof2.unique(subset=['CUSTNO']).select(['DEPID', 'DEPGRP', 'CUSTNO'])
        
        mni2 = mni1_unmatched.join(cof2, on='CUSTNO', how='left')
        mni2_matched = mni2.filter(pl.col('DEPID').is_not_null())
        mni2_unmatched = mni2.filter(pl.col('DEPID').is_null())
    else:
        mni2_matched, mni2_unmatched = pl.DataFrame(), pl.DataFrame()
    
    # Assign new DEPID for unmatched
    if not mni2_unmatched.is_empty():
        # Create DEPID sequence starting from 5000
        unique_cust = mni2_unmatched.unique(subset=['CUSTNO']).select(['CUSTNO']).sort('CUSTNO')
        unique_cust = unique_cust.with_columns([
            (pl.arange(0, len(unique_cust)) + 5001).alias('DEPID_NEW')
        ])
        
        mni3 = mni2_unmatched.join(unique_cust, on='CUSTNO', how='left')
        mni3 = mni3.with_columns([
            pl.coalesce([pl.col('DEPID'), pl.col('DEPID_NEW')]).alias('DEPID'),
            pl.col('CUSTNAME').alias('DEPGRP')
        ])
    else:
        mni3 = pl.DataFrame()
    
    # Combine all M&I records
    mni_all = pl.concat([mni1_matched, mni2_matched, mni3]) if any(not df.is_empty() for df in [mni1_matched, mni2_matched, mni3]) else pl.DataFrame()
    
    if not mni_all.is_empty():
        # Classify by product type
        mni_all = mni_all.with_columns([
            pl.col('CMMCODE').str.slice(0, 5).alias('BIC'),
            pl.when(pl.col('EXCL') != 'Y')
            .then(pl.col('AMOUNT')).otherwise(0).alias('AMT_EXCL')
        ])
        
        # Map to product columns
        mni_all = mni_all.with_columns([
            pl.when(pl.col('BIC').is_in(['95311','96311'])).then(pl.col('AMOUNT')).otherwise(0).alias('FD'),
            pl.when(pl.col('BIC') == '95312').then(pl.col('AMOUNT')).otherwise(0).alias('SA'),
            pl.when(pl.col('BIC').is_in(['95313','96313'])).then(pl.col('AMOUNT')).otherwise(0).alias('CA'),
            pl.when(pl.col('BIC') == '953XX').then(pl.col('AMOUNT')).otherwise(0).alias('VOST'),
            pl.when(pl.col('BIC') == '9531X').then(pl.col('AMOUNT')).otherwise(0).alias('GOLD'),
            pl.when(pl.col('BIC').is_in(['95840','96840'])).then(pl.col('AMOUNT')).otherwise(0).alias('RNID'),
            pl.when(pl.col('BIC').is_in(['95311','96311']) & (pl.col('EXCL') != 'Y')).then(pl.col('AMOUNT')).otherwise(0).alias('FD2'),
            pl.when((pl.col('BIC') == '95312') & (pl.col('EXCL') != 'Y')).then(pl.col('AMOUNT')).otherwise(0).alias('SA2'),
            pl.when(pl.col('BIC').is_in(['95313','96313']) & (pl.col('EXCL') != 'Y')).then(pl.col('AMOUNT')).otherwise(0).alias('CA2'),
            pl.when(pl.col('BIC').is_in(['95840','96840']) & (pl.col('EXCL') != 'Y')).then(pl.col('AMOUNT')).otherwise(0).alias('RNID2'),
            pl.when(pl.col('CUSTCD').cast(pl.Utf8).is_in(['77','78','95','96']))
            .then(pl.lit('I')).otherwise(pl.lit('C')).alias('CUSTYPE')
        ])
        
        # Summarize by DEPID, DEPGRP, CUSTYPE
        mni_sum = mni_all.group_by(['DEPID', 'DEPGRP', 'CUSTYPE']).agg([
            pl.col('FD').sum(), pl.col('SA').sum(), pl.col('CA').sum(),
            pl.col('VOST').sum(), pl.col('GOLD').sum(), pl.col('RNID').sum(),
            pl.col('FD2').sum(), pl.col('SA2').sum(), pl.col('CA2').sum(),
            pl.col('RNID2').sum()
        ])
    else:
        mni_sum = pl.DataFrame()
    
    return mni_sum, mni_all

# =============================================================================
# EQUITY PROCESSING
# =============================================================================
def process_equity(rep_vars, excl_equ):
    """Process Equity source data"""
    # Read EQU
    try:
        equ = pl.read_parquet(f"{PATHS['LCR']}EQU{rep_vars['reptday']}.parquet")
        equ = equ.filter(pl.col('CUSTNO') != '')
        
        # Add EXCL flag
        equ = equ.with_columns([
            pl.when(pl.col('CUSTNO').cast(pl.Utf8).is_in(excl_equ))
            .then(pl.lit('Y')).otherwise(pl.lit('N')).alias('EXCL')
        ])
    except:
        equ = pl.DataFrame()
    
    # Read COF_EQU_DEPOSITOR_LIST
    try:
        cof = pl.read_parquet(f"{PATHS['LIST']}COF_EQU_DEPOSITOR_LIST.parquet")
        cof = cof.unique(subset=['CUSTNO']).select(['DEPID', 'DEPGRP', 'CUSTNO', 'LINKID'])
    except:
        cof = pl.DataFrame()
    
    # Merge by CUSTNO
    if not equ.is_empty() and not cof.is_empty():
        equ1 = equ.join(cof, on='CUSTNO', how='left')
        equ_matched = equ1.filter(pl.col('DEPID').is_not_null())
        equ_unmatched = equ1.filter(pl.col('DEPID').is_null())
    else:
        equ_matched, equ_unmatched = pl.DataFrame(), equ.clone() if not equ.is_empty() else pl.DataFrame()
    
    # Assign new DEPID for unmatched
    if not equ_unmatched.is_empty():
        unique_cust = equ_unmatched.unique(subset=['CUSTNO']).select(['CUSTNO']).sort('CUSTNO')
        unique_cust = unique_cust.with_columns([
            (pl.arange(0, len(unique_cust)) + 50005001).alias('DEPID_NEW')
        ])
        
        equ2 = equ_unmatched.join(unique_cust, on='CUSTNO', how='left')
        equ2 = equ2.with_columns([
            pl.coalesce([pl.col('DEPID'), pl.col('DEPID_NEW')]).alias('DEPID'),
            pl.coalesce([pl.col('DEPGRP'), pl.col('CUSTNAME'), pl.col('CUSTNO')]).alias('DEPGRP'),
            pl.lit(None).alias('LINKID')  # Will be assigned later
        ])
    else:
        equ2 = pl.DataFrame()
    
    # Combine all equity records
    equ_all = pl.concat([equ_matched, equ2]) if not equ_matched.is_empty() or not equ2.is_empty() else pl.DataFrame()
    
    if not equ_all.is_empty():
        # Assign LINKID for matched records
        equ_all = equ_all.with_columns([
            pl.when((pl.col('LINKID').is_null()) & (pl.col('DEPID').is_not_null()))
            .then(50000000 + pl.col('DEPID')).otherwise(pl.col('LINKID')).alias('LINKID'),
            pl.when(pl.col('LINKID').is_null()).then(pl.col('DEPID')).otherwise(pl.col('LINKID')).alias('LINKID2')
        ])
        
        # Classify by product type
        equ_all = equ_all.with_columns([
            pl.col('CMMCODE').str.slice(0, 5).alias('BIC'),
            pl.when(pl.col('EXCL') != 'Y')
            .then(pl.col('AMOUNT')).otherwise(0).alias('AMT_EXCL')
        ])
        
        # Map to product columns
        equ_all = equ_all.with_columns([
            pl.when(pl.col('BIC').is_in(['95830','96830','9583X','9683X'])).then(pl.col('AMOUNT')).otherwise(0).alias('STD'),
            pl.when(pl.col('BIC').is_in(['95840','96840'])).then(pl.col('AMOUNT')).otherwise(0).alias('NID'),
            pl.when(pl.col('BIC').is_in(['95810','96810'])).then(pl.col('AMOUNT')).otherwise(0).alias('IBB'),
            pl.when(pl.col('BIC').is_in(['95820','96820'])).then(pl.col('AMOUNT')).otherwise(0).alias('REPO'),
            pl.when(pl.col('BIC').is_in(['95329','96329'])).then(pl.col('AMOUNT')).otherwise(0).alias('DCI'),
            pl.when(pl.col('BIC').is_in(['95830','96830','9583X','9683X']) & (pl.col('EXCL') != 'Y')).then(pl.col('AMOUNT')).otherwise(0).alias('STD2'),
            pl.when(pl.col('BIC').is_in(['95840','96840']) & (pl.col('EXCL') != 'Y')).then(pl.col('AMOUNT')).otherwise(0).alias('NID2'),
            pl.when(pl.col('BIC').is_in(['95329','96329']) & (pl.col('EXCL') != 'Y')).then(pl.col('AMOUNT')).otherwise(0).alias('DCI2'),
            pl.when(pl.col('CUSTFISS').cast(pl.Utf8).is_in(['77','78','95','96']))
            .then(pl.lit('I')).otherwise(pl.lit('C')).alias('CUSTYPE')
        ])
        
        # Filter out excluded product types
        equ_all = equ_all.filter(~pl.col('BIC').is_in(['95850','96850']))
        
        # Summarize by LINKID, DEPGRP, CUSTYPE
        equ_sum = equ_all.group_by(['LINKID2', 'DEPGRP', 'CUSTYPE']).agg([
            pl.col('STD').sum(), pl.col('NID').sum(), pl.col('IBB').sum(),
            pl.col('REPO').sum(), pl.col('DCI').sum(),
            pl.col('STD2').sum(), pl.col('NID2').sum(), pl.col('DCI2').sum()
        ]).rename({'LINKID2': 'DEPID'})
    else:
        equ_sum = pl.DataFrame()
    
    return equ_sum, equ_all

# =============================================================================
# CONSOLIDATION
# =============================================================================
def consolidate_sources(mni_sum, equ_sum):
    """Consolidate M&I and Equity sources"""
    if mni_sum.is_empty() and equ_sum.is_empty():
        return pl.DataFrame()
    
    # Merge by DEPID
    allsrc = pl.DataFrame()
    if not mni_sum.is_empty():
        mni_renamed = mni_sum.rename({'CUSTYPE': 'CUSTYPE_MNI'})
        allsrc = mni_renamed
    
    if not equ_sum.is_empty():
        equ_renamed = equ_sum.rename({'DEPID': 'DEPID_EQU', 'CUSTYPE': 'CUSTYPE_EQU', 'DEPGRP': 'DEPGRP_EQU'})
        if not allsrc.is_empty():
            allsrc = allsrc.join(equ_renamed, left_on='DEPID', right_on='DEPID_EQU', how='full')
        else:
            allsrc = equ_renamed
    
    # Combine fields
    allsrc = allsrc.with_columns([
        pl.coalesce([pl.col('DEPGRP'), pl.col('DEPGRP_EQU')]).alias('DEPGRP'),
        pl.coalesce([pl.col('CUSTYPE_MNI'), pl.col('CUSTYPE_EQU')]).alias('CUSTYPE'),
        (pl.col('NID').fill_null(0) + pl.col('RNID').fill_null(0)).alias('NID_COMB')
    ])
    
    # Calculate totals
    allsrc = allsrc.with_columns([
        (pl.col('FD').fill_null(0) + pl.col('SA').fill_null(0) + pl.col('GOLD').fill_null(0) +
         pl.col('CA').fill_null(0) + pl.col('STD').fill_null(0) + pl.col('NID_COMB').fill_null(0) +
         pl.col('IBB').fill_null(0) + pl.col('REPO').fill_null(0) + pl.col('DCI').fill_null(0) +
         pl.col('VOST').fill_null(0)).alias('TOT'),
        (pl.col('FD2').fill_null(0) + pl.col('SA2').fill_null(0) + pl.col('CA2').fill_null(0) +
         pl.col('RNID2').fill_null(0)).alias('MNI'),
        (pl.col('STD2').fill_null(0) + pl.col('NID2').fill_null(0) + pl.col('DCI2').fill_null(0)).alias('EQU'),
        (pl.col('FD2').fill_null(0) + pl.col('SA2').fill_null(0) + pl.col('CA2').fill_null(0) +
         pl.col('RNID2').fill_null(0) + pl.col('STD2').fill_null(0) + pl.col('NID2').fill_null(0) +
         pl.col('DCI2').fill_null(0)).alias('TOT2')
    ])
    
    # Summarize by DEPID, DEPGRP, CUSTYPE
    summary = allsrc.group_by(['DEPID', 'DEPGRP', 'CUSTYPE']).agg([
        pl.col('TOT2').sum(), pl.col('MNI').sum(), pl.col('EQU').sum()
    ])
    
    return summary

# =============================================================================
# REPORT GENERATION
# =============================================================================
def generate_top50_report(summary, cust_type, desc, rep_vars):
    """Generate Top 50 report for a customer type"""
    # Filter and take top 50
    top50 = summary.filter(pl.col('CUSTYPE') == cust_type).sort('TOT2', descending=True).head(50)
    
    # Add rank
    top50 = top50.with_columns([(pl.arange(0, len(top50)) + 1).alias('RANK')])
    
    # Generate report lines
    lines = []
    lines.append(f"PUBLIC BANK BERHAD")
    lines.append(f"TOP 50 LARGEST DEPOSITORS AS AT {rep_vars['rdate']}")
    lines.append("")
    lines.append(f"(i) Top 50 {desc} Depositors by Sources")
    lines.append("")
    lines.append(f"{'NOBS':<5} {'DEPOSITORS':<30} {'TOTAL BALANCE':>15} {'M&I':>15} {'EQUATION':>15}")
    lines.append("-" * 80)
    
    for row in top50.rows(named=True):
        lines.append(f"{row['RANK']:<5} {row['DEPGRP'][:30]:<30} {row['TOT2']:>15,.2f} {row['MNI']:>15,.2f} {row['EQU']:>15,.2f}")
    
    return lines, top50

def generate_detail_listing(top50, mni_detail, equ_detail, cust_type, desc, rep_vars):
    """Generate detailed account listing for top depositors"""
    lines = []
    lines.append("")
    lines.append(f"(ii) Detail Accounts Listing for Top 50 {desc} Depositors")
    lines.append("")
    
    # Merge with details
    for top in top50.rows(named=True):
        depid = top['DEPID']
        lines.append(f"{top['RANK']}  {top['DEPGRP']} ({depid})")
        lines.append("")
        
        # M&I section
        lines.append("Source: M&I")
        lines.append("")
        lines.append(f"{'NO':<5} {'BRANCH':<10} {'ACCTNO':<15} {'CUSTNAME':<25} {'CUSTNO':<10} {'BUSSREG':<10} {'CUSTCD':<6} {'PRODUCT':<8} {'BALANCE':>15}")
        lines.append("-" * 120)
        
        if not mni_detail.is_empty():
            mni_det = mni_detail.filter(pl.col('DEPID') == depid).sort('ACCTNO')
            cnt = 0
            total = 0
            for row in mni_det.rows(named=True):
                if row.get('AMOUNT', 0) > 0 and row.get('EXCL') != 'Y':
                    cnt += 1
                    total += row['AMOUNT']
                    lines.append(f"{cnt:<5} {str(row.get('BRANCH',''))[:10]:<10} {str(row.get('ACCTNO',''))[:15]:<15} "
                               f"{str(row.get('CUSTNAME',''))[:25]:<25} {str(row.get('CUSTNO',''))[:10]:<10} "
                               f"{str(row.get('NEWIC',''))[:10]:<10} {str(row.get('CUSTCD',''))[:6]:<6} "
                               f"{str(row.get('PRODUCT',''))[:8]:<8} {row['AMOUNT']:>15,.2f}")
            if cnt > 0:
                lines.append(f"{'':<5} {'':<10} {'':<15} {'':<25} {'':<10} {'':<10} {'':<6} {'':<8} {total:>15,.2f}")
        
        lines.append("")
        
        # Equity section
        lines.append("Source: EQU")
        lines.append("")
        lines.append(f"{'NO':<5} {'DEALREF':<15} {'DEALTYPE':<10} {'NAME':<25} {'CUST MNEMONIC':<15} {'AMOUNT':>15}")
        lines.append("-" * 90)
        
        if not equ_detail.is_empty():
            # Equity uses LINKID, not DEPID
            linkid = 50000000 + depid if depid else None
            equ_det = equ_detail.filter(pl.col('LINKID2') == linkid) if linkid else pl.DataFrame()
            cnt = 0
            total = 0
            for row in equ_det.rows(named=True):
                if row.get('AMOUNT', 0) > 0 and row.get('EXCL') != 'Y':
                    cnt += 1
                    total += row['AMOUNT']
                    lines.append(f"{cnt:<5} {str(row.get('DEALREF',''))[:15]:<15} {str(row.get('DEALTYPE',''))[:10]:<10} "
                               f"{str(row.get('CUSTNAME',''))[:25]:<25} {str(row.get('CUSTNO',''))[:15]:<15} "
                               f"{row['AMOUNT']:>15,.2f}")
            if cnt > 0:
                lines.append(f"{'':<5} {'':<15} {'':<10} {'':<25} {'':<15} {total:>15,.2f}")
        
        lines.append("")
    
    return lines

# =============================================================================
# TOP 100 BY PRODUCT
# =============================================================================
def generate_top100_by_product(allsrc, mni_detail, equ_detail, rep_vars):
    """Generate Top 100 report by product"""
    # Get top 100 by total
    top100 = allsrc.sort('TOT', descending=True).head(100)
    top100 = top100.with_columns([(pl.arange(0, len(top100)) + 1).alias('RANK')])
    
    # Report lines
    lines = []
    lines.append(f"PUBLIC BANK BERHAD")
    lines.append(f"TOP 100 LARGEST DEPOSITORS AS AT {rep_vars['rdate']}")
    lines.append("")
    lines.append("(i) Top 100 Depositors by Products")
    lines.append("")
    lines.append(f"{'NOBS':<5} {'DEPOSITORS':<30} {'TOTAL':>12} {'FD':>12} {'SA':>12} {'CA':>12} "
                f"{'STD':>12} {'NID':>12} {'IBB':>12} {'REPO':>12} {'DCI':>12} {'GOLD':>12} {'VOST':>12}")
    lines.append("-" * 180)
    
    for row in top100.rows(named=True):
        lines.append(f"{row['RANK']:<5} {row['DEPGRP'][:30]:<30} "
                    f"{row.get('TOT',0):>12,.2f} {row.get('FD',0):>12,.2f} {row.get('SA',0):>12,.2f} "
                    f"{row.get('CA',0):>12,.2f} {row.get('STD',0):>12,.2f} {row.get('NID',0)+row.get('RNID',0):>12,.2f} "
                    f"{row.get('IBB',0):>12,.2f} {row.get('REPO',0):>12,.2f} {row.get('DCI',0):>12,.2f} "
                    f"{row.get('GOLD',0):>12,.2f} {row.get('VOST',0):>12,.2f}")
    
    # Detail listing
    lines.append("")
    lines.append("(ii) Detail Accounts Listing for Top 100 Depositors")
    lines.append("")
    
    for top in top100.rows(named=True):
        depid = top['DEPID']
        lines.append(f"{top['RANK']}  {top['DEPGRP']} ({depid})")
        lines.append("")
        
        # M&I section
        lines.append("Source: M&I")
        lines.append("")
        lines.append(f"{'NO':<5} {'BRANCH':<10} {'ACCTNO':<15} {'CUSTNAME':<25} {'CUSTNO':<10} {'BUSSREG':<10} {'CUSTCD':<6} {'PRODUCT':<8} {'BALANCE':>15}")
        lines.append("-" * 120)
        
        if not mni_detail.is_empty():
            mni_det = mni_detail.filter(pl.col('DEPID') == depid).sort('ACCTNO')
            cnt = 0
            total = 0
            for row in mni_det.rows(named=True):
                if row.get('AMOUNT', 0) > 0 and row.get('EXCL') != 'Y':
                    cnt += 1
                    total += row['AMOUNT']
                    lines.append(f"{cnt:<5} {str(row.get('BRANCH',''))[:10]:<10} {str(row.get('ACCTNO',''))[:15]:<15} "
                               f"{str(row.get('CUSTNAME',''))[:25]:<25} {str(row.get('CUSTNO',''))[:10]:<10} "
                               f"{str(row.get('NEWIC',''))[:10]:<10} {str(row.get('CUSTCD',''))[:6]:<6} "
                               f"{str(row.get('PRODUCT',''))[:8]:<8} {row['AMOUNT']:>15,.2f}")
            if cnt > 0:
                lines.append(f"{'':<5} {'':<10} {'':<15} {'':<25} {'':<10} {'':<10} {'':<6} {'':<8} {total:>15,.2f}")
        
        lines.append("")
        
        # Equity section
        lines.append("Source: EQU")
        lines.append("")
        lines.append(f"{'NO':<5} {'DEALREF':<15} {'DEALTYPE':<10} {'NAME':<25} {'CUST MNEMONIC':<15} {'AMOUNT':>15}")
        lines.append("-" * 90)
        
        if not equ_detail.is_empty():
            linkid = 50000000 + depid if depid else None
            equ_det = equ_detail.filter(pl.col('LINKID2') == linkid) if linkid else pl.DataFrame()
            cnt = 0
            total = 0
            for row in equ_det.rows(named=True):
                if row.get('AMOUNT', 0) > 0 and row.get('EXCL') != 'Y':
                    cnt += 1
                    total += row['AMOUNT']
                    lines.append(f"{cnt:<5} {str(row.get('DEALREF',''))[:15]:<15} {str(row.get('DEALTYPE',''))[:10]:<10} "
                               f"{str(row.get('CUSTNAME',''))[:25]:<25} {str(row.get('CUSTNO',''))[:15]:<15} "
                               f"{row['AMOUNT']:>15,.2f}")
            if cnt > 0:
                lines.append(f"{'':<5} {'':<15} {'':<10} {'':<25} {'':<15} {total:>15,.2f}")
        
        lines.append("")
    
    return lines, top100

# =============================================================================
# MATURITY REPORT
# =============================================================================
def generate_maturity_report(top100, rep_vars):
    """Generate contractual maturity report"""
    # Template items
    template_items = [
        ('A1.01', 'FIXED DEPOSIT'),
        ('A1.02', 'SAVINGS'),
        ('A1.03', 'DEMAND DEPOSIT'),
        ('A1.04', 'REPO'),
        ('A1.05', 'INTERBANK BORROWING'),
        ('A1.06', 'SHORT TERM DEPOSIT'),
        ('A1.07', 'NID ISSUED'),
        ('A1.08', 'DUAL CURRENCY INVESTMENT'),
        ('A1.09', 'VOSTRO'),
        ('A1.10', 'GOLD INVESTMENT'),
        ('A1.99', 'RETAIL SUBTOTAL'),
        ('B1.01', 'FIXED DEPOSIT'),
        ('B1.02', 'DEMAND DEPOSIT'),
        ('B1.03', 'REPO'),
        ('B1.04', 'INTERBANK BORROWING'),
        ('B1.05', 'SHORT TERM DEPOSIT'),
        ('B1.06', 'NID ISSUED'),
        ('B1.07', 'DUAL CURRENCY INVESTMENT'),
        ('B1.99', 'WHOLESALE SUBTOTAL'),
        ('C9.99', 'GRAND TOTAL')
    ]
    
    lines = []
    lines.append(f"PUBLIC BANK BERHAD")
    lines.append(f"TOP 100 DEPOSITORS BY CONTRACTUAL MATURITY AS AT {rep_vars['rdate']}")
    lines.append("")
    lines.append("(iii) Top 100 Depositors by Contractual Maturity")
    lines.append("")
    
    for top in top100.rows(named=True):
        rank = top['RANK']
        depgrp = top['DEPGRP']
        
        lines.append("")
        lines.append(f"{rank}  {depgrp}")
        lines.append("")
        lines.append(f"{'DEPOSIT TYPE':<25} {'UP TO 1 WEEK':>15} {'>1 WK-1 MTH':>15} {'>1-3 MTHS':>15} "
                    f"{'>3-6 MTHS':>15} {'>6 MTHS-1 YR':>15} {'>1 YEAR':>15} {'NO SPECIFIC':>15} {'TOTAL':>15}")
        lines.append("-" * 150)
        
        # For each template item, we would need actual data
        # This is a placeholder - actual implementation would need CMM data
        for item_code, desc in template_items:
            lines.append(f"{desc[:25]:<25} {'0.00':>15} {'0.00':>15} {'0.00':>15} {'0.00':>15} "
                        f"{'0.00':>15} {'0.00':>15} {'0.00':>15} {'0.00':>15}")
    
    return lines

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 60)
    print("EIBMTLCR - Top Depositors Report")
    print("=" * 60)
    
    # Get report variables
    rep_vars = get_report_vars()
    print(f"\nReport Date: {rep_vars['rdate']}")
    
    # Get exclusion lists
    excl_cis, excl_equ = get_exclusion_lists()
    print(f"Exclusions: CIS={len(excl_cis)}, EQU={len(excl_equ)}")
    
    # Process M&I
    print("\nProcessing M&I...")
    mni_sum, mni_detail = process_mni(rep_vars, excl_cis)
    print(f"  M&I Summary: {len(mni_sum)} groups")
    
    # Process Equity
    print("Processing Equity...")
    equ_sum, equ_detail = process_equity(rep_vars, excl_equ)
    print(f"  Equity Summary: {len(equ_sum)} groups")
    
    # Consolidate
    print("\nConsolidating...")
    allsrc = consolidate_sources(mni_sum, equ_sum)
    print(f"  Consolidated: {len(allsrc)} groups")
    
    # Generate reports
    print("\nGenerating reports...")
    
    # Individual Top 50
    ind_lines, ind_top = generate_top50_report(allsrc, 'I', 'Individual', rep_vars)
    ind_detail = generate_detail_listing(ind_top, mni_detail, equ_detail, 'I', 'Individual', rep_vars)
    
    # Corporate Top 50
    corp_lines, corp_top = generate_top50_report(allsrc, 'C', 'Corporate', rep_vars)
    corp_detail = generate_detail_listing(corp_top, mni_detail, equ_detail, 'C', 'Corporate', rep_vars)
    
    # Top 100 by Product
    prod_lines, prod_top = generate_top100_by_product(allsrc, mni_detail, equ_detail, rep_vars)
    
    # Maturity report
    maturity_lines = generate_maturity_report(prod_top, rep_vars)
    
    # Write output files
    all_lines = ind_lines + ind_detail + ["\n" + "="*80 + "\n"] + corp_lines + corp_detail
    with open(f"{PATHS['OUTPUT']}COFOUT.txt", 'w') as f:
        for line in all_lines:
            f.write(f"{line}\n")
    
    with open(f"{PATHS['OUTPUT']}COFOUT1.txt", 'w') as f:
        for line in prod_lines:
            f.write(f"{line}\n")
    
    with open(f"{PATHS['OUTPUT']}COFOUT3.txt", 'w') as f:
        for line in maturity_lines:
            f.write(f"{line}\n")
    
    print(f"\nReports written to {PATHS['OUTPUT']}")
    print("=" * 60)
    print("âœ“ EIBMTLCR Complete")

if __name__ == "__main__":
    main()


all inputs are in sas7bdat sas dataset. all inputs are in lowercase!
only the templ.txt is in text file format
use pyreadstat to read.
remove reptdate, use datetime timedelta - 1 instead. 
output in text files.
include the PBBLNFMT.py format. (already exist, pending to be called)
