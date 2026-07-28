ACTUAL PRODUCTION OUTPUT:

EIBWHP01: REPORT ON PRODUCTS 131,132,720,725 AS AT  22/07/26                                       07:14 Thursday, July 23, 2026   1 
ALL CUSTOMERS                                                                                                                        
                                                                                                                                     
Obs       BNMCODE                           AMOUNT      WEIGHTED                                                                     
                                                                                                                                     
  1    6734000001000Y                 1,557,814.51    .                                                                              
  2    6734000002000Y                   811,104.87    .                                                                              
  3    6734000003000Y                 4,581,562.30             0                                                                     
  4    6734000005001Y                 3,074,040.01    .                                                                              
  5    6734000005002Y                   238,229.58    .                                                                              
  6    6734000005003Y                   162,661.89    .                                                                              
  7    6734000005004Y                   487,947.92    .                                                                              
  8    6734000005006Y                   249,561.53    .                                                                              
  9    6734000006100Y                28,861,403.92    .000005587                                                                     
 10    6734000006300Y                 1,566,435.02    .                                                                              
 11    6734000007000Y                 5,250,959.23             0                                                                     
 12    6734000008310Y                   925,716.68    .                                                                              
 13    6734000008320Y                       118.60    .                                                                              
 14    6734000009000Y                 3,225,819.38    .                                                                              


PYTHON OUTPUT:

EIBWHP01 REPORT GENERATED 27-07-2026
REPTMON: 202607, NOWK: 31
================================================================================
BNM RECORDS:     623910
LOAN RECORDS:    6232608
REPORT DATE: 27-07-2026
================================================================================


ORIGINAL SAS PROGRAM:

%INC PGM(PBBLNFMT);
DATA REPTDATE (KEEP=REPTDATE);
  SET BNM.REPTDATE;
  SELECT(DAY(REPTDATE));
    WHEN (8)  DO; SDD = 1;  WK = '1'; WK1 = '4'; END;
    WHEN(15)  DO; SDD = 9;  WK = '2'; WK1 = '1'; END;
    WHEN(22)  DO; SDD = 16; WK = '3'; WK1 = '2'; END;
    OTHERWISE DO; SDD = 23; WK = '4'; WK1 = '3'; END;
  END;
  MM = MONTH(REPTDATE);
  IF WK = '1' THEN DO;
     MM1 = MM - 1;
     IF MM1 = 0 THEN MM1 = 12;
  END;
  ELSE MM1 = MM;
  SDATE = MDY(MM,SDD,YEAR(REPTDATE));
  CALL SYMPUT('NOWK',PUT(WK,$1.));
  CALL SYMPUT('NOWK1',PUT(WK1,$1.));
  CALL SYMPUT('REPTMON',PUT(MM,Z2.));
  CALL SYMPUT('REPTMON1',PUT(MM1,Z2.));
  CALL SYMPUT('REPTYEAR',PUT(REPTDATE,YEAR4.));
  CALL SYMPUT('REPTDAY',PUT(DAY(REPTDATE),Z2.));
  CALL SYMPUT('RDATE',PUT(REPTDATE,DDMMYY8.));
  CALL SYMPUT('SDATE',PUT(SDATE,DDMMYY8.));
RUN;
*;
DATA LOAN (KEEP=ACCTNO NOTENO EFFAPR SECTORCD);
      SET LOAN.LNNOTE;
      IF  LOANTYPE IN (131,132,720,725);
      SECTORCD=PUT(SECTOR, $SECTCD.);

      IF INTAMT LE 0.01 THEN
         INTAMT = (INTRATE*NETPROC*NOTETERM/1200)-INTEARN2;
      IF NOTETERM > 12 THEN TERM = 12; ELSE TERM = NOTETERM;
         EFFFACT = (100*TERM*(INTAMT))/
                   (NOTETERM*(NETPROC+INTEARN2));
      EFFAPR=(NOTETERM*EFFFACT*(300*TERM+NOTETERM*EFFFACT))/
             ((NOTETERM*NOTETERM*EFFFACT)+(150*TERM*(NOTETERM+1)));
RUN;
*;
PROC SORT DATA=BNM.LOAN&REPTMON1&NOWK1 OUT=ALW1
(KEEP=ACCTNO NOTENO SECTORCD PRODUCT NOTETERM BALANCE PRODCD CUSTCD
      AMTIND ISSDTE INTRATE);
   BY ACCTNO NOTENO SECTORCD;
   WHERE PRODUCT IN (131,132,720,725);
RUN;
*;
PROC SORT DATA=BNM.LOAN&REPTMON&NOWK OUT=ALW
(KEEP=ACCTNO NOTENO SECTORCD PRODUCT NOTETERM EARNTERM BALANCE
      APPRDATE APPRLIM2 PRODCD CUSTCD AMTIND ISSDTE INTRATE);
   BY ACCTNO NOTENO SECTORCD;
   WHERE PRODUCT IN (131,132,720,725);
RUN;
*;
DATA ALW;
   MERGE ALW(IN=A) LOAN; BY ACCTNO NOTENO SECTORCD;
   IF A;
RUN;
*;
DATA ALW;
   KEEP SECTCD DISBURSE REPAID APPRLIM2 AMTIND CUSTCD NOACCT
        PRODUCT;
   MERGE ALW1(IN=A RENAME=(BALANCE=LASTBAL NOTETERM=LASTNOTE))
         ALW(IN=B);
   BY ACCTNO NOTENO SECTORCD;
   /*
   IF MONTH(ISSDTE)=MONTH(INPUT("&RDATE", DDMMYY8.)) AND
      YEAR(ISSDTE)=YEAR(INPUT("&RDATE", DDMMYY8.)) THEN
      NOACCT = 1;
   IF APPRDATE <= INPUT("&RDATE",DDMMYY8.);
   IF APPRDATE < INPUT("&SDATE",DDMMYY8.) THEN APPRLIM2 = 0;
   */
   NOACCT=1;
   DISBURSE=0; REPAID=0;
   IF A & B THEN DO;
      IF LASTBAL > BALANCE THEN REPAID = LASTBAL - BALANCE;
      ELSE DISBURSE = BALANCE - LASTBAL;
   END;
   IF ^B THEN REPAID = LASTBAL;
   IF ^A THEN DISBURSE = BALANCE;
   PRODUCT = DISBURSE * EFFAPR;
   SECTCD = PUT(SECTORCD,$SECTA.);
   IF SECTCD ^= ' ' THEN OUTPUT;
   SECTCD = PUT(SECTORCD,$SECTB.);
   IF SECTCD ^= ' ' THEN OUTPUT;
*;
PROC  SORT DATA=ALW; BY SECTCD;
PROC  SUMMARY DATA=ALW NWAY;
CLASS SECTCD;
VAR   DISBURSE PRODUCT;
OUTPUT OUT=ALWLOAN (DROP=_TYPE_) SUM=;
RUN;
*;
DATA ALWLOAN;
   KEEP BNMCODE AMTIND AMOUNT WEIGHTED;
   LENGTH BNMCODE $14.;
   SET ALWLOAN;
   WEIGHTED = PRODUCT / DISBURSE;
   BNMCODE = '673400000'||SECTCD||'Y';
   AMOUNT = DISBURSE; OUTPUT;
RUN;
*;
PROC SUMMARY DATA=ALWLOAN NWAY;
CLASS BNMCODE;
VAR AMOUNT WEIGHTED;
OUTPUT OUT=LALW&REPTMON&NOWK (DROP=_TYPE_ _FREQ_ ) SUM=;
RUN;
TITLE1 'EIBWHP01: REPORT ON PRODUCTS 131,132,720,725 AS AT ' &RDATE;
TITLE2 'ALL CUSTOMERS';
PROC PRINT DATA=LALW&REPTMON&NOWK;
FORMAT AMOUNT COMMA25.2;
RUN;
*;
PROC SUMMARY DATA=ALW NWAY;
WHERE CUSTCD IN ('66','67','68','69');
CLASS SECTCD;
VAR   DISBURSE PRODUCT;
OUTPUT OUT=ALWSMI (DROP=_FREQ_ _TYPE_) SUM=;
RUN;
*;
DATA ALWSMI;
   KEEP BNMCODE AMOUNT WEIGHTED DISBURSE;
   LENGTH BNMCODE $14.;
   SET ALWSMI;
   WEIGHTED = PRODUCT / DISBURSE;
   BNMCODE = '673400000'||SECTCD||'Y';
   AMOUNT = DISBURSE; OUTPUT;
RUN;
PROC SUMMARY DATA=ALWSMI NWAY;
CLASS BNMCODE;
VAR DISBURSE WEIGHTED;
OUTPUT OUT=LALW&REPTMON&NOWK (DROP=_TYPE_ _FREQ_ ) SUM=;
RUN;
TITLE 'EIBWHP01: SMI ACCTS (CUSTCD 66,67,68,69) AS AT ' &RDATE;
PROC PRINT DATA=LALW&REPTMON&NOWK;
FORMAT AMOUNT COMMA25.2;
RUN;
