OPTIONS YEARCUTOFF=1940 SORTDEV=3390 NONUMBER NODATE NOCENTER;
*;
DATA REPTDATE (KEEP=REPTDATE);
  SET BNM.REPTDATE;
  MM =MONTH(REPTDATE);
  MM1=MM - 1;
  IF MM1 = 0 THEN MM1 = 12;
  CALL SYMPUT('REPTMON',PUT(MM,Z2.));
  CALL SYMPUT('REPTMON1',PUT(MM1,Z2.));
  CALL SYMPUT('REPTYEAR',PUT(REPTDATE,YEAR4.));
  CALL SYMPUT('REPTDAY',PUT(DAY(REPTDATE),Z2.));
  CALL SYMPUT('RDATE',PUT(REPTDATE,DDMMYY8.));
  CALL SYMPUT('NDATE',PUT(REPTDATE,Z5.));
RUN;
*;
DATA NPGS.TRRF;
  SET NPGS.LNTRRF&REPTMON;

  IF  CVAR13 NE '         ';
  NDATE =CVAR13;
  STATUS=CVAR12;
  KEEP CVAR01 CVAR06 STATUS NDATE;
RUN;

DATA NPGS;
  SET NPGS.LNTRRF&REPTMON;

  CVARX1='          ';
  CVARX2='          ';
  CVARX3='    ';
  IF CVAR12='NPL'  THEN CVAR12A='NP';
                   ELSE CVAR12A='AP';
RUN;

DATA NPGS5;
  FORMAT ACCRUALX 10.2;
  SET NPGS;
  IF CVAR02 ='7Q';
  IF NATGUAR='06' AND CINSTCL='18';
  IF CVAR12 = 'NPL' THEN DO;
     CVAX13 = INPUT(CVAR13,DDMMYY10.);
     CVARX2 = PUT(INTNX('MONTH',CVAX13,1,'B')+6,DDMMYY10.);
     CVARX3 = 'CFBS';
  END;
  IF CVAR05 NOT IN (0,.) THEN CVARX5=PUT(CVAR05,DDMMYY10.);
  ELSE                        CVARX5='          ';
  IF CVAR17  =. THEN CVAR17  =0.00;
  IF ACCRUAL =. THEN ACCRUALX=0.00;
  ELSE               ACCRUALX=ACCRUAL;
RUN;
PROC SORT; BY CVAR01 CVAR06;

DATA SC7QT;
  SET NPGS5;
  FILE SC7QT DLM=';' DSD;
  PUT  @001 CVAR02                  /* 01 SCHEME NUMBER       */
            CVAR03                  /* 02 IC /BUSS. NUM.      */
            CVAR04                  /* 03 NAME OF CUSTOMER    */
            CVAR06                  /* 04 ACCOUNT NUMBER      */
            CVARX5                  /* 05 DISBURSEMENT DATE   */
            CVAR08                  /* 06 LOAN AMOUNT         */
            CVAR16                  /* 07 FACILITY TYPE       */
            CVAR09                  /* 08 O/S BALANCE         */
            CVAR17                  /* 09 PRINCIPAL BALANCE   */
            ACCRUALX                /* 10 INTEREST BALANCE    */
            CVAR11                  /* 11 ARREARS             */
            CVAR12A                 /* 12 STATUS              */
            CVAR13                  /* 13 NPL DATE            */
            CVARX2                  /* 14 NPL NOTIFICATN DATE */
            CVARX3                  /* 15 NPL REASON          */
            CVAR01                  /* 16 APPLICATN NUMBER    */
       ;
*;
PROC    PRINTTO PRINT=SC7QR;
TITLE1 'PUBLIC BANK BERHAD';
TITLE2 'DETAIL OF ACCTS (SCH=7Q) FOR SUBMISSION TO CGC @' &RDATE;
%INC PGM(NPGS5RPT);
*;
DATA NPGS5;
  FORMAT ACCRUALX 10.2;
  SET NPGS;
  IF CVAR02 ='8Q';
  IF NATGUAR='06' AND CINSTCL='18';
  IF CVAR12 = 'NPL' THEN DO;
     CVAX13 = INPUT(CVAR13,DDMMYY10.);
     CVARX2 = PUT(INTNX('MONTH',CVAX13,1,'B')+6,DDMMYY10.);
     CVARX3 = 'CFBS';
  END;
  IF CVAR05 NOT IN (0,.) THEN CVARX5=PUT(CVAR05,DDMMYY10.);
  ELSE                        CVARX5='          ';
  IF CVAR17  =. THEN CVAR17  =0.00;
  IF ACCRUAL =. THEN ACCRUALX=0.00;
  ELSE               ACCRUALX=ACCRUAL;
RUN;
PROC SORT; BY CVAR01 CVAR06;

DATA SC8QT;
  SET NPGS5;
  FILE SC8QT DLM=';' DSD;
  PUT  @001 CVAR02                  /* 01 SCHEME NUMBER       */
            CVAR03                  /* 02 IC /BUSS. NUM.      */
            CVAR04                  /* 03 NAME OF CUSTOMER    */
            CVAR06                  /* 04 ACCOUNT NUMBER      */
            CVARX5                  /* 05 DISBURSEMENT DATE   */
            CVAR08                  /* 06 LOAN AMOUNT         */
            CVAR16                  /* 07 FACILITY TYPE       */
            CVAR09                  /* 08 O/S BALANCE         */
            CVAR17                  /* 09 PRINCIPAL BALANCE   */
            ACCRUALX                /* 10 INTEREST BALANCE    */
            CVAR11                  /* 11 ARREARS             */
            CVAR12A                 /* 12 STATUS              */
            CVAR13                  /* 13 NPL DATE            */
            CVARX2                  /* 14 NPL NOTIFICATN DATE */
            CVARX3                  /* 15 NPL REASON          */
            CVAR01                  /* 16 APPLICATN NUMBER    */
       ;
*;
PROC    PRINTTO PRINT=SC8QR;
TITLE1 'PUBLIC BANK BERHAD';
TITLE2 'DETAIL OF ACCTS (SCH=8Q) FOR SUBMISSION TO CGC @' &RDATE;
%INC PGM(NPGS5RPT);
*;



this is the sas original code, should the lntrrf07.sas7bdat append into the same dataset? or no?
