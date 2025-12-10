*;
DATA REPTDATE;
  SET DEPO1.REPTDATE;
  REPTDATE=INPUT(SUBSTR(PUT(TBDATE, Z11.), 1, 8), MMDDYY8.);
  CALL SYMPUT('NODAY', PUT(DAY(REPTDATE), 2.));
  CALL SYMPUT('REPTYEAR', PUT(REPTDATE, YEAR4.));
  CALL SYMPUT('REPTMON', PUT(MONTH(REPTDATE), Z2.));
  CALL SYMPUT('REPTDAY', PUT(DAY(REPTDATE), Z2.));
  CALL SYMPUT('REPDT', PUT(REPTDATE, DDMMYY8.));
RUN;
DATA DEPO1(KEEP=ACCTNO BRANCH SECOND2 NAME);
   FORMAT SECOND2 $4.;
   SET DEPO1.FD DEPO1X.FD;
   IF SECOND IN (99991,99992);
   IF SECOND = 99991 THEN SECOND2 = 'PBB';
   IF SECOND = 99992 THEN SECOND2 = 'PIBB';
RUN;
PROC SORT DATA=DEPO1; BY ACCTNO BRANCH; RUN;
*;
DATA DEPO2(KEEP=ACCTNO BRANCH CDNO MATDATE TERM RATE CURBAL);
   SET DEPO2.FD DEPO2X.FD;
   IF CURBAL > 0;
RUN;
PROC SORT DATA=DEPO2; BY ACCTNO BRANCH; RUN;
*;
DATA ALL;
   MERGE DEPO1(IN=A) DEPO2(IN=B); BY ACCTNO BRANCH;
   IF A AND B;
RUN;
PROC SORT DATA=ALL; BY MATDATE; RUN;
*;
DATA FD
     FCYFD;
   SET ALL;
   IF (1590000000<=ACCTNO<=1599999999) OR
      (1689999999<=ACCTNO<=1699999999) OR
      (1789999999<=ACCTNO<=1799999999) THEN OUTPUT FCYFD;
   ELSE                OUTPUT FD;
RUN;
*;
DATA WOFF;
   SET FD;
   FILE FDTXT;
   IF _N_ = 1 THEN DO;
     PUT @1 'DAILY RM FD MATURITY HQ AS AT ' "&REPDT";
     PUT @1 'BRANCH'                ','
            'PBB/PIBB'              ','
            'NAME'                  ','
            'ACCTNO'                ','
            'PRINCIPAL AM(RM)'      ','
            'CDNO'                  ','
            'MATDATE'               ','
            'PREVIOUS RATE(% P.A)'  ','
            'PREVIOUS TERM (MONTH)' ',';
   END;
     PUT @1 BRANCH   ','
            SECOND2  ','
            NAME     ','
            ACCTNO   ','
            CURBAL   ','
            CDNO     ','
            MATDATE  ','
            RATE     ','
            TERM     ',';
RUN;
*;
DATA WOFF1;
   SET FCYFD;
   FILE FDTXT1;
   IF _N_ = 1 THEN DO;
     PUT @1 'DAILY FCYFD MATURITY HQ AS AT ' "&REPDT";
     PUT @1 'BRANCH'                ','
            'PBB/PIBB'              ','
            'NAME'                  ','
            'ACCTNO'                ','
            'PRINCIPAL AM(RM)'      ','
            'CDNO'                  ','
            'MATDATE'               ','
            'PREVIOUS RATE(% P.A)'  ','
            'PREVIOUS TERM (MONTH)' ',';
   END;
     PUT @1 BRANCH   ','
            SECOND2  ','
            NAME     ','
            ACCTNO   ','
            CURBAL   ','
            CDNO     ','
            MATDATE  ','
            RATE     ','
            TERM     ',';
RUN;
