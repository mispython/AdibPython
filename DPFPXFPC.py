sas original code:

****************************************************
* DAILY DEPOSIT EXTRACTION FOR BNM REPORTING       *
* (EL COMPUTATION)                                 *
****************************************************;
OPTIONS YEARCUTOFF=1930 NOCENTER;
%LET AGELIMIT = 12;
%LET MAXAGE   = 18;
%LET AGEBELOW = 11;

%INC PGM(PBBLNFMT);
%INC PGM(PBBDPFMT);

DATA REPTDATE;
  INFILE DPTRBL LRECL=900 OBS=1;
  INPUT @106 TBDATE PD6.;
  REPTDATE=INPUT(SUBSTR(PUT(TBDATE, Z11.), 1, 8), MMDDYY8.);
  SELECT;
     WHEN(1 <= DAY(REPTDATE) <= 8) CALL SYMPUT('NOWK', PUT('1', $1.));
     WHEN(9 <= DAY(REPTDATE) <= 15) CALL SYMPUT('NOWK', PUT('2', $1.));
     WHEN(16 <= DAY(REPTDATE) <= 22) CALL SYMPUT('NOWK', PUT('3', $1.));
     OTHERWISE CALL SYMPUT('NOWK', PUT('4', $1.));
  END;
  CALL SYMPUT('REPTYEAR', PUT(REPTDATE, YEAR4.));
  CALL SYMPUT('REPTMON', PUT(MONTH(REPTDATE), Z2.));
  CALL SYMPUT('REPTDAY', PUT(DAY(REPTDATE), Z2.));
  CALL SYMPUT('RDATE', PUT(REPTDATE, DDMMYY8.));
RUN;

LIBNAME BNM  "SAP.PIBB.D&REPTYEAR" DISP=OLD;

PROC FORMAT;
 *----------------------------------------------------------
     FORMAT APPLICABLE TO PDMD'S SUMMARY REPORT OF
     DAILY DEMAND DEPOSIT MOVEMENT BY RANGE
 ----------------------------------------------------------;
   INVALUE DDMOVE
      LOW - < 300000  = 300000
   300000 - < 500000  = 500000
   500000 - <1000000  = 1000000
  1000000 - <1500000  = 1500000
  1500000 - <2000000  = 2000000
  2000000 - <3000000  = 3000000
  3000000 - <4000000  = 4000000
  4000000 - <5000000  = 5000000
  5000000 - <10000000 = 10000000
  10000000 -  HIGH    = 10000001;
 *----------------------------------------------------------
     FORMAT FOR PDMD'S DAILY SUMMARY REPORT ON DEPOSITS
     MOVEMENT BY RANGE
 ----------------------------------------------------------;
   INVALUE MVTDEP
    LOW - 5000  = 5000
   5000 - 10000 = 10000
  10000 - 30000 = 30000
  30000 - 50000 = 50000
  50000 - 75000 = 75000
  75000 - HIGH  = 80000;

   INVALUE MVTACE
    LOW - 5000  = 5000
   5000 - 10000 = 10000
  10000 - 30000 = 30000
  30000 - 50000 = 50000
  50000 - 75000 = 75000
 75000 - 100000 = 100000
 100000 - HIGH  = 200000;
RUN;

DATA MELW1 (KEEP=INTP1-INTP999 MISC1-MISC999 SPTF1-SPTF999
                 ODFIN1-ODFIN999 REPTDATE)
     MELW12 (KEEP=SA1-SA999 SACAI1-SACAI999 CA1-CA999
                 HDA1-HDA999 REPTDATE)
     MELW13 (KEEP=ODCOM1-ODCOM999 ODIS1-ODIS999 ODMB1-ODMB999
                 ODDH1-ODDH999 ODCAG1-ODCAG999 REPTDATE);
  RETAIN REPTDATE;
  ARRAY INTP{999};
  ARRAY MISC{999};
  ARRAY SPTF{999};
  ARRAY ODFIN{999};
  ARRAY SA{999};
  ARRAY SACAI{999};
  ARRAY CA{999};
  ARRAY HDA{999};
  ARRAY ODCOM{999};
  ARRAY ODIS{999};
  ARRAY ODMB{999};
  ARRAY ODDH{999};
  ARRAY ODCAG{999};
  ARRAY SAI{999};
  ARRAY MBS{999};
  ARRAY CAI{999};
  ARRAY CAIG{999};
  ARRAY SNO{999};
  ARRAY MBSNO{999};
  ARRAY CNO{999};
  ARRAY CGNO{999};
  INFILE DPTRBL LRECL=900 END=LAST;
  IF _N_=1 THEN DO;
     INPUT @106 TBDATE PD6.;
     REPTDATE=INPUT(SUBSTR(PUT(TBDATE, Z11.), 1, 8), MMDDYY8.);
  END;

  INPUT @3   BANKNO   PD2.
        @24  REPTNO   PD3.
        @27  FMTCODE  PD2. @;

  IF BANKNO = 33 AND REPTNO = 1001 AND FMTCODE = 1 THEN DO;
  INPUT @106 BRANCH PD4.
        @110 ACCTNO PD6.
        @116 NAME $15.
        @143 DEBIT PD8.2
        @151 CREDIT PD7.2
        @158 CLOSEDT  PD6.
        @164 OPENDT   PD6.
        @176 CUSTCODE PD3.
        @286 PURPOSE $1.
        @338 OPENIND $1.
        @399 RACE $1.
        @397 PRODUCT PD2.
        @431 DEPTYPE $1.
        @432 INT1 PD6.2
        @466 CURBAL PD7.2
        @516 INT2 PD10.9
        @566 APPRLIMT PD6.
        @717 BDATE PD6.
        @830 COSTCTR PD4.;

  INTPAYBL=SUM(INT1, INT2);

  IF (3000<=COSTCTR<=3999);

  IF OPENIND NOT IN ('B','C','P') AND PRODUCT NOT IN (297,298) THEN DO;
     DYDPBAL=CURBAL; /* TO REPORT AS ACE SAVINGS & CURRENT PORTION */
     MOVEMENT=CREDIT-DEBIT;
     IF PRODUCT IN &ACE THEN MVRANGE=INPUT(ABS(MOVEMENT), MVTACE.);
     ELSE MVRANGE=INPUT(ABS(MOVEMENT), MVTDEP.);

     ACCYTD = 0;
     IF OPENDT NE 0  AND CLOSEDT EQ 0 THEN DO;
     IF YEAR(INPUT(SUBSTR(PUT(OPENDT, Z11.),1, 8), MMDDYY8.)) EQ
        YEAR(INPUT("&RDATE", DDMMYY8.)) THEN
        ACCYTD = 1;
     END;

  IF DEPTYPE='S' THEN DO;
     PRODCD=PUT(PRODUCT, SAPROD.);
     DPTYPE='S';
     CUSTCD=PUT(CUSTCODE, SACUSTCD.);
     IF CURBAL GE 0 THEN DO;
       SELECT(PRODCD);
         WHEN ('42120') DO;
           TOTSAVG+CURBAL;
           SA{BRANCH}+CURBAL;
         END;
         WHEN ('42320') DO;
           TOTSAVGI+CURBAL;
           SACAI{BRANCH}+CURBAL;
           SAI{BRANCH}+CURBAL;
           SNO{BRANCH}+1;
           IF PRODUCT = 214 THEN DO;
              TOTMBSA+CURBAL;
              MBS{BRANCH}+CURBAL;
              MBSNO{BRANCH}+1;
           END;
         END;
         OTHERWISE;
       END;
  /*   OUTPUT TEMP.DYDP; */
     END;
     ELSE IF PRODCD = '42320' THEN DO;
       SAI{BRANCH}+CURBAL;
       SNO{BRANCH}+1;
       IF PRODUCT = 214 THEN DO;
          TOTMBSA+CURBAL;
          MBS{BRANCH}+CURBAL;
          MBSNO{BRANCH}+1;
       END;
     END;
  END;
  IF DEPTYPE IN ('D','N') THEN DO;
     DPTYPE='D';
     IF CURBAL GE 0 THEN DO;
        PRODCD=PUT(PRODUCT, CAPROD.);
        SELECT(PRODUCT);
          WHEN(104) DO;
            TOTVOSC+CURBAL;
            CUSTCD='02';
          END;
          WHEN(105) DO;
            TOTVOSF+CURBAL;
            CUSTCD='81';
          END;
          OTHERWISE CUSTCD=PUT(CUSTCODE, DDCUSTCD.);
        END;
        SELECT(PRODCD);
          WHEN ('42110') DO;
             IF PRODUCT IN &ACE AND
                (CURBAL > 5000) THEN DO;
                SABAL = CURBAL - 5000;
                ACESA+SABAL;
                DYDPBAL = SABAL; DPTYPE='S';
      /*        OUTPUT TEMP.DYDP;  */
                CABAL = 5000;
                DYDPBAL = CABAL; DPTYPE='D';
      /*        OUTPUT TEMP.DYDP;  */
                TOTSAVG+SABAL;
                SA{BRANCH}+SABAL;
                TOTDMND+CABAL;
                ACECA+CABAL;
                CA{BRANCH}+CABAL;
             END;
             ELSE DO;
                IF PRODUCT IN &ACE THEN ACECA+CURBAL;
                TOTDMND+CURBAL;
                CA{BRANCH}+CURBAL;
      /*        OUTPUT TEMP.DYDP;  */
             END;
          END;
          WHEN ('42180') DO;
            TOTDMND+CURBAL;
            HDA{BRANCH}+CURBAL;
      /*    OUTPUT TEMP.DYDP;  */
          END;
          WHEN ('42310') DO;
            TOTDMNDI+CURBAL;
            SACAI{BRANCH}+CURBAL;
            CAI{BRANCH}+CURBAL;
            CNO{BRANCH}+1;
      /*    OUTPUT TEMP.DYDP;  */
            IF PRODUCT = 161 THEN DO;
               CAIG{BRANCH}+CURBAL;
               CGNO{BRANCH}+1;
            END;
          END;
          OTHERWISE;
        END;
     END;
     ELSE DO;
        PRODCD=PUT(PRODUCT, CAPROD.);
        IF PRODCD = '42310' THEN DO;
           CAI{BRANCH}+CURBAL;
           CNO{BRANCH}+1;
           IF PRODUCT = 161 THEN DO;
              CAIG{BRANCH}+CURBAL;
              CGNO{BRANCH}+1;
           END;
        END;

        CURBAL=(-1)*CURBAL;
        PRODCD=PUT(PRODUCT, ODPROD.);
        SELECT(PRODUCT);
          WHEN(104) DO;
            OVDVOSC+CURBAL;
            CUSTCD='02';
          END;
          WHEN(105) DO;
            OVDVOSF+CURBAL;
            CUSTCD='81';
          END;
          OTHERWISE CUSTCD=PUT(CUSTCODE, ODCUSTCD.);
        END;
        IF PRODCD='34240' THEN PRODCD = '34180';
        IF PRODCD IN ('34180','34380') THEN TOTOVFT+CURBAL;
     END;
  END;

  IF DEPTYPE IN ('C') THEN DO;
     IF PRODUCT IN (318) THEN DELETE;
     PRODCD=PUT(PRODUCT, FDPRODD.);
     CUSTCD=PUT(CUSTCODE, FDCUSTCD.);
     SELECT(PRODCD);
        WHEN ('42130') TOTFD+CURBAL;
        WHEN ('42132') TOTFDI+CURBAL;
        OTHERWISE;
     END;
  END;
  ELSE IF DEPTYPE IN ('S') THEN DO;
     /* WEEKLY & MONTHEND PROCESSING FOR SDRNGE DATASET */
     /* MAINLY FOR PDMD'S WISE PROFILE REPORT & OTHERS  */
     MTHEND='N';
     IF DAY(TODAY()) = 1 THEN MTHEND='Y';  /* MONTHEND CHECK */
     IF ("&REPTDAY" IN ('08','15','22') OR MTHEND='Y') AND
           CURBAL GE 0 THEN DO;
        /****************************************************/
        /** APPLY THE FORMAT                         **/
        /****************************************************/
        RACE=PUT(RACE, $RACE.);
        RANGE=INPUT(CURBAL, SDRANGE.);
        /******************************************/
        /* CALCULATION OF AGE RANGE         */
        /******************************************/
        AGE = 0;
        IF (BDATE NE 0) AND (BDATE NE .) THEN DO;
           BDATE =INPUT(SUBSTR(PUT(BDATE, Z11.),1, 8), MMDDYY8.);
           BDAY =DAY(BDATE);
           BMONTH =MONTH(BDATE);
           BYEAR =YEAR(BDATE);

           AGE=(&REPTYEAR - BYEAR);
           SELECT;
              WHEN (AGE = &AGELIMIT) DO;
                 IF (BMONTH = &REPTMON) AND
                    (BDAY > &REPTDAY) THEN AGE=&AGEBELOW;
                 ELSE IF BMONTH > &REPTMON THEN AGE=&AGEBELOW;
              END;
              WHEN (AGE = &MAXAGE) DO;
                 IF (BMONTH = &REPTMON) AND
                    (BDAY > &REPTDAY) THEN AGE=&AGELIMIT;
                 ELSE IF BMONTH > &REPTMON THEN AGE=&AGELIMIT;
              END;
              WHEN (AGE > &MAXAGE) AGE = &MAXAGE;
              WHEN (AGE < &AGELIMIT) AGE = &AGEBELOW;
              OTHERWISE AGE = &AGELIMIT;
           END;
        END;
        END;
   /*   OUTPUT TEMP.SAVG;  */
  END;

  IF CUSTCD IN ('80','81','82','83','84','85','86','87','88','89',
                '90','91','92','93','94','95','96','97','98','99')
                AND DEPTYPE IN ('S','D','N','C') THEN DO;
    SELECT(PRODCD);
      WHEN('42110','42120','42130','42310','42320','42133')
                    INTP{BRANCH}+INTPAYBL;
      WHEN('42132') SPTF{BRANCH}+INTPAYBL;
      OTHERWISE MISC{BRANCH}+INTPAYBL;
    END;
  END;
  SELECT;
    WHEN (PRODCD = '34180' AND CUSTCD = '02') ODCOM{BRANCH}+CURBAL;
    WHEN (PRODCD = '34180' AND CUSTCD = '03') ODIS{BRANCH}+CURBAL;
    WHEN (PRODCD = '34180' AND CUSTCD = '11') ODFIN{BRANCH}+CURBAL;
    WHEN (PRODCD = '34180' AND CUSTCD = '12') ODMB{BRANCH}+CURBAL;
    WHEN (PRODCD = '34180' AND CUSTCD = '13') ODDH{BRANCH}+CURBAL;
    WHEN (PRODCD = '34180' AND CUSTCD = '17') ODCAG{BRANCH}+CURBAL;
    OTHERWISE;
  END;
  END;
  END;
  IF LAST THEN OUTPUT MELW1 MELW12 MELW13;
RUN;

DATA MELW2  (KEEP=REPTDATE BRANCH ELDAY BNMCODE AMOUNT);
  SET MELW1;
  ARRAY INTP{999} INTP1-INTP999;
  ARRAY MISC{999} MISC1-MISC999;
  ARRAY SPTF{999} SPTF1-SPTF999;
  ARRAY ODFIN{999} ODFIN1-ODFIN999;
  SELECT("&NOWK");
    WHEN('1') DO;
      IF DAY(REPTDATE)=1 THEN ELDAY='DAYA';
      IF DAY(REPTDATE)=2 THEN ELDAY='DAYB';
      IF DAY(REPTDATE)=3 THEN ELDAY='DAYC';
      IF DAY(REPTDATE)=4 THEN ELDAY='DAYD';
      IF DAY(REPTDATE)=5 THEN ELDAY='DAYE';
      IF DAY(REPTDATE)=6 THEN ELDAY='DAYF';
      IF DAY(REPTDATE)=7 THEN ELDAY='DAYG';
      IF DAY(REPTDATE)=8 THEN ELDAY='DAYI';
    END;
    WHEN('2') DO;
      IF DAY(REPTDATE)=9 THEN ELDAY='DAYA';
      IF DAY(REPTDATE)=10 THEN ELDAY='DAYB';
      IF DAY(REPTDATE)=11 THEN ELDAY='DAYC';
      IF DAY(REPTDATE)=12 THEN ELDAY='DAYD';
      IF DAY(REPTDATE)=13 THEN ELDAY='DAYE';
      IF DAY(REPTDATE)=14 THEN ELDAY='DAYF';
      IF DAY(REPTDATE)=15 THEN ELDAY='DAYI';
    END;
    WHEN('3') DO;
      IF DAY(REPTDATE)=16 THEN ELDAY='DAYA';
      IF DAY(REPTDATE)=17 THEN ELDAY='DAYB';
      IF DAY(REPTDATE)=18 THEN ELDAY='DAYC';
      IF DAY(REPTDATE)=19 THEN ELDAY='DAYD';
      IF DAY(REPTDATE)=20 THEN ELDAY='DAYE';
      IF DAY(REPTDATE)=21 THEN ELDAY='DAYF';
      IF DAY(REPTDATE)=22 THEN ELDAY='DAYI';
    END;
    WHEN('4') DO;
      IF DAY(REPTDATE)=23 THEN ELDAY='DAYA';
      IF DAY(REPTDATE)=24 THEN ELDAY='DAYB';
      IF DAY(REPTDATE)=25 THEN ELDAY='DAYC';
      IF DAY(REPTDATE)=26 THEN ELDAY='DAYD';
      IF DAY(REPTDATE)=27 THEN ELDAY='DAYE';
      IF DAY(REPTDATE)=28 THEN DO;
         IF DAY(REPTDATE+1)=1 THEN ELDAY='DAYI';
         ELSE ELDAY='DAYF';
      END;
      IF DAY(REPTDATE)=29 THEN DO;
         IF DAY(REPTDATE+1)=1 THEN ELDAY='DAYI';
         ELSE ELDAY='DAYG';
      END;
      IF DAY(REPTDATE)=30 THEN DO;
         IF DAY(REPTDATE+1)=1 THEN ELDAY='DAYI';
         ELSE ELDAY='DAYH';
      END;
      IF DAY(REPTDATE)=31 THEN ELDAY='DAYI';
    END;
    OTHERWISE;
  END;
IF ELDAY NE ' ';
  DO I=1 TO 999;
    BRANCH=I;
    BNMCODE='3410011000000Y'; AMOUNT=ODFIN{I};
    IF AMOUNT NE . THEN OUTPUT MELW2;
    BNMCODE='4911080000000Y'; AMOUNT=INTP{I};
    IF AMOUNT NE . THEN OUTPUT MELW2;
    BNMCODE='4929980000000D'; AMOUNT=MISC{I};
    IF AMOUNT NE . THEN OUTPUT MELW2;
    BRANCH=SUM(I,3000);
    BNMCODE='4929980000000Y'; AMOUNT=SPTF{I};
    IF AMOUNT NE . THEN OUTPUT MELW2;
  END;
RUN;

PROC DATASETS LIB=WORK NOLIST; DELETE MELW1; RUN;

DATA MELW22 (KEEP=REPTDATE BRANCH ELDAY BNMCODE AMOUNT);
  SET MELW12;
  ARRAY SA{999}    SA1-SA999;
  ARRAY SACAI{999} SACAI1-SACAI999;
  ARRAY CA{999}    CA1-CA999;
  ARRAY HDA{999}   HDA1-HDA999;
  SELECT("&NOWK");
    WHEN('1') DO;
      IF DAY(REPTDATE)=1 THEN ELDAY='DAYA';
      IF DAY(REPTDATE)=2 THEN ELDAY='DAYB';
      IF DAY(REPTDATE)=3 THEN ELDAY='DAYC';
      IF DAY(REPTDATE)=4 THEN ELDAY='DAYD';
      IF DAY(REPTDATE)=5 THEN ELDAY='DAYE';
      IF DAY(REPTDATE)=6 THEN ELDAY='DAYF';
      IF DAY(REPTDATE)=7 THEN ELDAY='DAYG';
      IF DAY(REPTDATE)=8 THEN ELDAY='DAYI';
    END;
    WHEN('2') DO;
      IF DAY(REPTDATE)=9 THEN ELDAY='DAYA';
      IF DAY(REPTDATE)=10 THEN ELDAY='DAYB';
      IF DAY(REPTDATE)=11 THEN ELDAY='DAYC';
      IF DAY(REPTDATE)=12 THEN ELDAY='DAYD';
      IF DAY(REPTDATE)=13 THEN ELDAY='DAYE';
      IF DAY(REPTDATE)=14 THEN ELDAY='DAYF';
      IF DAY(REPTDATE)=15 THEN ELDAY='DAYI';
    END;
    WHEN('3') DO;
      IF DAY(REPTDATE)=16 THEN ELDAY='DAYA';
      IF DAY(REPTDATE)=17 THEN ELDAY='DAYB';
      IF DAY(REPTDATE)=18 THEN ELDAY='DAYC';
      IF DAY(REPTDATE)=19 THEN ELDAY='DAYD';
      IF DAY(REPTDATE)=20 THEN ELDAY='DAYE';
      IF DAY(REPTDATE)=21 THEN ELDAY='DAYF';
      IF DAY(REPTDATE)=22 THEN ELDAY='DAYI';
    END;
    WHEN('4') DO;
      IF DAY(REPTDATE)=23 THEN ELDAY='DAYA';
      IF DAY(REPTDATE)=24 THEN ELDAY='DAYB';
      IF DAY(REPTDATE)=25 THEN ELDAY='DAYC';
      IF DAY(REPTDATE)=26 THEN ELDAY='DAYD';
      IF DAY(REPTDATE)=27 THEN ELDAY='DAYE';
      IF DAY(REPTDATE)=28 THEN DO;
         IF DAY(REPTDATE+1)=1 THEN ELDAY='DAYI';
         ELSE ELDAY='DAYF';
      END;
      IF DAY(REPTDATE)=29 THEN DO;
         IF DAY(REPTDATE+1)=1 THEN ELDAY='DAYI';
         ELSE ELDAY='DAYG';
      END;
      IF DAY(REPTDATE)=30 THEN DO;
         IF DAY(REPTDATE+1)=1 THEN ELDAY='DAYI';
         ELSE ELDAY='DAYH';
      END;
      IF DAY(REPTDATE)=31 THEN ELDAY='DAYI';
    END;
    OTHERWISE;
  END;
IF ELDAY NE ' ';
  DO I=1 TO 999;
    BRANCH=I;
    BNMCODE='4212000000000Y'; AMOUNT=SA{I};
    IF AMOUNT NE . THEN OUTPUT;
    BNMCODE='4230000000000Y'; AMOUNT=SACAI{I};
    IF AMOUNT NE . THEN OUTPUT;
    BNMCODE='4211000000000Y'; AMOUNT=CA{I};
    IF AMOUNT NE . THEN OUTPUT;
    BNMCODE='4218000000000Y'; AMOUNT=HDA{I};
    IF AMOUNT NE . THEN OUTPUT;
  END;
RUN;

PROC APPEND DATA=MELW22 BASE=MELW2; RUN;
PROC DATASETS LIB=WORK NOLIST; DELETE MELW12 MELW22; RUN;

DATA MELW23 (KEEP=REPTDATE BRANCH ELDAY BNMCODE AMOUNT);
  SET MELW13;
  ARRAY ODCOM{999} ODCOM1-ODCOM999;
  ARRAY ODIS{999}  ODIS1-ODIS999;
  ARRAY ODMB{999}  ODMB1-ODMB999;
  ARRAY ODDH{999}  ODDH1-ODDH999;
  ARRAY ODCAG{999} ODCAG1-ODCAG999;
  SELECT("&NOWK");
    WHEN('1') DO;
      IF DAY(REPTDATE)=1 THEN ELDAY='DAYA';
      IF DAY(REPTDATE)=2 THEN ELDAY='DAYB';
      IF DAY(REPTDATE)=3 THEN ELDAY='DAYC';
      IF DAY(REPTDATE)=4 THEN ELDAY='DAYD';
      IF DAY(REPTDATE)=5 THEN ELDAY='DAYE';
      IF DAY(REPTDATE)=6 THEN ELDAY='DAYF';
      IF DAY(REPTDATE)=7 THEN ELDAY='DAYG';
      IF DAY(REPTDATE)=8 THEN ELDAY='DAYI';
    END;
    WHEN('2') DO;
      IF DAY(REPTDATE)=9 THEN ELDAY='DAYA';
      IF DAY(REPTDATE)=10 THEN ELDAY='DAYB';
      IF DAY(REPTDATE)=11 THEN ELDAY='DAYC';
      IF DAY(REPTDATE)=12 THEN ELDAY='DAYD';
      IF DAY(REPTDATE)=13 THEN ELDAY='DAYE';
      IF DAY(REPTDATE)=14 THEN ELDAY='DAYF';
      IF DAY(REPTDATE)=15 THEN ELDAY='DAYI';
    END;
    WHEN('3') DO;
      IF DAY(REPTDATE)=16 THEN ELDAY='DAYA';
      IF DAY(REPTDATE)=17 THEN ELDAY='DAYB';
      IF DAY(REPTDATE)=18 THEN ELDAY='DAYC';
      IF DAY(REPTDATE)=19 THEN ELDAY='DAYD';
      IF DAY(REPTDATE)=20 THEN ELDAY='DAYE';
      IF DAY(REPTDATE)=21 THEN ELDAY='DAYF';
      IF DAY(REPTDATE)=22 THEN ELDAY='DAYI';
    END;
    WHEN('4') DO;
      IF DAY(REPTDATE)=23 THEN ELDAY='DAYA';
      IF DAY(REPTDATE)=24 THEN ELDAY='DAYB';
      IF DAY(REPTDATE)=25 THEN ELDAY='DAYC';
      IF DAY(REPTDATE)=26 THEN ELDAY='DAYD';
      IF DAY(REPTDATE)=27 THEN ELDAY='DAYE';
      IF DAY(REPTDATE)=28 THEN DO;
         IF DAY(REPTDATE+1)=1 THEN ELDAY='DAYI';
         ELSE ELDAY='DAYF';
      END;
      IF DAY(REPTDATE)=29 THEN DO;
         IF DAY(REPTDATE+1)=1 THEN ELDAY='DAYI';
         ELSE ELDAY='DAYG';
      END;
      IF DAY(REPTDATE)=30 THEN DO;
         IF DAY(REPTDATE+1)=1 THEN ELDAY='DAYI';
         ELSE ELDAY='DAYH';
      END;
      IF DAY(REPTDATE)=31 THEN ELDAY='DAYI';
    END;
    OTHERWISE;
  END;
IF ELDAY NE ' ';
  DO I=1 TO 999;
    BRANCH=I;
    BNMCODE='3410002000000Y'; AMOUNT=ODCOM{I};
    IF AMOUNT NE . THEN OUTPUT;
    BNMCODE='3410003000000Y'; AMOUNT=ODIS{I};
    IF AMOUNT NE . THEN OUTPUT;
    BNMCODE='3410012000000Y'; AMOUNT=ODMB{I};
    IF AMOUNT NE . THEN OUTPUT;
    BNMCODE='3410013000000Y'; AMOUNT=ODDH{I};
    IF AMOUNT NE . THEN OUTPUT;
    BNMCODE='3410017000000Y'; AMOUNT=ODCAG{I};
    IF AMOUNT NE . THEN OUTPUT;
  END;
RUN;

PROC APPEND DATA=MELW23 BASE=MELW2; RUN;
PROC DATASETS LIB=WORK NOLIST; DELETE MELW13 MELW23; RUN;
*;
%MACRO APPEND;
%IF "&REPTDAY" EQ "01" OR
    "&REPTDAY" EQ "09" OR
    "&REPTDAY" EQ "16" OR
    "&REPTDAY" EQ "23" %THEN
    %DO;
      PROC DATASETS LIB=BNM NOLIST;
        DELETE MELW&REPTMON&NOWK;
      PROC APPEND DATA=MELW2 BASE=BNM.MELW&REPTMON&NOWK; RUN;
    %END;
%ELSE %DO;
      DATA BNM.MELW&REPTMON&NOWK;
        SET BNM.MELW&REPTMON&NOWK;
        IF REPTDATE EQ INPUT("&RDATE", DDMMYY8.) THEN DELETE;
      PROC APPEND DATA=MELW2 BASE=BNM.MELW&REPTMON&NOWK; RUN;
%END;
%MEND APPEND;
%APPEND;
*;
//



python equivalent:

"""
EIIDMELW - Islamic BNM EL Daily Deposit Extract (Production Ready)

Includes:
- PBBLNFMT: Loan formats (ACE products)
- PBBDPFMT: Deposit formats (SAPROD, CAPROD, etc.)

Key Differences from EIBDMELW:
- ONLY ISLAMIC (COSTCTR 3000-3999)
- ACE split logic (>5000)
- Product 318 excluded (DELETE)
- Different BNM codes:
  * 4929980000000D: MISC (vs 4929980000000Y)
  * 4929980000000Y: SPTF Islamic (vs 4929980000000I)
- Interest categories:
  * INTP: 42110/42120/42130/42310/42320/42133
  * SPTF: 42132
  * MISC: Others

Output: BNM.MELW{MON}{WK} (PIBB Islamic)
"""

import polars as pl
from datetime import datetime, timedelta
import os

# Directories
BNM_DIR = 'data/bnm_pibb/'
DPTRBL_DIR = 'data/dptrbl/'

os.makedirs(BNM_DIR, exist_ok=True)

print("EIIDMELW - Islamic BNM EL Daily Deposit Extract")
print("=" * 60)

# Read DPTRBL header
print("\nReading DPTRBL header...")
try:
    with open(f'{DPTRBL_DIR}DPTRBL.txt', 'r') as f:
        header = f.readline()
        tbdate_str = header[105:111]
    
    reptdate = datetime.strptime(tbdate_str, '%y%m%d').date()
    
    # Week determination
    day = reptdate.day
    if 1 <= day <= 8:
        nowk = '1'
    elif 9 <= day <= 15:
        nowk = '2'
    elif 16 <= day <= 22:
        nowk = '3'
    else:
        nowk = '4'
    
    reptyear = f'{reptdate.year}'
    reptmon = f'{reptdate.month:02d}'
    reptday = f'{reptdate.day:02d}'
    rdate = reptdate.strftime('%d%m%y')
    
    print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
    print(f"Week: {nowk}, Month: {reptmon}, Day: {reptday}")
except Exception as e:
    print(f"Error reading DPTRBL: {e}")
    import sys
    sys.exit(1)

print("=" * 60)

# ACE products
ACE_PRODUCTS = [161, 162, 163, 150, 151, 152, 181]

# Interest customer codes
INTEREST_CUSTCODES = ['80','81','82','83','84','85','86','87','88','89',
                      '90','91','92','93','94','95','96','97','98','99']

# ELDAY mapping
def get_elday(day, nowk):
    """Map day to ELDAY"""
    elday_map = {
        '1': {1:'DAYA', 2:'DAYB', 3:'DAYC', 4:'DAYD', 5:'DAYE', 6:'DAYF', 7:'DAYG', 8:'DAYI'},
        '2': {9:'DAYA', 10:'DAYB', 11:'DAYC', 12:'DAYD', 13:'DAYE', 14:'DAYF', 15:'DAYI'},
        '3': {16:'DAYA', 17:'DAYB', 18:'DAYC', 19:'DAYD', 20:'DAYE', 21:'DAYF', 22:'DAYI'},
        '4': {23:'DAYA', 24:'DAYB', 25:'DAYC', 26:'DAYD', 27:'DAYE'}
    }
    
    if nowk == '4':
        if day == 28:
            tomorrow = reptdate + timedelta(days=1)
            return 'DAYI' if tomorrow.day == 1 else 'DAYF'
        elif day == 29:
            tomorrow = reptdate + timedelta(days=1)
            return 'DAYI' if tomorrow.day == 1 else 'DAYG'
        elif day == 30:
            tomorrow = reptdate + timedelta(days=1)
            return 'DAYI' if tomorrow.day == 1 else 'DAYH'
        elif day == 31:
            return 'DAYI'
    
    return elday_map.get(nowk, {}).get(day, '')

elday = get_elday(reptdate.day, nowk)
print(f"ELDAY: {elday}")

# Initialize branch data
branch_data = {
    i: {
        'INTP': 0, 'MISC': 0, 'SPTF': 0, 'ODFIN': 0,
        'SA': 0, 'SACAI': 0, 'CA': 0, 'HDA': 0,
        'ODCOM': 0, 'ODIS': 0, 'ODMB': 0, 'ODDH': 0, 'ODCAG': 0
    }
    for i in range(1, 1000)
}

print("\nProcessing DPTRBL (Islamic ONLY)...")

try:
    with open(f'{DPTRBL_DIR}DPTRBL.txt', 'r') as f:
        for line_num, line in enumerate(f):
            if line_num == 0: continue
            if len(line) < 900: continue
            
            try:
                bankno = int(line[2:5])
                reptno = int(line[23:27])
                fmtcode = int(line[26:29])
            except:
                continue
            
            if not (bankno == 33 and reptno == 1001 and fmtcode == 1):
                continue
            
            try:
                branch = int(line[105:110])
                acctno = int(line[109:116])
                debit = float(line[142:151])
                credit = float(line[150:158])
                custcode = int(line[175:179]) if line[175:179].strip() else 0
                product = int(line[396:399]) if line[396:399].strip() else 0
                deptype = line[430:432].strip()
                int1 = float(line[431:438])
                int2 = float(line[515:526])
                curbal = float(line[465:473])
                costctr = int(line[829:834]) if line[829:834].strip() else 0
                openind = line[337:339].strip()
            except:
                continue
            
            # ONLY ISLAMIC (3000-3999) - KEY FILTER
            if not (3000 <= costctr <= 3999):
                continue
            
            if openind in ['B', 'C', 'P'] or product in [297, 298]:
                continue
            
            intpaybl = int1 + int2
            
            # Categorize PRODCD
            if deptype == 'S':
                prodcd = '42320' if product in [210, 211, 214] else '42120'
            elif deptype in ['D', 'N']:
                if product in [150, 151, 152, 181]:
                    prodcd = '42110'
                elif product in [61, 161]:
                    prodcd = '42310'
                else:
                    prodcd = '42180'
            elif deptype == 'C':
                # DELETE product 318
                if product == 318:
                    continue
                prodcd = '42132' if product in [317] else '42130'
            else:
                prodcd = ''
            
            # Categorize CUSTCD
            if product == 104:
                custcd = '02'
            elif product == 105:
                custcd = '81'
            elif custcode <= 99:
                custcd = f'{custcode:02d}'
            else:
                custcd = f'{custcode % 100:02d}'
            
            # Process by DEPTYPE
            if deptype == 'S' and curbal >= 0:
                if prodcd == '42120':
                    branch_data[branch]['SA'] += curbal
                elif prodcd == '42320':
                    branch_data[branch]['SACAI'] += curbal
            
            elif deptype in ['D', 'N']:
                if curbal >= 0:
                    # ACE SPLIT LOGIC
                    if prodcd == '42110' and product in ACE_PRODUCTS and curbal > 5000:
                        sabal = curbal - 5000
                        cabal = 5000
                        branch_data[branch]['SA'] += sabal
                        branch_data[branch]['CA'] += cabal
                    else:
                        if prodcd == '42110':
                            branch_data[branch]['CA'] += curbal
                        elif prodcd == '42180':
                            branch_data[branch]['HDA'] += curbal
                        elif prodcd == '42310':
                            branch_data[branch]['SACAI'] += curbal
                else:
                    # Overdraft
                    curbal_abs = abs(curbal)
                    
                    # Convert to OD prodcd (simplified)
                    od_prodcd = '34180'
                    
                    if od_prodcd == '34180':
                        if custcd == '02':
                            branch_data[branch]['ODCOM'] += curbal_abs
                        elif custcd == '03':
                            branch_data[branch]['ODIS'] += curbal_abs
                        elif custcd == '11':
                            branch_data[branch]['ODFIN'] += curbal_abs
                        elif custcd == '12':
                            branch_data[branch]['ODMB'] += curbal_abs
                        elif custcd == '13':
                            branch_data[branch]['ODDH'] += curbal_abs
                        elif custcd == '17':
                            branch_data[branch]['ODCAG'] += curbal_abs
            
            # Interest payable (CUSTCD 80-99)
            if custcd in INTEREST_CUSTCODES and deptype in ['S', 'D', 'N', 'C']:
                # CRITICAL: Different categorization
                if prodcd in ['42110', '42120', '42130', '42310', '42320', '42133']:
                    branch_data[branch]['INTP'] += intpaybl
                elif prodcd == '42132':
                    branch_data[branch]['SPTF'] += intpaybl
                else:
                    branch_data[branch]['MISC'] += intpaybl

except Exception as e:
    print(f"Error: {e}")
    import sys
    sys.exit(1)

print(f"  Processed deposit records")

# Generate BNM code records
print("\nGenerating BNM code records...")

melw_records = []

for branch in range(1, 1000):
    data = branch_data[branch]
    
    # Conventional branch codes
    if data['ODFIN'] != 0:
        melw_records.append({
            'REPTDATE': reptdate, 'BRANCH': branch, 'ELDAY': elday,
            'BNMCODE': '3410011000000Y', 'AMOUNT': data['ODFIN']
        })
    
    if data['INTP'] != 0:
        melw_records.append({
            'REPTDATE': reptdate, 'BRANCH': branch, 'ELDAY': elday,
            'BNMCODE': '4911080000000Y', 'AMOUNT': data['INTP']
        })
    
    # MISC: Different code (4929980000000D vs 4929980000000Y)
    if data['MISC'] != 0:
        melw_records.append({
            'REPTDATE': reptdate, 'BRANCH': branch, 'ELDAY': elday,
            'BNMCODE': '4929980000000D', 'AMOUNT': data['MISC']
        })
    
    # SPTF: branch+3000, different code (4929980000000Y vs 4929980000000I)
    if data['SPTF'] != 0:
        melw_records.append({
            'REPTDATE': reptdate, 'BRANCH': branch + 3000, 'ELDAY': elday,
            'BNMCODE': '4929980000000Y', 'AMOUNT': data['SPTF']
        })
    
    # Savings
    if data['SA'] != 0:
        melw_records.append({
            'REPTDATE': reptdate, 'BRANCH': branch, 'ELDAY': elday,
            'BNMCODE': '4212000000000Y', 'AMOUNT': data['SA']
        })
    
    if data['SACAI'] != 0:
        melw_records.append({
            'REPTDATE': reptdate, 'BRANCH': branch, 'ELDAY': elday,
            'BNMCODE': '4230000000000Y', 'AMOUNT': data['SACAI']
        })
    
    # Current
    if data['CA'] != 0:
        melw_records.append({
            'REPTDATE': reptdate, 'BRANCH': branch, 'ELDAY': elday,
            'BNMCODE': '4211000000000Y', 'AMOUNT': data['CA']
        })
    
    if data['HDA'] != 0:
        melw_records.append({
            'REPTDATE': reptdate, 'BRANCH': branch, 'ELDAY': elday,
            'BNMCODE': '4218000000000Y', 'AMOUNT': data['HDA']
        })
    
    # Overdrafts
    if data['ODCOM'] != 0:
        melw_records.append({
            'REPTDATE': reptdate, 'BRANCH': branch, 'ELDAY': elday,
            'BNMCODE': '3410002000000Y', 'AMOUNT': data['ODCOM']
        })
    
    if data['ODIS'] != 0:
        melw_records.append({
            'REPTDATE': reptdate, 'BRANCH': branch, 'ELDAY': elday,
            'BNMCODE': '3410003000000Y', 'AMOUNT': data['ODIS']
        })
    
    if data['ODMB'] != 0:
        melw_records.append({
            'REPTDATE': reptdate, 'BRANCH': branch, 'ELDAY': elday,
            'BNMCODE': '3410012000000Y', 'AMOUNT': data['ODMB']
        })
    
    if data['ODDH'] != 0:
        melw_records.append({
            'REPTDATE': reptdate, 'BRANCH': branch, 'ELDAY': elday,
            'BNMCODE': '3410013000000Y', 'AMOUNT': data['ODDH']
        })
    
    if data['ODCAG'] != 0:
        melw_records.append({
            'REPTDATE': reptdate, 'BRANCH': branch, 'ELDAY': elday,
            'BNMCODE': '3410017000000Y', 'AMOUNT': data['ODCAG']
        })

df_melw = pl.DataFrame(melw_records)
print(f"  Generated {len(df_melw):,} BNM code records")

# Append logic
filename = f'MELW{reptmon}{nowk}.parquet'
filepath = f'{BNM_DIR}{filename}'

print(f"\nSaving to {filename}...")

if reptday in ['01', '09', '16', '23']:
    print(f"  Week start (Day {reptday}): Creating new")
    df_melw.write_parquet(filepath)
else:
    print(f"  Mid-week (Day {reptday}): Appending")
    try:
        df_existing = pl.read_parquet(filepath)
        df_existing = df_existing.filter(pl.col('REPTDATE') != reptdate)
        df_combined = pl.concat([df_existing, df_melw])
        df_combined.write_parquet(filepath)
        print(f"    Combined: {len(df_combined):,} records")
    except:
        df_melw.write_parquet(filepath)

print(f"\n{'='*60}")
print(f"âœ“ EIIDMELW Complete!")
print(f"{'='*60}")
print(f"\nOutput: {filename}")
print(f"  Records: {len(df_melw):,}")
print(f"  ELDAY: {elday}, Week: {nowk}")
print(f"\nKey Differences from EIBDMELW:")
print(f"  âœ“ Islamic ONLY (COSTCTR 3000-3999)")
print(f"  âœ“ ACE split logic (>5000)")
print(f"  âœ“ Product 318 excluded (DELETE)")
print(f"  âœ“ BNM codes:")
print(f"    - MISC: 4929980000000D (not 4929980000000Y)")
print(f"    - SPTF: 4929980000000Y (not 4929980000000I)")
print(f"  âœ“ Interest categories:")
print(f"    - INTP: 42110/42120/42130/42310/42320/42133")
print(f"    - SPTF: 42132")
print(f"    - MISC: Others")



also include the PBBLNFMT and PBBDPTFMT (already exist in py file)
