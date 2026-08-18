below is the KALMLIQ4:

*;
%LET IREP=('01','02','11','12','81');
%LET NREP=('13','17','20','60','71','72','74','76','79','85');
*;
DATA K3TBL3;
  FORMAT MATDT YYMMDD8.;
  SET BNMK.K3TBL&REPTMON&NOWK;
  PART='95';
  IF UTREF='RRS' AND UTSTY='MGS' AND UTDLP='MSS';
  IF ISSDT > REPTDATE  THEN DELETE;
  AMTUSD=0; AMTSGD=0;
  AMOUNT=(UTPCP*UTFCV)*0.01;
  AMOUNT=SUM(AMOUNT,UTAICT); /* SALES PROCEEDS */
  CUST=PUT(UTCTP,$CTYPE.);
  IF UTIDT NE ' '     THEN MATDT=INPUT(UTIDT,YYMMDD10.);
  IF CUST IN &NREP    THEN ITEM='830';  ELSE
  IF CUST IN &IREP    THEN ITEM='820';
  IF CUST NE '  ';




below is the KALMLIQX:

DATA K1TBX;
  LENGTH BNMCODE $5.;
  SET BNMK.K1TBL&REPTMON&NOWK (RENAME=(GWMDT=MATDT GWBALC=AMOUNT));
  IF  GWMVT = 'P';
  IF  GWOCY='XAU'  THEN DELETE;
  IF  GWCCY='XAU'  THEN DELETE;
  AMTUSD=0.00; AMTSGD=0.00;
  IF GWCCY='USD'  THEN AMTUSD=AMOUNT;
  IF GWCCY='SGD'  THEN AMTSGD=AMOUNT;
  BNMCODE=' ';
  IF GWDLP IN ('FXS','FXO','FXF','SF1','SF2','TS1','TS2',
               'FBP','FF1','FF2');

DATA K1TBX1;
  SET K1TBX;
  IF GWOCY EQ 'MYR' AND GWMVT EQ 'P' AND GWMVTS EQ 'P' THEN
  SELECT(GWDLP);
    WHEN('FXS') IF GWCCY NE 'MYR' THEN
                SELECT(GWCTP);
                  WHEN('BC') BNMCODE='57100';
                  WHEN('BB') BNMCODE='57100';
                  WHEN('BI') BNMCODE='57100';
                  WHEN('BM') BNMCODE='57100';
                  WHEN('CE') BNMCODE='57100';
                  WHEN('BA','BW','BE') BNMCODE='57100';
                  OTHERWISE DO;
                    IF NOT('BA' <= GWCTP <= 'BZ')
                       AND GWCNAL EQ 'MY' AND GWSAC NE 'UF' THEN
                       BNMCODE='57100';
                    IF GWSAC EQ 'UF' THEN
                       BNMCODE='57100';
                  END;
                END;
    WHEN('FBP') IF GWCCY NE 'MYR' THEN  BNMCODE='57100';
    WHEN('FXO','FXF') IF GWCCY NE 'MYR' THEN
                SELECT(GWCTP);
                  WHEN('BC') BNMCODE='57100';
                  WHEN('BB') BNMCODE='57100';
                  WHEN('BI') BNMCODE='57100';
                  WHEN('BM') BNMCODE='57100';
                  WHEN('BA','BW','BE') BNMCODE='57100';
                  OTHERWISE DO;
                    IF NOT('BA' <= GWCTP <= 'BZ')
                       AND GWCNAL EQ 'MY' AND GWSAC NE 'UF' THEN
                       BNMCODE='57100';
                    IF GWSAC EQ 'UF' THEN
                       BNMCODE='57100';
                  END;
                END;
    WHEN('SF1','SF2','TS1','TS2','FF1','FF2') IF GWCCY NE 'MYR' THEN
                SELECT(GWCTP);
                  WHEN('BC') BNMCODE='57100';
                  WHEN('BB') BNMCODE='57100';
                  WHEN('BI') BNMCODE='57100';
                  WHEN('BM') BNMCODE='57100';
                  WHEN('BA','BW','BE') BNMCODE='57100';
                  OTHERWISE DO;
                    IF NOT('BA' <= GWCTP <= 'BZ')
                       AND GWCNAL EQ 'MY' AND GWSAC NE 'UF' THEN
                       BNMCODE='57100';
                    IF GWSAC EQ 'UF' THEN
                       BNMCODE='57100';
                  END;
                END;
    OTHERWISE;
  END;

  IF GWOCY EQ 'MYR' AND GWMVT EQ 'P' AND GWMVTS EQ 'S' THEN
  SELECT(GWDLP);
    WHEN('FXS') IF GWCCY NE 'MYR' THEN
                SELECT(GWCTP);
                  WHEN('BC') BNMCODE='57400';
                  WHEN('BB') BNMCODE='57400';
                  WHEN('BI') BNMCODE='57400';
                  WHEN('BM') BNMCODE='57400';
                  WHEN('CE') BNMCODE='57400';
                  WHEN('BA','BW','BE') BNMCODE='57400';
                  OTHERWISE DO;
                    IF NOT('BA' <= GWCTP <= 'BZ')
                       AND GWCNAL EQ 'MY' AND GWSAC NE 'UF' THEN
                       BNMCODE='57400';
                    IF GWSAC EQ 'UF' THEN
                       BNMCODE='57400';
                  END;
                END;
    WHEN('FXO','FXF') IF GWCCY NE 'MYR' THEN
                SELECT(GWCTP);
                  WHEN('BC') BNMCODE='57400';
                  WHEN('BB') BNMCODE='57400';
                  WHEN('BI') BNMCODE='57400';
                  WHEN('BM') BNMCODE='57400';
                  WHEN('BA','BW','BE') BNMCODE='57400';
                  OTHERWISE DO;
                    IF NOT('BA' <= GWCTP <= 'BZ')
                       AND GWCNAL EQ 'MY' AND GWSAC NE 'UF' THEN
                       BNMCODE='57400';
                    IF GWCTP='CE'    THEN BNMCODE='57400';
                    IF GWSAC EQ 'UF' THEN
                       BNMCODE='57400';
                  END;
                END;
    WHEN('SF1','SF2','TS1','TS2','FF1','FF2') IF GWCCY NE 'MYR' THEN
                SELECT(GWCTP);
                  WHEN('BC') BNMCODE='57400';
                  WHEN('BB') BNMCODE='57400';
                  WHEN('BI') BNMCODE='57400';
                  WHEN('BM') BNMCODE='57400';
                  WHEN('BA','BW','BE') BNMCODE='57400';
                  OTHERWISE DO;
                    IF NOT('BA' <= GWCTP <= 'BZ')
                       AND GWCNAL EQ 'MY' AND GWSAC NE 'UF' THEN
                       BNMCODE='57400';
                    IF GWSAC EQ 'UF' THEN
                       BNMCODE='57400';
                  END;
                END;
    OTHERWISE;
  END;
  IF BNMCODE NE ' ' THEN OUTPUT;
*;
DATA K1TBX2;
  SET K1TBX;
  IF GWCCY NE 'MYR' AND GWOCY NE 'MYR'
     AND GWMVT EQ 'P' AND GWMVTS EQ 'P' THEN
     SELECT(GWDLP);
       WHEN('FXS')             BNMCODE='57600';
       WHEN('FXO','FXF')       BNMCODE='57600';
       WHEN('SF2','FF1','FF2') BNMCODE='57600';
       WHEN('SF1','TS1','TS2') BNMCODE='57600';
       OTHERWISE;
     END;
  IF BNMCODE NE ' ' THEN OUTPUT;
*;
DATA K1TBX;
  KEEP PART ITEM AMOUNT AMTUSD AMTSGD MATDT;
  SET K1TBX1 K1TBX2;
  IF  AMOUNT < 0 THEN AMOUNT=ABS(AMOUNT);
  IF  BNMCODE='57100' THEN DO;
      AMTUSD=0.00; AMTSGD=0.00;
      PART='95'; ITEM='911';
      OUTPUT;
      PART='96'; ITEM='711';
      OUTPUT;
  END;
  IF  BNMCODE='57400' THEN DO;
      AMTUSD=0.00; AMTSGD=0.00;
      PART='95'; ITEM='711';
      OUTPUT;
      PART='96'; ITEM='911';
      OUTPUT;
  END;
  IF  BNMCODE='57600' THEN DO;
      PART='96'; ITEM='711'; OUTPUT;
      PART='96'; ITEM='911'; OUTPUT;
  END;
