//EIBWDPLE JOB MISEIS,EIBWDPLE,CLASS=A,MSGCLASS=X,MSGLEVEL=(1,1),
//         NOTIFY=&SYSUID
/*JOBPARM S=S1M2
//* *//
//EIBWDPLE EXEC SAS609,REGION=8M,WORK='6000,6000'
//DPAA     DD DSN=RBP2.B033.ODPA.EXT.FILE.MIS(0),DISP=SHR
//LMTDET   DD DSN=SAP.PBB.DPDET(+1),
//            DISP=(NEW,CATLG,DELETE),
//            DCB=(RECFM=FS,LRECL=27648,BLKSIZE=27648),
//            SPACE=(CYL,(10,10),),UNIT=(SYSDA,8)

 OPTIONS YEARCUTOFF=1950 SORTDEV=3390 NONUMBER NODATE NOCENTER;
 *;
 DATA LMTDET.LMTDET;
   INFILE DPAA FIRSTOBS = 1;
   INPUT @001 AANO           $13.
         @021 APRVDT          11.
         @032 APRVAMT         11.
         @043 ACCTNO          11.
         @054 TOTLMTAMT       11.
         @065 LASTMNTDT       11.
         @076 LMTID            3.
         @079 LMTAMT          11.
         @090 LMTSTARTDT      11.
         @101 LMTENDDT        11.
         @112 LMTTERM          3.
         @115 LMTTERMID       $1.
         @116 LMTPAIDIND      $1.
         @117 COLL1           11.
         @128 COLL2           11.
         @139 COLL3           11.
         @150 COLL4           11.
         @161 COLL5           11.
         @172 COLL6           11.
         @183 COLL7           11.
         @194 COLL8           11.
         @205 COLL9           11.
         @216 COLL10          11.
   ;
 RUN;
 *;
 DATA LMTDET.REPTDATE(KEEP=EXTDATE REPTDATE);
    REPTDATE=TODAY() - 1;
    YYYY=PUT(YEAR(REPTDATE), Z4.);
    MM=PUT(MONTH(REPTDATE),Z2.);
    DD=PUT(DAY(REPTDATE),Z2.);
    DAY1=MDY(1,1,YEAR(REPTDATE));
    DAYS = TODAY() - DAY1;
    TEMPDATE=COMPRESS(MM||DD||YYYY||DAYS, ' ');
    EXTDATE=TEMPDATE * 1;
 RUN;
