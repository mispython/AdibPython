DATA ADDR.REPTDATE;
   REPTDATE = TODAY();

DATA ADDR.SAVINGS;
   INFILE DPADDR;
   INPUT @1   BANKNO   PD2.
         @3   APPCODE  $1.
         @4   ACCTNO   PD6.
         @10  BRANCH   PD4.
         @14  NAME     $24.
         @38  OLDIC    $11.
         @49  OPENDATE PD6.
         @55  PRODUCT  PD2.
         @57  OPENIND  $1.
         @58  PURPOSE  $1.
         @59  RACE     $1.
         @60  USER3    $1.
         @61  DORMANT  $1.
         @62  DEPTYPE  $1.
         @63  BDATE    PD6.
         @69  DEPTNO   PD3.
         @72  NEWIC    $12.
         @84  LEDGBAL  PD7.2
         @91  CURBAL   PD7.2
         @98  YTDBAL   PD8.2
         @106 YTDDAYS  PD2.
         @108 NAMETYPE 1.
         @109 NAMELN1  $40.
         @149 NAMELN2  $40.
         @189 NAMELN3  $40.
         @229 NAMELN4  $40.
         @269 NAMELN5  $40.
         @309 NAMELN6  $40.
         @349 NAMELN7  $40.
         @389 NAMELN8  $40.;
RUN;
