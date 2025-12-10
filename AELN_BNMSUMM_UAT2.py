*;
DATA SDBMS(DROP=OPENDD OPENMM OPENYY);
  INFILE SDBMS FIRSTOBS=2;
  INPUT @001 BRANCH       $3.
        @005 NAME        $50.
        @056 IC          $20.
        @077 NATIONALITY  $2.
        @079 BOXNO       $10.     /* SDB BOX NO.     */
        @090 HIRERTY      $1.     /* HIRER TYPE      */
        @092 ADDRESS    $250.
        @343 ACCTHOLDER   $1.
        @345 ACCTNO      $20.
        @366 PRIPHONE    $20.
        @387 MOBILENO    $20.
        @408 OPENDD       $2.
        @410 OPENMM       $2.
        @412 OPENYY       $4.
        @419 RENTALDATE   DDMMYY8. /* RENTALDUEDATE     */
        @428 LASTRENTPAY  DDMMYY8. /* LASTRENTALPAYMENT */
        @437 MTHOVERDUE1   2.      /* MTHOVERDUE(3-<4)  */
        @439 MTHOVERDUE2   2.      /* MTHOVERDUE(4-<8)  */
        @441 MTHOVERDUE3   2.      /* MTHOVERDUE(>8)    */
        @443 TOTALOVERDUE  2.
        ;
  OPENDT = MDY(OPENMM,OPENDD,OPENYY);
RUN;
PROC PRINT DATA=SDBMS;RUN;
*;
DATA STATUS.SDB&REPTMON&NOWK&REPTYEAR   (DROP=NAME ADDRESS PRIPHONE
                                              MOBILENO)
     R1STAT.R1SDB&REPTMON&NOWK&REPTYEAR (KEEP=IC NAME ADDRESS PRIPHONE
                                              MOBILENO);
     SET SDBMS;
RUN;
