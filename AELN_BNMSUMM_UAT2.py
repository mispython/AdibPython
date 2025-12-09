DATA REPTDATE;
  REPTDATE = MDY(MONTH(TODAY()),1,YEAR(TODAY()))-1;
  MM = MONTH(REPTDATE);
  CALL SYMPUT('REPTMON',PUT(MM,Z2.));
  CALL SYMPUT('REPTYEAR',PUT(REPTDATE,YEAR2.));
RUN;


OPTIONS NOCENTER YEARCUTOFF=1950 LS=132;

DATA ILOAN.ICISBASEL&REPTMON&REPTYEAR;
   INFILE CIS;
   INPUT    @01   GRPING        $11.
            @13   GROUPNO       $11.
            @24   CUSTNO        $11.
            @35   FULLNAME      $40.
            @75   ACCTNO         10.
            @85   NOTENO          5.
            @90   PRODUCT        $3.
            @93   AMTINDC        $3.
            @96   BALAMT         24.2
            @123  TOTAMT         24.2
            @150  RLENCODE       $3.
            @155  PRIMSEC        $1.;
RUN;

DATA ILOAN.ICISRPTCC&REPTMON&REPTYEAR;
   INFILE RCIS;
   INPUT    @01   GRPING         11.
            @15   C1CUST        $11.
            @30   C1TYPE         $1.
            @35   C1CODE         $3.
            @40   C1DESC        $15.
            @60   C2CUST        $11.
            @75   C2TYPE         $1.
            @80   C2CODE         $3.
            @85   C2DESC        $15.;
RUN;
