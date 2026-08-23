REPTMON: 07, RDATE: 310726
Input path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRTLIO
Output path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRTLIO
DP columns: ['cvar13', 'cvar04', 'cvar08', 'cvar06', 'cvar01', 'branch', 'product', 'censust', 'sch', 'cinstcl', 'natguar', 'cvar02', 'cvar03', 'cvar05', 'cvar07', 'cvar10', 'cvar09', 'cvar11', 'cvar12', 'cvar14','cvar15']
DP shape: (0, 21)
LN columns: ['product', 'censust', 'sch', 'cinstcl', 'natguar', 'cvar02', 'cvar01', 'cvar06', 'cvar03', 'cvar04', 'cvar14', 'cvar13', 'cvar08', 'cvar09', 'cvar10', 'cvar11', 'cvar05', 'cvar07', 'cvar12', 'cvar15','branch']
LN shape: (2105, 21)
DP is empty, using only LN data
Combined shape: (2105, 21)
Combined columns: ['product', 'censust', 'sch', 'cinstcl', 'natguar', 'cvar02', 'cvar01', 'cvar06', 'cvar03', 'cvar04', 'cvar14', 'cvar13', 'cvar08', 'cvar09', 'cvar10', 'cvar11', 'cvar05', 'cvar07', 'cvar12', 'cvar15', 'branch']
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRTLIO.py", line 284, in <module>
    eibrtlio()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRTLIO.py", line 191, in eibrtlio
    sas.df2sd(npgs3_pandas, table='npgs3', libref='work')
UnboundLocalError: local variable 'sas' referenced before assignment


below is sas orignal code:

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
DATA NPGS.TL;
  SET NPGS.DPNPGS&REPTMON
      NPGS.LNIPGS&REPTMON;
  IF  CVAR13 NE '         ';
  NDATE =CVAR13;
  STATUS=CVAR12;
  KEEP CVAR01 CVAR06 STATUS NDATE;
*;
DATA NPGS;
  SET NPGS.DPNPGS&REPTMON
      NPGS.LNIPGS&REPTMON;
  IF CVAR12='NPL'  THEN CVAR12A='NP';
                   ELSE CVAR12A='AP';
*;
DATA NPGS3;
  SET NPGS;
  IF  NATGUAR='06' AND CINSTCL='18';
  CVARXX='          ';
PROC SORT; BY CVAR01 CVAR06;
*;
DATA SC93T;
  SET NPGS3;
  FILE SC167T;
  PUT  @001 CVAR01   10.       ';'
       @012 CVAR02   $2.       ';'
       @015 CVAR03   $15.      ';'
       @031 CVAR04   $50.      ';'
       @082 CVAR05   DDMMYY10. ';'
       @093 CVAR06   10.       ';'
       @104 CVAR07   $2.       ';'  /* NETPROC   */
       @107 CVAR08   10.2      ';'  /* NETPROC   */
       @118 CVAR09   10.2      ';'  /* O/S       */
       @129 CVAR10   10.2      ';'  /* INTEREST  */
       @140 CVAR11   5.        ';'
       @146 CVAR12A  $4.       ';'  /* AP OR NP */
       @151 CVAR13   $10.      ';'
       @162 CVAR14    $4.      ';'  /* NPL NOTIFICATION */
       @167 CVAR15    $5.      ';'  /* NPL REASON */
       ;
*;
PROC    PRINTTO PRINT=SC167R;
TITLE1 'PUBLIC ISLAMIC BANK BERHAD';
TITLE2 'DETAIL OF ACCTS FOR SUBMISSION TO CGC @' &RDATE;
%INC PGM(CGCRPT);

looks like it is calling CGCRPT.
