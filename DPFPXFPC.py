Processing date: 2026-07-31
REPTMON: 07, REPTMON1: 06
RDATE: 310726, NDATE: 3107

Looking for input files:
DP file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRP159/dpipgs07.sas7bdat
LN file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRP159/lnipgs07.sas7bdat
DP file exists: True
LN file exists: True

Reading DP file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRP159/dpipgs07.sas7bdat
DP file read successfully. Rows: 0, Columns: 20

Reading LN file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRP159/lnipgs07.sas7bdat
LN file read successfully. Rows: 10, Columns: 20

Only LN data available: 10 rows

Writing MEFT.txt...

============================================================
PUBLIC BANK BERHAD
DETAIL OF ACCTS (MEF PRODUCTS) FOR SUBMISSION TO CGC @ 310726
============================================================

Parquet output saved to: eibrp159_output.parquet

Creating SAS7BDAT file using saspy...
Using SAS Config named: default
SAS Connection established. Subprocess id is 2352760

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
SAS dataset verification:
{'LOG': '\n21   \n22   \n23               proc contents data=work.npgs_output;\nERROR: File WORK.NPGS_OUTPUT.DATA does not exist.\n24               run;\nNOTE: Statements not processed because of errors noted above.\nNOTE: The SAS System stopped processing this step because of errors.\nNOTE: PROCEDURE CONTENTS used (Total process time):\n      real time           0.05 seconds\n      cpu time            0.01 seconds\n      \n25   \n26   ', 'LST': ''}
SAS log contains errors:
{'LOG': '\n28   \n29   \n30               libname outlib "/sas/python/virt_edw/Data_Warehouse/MIS";\nNOTE: Libref OUTLIB was successfully assigned as follows: \n      Engine:        V9 \n      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS\n31               data outlib.eibrp159_output;\nERROR: File WORK.NPGS_OUTPUT.DATA does not exist.\n32                   set work.npgs_output;\n33               run;\nNOTE: The SASSystem stopped processing this step because of errors.\nWARNING: The data set OUTLIB.EIBRP159_OUTPUT may be incomplete.  When this step was stopped there were 0 observations and 0 \n         variables.\nWARNING: Data set OUTLIB.EIBRP159_OUTPUT was not replaced because this step was stopped.\nNOTE: DATA statement used (Total process time):\n      real time           0.00 seconds\n      cpu time            0.00 seconds\n\n34   \n35   ', 'LST': ''}
SAS Connection terminated. Subprocess id was 2352760

Summary:
MEFT.txt file created with 10 records
Report saved to MEFR.txt



below is sas original sas code:


*;
DATA REPTDATE (KEEP=REPTDATE);
  SET MNILN.REPTDATE;
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
DATA NPGS;
  SET NPGS.DPIPGS&REPTMON
      NPGS.LNIPGS&REPTMON;
  CVARXX='          ';
*;
DATA MEFT;
  SET NPGS;
  FILE MEFT;
  PUT  @001 CVAR01   10.       ';'
       @012 CVAR02   $2.       ';'
       @015 CVAR03   $15.      ';'
       @031 CVAR04   $50.      ';'
       @082 CVAR05   DDMMYY10. ';'
       @093 CVARXX   $10.
       @103 CVAR06   10.       ';'
       @114 CVAR07   $2.       ';'
       @117 CVAR08   10.2      ';'
       @128 CVAR09   10.2      ';'
       @139 CVAR10   10.2      ';'
       @150 CVAR11   5.        ';'
       @156 CVAR12    $3.      ';'
       @160 CVAR13    $10.     ';'
       @171 CVAR14    $4.      ';'
       @176 CVAR15    $5.      ';'
       ;
*;
PROC   PRINTTO PRINT=MEFR;
TITLE1 'PUBLIC BANK BERHAD';
TITLE2 'DETAIL OF ACCTS (MEF PRODUCTS) FOR SUBMISSION TO CGC @' &RDATE;
%INC PGM(NPGSRPT);
*;
