============================================================
EIFLTEXP PROCESSING STARTED
============================================================
MNI Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/MNI
IMNI Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/IMNI
PIDM Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/PIDM
Output Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFLTEXP
============================================================

[STEP 1] Loading FDMTHLY data...
  - MNI FDMTHLY columns: ['state', 'custcode', 'bic', 'lstmatdt', 'branch', 'acctno', 'purpose', 'name', 'openind', 'curbal']
  - MNI FDMTHLY loaded: 2756145 records
  - MNI FDMTHLY dtypes: [String, String, String, Float64, Float64]
  - IMNI FDMTHLY loaded: 431257 records
  - IMNI FDMTHLY dtypes: [Float64, Float64, Float64, String, String]
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIFLTEXP_FLOAT_EXPOSURE.py", line 200, in <module>
    fdmthly_combined = pl.concat([fdmthly_df, ifdmthly_df], how="diagonal")
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 247, in concat
    out = wrap_df(plr.concat_df_diagonal(elems))
polars.exceptions.SchemaError: type Float64 is incompatible with expected type String




below is the original sas code btw

OPTIONS YEARCUTOFF=1950 NOCENTER;
PROC SORT DATA=MNI.FDMTHLY
  OUT=FDMTHLY (KEEP=ACCTNO BRANCH INTPLAN CURBAL BIC AMTIND INTPAY);
  BY ACCTNO;
PROC SORT DATA=IMNI.FDMTHLY
  OUT=IFDMTHLY (KEEP=ACCTNO BRANCH INTPLAN CURBAL BIC AMTIND INTPAY);
  BY ACCTNO;
DATA FDMTHLY;
    SET FDMTHLY IFDMTHLY;
    LEDGBAL = CURBAL;
RUN;
DATA CURN;
   SET MNI.CURN124
       IMNI.CURN124;
   IF  PRODUCT = 139 THEN DELETE;
RUN;
DATA DEPOSIT;
  SET MNI.SAVG124
             (IN=A KEEP=ACCTNO PRODUCT CURBAL LEDGBAL PRODCD AMTIND
                            INTPAYBL BRANCH)
      IMNI.SAVG124
             (IN=A KEEP=ACCTNO PRODUCT CURBAL LEDGBAL PRODCD AMTIND
                            INTPAYBL BRANCH)
          CURN   (IN=B KEEP=ACCTNO PRODUCT CURBAL LEDGBAL PRODCD AMTIND
                            INTPAYBL BRANCH)
      FDMTHLY  (KEEP=ACCTNO INTPLAN  CURBAL LEDGBAL BIC AMTIND  INTPAY
                     BRANCH RENAME=(BIC=PRODCD INTPLAN=PRODUCT
                                 INTPAY=INTPAYBL));
     IF PRODCD IN ('42110','42310','42120','42320','42130','42610',
                   '42133','42132','42180','42610','42630','34180',
                   '42199','42699');
     IF PRODUCT = 166 THEN PRODCD = '42310';
     IF PRODCD IN ('42199','42699') AND
     PRODUCT NOT IN (72,413) THEN DELETE;
     IF PRODUCT IN (30,31,32,33,34) THEN DELETE;
     IF INTPAYBL < 0 THEN INTPAYBL = 0;
RUN;
  /* FLOAT */
DATA FLOAT;
   SET PIDMS.FLOAT;
RUN;
PROC SUMMARY DATA=FLOAT NWAY;
  CLASS ACCTNO;
  VAR FLOAT;
OUTPUT OUT=FLOAT SUM=;
RUN;
PROC SORT DATA=DEPOSIT; BY ACCTNO;
DATA DEPOSIT;
   MERGE DEPOSIT(IN=A) FLOAT(IN=B);
   BY ACCTNO;
   IF CURBAL < 0 THEN CURBAL = 0;

   AVBAL = SUM(CURBAL,(-1)*FLOAT);
   /*
   IF AVBAL < 0 THEN DO;
      FLOAT = CURBAL;
      AVBAL = 0;
   END;
   */
   AVBALTT = SUM(AVBAL,INTPAYBL);
   CURBALTT = SUM(CURBAL,INTPAYBL);
   IF B AND NOT A;
RUN;
PROC PRINT DATA=DEPOSIT;
SUM FLOAT;
RUN;
