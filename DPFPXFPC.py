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

Creating SAS7BDAT file using pyreadstat...
pyreadstat failed: module 'pyreadstat' has no attribute 'write_sas7bdat'
Trying saspy instead...
Using SAS Config named: default
SAS Connection established. Subprocess id is 2089509

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
SAS log contains errors:
{'LOG': '\n94   \n95   \n96                   libname outlib ".";\nNOTE: Libref OUTLIB was successfully assigned as follows: \n      Engine:        V9 \n      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS\n97                   data outlib.eibrp159_output;\nERROR: File WORK.NPGS_OUTPUT.DATA does not exist.\n98                       set work.npgs_output;\n99                   run;\nNOTE: The SAS System stopped processing this step because of errors.\nWARNING: The data set OUTLIB.EIBRP159_OUTPUT may be incomplete.  When this step was stopped there were 0 observations and 0 \n         variables.\nWARNING: Data set OUTLIB.EIBRP159_OUTPUT was not replaced because this step was stopped.\nNOTE: DATA statement used (Total process time):\n      real time           0.00 seconds\n      cpu time            0.00 seconds\n      \n100  \n101  ', 'LST': ''}
SAS Connection terminated. Subprocess id was 2089509

Summary:
MEFT.txt file created with 10 records
Report saved to MEFR.txt
