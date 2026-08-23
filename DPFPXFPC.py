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
Using SAS Config named: default
SAS Connection established. Subprocess id is 2012031

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
SAS Connection terminated. Subprocess id was 2012031
SAS7BDAT output saved to: eibrp159_output.sas7bdat

Summary:
MEFT.txt file created with 10 records
Report saved to MEFR.txt
