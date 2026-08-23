Processing date: 2026-07-31
REPTMON: 07, REPTMON1: 06
RDATE: 310726, NDATE: 3107

Looking for input files:
DP file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRP159/dpipgs07.sas7bdat
LN file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRP159/lnipgs07.sas7bdat
DP file exists: True
LN file exists: True

Files in directory /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRP159:
  - MEFR.txt
  - MEFT.txt
  - dpipgs07.sas7bdat
  - lnipgs07.sas7bdat

Reading DP file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRP159/dpipgs07.sas7bdat
DP file read successfully. Rows: 0, Columns: 20
DP columns: ['CVAR13', 'CVAR04', 'CVAR08', 'CVAR06', 'CVAR01', 'BRANCH', 'PRODUCT', 'CENSUST', 'CINSTCL', 'NATGUAR', 'CVAR02', 'CVAR03', 'CVAR05', 'CVAR07', 'CVAR10', 'CVAR09', 'CVAR11', 'CVAR12', 'CVAR14', 'CVAR15']
DP columns (lowercase): ['cvar13', 'cvar04', 'cvar08', 'cvar06', 'cvar01', 'branch', 'product', 'censust', 'cinstcl', 'natguar', 'cvar02', 'cvar03', 'cvar05', 'cvar07', 'cvar10', 'cvar09', 'cvar11', 'cvar12', 'cvar14', 'cvar15']

Reading LN file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRP159/lnipgs07.sas7bdat
LN file read successfully. Rows: 10, Columns: 20
LN columns: ['PRODUCT', 'CENSUST', 'CINSTCL', 'NATGUAR', 'CVAR01', 'CVAR06', 'CVAR03', 'CVAR04', 'CVAR14', 'CVAR13', 'CVAR08', 'CVAR09', 'CVAR10', 'CVAR11', 'CVAR02', 'CVAR05', 'CVAR07', 'CVAR12', 'CVAR15', 'BRANCH']
LN columns (lowercase): ['product', 'censust', 'cinstcl', 'natguar', 'cvar01', 'cvar06', 'cvar03', 'cvar04', 'cvar14', 'cvar13', 'cvar08', 'cvar09', 'cvar10', 'cvar11', 'cvar02', 'cvar05', 'cvar07', 'cvar12', 'cvar15', 'branch']

Only LN data available: 10 rows
Added cvarxx column. Total rows: 10

Writing MEFT.txt...
Row 0:           ;  ;KT0306064W     ;FENRA TRADING                                     ;29/01/2018;        ...
Row 1:           ;  ;KT0136232A     ;YA SIN ENTERPRISE                                 ;14/03/2018;        ...
Row 2:           ;  ;KT0136232A     ;YA SIN ENTERPRISE                                 ;14/03/2018;        ...

============================================================
PUBLIC BANK BERHAD
DETAIL OF ACCTS (MEF PRODUCTS) FOR SUBMISSION TO CGC @ 310726
============================================================
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRP159.py:190: DeprecationWarning: `pl.count()` is deprecated. Please use `pl.len()` instead.
(Deprecated in version 0.20.5)
  summary = npgs_df.group_by("cvar02").agg(pl.count().alias("count"))

Parquet output saved to: eibrp159_output.parquet
Using SAS Config named: default
SAS Connection established. Subprocess id is 1951070

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
SAS Connection terminated. Subprocess id was 1951070
SAS7BDAT output saved to: eibrp159_output.sas7bdat

Summary:
MEFT.txt file created with 10 records
Report saved to MEFR.txt
