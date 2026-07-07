No date provided - using August 8, 2026

======================================================================
EIBMLIBT - LOAN MATURITY PROFILE PROCESSOR
======================================================================

Report Date: 08/08/2026
Week Number: 1
Report Month: 08
Report Year: 2026

Looking for BTRAD file: btrad081.sas7bdat
  Reading SAS file...
  Total records read: 973
  Records after filtering: 562

  Total records processed: 562
  Output records created: 856

  Records with missing remmth (code '07'): 854

Writing SAS dataset to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMLIBT/BT.sas7bdat

ERROR: module 'pyreadstat' has no attribute 'write_sas7bdat'
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMLIBT.py", line 400, in main
    pyreadstat.write_sas7bdat(
AttributeError: module 'pyreadstat' has no attribute 'write_sas7bdat'



try to use saspy to read to sas7bdat, and ensure to get below output:

BNMCODE	        AMOUNT
9321909010000Y	207404646.61
9321909020000Y	757154447.68
9321909030000Y	1431426118.7
9321909040000Y	106996401.31
9521909010000Y	207404646.61
9521909020000Y	757154447.68
9521909030000Y	1431426118.7
9521909040000Y	106996401.31
