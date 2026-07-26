================================================================================
EIMIR202 - NPL HIRE PURCHASE DIRECT REPORT
================================================================================
REPORT DATE: 2026-07-25
RDATE: 25072026
BRANCH RECORDS: 376

READING LOANTEMP.SAS7BDAT...
LOANTEMP RECORDS: 663,747

PROCESSING LOAN DATA...
LOAN1 RECORDS: 1,321
WARNING: 1,321 records have no matching BRHCODE
AGGREGATING BY ARREARS BUCKETS...

GENERATING FORMATTED REPORT...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR202.py", line 272, in <module>
    CATEGORIES = con.execute("SELECT DISTINCT CAT, TYPE FROM REPORT_DATA ORDER BY CAT").fetchall()
_duckdb.CatalogException: Catalog Error: Table with name REPORT_DATA does not exist!
Did you mean "BRHDATA"?

LINE 1: SELECT DISTINCT CAT, TYPE FROM REPORT_DATA ORDER BY CAT
