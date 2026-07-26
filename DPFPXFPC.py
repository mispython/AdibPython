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
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR202.py", line 132, in <module>
    con.execute("""
_duckdb.ConversionException: Conversion Error: Could not convert string 'B001' to DOUBLE when casting from source column BRANCH

LINE 7:     LEFT JOIN BRHDATA b ON l.BRANCH = b.BRANCH
