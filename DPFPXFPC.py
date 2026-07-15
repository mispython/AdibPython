Calculating report dates...
Report Date: 30062026, Week: 4
Copying files from DEPOBACK to BNM...
Warning: fdwkly.sas7bdat not found in /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/MNI
Copied: fdmthly.sas7bdat
Processing FDMTHLY data...
Read 2756145 records from fdmthly.sas7bdat
Columns: STATE, CUSTCODE, BIC, LSTMATDT, BRANCH, ACCTNO, PURPOSE, NAME, OPENIND, CURBAL, ORGDATE, MATDATE, RATE, ACCTTYPE, TERM, INTPLAN, RENEWAL, INTPAY, INTDATE, LASTACTV, AMTIND, FORATE
Loaded 2756145 records from fdmthly.sas7bdat
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQFDSP.py", line 222, in <module>
    pl.col("OPENIND").cast(pl.Utf8).str.strip()
AttributeError: 'ExprStringNameSpace' object has no attribute 'strip'
