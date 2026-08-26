SAS Connection established. Subprocess id is 2653357

REPTMON: 07, REPTMON1: 06, RDATE: 310726
Reading SAS7BDAT files...
LN columns: 29, DP columns: 29
============================================================
PUBLIC BANK BERHAD
DETAIL OF ACCTS (SCH=93) FOR SUBMISSION TO CGC @ 310726
============================================================
============================================================
PUBLIC BANK BERHAD
DETAIL OF ACCTS (SCH=94) FOR SUBMISSION TO CGC @ 310726
============================================================
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRSMEZ.py", line 337, in <module>
    eibrsmez()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRSMEZ.py", line 271, in eibrsmez
    .then(pl.col("cvar05").cast(pl.Int64).cast(pl.Utf8).str.str_pad(10, '0'))
AttributeError: 'ExprStringNameSpace' object has no attribute 'str_pad'
SAS Connection terminated. Subprocess id was 2653357
