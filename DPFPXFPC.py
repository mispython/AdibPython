PBBDPFMT imported successfully
============================================================
EIBQINST - Trustee and Client Account Quarterly Reporting
============================================================

Report Date: 2026-07-31 (Week: 4)

Loading data...
Error loading FLOAT: 'ExprStringNameSpace' object has no attribute 'strip'
  FLOAT: 0 records
Error loading IBGPIDM: 'ExprStringNameSpace' object has no attribute 'strip'
  IBGPIDM: 0 records
Error loading REMIT/UNCLAIM: 'ExprStringNameSpace' object has no attribute 'strip'
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQINST.py", line 172, in load_remit
    remit = remit.with_columns(pl.col('paymode').cast(pl.Utf8).str.strip())
AttributeError: 'ExprStringNameSpace' object has no attribute 'strip'
  REMIT/UNCLAIM: 0 records
