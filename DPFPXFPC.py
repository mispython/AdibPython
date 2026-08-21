Report Date: 200826 (20-08-26)
DPDATE: 200826 EQDATE: 200826
DPST records: 90327
DPST TICKETNO sample: ['', 'N319474', 'N362347', 'N362466', 'N377670']
DPST TICKETNO dtype: String
EQTN records: 532
EQTN TICKETNO sample: ['Z31222       ', 'Z31350       ', 'Z31156       ', 'Z31236       ', 'Z31336       ']
EQTN TICKETNO dtype: String
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDDCIA.py", line 246, in <module>
    dpst = dpst.with_columns(pl.col("TICKETNO").str.strip().str.zfill(7))
AttributeError: 'ExprStringNameSpace' object has no attribute 'strip'
