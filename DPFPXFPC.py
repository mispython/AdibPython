Report Date: 200826 (20-08-26)
DPDATE: 200826 EQDATE: 200826
DPST records: 90327
EQTN records: 532
DCID records after merge and filter: 0
Reading CA file...
Reading SA file...
Reading FCY file...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDDCIA.py", line 275, in <module>
    dpdata = pl.concat([sa, ca, fcy])
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 234, in concat
    out = wrap_df(plr.concat_df(elems))
polars.exceptions.SchemaError: type Int32 is incompatible with expected type Float64


why is after the filter, dcid records 0?

fix the error
