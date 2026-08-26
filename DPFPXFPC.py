SAS Connection established. Subprocess id is 3070019

REPTMON: 07, REPTMON1: 06, RDATE: 310726
Reading SAS7BDAT files...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRSMEZ.py", line 265, in <module>
    eibrsmez()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRSMEZ.py", line 55, in eibrsmez
    smez_df = pl.concat([ln_df, dp_df])
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 234, in concat
    out = wrap_df(plr.concat_df(elems))
polars.exceptions.ShapeError: unable to append to a DataFrame of width 28 with a DataFrame of width 24
SAS Connection terminated. Subprocess id was 3070019
