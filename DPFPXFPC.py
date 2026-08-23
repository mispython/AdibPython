REPTMON: 07, RDATE: 310726
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRP159.py", line 179, in <module>
    eibrp159()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRP159.py", line 49, in eibrp159
    npgs_df = pl.concat([dp_df, ln_df], how="vertical")
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 234, in concat
    out = wrap_df(plr.concat_df(elems))
polars.exceptions.ShapeError: unable to vstack, column names don't match: "cvar13" and "product"

i tried change the timedelta(days=23) to use july's data input
