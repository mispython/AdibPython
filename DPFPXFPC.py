Reading EGOLD file...
EGOLD records: 614
Reading OTHER file...
OTHER records: 78
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDEGLD_GOLD.py", line 139, in <module>
    GOLDTRAN = pl.concat([EGOLD, OTHER])
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 234, in concat
    out = wrap_df(plr.concat_df(elems))
polars.exceptions.ShapeError: unable to append to a DataFrame of width 14 with a DataFrame of width 16
