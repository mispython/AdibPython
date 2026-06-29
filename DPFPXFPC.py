SAS Connection established. Subprocess id is 713204

✓ SAS session initialized
Report Date: 31/05/26, Week: 4, Month: 05

Processing Savings...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQDISE.py", line 155, in <module>
    saving = pl.concat([saving, uma])
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 234, in concat
    out = wrap_df(plr.concat_df(elems))
polars.exceptions.ShapeError: unable to append to a DataFrame of width 86 with a DataFrame of width 84
SAS Connection terminated. Subprocess id was 713204
