Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWCRMA.py", line 77, in <module>
    SAVING = pl.concat([read_sas7bdat(DEPO_SAV), read_sas7bdat(IDEPO_SAV)], how="vertical", rechunk=True)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 234, in concat
    out = wrap_df(plr.concat_df(elems))
polars.exceptions.ShapeError: unable to append to a DataFrame of width 86 with a DataFrame of width 88
