Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWCRMA.py", line 369, in <module>
    FD = build_deposit_table(DEPO_FD, IDEPO_FD, "fd")
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWCRMA.py", line 358, in build_deposit_table
    combined = pl.concat([depo, idepo], how="vertical", rechunk=True)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 234, in concat
    out = wrap_df(plr.concat_df(elems))
polars.exceptions.SchemaError: type Float64 is incompatible with expected type Null
