REPTMON: 07, RDATE: 310726
Input path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRTLIO
Output path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRTLIO
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRTLIO.py", line 221, in <module>
    eibrtlio()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRTLIO.py", line 52, in eibrtlio
    combined_df = pl.concat([dp_df, ln_df])
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 234, in concat
    out = wrap_df(plr.concat_df(elems))
polars.exceptions.ShapeError: unable to vstack, column names don't match: "CVAR13" and "PRODUCT"
