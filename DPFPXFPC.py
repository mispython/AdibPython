Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMNLGL.py", line 21, in <module>
    yy = int(df_glfile_header['YY'][0])
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 1403, in __getitem__
    return get_df_item_by_key(self, key)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/getitem.py", line 163, in get_df_item_by_key
    return df.get_column(key)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 9036, in get_column
    return wrap_s(self._df.get_column(name))
polars.exceptions.ColumnNotFoundError: "YY" not found
