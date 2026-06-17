Report Parameters: {'REPTYEAR': '26', 'REPTMON': '06', 'REPTDAY': '16', 'PREVMON': '05', 'PREVDAY': '31', 'RDATE': '16-06-2026', 'RDATEX': '0626', 'SDATE': '60701'}
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDBT12.py:77: DeprecationWarning: the argument `dtypes` for `read_csv` is deprecated. It was renamed to `schema_overrides` in version 0.20.31.
  df = pl.read_csv(
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDBT12.py", line 117, in <module>
    btdtl = read_btdtl_text(INPUT_BTPM12_FILE)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDBT12.py", line 77, in read_btdtl_text
    df = pl.read_csv(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 128, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 128, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 128, in wrapper
    return function(*args, **kwargs)
TypeError: read_csv() got an unexpected keyword argument 'newline_character'
