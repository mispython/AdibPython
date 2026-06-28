2026-06-28 16:07:25,942 - INFO - PBBDPFMT formats loaded successfully
2026-06-28 16:07:25,942 - INFO - Report Date: 2025-12-31
2026-06-28 16:07:25,942 - INFO - REPTMON: 12, REPTYEAR: 25
2026-06-28 16:07:25,942 - INFO - SDESC: PUBLIC BANK BERHAD (ISLAMIC)
2026-06-28 16:07:25,950 - INFO - REPTDATE saved to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQFAR2
2026-06-28 16:07:25,950 - INFO - Processing Islamic deposit data from cisdepd.sas7bdat
2026-06-28 16:07:25,950 - INFO - Reading SAS file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQFAR2/cisdepd.sas7bdat
2026-06-28 16:10:33,544 - INFO - Successfully read cisdepd.sas7bdat: 7,733,240 rows, 104 columns
2026-06-28 16:10:37,172 - INFO - Islamic DEPOSIT records: 7,733,240
2026-06-28 16:10:37,172 - INFO - Applying product format mappings from PBBDPFMT
2026-06-28 16:10:37,211 - ERROR - Error in Islamic processing: '<=' not supported between instances of 'int' and 'str'
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQFISF_ISLAMIC_INSURANCE.py", line 295, in main
    rpt_base = apply_format_mappings(deposit_df)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQFISF_ISLAMIC_INSURANCE.py", line 132, in apply_format_mappings
    df = df.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/lazy.py", line 1088, in __call__
    rv = self.function(slp, *args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/expr/expr.py", line 4655, in _wrap
    return function(sl[0], *args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/expr/expr.py", line 4879, in wrap_f
    return x.map_elements(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/series/series.py", line 5838, in map_elements
    self._s.map_elements(
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQFISF_ISLAMIC_INSURANCE.py", line 134, in <lambda>
    lambda x: _get_product_format(x),
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQFISF_ISLAMIC_INSURANCE.py", line 161, in _get_product_format
    if 200 <= prodcd <= 300:
TypeError: '<=' not supported between instances of 'int' and 'str'
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQFISF_ISLAMIC_INSURANCE.py", line 331, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQFISF_ISLAMIC_INSURANCE.py", line 295, in main
    rpt_base = apply_format_mappings(deposit_df)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQFISF_ISLAMIC_INSURANCE.py", line 132, in apply_format_mappings
    df = df.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/lazy.py", line 1088, in __call__
    rv = self.function(slp, *args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/expr/expr.py", line 4655, in _wrap
    return function(sl[0], *args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/expr/expr.py", line 4879, in wrap_f
    return x.map_elements(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/series/series.py", line 5838, in map_elements
    self._s.map_elements(
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQFISF_ISLAMIC_INSURANCE.py", line 134, in <lambda>
    lambda x: _get_product_format(x),
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQFISF_ISLAMIC_INSURANCE.py", line 161, in _get_product_format
    if 200 <= prodcd <= 300:
TypeError: '<=' not supported between instances of 'int' and 'str'
