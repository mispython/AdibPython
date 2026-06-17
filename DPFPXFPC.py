Report Parameters: {'REPTYEAR': '26', 'REPTMON': '06', 'REPTDAY': '16', 'PREVMON': '05', 'PREVDAY': '31', 'RDATE': '16-06-2026', 'RDATEX': '0626', 'SDATE': '60701'}
Warning: Error parsing line 1: invalid literal for int() with base 10: '1BKT2'
  Line content: 1BKT20260531                                                ...
Successfully read 870 records from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/BTPM12.txt
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDBT12.py", line 141, in <module>
    btdtl = btdtl.with_columns(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.InvalidOperationError: conversion from `str` to `i32` failed in column 'MATDT' for 870 out of 870 values: ["9P", "6P", … "9P"]

Did not show all failed cases as there were too many.
