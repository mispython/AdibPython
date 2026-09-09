Step 1: Setting report date...
Report Date: 2026-09-08 18:36:34.037699, RDATE: 24357
Step 2: Processing credit facility table...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBTNPGS.py", line 86, in <module>
    crft_data = crft_data.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.InvalidOperationError: conversion from `str` to `i64` failed in column 'data' for 6126 out of 6126 values: ["             ", "SGXX         ", … "SGCX         "]

Did not show all failed cases as there were too many.


bopess is all uppercase BOPESS.txt
