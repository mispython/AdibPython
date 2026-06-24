============================================================
EIFLTEXP PROCESSING STARTED
============================================================
MNI Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/MNI
IMNI Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/IMNI
PIDM Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/PIDM
Output Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFLTEXP
============================================================

[STEP 1] Loading FDMTHLY data...
  - MNI FDMTHLY columns: ['acctno', 'branch', 'intplan', 'curbal', 'bic', 'amtind', 'intpay']
  - MNI FDMTHLY loaded: 2756145 records
  - MNI FDMTHLY dtypes: [Float64, Float64, Float64, Float64, String, String, Float64]
  - IMNI FDMTHLY loaded: 431257 records
  - IMNI FDMTHLY dtypes: [Float64, Float64, Float64, Float64, String, String, Float64]
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIFLTEXP_FLOAT_EXPOSURE.py", line 158, in standardize_schema_for_concat
    df = df.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.InvalidOperationError: conversion from `str` to `f64` failed in column 'amtind' for 2756145 out of 2756145 values: ["D", "D", … "D"]

Did not show all failed cases as there were too many.

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIFLTEXP_FLOAT_EXPOSURE.py", line 229, in <module>
    standardized_fdmthly = standardize_schema_for_concat(fdmthly_dfs)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIFLTEXP_FLOAT_EXPOSURE.py", line 163, in standardize_schema_for_concat
    df = df.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.InvalidOperationError: conversion from `str` to `f64` failed in column 'amtind' for 2756145 out of 2756145 values: ["D", "D", … "D"]

Did not show all failed cases as there were too many.
