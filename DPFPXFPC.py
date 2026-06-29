eport Date: 31/05/26
Start Date: 23/05/26
Week: 4
Month: 05
Year: 2026

Loading UMA data...
✓ Loaded 31915 UMA records
Processing Saving Accounts...
✓ Processed 4282476 saving accounts
✓ Saved Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQDISE/savg054.parquet
⚠ Error saving SAS file /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQDISE/savg054.sas7bdat: module 'pyreadstat' has no attribute 'write_sas7bdat'
Processing Current Accounts...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQDISE.py", line 305, in <module>
    current = current.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.InvalidOperationError: conversion from `str` to `f64` failed in column 'FORATE' for 919361 out of 919361 values: ["", "", … ""]

Did not show all failed cases as there were too many.
