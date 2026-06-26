PROCESSING CONVENTIONAL BANKING FLOAT DATA
==================================================
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBFLOAT_FLOAT.py", line 493, in <module>
    conventional_result = process_conventional_float()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBFLOAT_FLOAT.py", line 103, in process_conventional_float
    fdmthly_renamed = fdmthly_processed.select([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "intplan"; valid columns: ["acctno", "branch", "curbal", "ledgbal", "amtind"]
