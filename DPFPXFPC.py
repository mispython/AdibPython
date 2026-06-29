Report Date: 31/05/26
Start Date: 23/05/26
Week: 4
Month: 05
Year: 2026
SAS Connection established. Subprocess id is 743412

✓ SAS session initialized

Processing UMA...
✓ 31,915 UMA records (BNKIND=PBB)

Processing Saving Accounts...
✓ 4,282,476 saving accounts
✓ savg054.parquet
✓ savg054.sas7bdat

Processing Current Accounts...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQDISE.py", line 481, in <module>
    current = current.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.InvalidOperationError: arithmetic on string and numeric not allowed, try an explicit cast first
SAS Connection terminated. Subprocess id was 743412
