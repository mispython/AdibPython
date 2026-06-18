Testing with REPTDATE: 2026-06-16
REPTMON: 06, NOWK: 3, REPTDAY: 16, REPTDT: 20260616
Reading EGOLD file...
EGOLD records: 614
Reading OTHER file...
OTHER records: 78
Total combined records: 692
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDEGLD_GOLD.py", line 198, in <module>
    GOLDTRAN = GOLDTRAN.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.InvalidOperationError: round can only be used on numeric types
