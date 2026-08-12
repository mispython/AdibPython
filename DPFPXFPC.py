============================================================
STEP 1: Reading fixed-width file...
============================================================
File size: 185,874,284 bytes
Records: 434,285
Parsing records...
Parsing complete!

============================================================
STEP 2: Converting data types...
============================================================
Converting string columns...
Converting numeric columns...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMADRE.py", line 171, in <module>
    df = df.with_columns(
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
polars.exceptions.SchemaError: unexpected value while building Series of type Float64; found value of type Int64: 8187539008
