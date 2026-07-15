Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDFD1M.py", line 47, in <module>
    df_fd = df_fd.with_columns([
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
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDFD1M.py", line 51, in <lambda>
    lambda x: datetime.strptime(x, '%m%d%Y').date() if len(x) == 8 else None,
  File "/usr/lib64/python3.9/_strptime.py", line 568, in _strptime_datetime
    tt, fraction, gmtoff_fraction = _strptime(data_string, format)
  File "/usr/lib64/python3.9/_strptime.py", line 352, in _strptime
    raise ValueError("unconverted data remains: %s" %
ValueError: unconverted data remains: 1
