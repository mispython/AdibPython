Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDP169.py", line 167, in <module>
    odlmt = odlmt.with_columns([
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
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDP169.py", line 170, in <lambda>
    lambda x: datetime.strptime(f"{int(x):011d}"[:8], "%m%d%Y").date()
  File "/usr/lib64/python3.9/_strptime.py", line 568, in _strptime_datetime
    tt, fraction, gmtoff_fraction = _strptime(data_string, format)
  File "/usr/lib64/python3.9/_strptime.py", line 349, in _strptime
    raise ValueError("time data %r does not match format %r" %
ValueError: time data '00000023' does not match format '%m%d%Y'
