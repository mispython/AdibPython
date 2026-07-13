NOWK: 4, NOWK1: 3, REPTMON: 06, RDATE: 300626
CIS sample size: 694 records
HPACC sample size: 1000 records
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMHPTOP.py", line 313, in <module>
    eimhptop()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMHPTOP.py", line 79, in eimhptop
    hpacc_df = hpacc_df.filter(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 5325, in filter
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.InvalidOperationError: 'is_in' cannot check for List(String) values in Float64 data
