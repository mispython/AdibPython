Processing for week 04 of 01/26
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/CMPSTAFF/EIBMCMPL_CMPSTAFF.py", line 71, in <module>
    brch_df = pl.read_parquet(lookup_path / "LKP_BRANCH.parquet")
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 128, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 128, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/io/parquet/functions.py", line 289, in read_parquet
    return lf.collect()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 330, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2335, in collect
    return wrap_df(ldf.collect(engine, callback))
FileNotFoundError: No such file or directory (os error 2): /sasdata/rawdata/lookup/LKP_BRANCH.parquet

This error occurred with the following context stack:
        [1] 'parquet scan'
        [2] 'sink'
