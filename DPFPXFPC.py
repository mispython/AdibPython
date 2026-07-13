NOWK: 4, NOWK1: 3, REPTMON: 06, RDATE: 300626
CIS sample size: 694 records
HPACC sample size: 500 records
PRODUCT column type: Float64
PRODUCT unique values: [5.0, 15.0, 61.0, 70.0, 71.0, 200.0, 205.0, 210.0, 212.0, 216.0]
After filtering: 115 records
Error reading LKP_BRANCH: [Errno 2] No such file or directory: '/sas/python/virt_edw/Data_Warehouse/MIS/LKP_BRANCH'
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMHPTOP.py", line 337, in <module>
    eimhptop()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMHPTOP.py", line 211, in eimhptop
    hpacc_df = hpacc_df.join(brhdata_df, on="BRANCH", how="left")
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 128, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 8242, in join
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.SchemaError: datatypes of join keys don't match - `BRANCH`: f64 on left does not match `BRANCH`: null on right (and no other type was available to cast to)
