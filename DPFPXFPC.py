EPTDATE: 2026-08-31 14:57:36.889574
REPTMON: 08
REPTYEAR2: 26
REPTDAY: 31
NOWK: 4

Reading CRFTABL...
CRFT records after filter: 98023
Reading MAST file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/btmast08426.sas7bdat
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRCGCS.py", line 261, in <module>
    crft = crft.join(mast, on="acctno", how="inner")
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
polars.exceptions.SchemaError: datatypes of join keys don't match - `acctno`: i64 on left does not match `acctno`: str on right (and no other type was available to cast to
