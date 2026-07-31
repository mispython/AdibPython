Processing for month: 07 (Report Date: 2026-07-30)
Report Date based on: current date minus 1 day

Loading SA data...

Loading CA data...

Loading FD base data...
Using ACCTTYPE as PRODUCT for FD base (SACA)

Loading FDCD data...

Combining deposits...

Parsing CLIENT file...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMTRUT.py", line 290, in <module>
    CLIENT = CLIENT.join(DEP.select("ACCTNO").unique(), on="ACCTNO", how="inner")
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
polars.exceptions.SchemaError: datatypes of join keys don't match - `ACCTNO`: i64 on left does not match `ACCTNO`: f64 on right (and no other type was available to cast to)
