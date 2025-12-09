Traceback (most recent call last):
  File "/pythonITD/mis_dev/sas_migration/CIGR/output/EIBWCIGR.py", line 47, in <module>
    cisbasel_df = pl.read_parquet(CIS_FILE).select([
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 9854, in select
    self.lazy()
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 330, in wrapper
    return function(*args, **kwargs)
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2335, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.InvalidOperationError: conversion from `str` to `f64` failed in column 'BALAMT' for 1 out of 214 values: ["."]
