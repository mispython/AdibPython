Traceback (most recent call last):
  File "/pythonITD/mis_dev/sas_migration/EGOLD/EIBDEGLD.py", line 112, in <module>
    OTHER = OTHER.with_columns([
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10020, in with_columns
    self.lazy()
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 330, in wrapper
    return function(*args, **kwargs)
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2335, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "TRXNYY"; valid columns: []
