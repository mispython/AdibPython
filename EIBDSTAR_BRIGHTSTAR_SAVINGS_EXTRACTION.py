REPTDATE = 2025-11-26, PREVDATE = 2025-11-25
RDATE (SAS format) = 24071
MON=11, DAY=26, YEAR=25
Filter date (reptdte) = 251126
SAVING.csv shape: (8320, 81)
CIS shape: (224992, 100)
PRISEC sample values: [3.0, None, 901.0, 25.0, 902.0]
Traceback (most recent call last):
  File "/pythonITD/mis_dev/sas_migration/BRIGHTSTAR/EIBDSTAR_BRIGHTSTAR_SAVINGS_EXTRACTION.py", line 71, in <module>
    saving.filter(pl.col("PRODUCT") == 208)
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 5198, in filter
    self.lazy()
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 330, in wrapper
    return function(*args, **kwargs)
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2335, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ComputeError: cannot compare string with numeric type (i32)

