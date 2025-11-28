Traceback (most recent call last):
  File "/pythonITD/mis_dev/sas_migration/BRIGHTSTAR/EIBDSTAR_BRIGHTSTAR_SAVINGS_EXTRACTION.py", line 20, in <module>
    saving = pl.read_csv(
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 128, in wrapper
    return function(*args, **kwargs)
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 128, in wrapper
    return function(*args, **kwargs)
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 128, in wrapper
    return function(*args, **kwargs)
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/io/csv/functions.py", line 549, in read_csv
    df = _read_csv_impl(
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/io/csv/functions.py", line 697, in _read_csv_impl
    pydf = PyDataFrame.read_csv(
polars.exceptions.ComputeError: invalid utf-8 sequence
