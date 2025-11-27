REPTDATE = 2025-11-26, PREVDATE = 2025-11-25
RDATE (SAS format) = 24071
MON=11, DAY=26, YEAR=25
Filter date (reptdte) = 251126
Traceback (most recent call last):
  File "/pythonITD/mis_dev/sas_migration/BRIGHTSTAR/EIBDSTAR_BRIGHTSTAR_SAVINGS_EXTRACTION.py", line 45, in <module>
    saving = pl.read_csv(saving_csv_path)
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
polars.exceptions.ComputeError: could not parse `6.01884E15` as dtype `i64` at column 'ODXSAMT' (column number 49)

The current offset in the file is 895 bytes.

You might want to try:
- increasing `infer_schema_length` (e.g. `infer_schema_length=10000`),
- specifying correct dtype with the `schema_overrides` argument
- setting `ignore_errors` to `True`,
- adding `6.01884E15` to the `null_values` list.

Original error: ```remaining bytes non-empty```
