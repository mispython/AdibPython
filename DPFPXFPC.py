Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDEGLD_GOLD.py", line 36, in <module>
    EGOLD = pl.read_csv(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 128, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 128, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 128, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/io/csv/functions.py", line 549, in read_csv
    df = _read_csv_impl(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/io/csv/functions.py", line 697, in _read_csv_impl
    pydf = PyDataFrame.read_csv(
polars.exceptions.ComputeError: could not parse `6570142722` as dtype `i32` at column 'column_5' (column number 5)

The current offset in the file is 147 bytes.

You might want to try:
- increasing `infer_schema_length` (e.g. `infer_schema_length=10000`),
- specifying correct dtype with the `schema_overrides` argument
- setting `ignore_errors` to `True`,
- adding `6570142722` to the `null_values` list.

Original error: ```invalid primitive value found during CSV parsing```
