Traceback (most recent call last):
  File "/sas/pythonITD/mis/job/LOAN/EIBMCHNL.py", line 257, in <module>
    last_month_update = read_or_create_empty(
  File "/sas/pythonITD/mis/job/LOAN/EIBMCHNL.py", line 138, in read_or_create_empty
    df = normalize_func(df)
  File "/sas/pythonITD/mis/job/LOAN/EIBMCHNL.py", line 120, in normalize_channel_update_schema
    return df.select([
  File "/sas/pythonITD/mis/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 8968, in select
    return self.lazy().select(*exprs, **named_exprs).collect(_eager=True)
  File "/sas/pythonITD/mis/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2032, in collect
    return wrap_df(ldf.collect(callback))
polars.exceptions.InvalidOperationError: conversion from `str` to `date` failed in column 'REPORT_DATE' for 336 out of 336 values: ["01/12/2011", "01/12/2011", … "31/10/2025"]

You might want to try:
- setting `strict=False` to set values that cannot be converted to `null`
- using `str.strptime`, `str.to_date`, or `str.to_datetime` and providing a format string
