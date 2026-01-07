Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/EIBMREPO_UAT.py", line 197, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/EIBMREPO_UAT.py", line 150, in main
    raw_df = parse_rpvdata()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/EIBMREPO_UAT.py", line 118, in parse_rpvdata
    df = df.with_columns(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10020, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 330, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2335, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.InvalidOperationError: conversion from `str` to `date` failed in column 'DATEWOFF' for 636 out of 776 values: ["", "", … ""]

You might want to try:
- setting `strict=False` to set values that cannot be converted to `null`
- using `str.strptime`, `str.to_date`, or `str.to_datetime` and providing a format string
