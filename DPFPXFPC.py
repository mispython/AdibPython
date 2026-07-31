Global variables: {'NOWK': '4', 'NOWK1': '3', 'REPTMON': '07', 'REPTMON1': '07', 'REPTYEAR': '2026', 'REPTDAY': '30', 'RDATE': '300726', 'SDATE': '230726'}
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMBRAS.py", line 136, in <module>
    lnnote_pbb = read_lnnote(input_pbb_path / "lnnote.sas7bdat", reptmon_int, reptyear_int)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMBRAS.py", line 117, in read_lnnote
    filtered = filter_lnnote_chunk(pdf_chunk, reptmon, reptyear)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMBRAS.py", line 79, in filter_lnnote_chunk
    df = df.with_columns(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.InvalidOperationError: conversion from `str` to `date` failed in column 'ISSUEDT' for 369391 out of 500000 values: ["12420000", "71019991", … "11920040"]

You might want to try:
- setting `strict=False` to set values that cannot be converted to `null`
- using `str.strptime`, `str.to_date`, or `str.to_datetime` and providing a format string
