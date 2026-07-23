IS/XMIS/EIMRESHI.py
EIMRESHI - HP Loan Summary & Detail Report
============================================================
============================================================
Report Date: 22/07/2026
Week: 3
RDATE: 220726
============================================================

Reading loan data from SAS files...
  Reading loantemp.sas7bdat...
  LOANTEMP raw rows: 663,747
  LOANTEMP after filtering: 387,612 rows
  Reading lnnote.sas7bdat (chunked, filtered as it streams)...
    ...scanned 1,792,047 raw rows total          
  LNNOTE after filtering: 386,949 rows
  ACCTNO dtype  -> lnnote: Float64, loantemp: Float64
  NOTENO dtype  -> lnnote: Float64, loantemp: Float64
  ACCTNO sample -> lnnote: [2902719229.0, 2903373211.0, 2904870426.0], loantemp: [8709015015.0, 8826241403.0, 8862941712.0]
  NOTENO sample -> lnnote: [90010.0, 90010.0, 90010.0], loantemp: [90010.0, 90010.0, 94010.0]
  Merging data...
  HP Loans after merge: 386,249 accounts

Processing HP loans...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHI.py", line 301, in <module>
    df_hploan = df_hploan.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.InvalidOperationError: conversion from `str` to `datetime[μs]` failed in column 'ISSUEDT' for 275861 out of 386249 values: ["80520152", "12320130", … "81520162"]

You might want to try:
- setting `strict=False` to set values that cannot be converted to `null`
- using `str.strptime`, `str.to_date`, or `str.to_datetime` and providing a format string
