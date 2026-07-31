EIMRESHP - HP Loan Summary & Detail Report
============================================================

Determining report date...
Report Date: 30/07/2026
Week: 4
============================================================

Reading LOANTEMP.sas7bdat...
  LOANTEMP records: 387,612

Reading LNNOTE.sas7bdat...
  LNNOTE records: 386,949

Merging loan data...
  Merged HP Loans: 386,249 accounts

Processing HP loans...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py", line 150, in <module>
    df_hploan = df_hploan.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.SchemaError: invalid series dtype: expected `String`, got `duration[ms]` for series with name `ISSUEDT`
