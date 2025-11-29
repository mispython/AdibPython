Traceback (most recent call last):
  File "/sas/pythonITD/mis/job/LOAN/EIBMCHNL.py", line 112, in <module>
    print(f"  Date range: {last_month_channel['MONTH'].unique().to_list()}")
  File "/sas/pythonITD/mis/lib64/python3.9/site-packages/polars/series/utils.py", line 107, in wrapper
    return s.to_frame().select_seq(f(*args, **kwargs)).to_series()
  File "/sas/pythonITD/mis/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 8993, in select_seq
    return self.lazy().select_seq(*exprs, **named_exprs).collect(_eager=True)
  File "/sas/pythonITD/mis/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2032, in collect
    return wrap_df(ldf.collect(callback))
polars.exceptions.InvalidOperationError: `unique` operation not supported for dtype `object`
