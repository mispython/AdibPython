Traceback (most recent call last):
  File "/sas/pythonITD/mis/job/LOAN/EIBMCHNL.py", line 261, in <module>
    combined_channel_update = pl.concat(
  File "/sas/pythonITD/mis/lib64/python3.9/site-packages/polars/functions/eager.py", line 184, in concat
    out = wrap_ldf(
  File "/sas/pythonITD/mis/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2032, in collect
    return wrap_df(ldf.collect(callback))
polars.exceptions.ComputeError: schema names differ: got DESC, expected LINE
