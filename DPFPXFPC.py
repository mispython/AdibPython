Reading input files...
  Read imast: 1697 rows
  Read imast2: 2246 rows
  Read icred: 10414 rows
  Read isuba: 61339 rows
  Read iprov: 87 rows
  Read iamsubacc: 0 rows
  Read ibtrad: 3418 rows
  Read ibtdtl: 3418 rows
  Read lnacct: 1205962 rows

Processing MAST...
MAST processed: 1697 rows

Processing MAST2...
MAST2 processed

Processing CRED...
CRED processed: 3419 rows

Processing BNM Trade data...
BNM Trade data processed

Processing SUBA...
SUBA processed: 61339 rows (SUBA9: 1184, SUBA_MAIN: 10411)

Processing ACCT...
ACCT processed: 1644 rows

Processing BTR2...
BTR2 processed: 10411 rows

Processing SUBCR...
SUBCR processed: 895 rows

Creating final SUBA...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIWBTCR_ISLAMIC_WEEKLY_BANKTRADE_CCR.py", line 702, in <module>
    suba_final = suba_final.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.InvalidOperationError: `to_string` operation not supported for dtype `f64`
