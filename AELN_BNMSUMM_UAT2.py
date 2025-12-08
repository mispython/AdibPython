============================================================
EIBMRNID - EXACT SAS OUTPUT
============================================================
TEST MODE: Using fixed report date for historical data
Report Date: 30/06/2017
Start Date: 01/06/2017

📂 Processing data...
  Original NID records: 580
  Merged with TRNCH

🔍 Checking for yield columns...
  Yield columns found: intplrate_bid, intplrate_offer
  Sample intplrate_bid: [4.06, 0.10999999999999999, 3.46]
  Sample intplrate_offer: [3.96, 0.09999999999999999, 3.36]
  Records with positive balance: 580
  Sample remmth values: [6.633333333333333, 4.4, 10.366666666666667, 13.6, 8.766666666666667]

💾 Saving processed data to Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/Job/Output/EIBMRNID_DATA.parquet
  Saved 580 records to Parquet

📊 Creating Table 1...
  Table 1 filtered records: 12
  Sample matdt/startdt values:
    matdt=2018-04-06, startdt=2017-01-06
    matdt=2018-04-12, startdt=2017-01-12
    matdt=2018-04-12, startdt=2017-01-12
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/EIBMRNID_NID.py", line 537, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/EIBMRNID_NID.py", line 389, in main
    tbl1 = tbl1_filtered.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10020, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 330, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2335, in collect
    return wrap_df(ldf.collect(engine, callback))
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/expr/expr.py", line 4351, in __call__
    result = self.function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/expr/expr.py", line 4733, in wrap_f
    return x.map_elements(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/series/series.py", line 5808, in map_elements
    self._s.map_elements(
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/EIBMRNID_NID.py", line 74, in apply_remfmta
    if val is None or pl.is_nan(val):
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/__init__.py", line 487, in __getattr__
    raise AttributeError(msg)
AttributeError: module 'polars' has no attribute 'is_nan'
