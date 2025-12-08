============================================================
EIBMRNID Report Generator
============================================================
Report Date: 30/11/2025
Start Date: 01/11/2025
NID File: /stgsrcsys/host/uat/rnidm09.sas7bdat

📂 Reading SAS files...
✅ Loaded 580 records
/sas/python/virt_edw/Data_Warehouse/MIS/Job/EIBMRNID_NID.py:161: MapWithoutReturnDtypeWarning: 'return_dtype' of function python_udf must be set

A later expression might fail because the output type is not known. Set return_dtype=pl.self_dtype() if the type is unchanged, or set the proper output data type.
  df = df.with_columns(
/sas/python/virt_edw/Data_Warehouse/MIS/Job/EIBMRNID_NID.py:161: MapWithoutReturnDtypeWarning: Calling `map_elements` without specifying `return_dtype` can lead to unpredictable results. Specify `return_dtype` to silence this warning.
  df = df.with_columns(

❌ ERROR: TypeError: '<=' not supported between instances of 'float' and 'datetime.date'
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/EIBMRNID_NID.py", line 61, in main
    result = process_eibmrnid(NID_FILE, trnch_path, OUTPUT_DIR, reptdate)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/EIBMRNID_NID.py", line 161, in process_eibmrnid
    df = df.with_columns(
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
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/EIBMRNID_NID.py", line 140, in calc_remmth
    if not matdt or matdt <= reptdate: return None
TypeError: '<=' not supported between instances of 'float' and 'datetime.date'
You have mail in /var/spool/mail/sas_edw_dev
