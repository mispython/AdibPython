BATCH MODE =  D
Daily
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/BTRADE/MIS_BILLSTRAN_UAT.py", line 40, in <module>
    pq_bill["EXPRDATE"] = pq_bill["EXPRDATE"].apply(yyyymmdd_to_sas_date)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/series.py", line 4917, in apply
    return SeriesApply(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/apply.py", line 1427, in apply
    return self.apply_standard()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/apply.py", line 1507, in apply_standard
    mapped = obj._map_values(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/base.py", line 921, in _map_values
    return algorithms.map_array(arr, mapper, na_action=na_action, convert=convert)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/algorithms.py", line 1743, in map_array
    return lib.map_infer(values, mapper, convert=convert)
  File "lib.pyx", line 2972, in pandas._libs.lib.map_infer
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/BTRADE/MIS_BILLSTRAN_UAT.py", line 35, in yyyymmdd_to_sas_date
    date_obj = pd.to_datetime(date_str, format='%Y%m%d')
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/tools/datetimes.py", line 1101, in to_datetime
    result = convert_listlike(np.array([arg]), format)[0]
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/tools/datetimes.py", line 433, in _convert_listlike_datetimes
    return _array_strptime_with_fallback(arg, name, utc, format, exact, errors)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/tools/datetimes.py", line 467, in _array_strptime_with_fallback
    result, tz_out = array_strptime(arg, fmt, exact=exact, errors=errors, utc=utc)
  File "strptime.pyx", line 501, in pandas._libs.tslibs.strptime.array_strptime
  File "strptime.pyx", line 451, in pandas._libs.tslibs.strptime.array_strptime
  File "strptime.pyx", line 583, in pandas._libs.tslibs.strptime._parse_with_format
ValueError: time data "0" doesn't match format "%Y%m%d", at position 0. You might want to try:
    - passing `format` if your strings have a consistent format;
    - passing `format='ISO8601'` if your strings are all ISO8601 but not necessarily in exactly the same format;
    - passing `format='mixed'`, and the format will be inferred for each element individually. You might want to use `dayfirst` alongside this.
