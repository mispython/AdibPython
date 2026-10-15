WARNING: expected to run on day 1 of the month, but today is day 15. Report date will be 2026-10-14.
Run day:     2026-10-15
Report Date: 31/07/26  (filename suffix: 0726)
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDOMYR.py", line 359, in <module>
    camv_files = process_camv()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDOMYR.py", line 233, in process_camv
    df['DDCUSTCD'] = df['CUSTCODE'].apply(ddcustcd_format)
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
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDOMYR.py", line 61, in ddcustcd_format
    return PBBDPFMT.ddcustcd_format(int(cust_code))
AttributeError: module 'PBBDPFMT' has no attribute 'ddcustcd_format'
