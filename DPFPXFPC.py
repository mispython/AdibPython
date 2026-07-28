Processing Bad Debt Write-Off List (Conventional Banking)
Report Date: 27/07/2026
Week: 3, Previous Month: 06
Reading LNNOTE (optimized, single pass)...
Successfully read 6232608 records from LNNOTE
Step 1: Creating NPLA...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIFFTXT1.py", line 456, in <module>
    df_npla['branch'] = df_npla['ntbrch'].apply(
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
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIFFTXT1.py", line 457, in <lambda>
    lambda x: f"{get_branch_name(x)} {x:03d}"
ValueError: Unknown format code 'd' for object of type 'float'
