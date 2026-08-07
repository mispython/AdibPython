Report Date: 31/07/26
Traceback (most recent call last):
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/indexes/base.py", line 3805, in get_loc
    return self._engine.get_loc(casted_key)
  File "index.pyx", line 167, in pandas._libs.index.IndexEngine.get_loc
  File "index.pyx", line 196, in pandas._libs.index.IndexEngine.get_loc
  File "pandas/_libs/hashtable_class_helper.pxi", line 7081, in pandas._libs.hashtable.PyObjectHashTable.get_item
  File "pandas/_libs/hashtable_class_helper.pxi", line 7089, in pandas._libs.hashtable.PyObjectHashTable.get_item
KeyError: 'CURCODE'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDOMYR.py", line 256, in <module>
    process_camv()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDOMYR.py", line 169, in process_camv
    df['CATEGORY'] = df.apply(classify, axis=1)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/frame.py", line 10374, in apply
    return op.apply().__finalize__(self, method="apply")
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/apply.py", line 916, in apply
    return self.apply_standard()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/apply.py", line 1063, in apply_standard
    results, res_index = self.apply_series_generator()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/apply.py", line 1081, in apply_series_generator
    results[i] = self.func(v, *self.args, **self.kwargs)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDOMYR.py", line 153, in classify
    if is_fy_product and row['CURCODE'] != 'MYR':
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/series.py", line 1121, in __getitem__
    return self._get_value(key)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/series.py", line 1237, in _get_value
    loc = self.index.get_loc(label)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/indexes/base.py", line 3812, in get_loc
    raise KeyError(key) from err
KeyError: 'CURCODE'

i also changed the timedelta(days=7) just for testing, to use the 3107 date of inputs. and the input path also i have removed the /omy. no need
