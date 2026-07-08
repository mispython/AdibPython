Using SAS Config named: default
SAS Connection established. Subprocess id is 184654

Islamic Banking Statistics - 07/07/2026
Processing data for date: 2026-07-07
Traceback (most recent call last):
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/indexes/base.py", line 3805, in get_loc
    return self._engine.get_loc(casted_key)
  File "index.pyx", line 167, in pandas._libs.index.IndexEngine.get_loc
  File "index.pyx", line 196, in pandas._libs.index.IndexEngine.get_loc
  File "pandas/_libs/hashtable_class_helper.pxi", line 7081, in pandas._libs.hashtable.PyObjectHashTable.get_item
  File "pandas/_libs/hashtable_class_helper.pxi", line 7089, in pandas._libs.hashtable.PyObjectHashTable.get_item
KeyError: 'openind'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDISLM.py", line 47, in <module>
    current_filtered = current_df[~current_df['openind'].isin(['B','C','P'])][['branch', 'product', 'curbal']].copy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/frame.py", line 4102, in __getitem__
    indexer = self.columns.get_loc(key)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/indexes/base.py", line 3812, in get_loc
    raise KeyError(key) from err
KeyError: 'openind'
SAS Connection terminated. Subprocess id was 184654
