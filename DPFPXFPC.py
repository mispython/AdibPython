SAS Connection established. Subprocess id is 185898

SAS Connection established successfully
Islamic Banking Statistics - 07/07/2026
Processing data for date: 2026-07-07

================================================================================
INSPECTING INPUT DATASETS
================================================================================
Error reading SAVING: read_sas7bdat() got an unexpected keyword argument 'rows_limit'
Error reading CURRENT: read_sas7bdat() got an unexpected keyword argument 'rows_limit'

================================================================================
SECTION 1: DAILY ISLAMIC BALANCE SUMMARY (DYIBU)
================================================================================
Loaded CURRENT: 162640 rows, 147 columns
Loaded SAVING: 2298576 rows, 88 columns
Using column 'OPENIND' for open indicator
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDISLM.py", line 107, in <module>
    current_filtered = current_df[~current_df[openind_col].isin(['B','C','P'])][['branch', 'product', 'curbal']].copy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/frame.py", line 4108, in __getitem__
    indexer = self.columns._get_indexer_strict(key, "columns")[1]
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/indexes/base.py", line 6200, in _get_indexer_strict
    self._raise_if_missing(keyarr, indexer, axis_name)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/indexes/base.py", line 6249, in _raise_if_missing
    raise KeyError(f"None of [{key}] are in the [{axis_name}]")
KeyError: "None of [Index(['branch', 'product', 'curbal'], dtype='object')] are in the [columns]"
SAS Connection terminated. Subprocess id was 185898
