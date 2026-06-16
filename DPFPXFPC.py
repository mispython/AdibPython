Report Date: 2026-06-15
Previous Month: 05
Next Month Start: 2026-07-01
Loaded Islamic BTBASE file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/ibtbase05.sas7bdat
Islamic BTBASE columns: ['ACCTNO', 'TRANSREF', 'PREOUTSTD']
Islamic BTBASE rows: 14
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIID2CBT.py", line 81, in <module>
    df_btdtl = df_btdtl[['BRANCH', 'ACCTNO', 'TRANSREF', 'PRODTYPE', 'OUTSTAND', 'MATDATE', 'FACILITY', 'RETAILID']].copy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/frame.py", line 4108, in __getitem__
    indexer = self.columns._get_indexer_strict(key, "columns")[1]
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/indexes/base.py", line 6200, in _get_indexer_strict
    self._raise_if_missing(keyarr, indexer, axis_name)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/indexes/base.py", line 6252, in _raise_if_missing
    raise KeyError(f"{not_found} not in index")
KeyError: "['PRODTYPE'] not in index"
