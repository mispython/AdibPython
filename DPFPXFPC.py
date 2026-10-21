[WARN] PBBLNFMT error: object of type 'function' has no len()
========== START JOB EIBWHP01 ==========
[DATE] 30/09/26  REPTMON=09 NOWK=4
[WARN] Using latest loan: ln10226.sas7bdat
[WARN] Using latest loan: ln10226.sas7bdat
[INFO] Current : ln10226.sas7bdat
[INFO] Previous: ln10226.sas7bdat
[INFO] LNNOTE  : enrh_ln_note_m09.sas7bdat
[PROCESS] Reading BNM (current) – pruned columns...
  Current after filter: 172,499
[PROCESS] Reading BNM (previous) – pruned columns...
  Previous after filter: 172,499
[JOB FAILED] "['SECTORCD'] not in index"
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py", line 357, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py", line 340, in main
    expanded = process(curr_path, prev_path, lnnote_path)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py", line 175, in process
    curr[['ACCTNO', 'NOTENO', 'SECTORCD']],
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/frame.py", line 4108, in __getitem__
    indexer = self.columns._get_indexer_strict(key, "columns")[1]
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/indexes/base.py", line 6200, in _get_indexer_strict
    self._raise_if_missing(keyarr, indexer, axis_name)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/indexes/base.py", line 6252, in _raise_if_missing
    raise KeyError(f"{not_found} not in index")
KeyError: "['SECTORCD'] not in index"
