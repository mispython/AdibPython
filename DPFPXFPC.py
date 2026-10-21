========== START JOB EIBWHP02 ==========
[INFO] REPTDATE=2026-10-15 -> {'NOWK': '2', 'NOWK1': '1', 'REPTMON': '10', 'REPTMON1': '10', 'REPTYEAR': '2026', 'REPTDAY': '15', 'RDATE': '15/10/26', 'SDATE': '09/10/26'}
[SHR] Validated input dataset: loan101.sas7bdat
[SHR] Validated input dataset: loan102.sas7bdat
[SHR] Validated input dataset: uloan102.sas7bdat
[READ] loan101.sas7bdat: 2658344 rows, 10 cols (usecols=['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'NOTETERM', 'BALANCE', 'PRODCD', 'CUSTCD', 'AMTIND', 'ISSDTE', 'BRANCH'], num_processes=4)
[WARN] loan101.sas7bdat: requested column(s) not found and silently dropped by pyreadstat: ['ISSDTE']. Check for a naming mismatch (case, abbreviation, etc). Actual columns present: ['ACCTNO', 'NOTENO', 'NOTETERM', 'BALANCE', 'AMTIND', 'CUSTCD', 'PRODCD', 'PRODUCT', 'SECTORCD', 'BRANCH']
[READ] loan102.sas7bdat: 2664794 rows, 13 cols (usecols=['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'NOTETERM', 'EARNTERM', 'BALANCE', 'APPRDATE', 'APPRLIM2', 'PRODCD', 'CUSTCD', 'AMTIND', 'ISSDTE', 'BRANCH'], num_processes=4)
[WARN] loan102.sas7bdat: requested column(s) not found and silently dropped by pyreadstat: ['ISSDTE']. Check for a naming mismatch (case, abbreviation, etc). Actual columns present: ['ACCTNO', 'NOTENO', 'NOTETERM', 'EARNTERM', 'APPRDATE', 'BALANCE', 'AMTIND', 'APPRLIM2', 'CUSTCD', 'PRODCD', 'PRODUCT', 'SECTORCD', 'BRANCH']
[WARN] Column 'ISSDTE' not present in either period's data; filling with NaN.
[READ] uloan102.sas7bdat: 24781 rows, 2 cols (usecols=['SECTORCD', 'AMTIND', 'CUSTCD', 'BRANCH'], num_processes=4)
[WARN] uloan102.sas7bdat: requested column(s) not found and silently dropped by pyreadstat: ['SECTORCD', 'AMTIND']. Check for a naming mismatch (case, abbreviation, etc). Actual columns present: ['BRANCH', 'CUSTCD']
[JOB FAILED] 'SECTORCD'
Traceback (most recent call last):
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/indexes/base.py", line 3805, in get_loc
    return self._engine.get_loc(casted_key)
  File "index.pyx", line 167, in pandas._libs.index.IndexEngine.get_loc
  File "index.pyx", line 196, in pandas._libs.index.IndexEngine.get_loc
  File "pandas/_libs/hashtable_class_helper.pxi", line 7081, in pandas._libs.hashtable.PyObjectHashTable.get_item
  File "pandas/_libs/hashtable_class_helper.pxi", line 7089, in pandas._libs.hashtable.PyObjectHashTable.get_item
KeyError: 'SECTORCD'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP02.py", line 503, in <module>
    run_job()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP02.py", line 486, in run_job
    ualw = build_ualw(uloan_curr_path)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP02.py", line 351, in build_ualw
    f"SECTORCD sample: {df['SECTORCD'].dropna().unique()[:10].tolist()}")
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/frame.py", line 4102, in __getitem__
    indexer = self.columns.get_loc(key)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/indexes/base.py", line 3812, in get_loc
    raise KeyError(key) from err
KeyError: 'SECTORCD'
