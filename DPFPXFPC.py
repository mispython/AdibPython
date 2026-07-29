========== START JOB EIBWHP02 ==========
[INFO] REPTDATE=2026-07-28 -> {'NOWK': '4', 'NOWK1': '3', 'REPTMON': '07', 'REPTMON1': '07', 'REPTYEAR': '2026', 'REPTDAY': '28', 'RDATE': '28/07/26', 'SDATE': '23/07/26'}
[SHR] Validated input dataset: loan073.sas7bdat
[SHR] Validated input dataset: loan074.sas7bdat
[SHR] Validated input dataset: uloan074.sas7bdat
[WARN] SECTA_FORMAT / SECTB_FORMAT are both empty. Every row will be dropped by expand_sector_formats(). Fill in the real format mappings before trusting this report.
[READ] loan073.sas7bdat: 623910 rows, 11 cols (usecols=['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'NOTETERM', 'BALANCE', 'PRODCD', 'CUSTCD', 'AMTIND', 'ISSDTE', 'BRANCH'], num_processes=4)
[READ] loan074.sas7bdat: 2636878 rows, 13 cols (usecols=['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'NOTETERM', 'EARNTERM', 'BALANCE', 'APPRDATE', 'APPRLIM2', 'PRODCD', 'CUSTCD', 'AMTIND', 'ISSDTE', 'BRANCH'], num_processes=4)
[WARN] loan074.sas7bdat: requested column(s) not found and silently dropped by pyreadstat: ['ISSDTE']. Check for a naming mismatch (case, abbreviation, etc). Actual columns present: ['ACCTNO', 'NOTENO', 'NOTETERM', 'EARNTERM', 'APPRDATE', 'BALANCE', 'AMTIND', 'APPRLIM2', 'CUSTCD', 'PRODCD', 'PRODUCT', 'SECTORCD', 'BRANCH']
[READ] uloan074.sas7bdat: 136591 rows, 1 cols (usecols=['SECTORCD', 'DISBURSE', 'REPAID', 'APPRLIM2', 'AMTIND', 'CUSTCD', 'BRANCH'], num_processes=4)
[WARN] uloan074.sas7bdat: requested column(s) not found and silently dropped by pyreadstat: ['SECTORCD', 'DISBURSE', 'REPAID', 'APPRLIM2', 'AMTIND', 'CUSTCD']. Check for a naming mismatch (case, abbreviation, etc). Actual columns present: ['BRANCH']
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
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP02.py", line 403, in <module>
    run_job()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP02.py", line 386, in run_job
    ualw = build_ualw(uloan_curr_path)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP02.py", line 290, in build_ualw
    expanded = expand_sector_formats(df, sector_col="SECTORCD")
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP02.py", line 192, in expand_sector_formats
    tmp["SECTCD"] = tmp[sector_col].astype(str).map(fmt)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/frame.py", line 4102, in __getitem__
    indexer = self.columns.get_loc(key)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/indexes/base.py", line 3812, in get_loc
    raise KeyError(key) from err
KeyError: 'SECTORCD'
