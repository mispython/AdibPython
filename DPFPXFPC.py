========== START JOB EIBWHP02 ==========
[INFO] REPTDATE=2026-07-28 -> {'NOWK': '4', 'NOWK1': '3', 'REPTMON': '07', 'REPTMON1': '07', 'REPTYEAR': '2026', 'REPTDAY': '28', 'RDATE': '28/07/26', 'SDATE': '23/07/26'}
[SHR] Validated input dataset: loan073.sas7bdat
[SHR] Validated input dataset: loan074.sas7bdat
[SHR] Validated input dataset: uloan074.sas7bdat
[WARN] SECTA_FORMAT / SECTB_FORMAT are both empty. Every row will be dropped by expand_sector_formats(). Fill in the real format mappings before trusting this report.
[READ] loan073.sas7bdat: 623910 rows, 11 cols (usecols=['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'NOTETERM', 'BALANCE', 'PRODCD', 'CUSTCD', 'AMTIND', 'ISSDTE', 'BRANCH'], num_processes=4)
[READ] loan074.sas7bdat: 2636878 rows, 13 cols (usecols=['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'NOTETERM', 'EARNTERM', 'BALANCE', 'APPRDATE', 'APPRLIM2', 'PRODCD', 'CUSTCD', 'AMTIND', 'ISSDTE', 'BRANCH'], num_processes=4)
[WARN] loan074.sas7bdat: requested column(s) not found and silently dropped by pyreadstat: ['ISSDTE']. Check for a naming mismatch (case, abbreviation, etc). Actual columns present: ['ACCTNO', 'NOTENO', 'NOTETERM', 'EARNTERM', 'APPRDATE', 'BALANCE', 'AMTIND', 'APPRLIM2', 'CUSTCD', 'PRODCD', 'PRODUCT', 'SECTORCD', 'BRANCH']
[READ] uloan074.sas7bdat: 25115 rows, 4 cols (usecols=['SECTORCD', 'DISBURSE', 'REPAID', 'APPRLIM2', 'AMTIND', 'CUSTCD', 'BRANCH'], num_processes=4)
[WARN] uloan074.sas7bdat: requested column(s) not found and silently dropped by pyreadstat: ['DISBURSE', 'REPAID', 'APPRLIM2']. Check for a naming mismatch (case, abbreviation, etc). Actual columns present: ['AMTIND', 'BRANCH', 'CUSTCD', 'SECTORCD']
[JOB FAILED] "['APPRLIM2'] not in index"
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP02.py", line 403, in <module>
    run_job()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP02.py", line 386, in run_job
    ualw = build_ualw(uloan_curr_path)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP02.py", line 293, in build_ualw
    return expanded[keep]
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/frame.py", line 4108, in __getitem__
    indexer = self.columns._get_indexer_strict(key, "columns")[1]
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/indexes/base.py", line 6200, in _get_indexer_strict
    self._raise_if_missing(keyarr, indexer, axis_name)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/indexes/base.py", line 6252, in _raise_if_missing
    raise KeyError(f"{not_found} not in index")
KeyError: "['APPRLIM2'] not in index"

i just obtained new uloan input
