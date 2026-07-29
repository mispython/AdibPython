========== START JOB EIBWHP02 ==========
[INFO] REPTDATE=2026-07-28 -> {'NOWK': '4', 'NOWK1': '3', 'REPTMON': '07', 'REPTMON1': '07', 'REPTYEAR': '2026', 'REPTDAY': '28', 'RDATE': '28/07/26', 'SDATE': '23/07/26'}
[SHR] Validated input dataset: loan073.sas7bdat
[SHR] Validated input dataset: loan074.sas7bdat
[SHR] Validated input dataset: uloan074.sas7bdat
[READ] loan073.sas7bdat: 623910 rows, 11 cols (usecols=['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'NOTETERM', 'BALANCE', 'PRODCD', 'CUSTCD', 'AMTIND', 'ISSDTE', 'BRANCH'], num_processes=4)
[READ] loan074.sas7bdat: 2636878 rows, 13 cols (usecols=['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'NOTETERM', 'EARNTERM', 'BALANCE', 'APPRDATE', 'APPRLIM2', 'PRODCD', 'CUSTCD', 'AMTIND', 'ISSDTE', 'BRANCH'], num_processes=4)
[WARN] loan074.sas7bdat: requested column(s) not found and silently dropped by pyreadstat: ['ISSDTE']. Check for a naming mismatch (case, abbreviation, etc). Actual columns present: ['ACCTNO', 'NOTENO', 'NOTETERM', 'EARNTERM', 'APPRDATE', 'BALANCE', 'AMTIND', 'APPRLIM2', 'CUSTCD', 'PRODCD', 'PRODUCT', 'SECTORCD', 'BRANCH']
[READ] uloan074.sas7bdat: 25115 rows, 4 cols (usecols=['SECTORCD', 'AMTIND', 'CUSTCD', 'BRANCH'], num_processes=4)
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP02.py:404: FutureWarning: The behavior of DataFrame concatenation with empty or all-NA entries is deprecated. In a future version, this will no longer exclude empty or all-NA columns when determining the result dtypes. To retain the old behavior, exclude the relevant entries before the concat operation.
  combined = pd.concat([alw, ualw], ignore_index=True)
[SYSOUT] Report written to spool: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWHP02/EIBWHP02_20260729_153413.lst


what is extension .lst, why not use .txt
