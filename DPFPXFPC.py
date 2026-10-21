========== START JOB EIBWHP02 ==========
[INFO] REPTDATE=2026-10-15 -> {'NOWK': '2', 'NOWK1': '1', 'REPTMON': '10', 'REPTMON1': '10', 'REPTYEAR': '2026', 'REPTDAY': '15', 'RDATE': '15/10/26', 'SDATE': '09/10/26'}
[SHR] Validated input dataset: loan101.sas7bdat
[SHR] Validated input dataset: loan102.sas7bdat
[SHR] Validated input dataset: uloan102.sas7bdat
[READ] loan101.sas7bdat: 2658344 rows, 10 cols (usecols=['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'NOTETERM', 'BALANCE', 'PRODCD', 'CUSTCD', 'AMTIND', 'BRANCH'], num_processes=4)
[READ] loan102.sas7bdat: 2664794 rows, 13 cols (usecols=['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'NOTETERM', 'EARNTERM', 'BALANCE', 'APPRDATE', 'APPRLIM2', 'PRODCD', 'CUSTCD', 'AMTIND', 'BRANCH'], num_processes=4)
[READ] uloan102.sas7bdat: 24781 rows, 3 cols (usecols=['SECTORCD_ORI', 'CUSTCD', 'BRANCH'], num_processes=4)
[SYSOUT] Report written to spool: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWHP02/EIBWHP02_20261021_190453.txt
========== END JOB EIBWHP02 ==========
You have mail in /var/spool/mail/sas_edw_dev
