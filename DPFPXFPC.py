========== START JOB EIBWHP02 ==========
[INFO] REPTDATE=2026-10-15 -> {'NOWK': '2', 'NOWK1': '1', 'REPTMON': '10', 'REPTMON1': '10', 'REPTYEAR': '26', 'REPTDAY': '15', 'RDATE': '15/10/26', 'SDATE': '09/10/26'}
[SHR] Validated input dataset: loan101.sas7bdat
[SHR] Validated input dataset: loan102.sas7bdat
[SHR] Validated input dataset: uloan10226.sas7bdat
[READ] loan101.sas7bdat: 2658344 rows, 10 cols (usecols=['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'NOTETERM', 'BALANCE', 'PRODCD', 'CUSTCD', 'AMTIND', 'BRANCH'], num_processes=4)
[DEBUG] alw1 (prior period) raw rows: 2658344; PRODUCT sample: [5.0, 247.0, 212.0, 600.0, 609.0, 210.0, 205.0, 993.0, 634.0, 248.0]
[DEBUG] alw1 after PRODUCT filter [131, 132, 720, 725]: 192327 rows
[READ] loan102.sas7bdat: 2664794 rows, 13 cols (usecols=['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'NOTETERM', 'EARNTERM', 'BALANCE', 'APPRDATE', 'APPRLIM2', 'PRODCD', 'CUSTCD', 'AMTIND', 'BRANCH'], num_processes=4)
[DEBUG] alw0 (current period) raw rows: 2664794; PRODUCT sample: [5.0, 247.0, 212.0, 600.0, 609.0, 210.0, 205.0, 993.0, 634.0, 248.0]
[DEBUG] alw0 after PRODUCT filter [131, 132, 720, 725]: 197720 rows
[DEBUG] merged rows: 197822; _merge counts: {'both': 192225, 'right_only': 5495, 'left_only': 102}
[DEBUG] merged SECTORCD sample (raw): ['9700', '6120', '5030', '6130', '6110', '3111', '8999', '7191', '7116', '5001']
[DEBUG] merged SECTORCD normalized sample: ['9700', '6120', '5030', '6130', '6110', '3111', '8999', '7191', '7116', '5001']
[DEBUG] expand_sector_formats(SECTORCD): normalized sample: ['9700', '6120', '5030', '6130', '6110', '3111', '8999', '7191', '7116', '5001']
[DEBUG]   $SECTA: 5599 of 197822 rows kept; SECTCD sample: ['3000', '7000', '5001', '1000', '9000', '2000', '5004', '5006', '5003', '5002']
[DEBUG]   $SECTB: 7860 of 197822 rows kept; SECTCD sample: ['6100', '6300', '8310', '8320']
[DEBUG] alw after SECTA/SECTB expansion: 13459 rows (from 197822 pre-expansion rows)
[DEBUG] alw SECTCD value counts (top 10): {'6100': 7062, '3000': 1516, '7000': 1432, '5001': 1061, '9000': 809, '6300': 474, '1000': 441, '8310': 323, '5004': 150, '2000': 80}
[READ] uloan10226.sas7bdat: 24781 rows, 3 cols (usecols=['SECTORCD_ORI', 'CUSTCD', 'BRANCH'], num_processes=4)
[DEBUG] uloan raw rows: 24781; SECTORCD_ORI sample: ['9700', '8914', '3825', '8110', '5006', '6120', '6310', '5005', '6130', '9203']
[DEBUG] expand_sector_formats(SECTORCD_ORI): normalized sample: ['9700', '8914', '3825', '8110', '5006', '6120', '6310', '5005', '6130', '9203']
[DEBUG]   $SECTA: 4242 of 24781 rows kept; SECTCD sample: ['3000', '5006', '5005', '9000', '9999', '5001', '2000', '5004', '7000', '1000']
[DEBUG]   $SECTB: 2262 of 24781 rows kept; SECTCD sample: ['6100', '6300', '8310']
[DEBUG] ualw after SECTA/SECTB expansion: 6504 rows (from 24781 pre-expansion rows)
[DEBUG] combined (alw+ualw) rows entering summarize: 19963
[DEBUG] combined PRODCD sample: ['34111']
[DEBUG] combined CUSTCD sample: ['42', '46', '44', '41', '47', '62', '43', '49', '79', '59', '48', '61', '51', '63', '86']
[DEBUG] PRODCD prefix value counts (top 10): {'341': 13459, '<NA': 6504}
[DEBUG] rows after PRODCD prefix ('341', '342', '343', '344') + SECTCD!=0210 filter: 13459
[DEBUG] alwx (first summary) rows: 395
[DEBUG] alwx CUSTCD sample: ['41', '42', '44', '46', '43', '47', '61', '62', '48', '49', '51', '59', '63', '79']
[DEBUG] rows matching SMI_CUSTCD ['41', '44', '48', '52']: 120 of 395
[DEBUG] final alwloan rows: 43; total DISBURSE: 17743988.4880772
[SYSOUT] Report written to spool: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWHP02/EIBWHP02_20261021_191322.txt
========== END JOB EIBWHP02 ==========
