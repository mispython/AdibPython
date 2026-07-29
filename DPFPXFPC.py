========== START JOB EIBWHP02 ==========
[INFO] REPTDATE=2026-07-28 -> {'NOWK': '4', 'NOWK1': '3', 'REPTMON': '07', 'REPTMON1': '07', 'REPTYEAR': '2026', 'REPTDAY': '28', 'RDATE': '28/07/26', 'SDATE': '23/07/26'}
[SHR] Validated input dataset: loan073.sas7bdat
[SHR] Validated input dataset: loan074.sas7bdat
[SHR] Validated input dataset: uloan074.sas7bdat
[READ] loan073.sas7bdat: 623910 rows, 11 cols (usecols=['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'NOTETERM', 'BALANCE', 'PRODCD', 'CUSTCD', 'AMTIND', 'ISSDTE', 'BRANCH'], num_processes=4)
[DEBUG] alw1 (prior period) raw rows: 623910; PRODUCT sample: [103.0, 142.0, 187.0, 192.0, 183.0, 113.0, 112.0, 152.0, 154.0, 135.0]
[DEBUG] alw1 after PRODUCT filter [131, 132, 720, 725]: 4295 rows
[READ] loan074.sas7bdat: 2636878 rows, 13 cols (usecols=['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'NOTETERM', 'EARNTERM', 'BALANCE', 'APPRDATE', 'APPRLIM2', 'PRODCD', 'CUSTCD', 'AMTIND', 'ISSDTE', 'BRANCH'], num_processes=4)
[WARN] loan074.sas7bdat: requested column(s) not found and silently dropped by pyreadstat: ['ISSDTE']. Check for a naming mismatch (case, abbreviation, etc). Actual columns present: ['ACCTNO', 'NOTENO', 'NOTETERM', 'EARNTERM', 'APPRDATE', 'BALANCE', 'AMTIND', 'APPRLIM2', 'CUSTCD', 'PRODCD', 'PRODUCT', 'SECTORCD', 'BRANCH']
[DEBUG] alw0 (current period) raw rows: 2636878; PRODUCT sample: [5.0, 247.0, 212.0, 600.0, 609.0, 210.0, 205.0, 993.0, 634.0, 248.0]
[DEBUG] alw0 after PRODUCT filter [131, 132, 720, 725]: 129003 rows
[DEBUG] merged rows: 129005; _merge counts: {'right_only': 124710, 'both': 4293, 'left_only': 2}
[DEBUG] merged SECTORCD sample (raw): ['9700', '6120', '5030', '6130', '8999', '7191', '7116', '5001', '1113', '3919']
[DEBUG] merged SECTORCD normalized sample: ['9700', '6120', '5030', '6130', '8999', '7191', '7116', '5001', '1113', '3919']
[DEBUG] alw after SECTA/SECTB expansion: 7652 rows (from 129005 pre-expansion rows)
[DEBUG] alw SECTCD value counts (top 10): {'6100': 4240, '3000': 854, '7000': 675, '5001': 566, '9000': 433, '6300': 259, '1000': 220, '8310': 202, '5004': 77, '2000': 50}
[READ] uloan074.sas7bdat: 25115 rows, 4 cols (usecols=['SECTORCD', 'AMTIND', 'CUSTCD', 'BRANCH'], num_processes=4)
[DEBUG] uloan raw rows: 25115; SECTORCD sample: ['8310', '9700', '6120', '6310', '8999', '3911', '6110', '3250', '3811', '5001']
[DEBUG] ualw after SECTA/SECTB expansion: 6655 rows (from 25115 pre-expansion rows)
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP02.py:436: FutureWarning: The behavior of DataFrame concatenation with empty or all-NA entries is deprecated. In a future version, this will no longer exclude empty or all-NA columns when determining the result dtypes. To retain the old behavior, exclude the relevant entries before the concat operation.
  combined = pd.concat([alw, ualw], ignore_index=True)
[DEBUG] combined (alw+ualw) rows entering summarize: 14307
[DEBUG] combined PRODCD sample: ['34111']
[DEBUG] combined CUSTCD sample: ['46', '44', '42', '41', '47', '62', '43', '49', '79', '59', '48', '61', '51', '63', '86']
[DEBUG] PRODCD prefix value counts (top 10): {'341': 7652, 'nan': 6655}
[DEBUG] rows after PRODCD prefix ('341', '342', '343', '344') + SECTCD!=0210 filter: 7652
[DEBUG] alwx (first summary) rows: 310
[DEBUG] alwx CUSTCD sample: ['41', '42', '44', '46', '43', '47', '48', '49', '51', '62', '59', '61', '63', '79']
[DEBUG] rows matching SMI_CUSTCD ['66', '67', '68', '69']: 0 of 310
[SYSOUT] Report written to spool: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWHP02/EIBWHP02_20260729_155909.txt
========== END JOB EIBWHP02 ==========
