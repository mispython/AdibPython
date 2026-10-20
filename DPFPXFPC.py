EIMRESHI - HP Loan Summary & Detail Report
============================================================
============================================================
HP_PRODUCTS (from PBBLNFMT.HPD): [128, 130, 131, 132, 380, 381, 700, 705, 720, 725]
Report title products (SAS hardcoded): 128,130,380,381,700,705
Report Date: 30/09/2026
Week: 4  |  RDATE: 300926
LNNOTE file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m09.sas7bdat
LOANTEMP file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMREPOI/loantemp_iccd2.sas7bdat
============================================================

Reading loan data from SAS files...
  Reading loantemp.sas7bdat...
  LOANTEMP raw rows: 631,802
  LOANTEMP after filtering: 404,862 rows
  Reading LNNOTE (chunked, filtered as it streams)...
    ...scanned 8,125,473 raw rows total          
  LNNOTE after filtering: 1,685,338 rows
  ACCTNO dtype  -> lnnote: Float64, loantemp: Float64
  NOTENO dtype  -> lnnote: Float64, loantemp: Float64
  Merging data...
  HP Loans after merge: 400,895 accounts

Processing HP loans...
  WARNING: 400,895 of 400,895 rows had ISSUEDT that didn't parse as MMDDYYYY. Samples: ['24141.0', '23954.0', '24269.0', '23604.0', '22224.0']
  Processed: 400,895 HP loans

Creating account groups...
  HPLOAN1 (All): 400,895
  HPLOAN2 (NPL): 2,258
  HPLOAN3 (Restructured): 4,758
  HPLOAN4 (Restructured NPL): 287

Generating summary reports...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHI.py:482: DeprecationWarning: `pl.count()` is deprecated. Please use `pl.len()` instead.
  pl.count().alias('COUNT'),
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHI.py:486: DeprecationWarning: The argument `columns` for `DataFrame.pivot` is deprecated. It has been renamed to `on`.
  df_pivot = df_agg.pivot(
  Generated: EIMRESHI_SUMMARY_01_CREDIT_RISK_SCORE.txt
  Generated: EIMRESHI_SUMMARY_02_SOURCE_OF_BUSINESS.txt
  Generated: EIMRESHI_SUMMARY_03_MARGIN_OF_FINANCE.txt
  Generated: EIMRESHI_SUMMARY_04_LOAN_TERM.txt
  Generated: EIMRESHI_SUMMARY_05_AMT_FINANCE.txt
  Generated: EIMRESHI_SUMMARY_06_BY_STATE.txt
  Generated: EIMRESHI_SUMMARY_07_BY_MAKE_OF_VEHICLE.txt
  Generated: EIMRESHI_SUMMARY_08_BY_MAKE_OF_VEHICLE_=_OTHERS.txt
  Generated: EIMRESHI_SUMMARY_09_CREDIT_RISK_SCORE.txt
  Generated: EIMRESHI_SUMMARY_10_SOURCE_OF_BUSINESS.txt
  Generated: EIMRESHI_SUMMARY_11_MARGIN_OF_FINANCE.txt
  Generated: EIMRESHI_SUMMARY_12_LOAN_TERM.txt
  Generated: EIMRESHI_SUMMARY_13_AMT_FINANCE.txt
  Generated: EIMRESHI_SUMMARY_14_BY_STATE.txt
  Generated: EIMRESHI_SUMMARY_15_BY_MAKE_OF_VEHICLE.txt
  Generated: EIMRESHI_SUMMARY_16_BY_MAKE_OF_VEHICLE_=_OTHERS.txt
  Generated: EIMRESHI_SUMMARY_17_CREDIT_RISK_SCORE.txt
  Generated: EIMRESHI_SUMMARY_18_SOURCE_OF_BUSINESS.txt
  Generated: EIMRESHI_SUMMARY_19_MARGIN_OF_FINANCE.txt
  Generated: EIMRESHI_SUMMARY_20_LOAN_TERM.txt
  Generated: EIMRESHI_SUMMARY_21_AMT_FINANCE.txt
  Generated: EIMRESHI_SUMMARY_22_BY_STATE.txt
  Generated: EIMRESHI_SUMMARY_23_BY_MAKE_OF_VEHICLE.txt
  Generated: EIMRESHI_SUMMARY_24_BY_MAKE_OF_VEHICLE_=_OTHERS.txt
  Generated: EIMRESHI_SUMMARY_25_CREDIT_RISK_SCORE.txt
  Generated: EIMRESHI_SUMMARY_26_SOURCE_OF_BUSINESS.txt
  Generated: EIMRESHI_SUMMARY_27_MARGIN_OF_FINANCE.txt
  Generated: EIMRESHI_SUMMARY_28_LOAN_TERM.txt
  Generated: EIMRESHI_SUMMARY_29_AMT_FINANCE.txt
  Generated: EIMRESHI_SUMMARY_30_BY_STATE.txt
  Generated: EIMRESHI_SUMMARY_31_BY_MAKE_OF_VEHICLE.txt
  Generated: EIMRESHI_SUMMARY_32_BY_MAKE_OF_VEHICLE_=_OTHERS.txt
  Generated 32 summary reports

Generating detail report...
  Detail report: 2,258 NPL accounts
  Total balance: 72,020,047.84

============================================================
EIMRESHI Complete!
============================================================

Data Statistics:
  Total HP loans processed: 400,895

4 Account Groups:
  1. All HP accounts: 400,895
  2. NPL (>=3 months OR F/I/R): 2,258
  3. Restructured (NOTENO >= 98010): 4,758
  4. Restructured NPL: 287

Output Directory: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMREPOI
  - 32 summary reports
  - 1 detail report (NPL accounts)
You have mail in /var/spool/mail/sas_edw_dev
