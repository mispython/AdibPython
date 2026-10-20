EIMRESHI - HP Loan Summary & Detail Report
============================================================
============================================================
HP_PRODUCTS (from PBBLNFMT.HPD): [128, 130, 131, 132, 380, 381, 700, 705, 720, 725]
Report title products (SAS hardcoded): 128,130,380,381,700,705
Report Date: 30/09/2026
Week: 4  |  RDATE: 300926
LNNOTE file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m09.sas7bdat
LOANTEMP file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMREPOI/loantemp_iccd2.sas7bdat
Output CCDTXT1: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMREPOI/EIMRESHI_SUMMARY_ALL_CCDTXT1.txt
Output CCDTXT2: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMREPOI/EIMRESHI_DETAIL_NPL_CCDTXT2.txt
============================================================

Reading loan data from SAS files...
  Reading loantemp.sas7bdat...
  LOANTEMP raw rows: 631,802
  LOANTEMP after filtering: 404,862 rows
  Reading LNNOTE (chunked, filtered as it streams)...
    WARNING: usecols NOT fully honoured. Extra columns decoded: []...
    ...scanned 8,125,473 raw rows total          
  LNNOTE after filtering: 1,685,338 rows
  ACCTNO dtype  -> lnnote: Float64, loantemp: Float64
  NOTENO dtype  -> lnnote: Float64, loantemp: Float64
  Merging data...
  HP Loans after merge: 400,895 accounts

Processing HP loans...
  ISSDTE sample (ISSUEDT -> parsed): [(24141.0, datetime.datetime(2026, 2, 4, 0, 0)), (23954.0, datetime.datetime(2025, 8, 1, 0, 0)), (24269.0, datetime.datetime(2026, 6, 12, 0, 0))]
  Processed: 400,895 HP loans

Creating account groups...
  HPLOAN1 (All): 400,895
  HPLOAN2 (NPL): 2,258
  HPLOAN3 (Restructured): 4,758
  HPLOAN4 (Restructured NPL): 287

Generating summary reports...
  Wrote section 01: CREDIT RISK SCORE - PRODUCT 128,130,380,381,700,705
  Wrote section 02: CREDIT RISK SCORE - NPL ACCOUNT
  Wrote section 03: CREDIT RISK SCORE - RESTRUCTURE ACCOUNT
  Wrote section 04: CREDIT RISK SCORE - RESTRUCTURE NPL ACCOUNT
  Wrote section 05: SOURCE OF BUSINESS - PRODUCT 128,130,380,381,700,705
  Wrote section 06: SOURCE OF BUSINESS - NPL ACCOUNT
  Wrote section 07: SOURCE OF BUSINESS - RESTRUCTURE ACCOUNT
  Wrote section 08: SOURCE OF BUSINESS - RESTRUCTURE NPL ACCOUNT
  Wrote section 09: MARGIN OF FINANCE - PRODUCT 128,130,380,381,700,705
  Wrote section 10: MARGIN OF FINANCE - NPL ACCOUNT
  Wrote section 11: MARGIN OF FINANCE - RESTRUCTURE ACCOUNT
  Wrote section 12: MARGIN OF FINANCE - RESTRUCTURE NPL ACCOUNT
  Wrote section 13: LOAN TERM - PRODUCT 128,130,380,381,700,705
  Wrote section 14: LOAN TERM - NPL ACCOUNT
  Wrote section 15: LOAN TERM - RESTRUCTURE ACCOUNT
  Wrote section 16: LOAN TERM - RESTRUCTURE NPL ACCOUNT
  Wrote section 17: AMT FINANCE - PRODUCT 128,130,380,381,700,705
  Wrote section 18: AMT FINANCE - NPL ACCOUNT
  Wrote section 19: AMT FINANCE - RESTRUCTURE ACCOUNT
  Wrote section 20: AMT FINANCE - RESTRUCTURE NPL ACCOUNT
  Wrote section 21: BY STATE - PRODUCT 128,130,380,381,700,705
  Wrote section 22: BY STATE - NPL ACCOUNT
  Wrote section 23: BY STATE - RESTRUCTURE ACCOUNT
  Wrote section 24: BY STATE - RESTRUCTURE NPL ACCOUNT
  Wrote section 25: BY MAKE OF VEHICLE - PRODUCT 128,130,380,381,700,705
  Wrote section 26: BY MAKE OF VEHICLE - NPL ACCOUNT
  Wrote section 27: BY MAKE OF VEHICLE - RESTRUCTURE ACCOUNT
  Wrote section 28: BY MAKE OF VEHICLE - RESTRUCTURE NPL ACCOUNT
  Wrote section 29: BY MAKE OF VEHICLE = OTHERS - PRODUCT 128,130,380,381,700,705
  Wrote section 30: BY MAKE OF VEHICLE = OTHERS - NPL ACCOUNT
  Wrote section 31: BY MAKE OF VEHICLE = OTHERS - RESTRUCTURE ACCOUNT
  Wrote section 32: BY MAKE OF VEHICLE = OTHERS - RESTRUCTURE NPL ACCOUNT
  Total summary sections written: 32
  CCDTXT1 file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMREPOI/EIMRESHI_SUMMARY_ALL_CCDTXT1.txt

Generating detail report...
  Detail report: 2,258 NPL accounts
  Total balance: 72,020,047.84
  CCDTXT2 file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMREPOI/EIMRESHI_DETAIL_NPL_CCDTXT2.txt

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
  - CCDTXT1 equivalent: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMREPOI/EIMRESHI_SUMMARY_ALL_CCDTXT1.txt
    (32 summary sections in one file)
  - CCDTXT2 equivalent: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMREPOI/EIMRESHI_DETAIL_NPL_CCDTXT2.txt
    (1 detail report)
