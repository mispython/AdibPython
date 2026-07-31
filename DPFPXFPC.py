EIMRESHP - HP Loan Summary & Detail Report
============================================================

Determining report date...
Report Date: 30/07/2026
Week: 4
============================================================

Reading LOANTEMP.sas7bdat...
  LOANTEMP records: 387,612

Reading LNNOTE.sas7bdat...
  LNNOTE records: 386,949

Merging loan data...
  Merged HP Loans: 386,249 accounts

Processing HP loans...
  Step 1 complete - Basic calculations done
  Step 2 complete - Categorizations done
  Step 3 complete - Vehicle classifications done
  Step 4 complete - Arrears calculation done
  Processed: 386,249 HP loans

Creating account groups...
  HPLOAN1 (All): 386,249
  HPLOAN2 (NPL): 1,824
  HPLOAN3 (Restructured): 4,305
  HPLOAN4 (Restructured NPL): 209

Generating summary reports...
  Generating report 1/36: CREDIT RISK SCORE - PRODUCT 128,130,380,381,700,705...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 1: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(505, 1)
  Generating report 2/36: CREDIT RISK SCORE - NPL ACCOUNT...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 2: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(180, 1)
  Generating report 3/36: CREDIT RISK SCORE - RESTRUCTURE ACCOUNT...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 3: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(223, 1)
  Generating report 4/36: CREDIT RISK SCORE - RESTRUCTURE NPL ACCOUNT...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 4: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(75, 1)
  Generating report 5/36: SOURCE OF BUSINESS - PRODUCT 128,130,380,381,700,705...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 5: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(157, 1)
  Generating report 6/36: SOURCE OF BUSINESS - NPL ACCOUNT...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 6: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(68, 1)
  Generating report 7/36: SOURCE OF BUSINESS - RESTRUCTURE ACCOUNT...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 7: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(71, 1)
  Generating report 8/36: SOURCE OF BUSINESS - RESTRUCTURE NPL ACCOUNT...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 8: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(37, 1)
  Generating report 9/36: MARGIN OF FINANCE - PRODUCT 128,130,380,381,700,705...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 9: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(412, 1)
  Generating report 10/36: MARGIN OF FINANCE - NPL ACCOUNT...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 10: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(194, 1)
  Generating report 11/36: MARGIN OF FINANCE - RESTRUCTURE ACCOUNT...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 11: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(127, 1)
  Generating report 12/36: MARGIN OF FINANCE - RESTRUCTURE NPL ACCOUNT...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 12: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(58, 1)
  Generating report 13/36: LOAN TERM - PRODUCT 128,130,380,381,700,705...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 13: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(503, 1)
  Generating report 14/36: LOAN TERM - NPL ACCOUNT...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 14: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(211, 1)
  Generating report 15/36: LOAN TERM - RESTRUCTURE ACCOUNT...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 15: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(292, 1)
  Generating report 16/36: LOAN TERM - RESTRUCTURE NPL ACCOUNT...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 16: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(112, 1)
  Generating report 17/36: AMT FINANCE - PRODUCT 128,130,380,381,700,705...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 17: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(673, 1)
  Generating report 18/36: AMT FINANCE - NPL ACCOUNT...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 18: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(285, 1)
  Generating report 19/36: AMT FINANCE - RESTRUCTURE ACCOUNT...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 19: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(324, 1)
  Generating report 20/36: AMT FINANCE - RESTRUCTURE NPL ACCOUNT...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 20: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(121, 1)
  Generating report 21/36: BY STATE - PRODUCT 128,130,380,381,700,705...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 21: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(378, 1)
  Generating report 22/36: BY STATE - NPL ACCOUNT...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 22: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(115, 1)
  Generating report 23/36: BY STATE - RESTRUCTURE ACCOUNT...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 23: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(179, 1)
  Generating report 24/36: BY STATE - RESTRUCTURE NPL ACCOUNT...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 24: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(61, 1)
  Generating report 25/36: BY MAKE OF VEHICLE - PRODUCT 128,130,380,381,700,705...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 25: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(472, 1)
  Generating report 26/36: BY MAKE OF VEHICLE - NPL ACCOUNT...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 26: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(175, 1)
  Generating report 27/36: BY MAKE OF VEHICLE - RESTRUCTURE ACCOUNT...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 27: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(217, 1)
  Generating report 28/36: BY MAKE OF VEHICLE - RESTRUCTURE NPL ACCOUNT...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 28: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(76, 1)
  Generating report 29/36: BY MAKE OF VEHICLE = OTHERS - PRODUCT 128,130,380,381,700,705...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 29: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(231, 1)
  Generating report 30/36: BY MAKE OF VEHICLE = OTHERS - NPL ACCOUNT...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 30: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(87, 1)
  Generating report 31/36: BY MAKE OF VEHICLE = OTHERS - RESTRUCTURE ACCOUNT...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 31: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(100, 1)
  Generating report 32/36: BY MAKE OF VEHICLE = OTHERS - RESTRUCTURE NPL ACCOUNT...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHP.py:340: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  df_pivot = df_agg.pivot(
    Warning: Failed to generate report 32: can only call `.item()` without "row" or "column" values if the DataFrame has a single element; shape=(50, 1)
  Generated 0 summary reports

Generating detail report...
  Detail report: 1,824 NPL accounts
  Total balance: RM 56,223,885.12

============================================================
EIMRESHP Complete!
============================================================

Outputs:
  - 0 summary reports (by category)
  - 1 detail report (NPL accounts)

HP Products: [128, 130, 380, 381, 700, 705]

4 Account Groups:
  1. All HP accounts: 386,249
  2. NPL (>=3 months OR F/I/R): 1,824
  3. Restructured (NOTENO >= 98010): 4,305
  4. Restructured NPL: 209

Report Categories:
  1. Credit Risk Score
  2. Source of Business
  3. Margin of Finance
  4. Loan Term
  5. Amount Financed
  6. By State
  7. By Make of Vehicle
  8. Make = OTHERS
