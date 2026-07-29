[FMT] Loaded SECTCD: 0 entries
========== START JOB EIBWHP01 ==========
[DATE] Report: 28/07/26
[DATE] REPTMON=07, NOWK=4
[DATE] REPTMON1=07, NOWK1=3
[WARN] Using latest loan as current: loan064.sas7bdat
[WARN] Using same file for previous (no alternative found)

[READ] Loading files to parquet cache (if needed)...
[READ] Using cache: loan064.parquet
[READ] Using cache: loan064.parquet
[READ] Using cache: lnnote.parquet
  Current BNM: 623,910 rows
  Previous BNM: 623,910 rows
  LNNOTE: 6,232,608 rows
[PROCESS] Starting DuckDB processing...
[PROCESS] Filtering products and computing EFFAPR...
  Current BNM filtered: 4,295
  Previous BNM filtered: 4,295
[PROCESS] Computing EFFAPR for LNNOTE...
100% ▕██████████████████████████████████████▏ (00:00:03.10 elapsed)     
  Processing LNNOTE chunk 1/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 2/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 3/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 4/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 5/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 6/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 7/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 8/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 9/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 10/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 11/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 12/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 13/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 14/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 15/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 16/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 17/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 18/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 19/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 20/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 21/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 22/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 23/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 24/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 25/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 26/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 27/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 28/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 29/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 30/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 31/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  Processing LNNOTE chunk 32/32...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py:212: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['EFFAPR'] = df.apply(compute_effapr, axis=1)
  LNNOTE processed: 6,232,608
[PROCESS] Merging data...
  Merged rows: 4,295
[PROCESS] Computing DISBURSE/REPAID...
  After filtering zero DISBURSE: 0
[PROCESS] Expanding by SECTA/SECTB...
  Expanded rows: 0

[PROCESS] Summarising all customers...
  ALL: No data found
[PROCESS] Summarising SMI (CUSTCD 66-69)...
[JOB FAILED] 'CUSTCD'
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py", line 518, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py", line 493, in main
    smi_summary = summarise(expanded, "SMI", custcd_filter=SMI_CUSTCD)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py", line 386, in summarise
    expanded = expanded[expanded['CUSTCD'].isin(custcd_filter)].copy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/frame.py", line 4102, in __getitem__
    indexer = self.columns.get_loc(key)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/indexes/range.py", line 417, in get_loc
    raise KeyError(key)
KeyError: 'CUSTCD'
