PROCESSING DATE: 2026-01-01 (TODAY()-1)
ACCUMULATING WITH: 2025-12 (previous month)

================================================================================
PROCESSING WITH PREVIOUS MONTH ACCUMULATION
================================================================================

>>> 1. CHANNEL SUMMARY (EIBMCHNL)
Accumulating with 2025-12 data...
  File not found: /host/cis/parquet/year=2026/month=01/day=01/CIPHONET_ALL_SUMMARY.parquet

>>> 2. CHANNEL UPDATE (EIBMCHN2)
Accumulating with 2025-12 data...
  File not found: /host/cis/parquet/year=2026/month=01/day=01/CIPHONET_FULL_SUMMARY.parquet

>>> 3. OTC DETAIL (EIBMCHNL - MERGE)
Processing ALL 375 BCODE branches...
  BCODE total branches: 376
  OTC file not found: /host/cis/parquet/year=2026/month=01/day=01/CIPHONET_OTC_SUMMARY.parquet

================================================================================
WRITING OUTPUT FILES
================================================================================
✓ OTC_DETAIL.parquet: /host/mis/parquet/crm/year=2026/month=01/OTC_DETAIL.parquet
['BRANCHNO', 'TOLPROMPT', 'TOLUPDATE']
['CHANNEL', 'TOLPROMPT', 'TOLUPDATE', 'MONTH']
['DESC', 'ATM', 'EBK', 'OTC', 'TOTAL', 'DATE']
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/CIS/MIS_CRM_CHANNEL_PRD_TEST.py", line 530, in <module>
    log3 = set_data(otc_detail, "crm", "ctrl_crm", otc_data , otc_ctl)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/CIS/MIS_CRM_CHANNEL_PRD_TEST.py", line 486, in set_data
    sas.df2sd(df,table=cur_data, libref='work')
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasbase.py", line 1577, in df2sd
    return self.dataframe2sasdata(df, table, libref, results, keep_outer_quotes, embedded_newlines, LF, CR, colsep, colrep,
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasbase.py", line 1690, in dataframe2sasdata
    charlens = self.df_char_lengths(df, encode_errors, char_lengths)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasbase.py", line 1476, in df_char_lengths
    if df.dtypes[name].kind in ('O','S','U','V'):
TypeError: list indices must be integers or slices, not str
