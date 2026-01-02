PROCESSING DATE: 2026-01-01 (TODAY()-1)
ACCUMULATING WITH: 2025-12 (previous month)

================================================================================
PROCESSING WITH PREVIOUS MONTH ACCUMULATION
================================================================================

>>> 1. CHANNEL SUMMARY (EIBMCHNL)
Accumulating with 2025-12 data...
  Looking for file: /host/cis/parquet/year=2026/month=01/day=01/CIPHONET_ALL_SUMMARY.parquet
  File not found, checking if day directory exists...
  File not found at either location

>>> 2. CHANNEL UPDATE (EIBMCHN2)
Accumulating with 2025-12 data...
  Looking for update file: /host/cis/parquet/year=2026/month=01/day=01/CIPHONET_FULL_SUMMARY.parquet
  File not found, checking alternatives...
  Update file not found

>>> 3. OTC DETAIL (EIBMCHNL - MERGE)
Processing ALL 375 BCODE branches...
  BCODE total branches: 376
  Looking for OTC file: /host/cis/parquet/year=2026/month=01/day=01/CIPHONET_OTC_SUMMARY.parquet
  OTC file not found, checking alternatives...
  OTC file not found, creating zero-filled data

================================================================================
WRITING OUTPUT FILES
================================================================================
✓ OTC_DETAIL.parquet: /host/mis/parquet/crm/year=2026/month=01/OTC_DETAIL.parquet
  Columns: ['BRANCHNO', 'TOLPROMPT', 'TOLUPDATE']

>>> TRANSFERRING TO SAS DATASETS

1. Assigning SAS libraries...
  Assigning crm to /stgsrcsys/host/uat
  Assigning ctrl_crm to /sas/python/virt_edw/Data_Warehouse/SASTABLE

4. Transferring OTC_DETAIL...
  Dataset name: OTC_DETAIL_0126
  OTC data name: OTC_DETAIL_0126
  Control table: otc_detail_ctl
  Assigning SAS library OTCLIB to /host/mis/parquet/crm/year=2026/month=01
  Creating OTC_DETAIL_0126...
  DataFrame type: <class 'polars.dataframe.frame.DataFrame'>
  DataFrame columns: ['BRANCHNO', 'TOLPROMPT', 'TOLUPDATE']
  Pandas DataFrame shape: (376, 3)
  ✗ SAS file not found at: /host/mis/parquet/crm/year=2026/month=01/OTC_DETAIL_0126.sas7bdat
  Listing files in /host/mis/parquet/crm/year=2026/month=01:
    - OTC_DETAIL.parquet
    - otc_detail_0126.sas7bdat
  Direct write failed, trying set_data method...

  Transferring data to SAS...
    DataFrame type: <class 'polars.dataframe.frame.DataFrame'>
    DataFrame shape: (376, 3)
    Pandas DataFrame shape: (376, 3)
    Pandas columns: ['BRANCHNO', 'TOLPROMPT', 'TOLUPDATE']
    Writing to work.OTC_DETAIL_0126
    Getting metadata from ctrl_crm.otc_detail_ctl
    Metadata records: 0
    No metadata found, using simple data step
  ✓ OTC_DETAIL transferred via set_data

✓ SAS transfer operations completed

================================================================================
VERIFICATION
================================================================================

1. PROCESSING DETAILS:
   - Processing date: 2026-01-01
   - Previous month: 2025-12
   - Current month path: /host/mis/parquet/crm/year=2026/month=01
   - Previous month path: /host/mis/parquet/crm/year=2025/month=12

4. OTC_DETAIL:
   - Total branches: 376
   - First BRANCHNO: 1
   - Last BRANCHNO: 998

================================================================================
PROCESS COMPLETE
================================================================================
