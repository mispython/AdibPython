MONTHLY JOB: Processing LAST DAY of previous month
PROCESSING DATE: 2025-12-31 (LAST DAY OF PREVIOUS MONTH)
ACCUMULATING WITH: 2025-11 (month before processing month)
OUTPUT PATH: year=2025/month=12

================================================================================
MONTHLY JOB: PROCESSING December 2025 DATA
================================================================================

>>> 1. CHANNEL SUMMARY (EIBMCHNL)
Accumulating with 2025-11 data...
  Looking for: /host/cis/parquet/year=2025/month=12/day=31/CIPHONET_ALL_SUMMARY.parquet
  Columns: ['CHANNEL', 'PROMPT', 'UPDATED']
  Today's records: 3
  MONTH: DEC25
  Previous month data: 540 records
  Current month existing: 4323 records
  Removing existing DEC25 data...
  After removal: 4320
  Adding previous month: 540
  After accumulation: 4863 total records

>>> 2. CHANNEL UPDATE (EIBMCHN2)
Accumulating with 2025-11 data...
  Looking for: /host/cis/parquet/year=2025/month=12/day=31/CIPHONET_FULL_SUMMARY.parquet
  Columns: ['LINE', 'ATM', 'EBK', 'OTC', 'TOTAL', 'REPORT_DATE']
  Today's update records: 2
  Previous month data: 338 records
  Current month existing: 2706 records
  Removing existing 31/12/2025 data...
  After removal: 2704
  Adding previous month: 338
  After accumulation: 3044 total records

>>> 3. OTC DETAIL (EIBMCHNL - MERGE)
Processing ALL 375 BCODE branches...
  BCODE total branches: 376
  Looking for: /host/cis/parquet/year=2025/month=12/day=31/CIPHONET_OTC_SUMMARY.parquet
  OTC columns: ['CHANNEL', 'PROMPT', 'UPDATED']
  OTC records: 264
  OTC after conversion: 264 records

  FINAL OTC_DETAIL:
    Total records: 376 (should be 375)
    TOLPROMPT sum: 22,115

================================================================================
WRITING OUTPUT FILES
================================================================================
✓ CHANNEL_SUM.parquet: /host/mis/parquet/crm/year=2025/month=12/CHANNEL_SUM.parquet
  Total records: 4863
✓ CHANNEL_UPDATE.parquet: /host/mis/parquet/crm/year=2025/month=12/CHANNEL_UPDATE.parquet
  Total records: 3044
✓ OTC_DETAIL.parquet: /host/mis/parquet/crm/year=2025/month=12/OTC_DETAIL.parquet
  Total records: 376

DataFrame columns:
OTC Detail: ['BRANCHNO', 'TOLPROMPT', 'TOLUPDATE']
Channel Summary: ['CHANNEL', 'TOLPROMPT', 'TOLUPDATE', 'MONTH']
Channel Update: ['DESC', 'ATM', 'EBK', 'OTC', 'TOTAL', 'DATE']

================================================================================
TRANSFERRING DATA TO SAS
================================================================================

1. Assigning SAS libraries...
Libraries assigned successfully

2. Transferring data to SAS (simplified method)...
  Transferring channel_sum with 4863 records...
  ? crm.channel_sum created (unexpected result type: <class 'saspy.sasdata.SASdata'>)
  Transferring channel_update with 3044 records...
  ? crm.channel_update created (unexpected result type: <class 'saspy.sasdata.SASdata'>)
  Transferring otc_detail_2512 with 376 records...
  ? crm.otc_detail_2512 created (unexpected result type: <class 'saspy.sasdata.SASdata'>)

✓ SAS data transfer attempted (simplified method)

================================================================================
MONTHLY PROCESS COMPLETED
Processed: 2025-12-31 (Last day of December 2025)
================================================================================
