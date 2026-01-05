MONTHLY JOB: Processing December 2025
Processing: 2025-12-31

============================================================
MONTHLY PROCESSING STARTED
============================================================

1. CHANNEL SUMMARY
  Columns: ['CHANNEL', 'PROMPT', 'UPDATED']
  Today's records: 3
  MONTH: DEC25
  First run - adding previous month data: 540 records
  After accumulation: 543 total records

2. OTC DETAIL
  OTC columns: ['CHANNEL', 'PROMPT', 'UPDATED']
  OTC records: 264
  OTC after conversion: 264 records

  FINAL OTC_DETAIL:
    Total records: 376
    TOLPROMPT sum: 22,115

3. CHANNEL UPDATE
  Columns: ['LINE', 'ATM', 'EBK', 'OTC', 'TOTAL', 'REPORT_DATE']
  Today's update records: 2
  First run - adding previous month data: 338 records
  After accumulation: 340 total records

============================================================
WRITING OUTPUT FILES
============================================================
✓ CHANNEL_SUM: 543 records → /host/mis/parquet/crm/year=2025/month=12/CHANNEL_SUM.parquet
✓ CHANNEL_UPDATE: 340 records → /host/mis/parquet/crm/year=2025/month=12/CHANNEL_UPDATE.parquet
✓ OTC_DETAIL: 376 records → /host/mis/parquet/crm/year=2025/month=12/OTC_DETAIL.parquet

============================================================
TRANSFERRING TO SAS
============================================================
  Creating crm.channel_sum...
  ✓ channel_sum created with unknown rows
  Creating crm.channel_update...
  ✓ channel_update created with unknown rows
  Creating crm.otc_detail_2512...
  ✓ otc_detail_2512 created with unknown rows

============================================================
PROCESS COMPLETED
============================================================
Date: 2025-12-31
Output: /host/mis/parquet/crm/year=2025/month=12

Records processed:
  Channel Summary: 543
  Channel Update:  340
  OTC Detail:      376
============================================================
