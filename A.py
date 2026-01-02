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
  Total records: 376

DataFrame columns:
OTC Detail: ['BRANCHNO', 'TOLPROMPT', 'TOLUPDATE']
Channel Summary: Empty
Channel Update: Empty

================================================================================
TRANSFERRING DATA TO SAS
================================================================================

1. Assigning SAS libraries...
Libraries assigned successfully

2. Transferring data to SAS...
  Transferring OTC DETAIL (376 records)...
Final table created : 
158  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
158! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
159  
160  
161              proc sql noprint;
162                   create table crm.otc_detail_2601 as
163                   select BRANCHNO, TOLPROMPT, TOLUPDATE from ctrl_crm.otc_detail_ctl(obs=0)
164                   union corr
165                   select BRANCHNO,
166   TOLPRO...

✓ SAS data transfer completed successfully!

================================================================================
PROCESS COMPLETED
================================================================================
