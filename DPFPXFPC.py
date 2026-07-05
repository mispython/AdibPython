2026-07-05 11:42:46,834 - INFO - ============================================================
2026-07-05 11:42:46,834 - INFO - Starting REPO Processing Pipeline
2026-07-05 11:42:46,834 - INFO - ============================================================
2026-07-05 11:42:46,834 - INFO - STEP 1: Extracting dates from input files
2026-07-05 11:42:46,834 - INFO - RPVBDATA TBDATE: 20251201
2026-07-05 11:42:46,834 - INFO - REPTDATE: 2025-11-30 (1125)
2026-07-05 11:42:46,834 - INFO - PREVDATE: 2025-10-31 (1025)
2026-07-05 11:42:46,834 - INFO - STEP 2: Processing SRSDATA dates
2026-07-05 11:42:46,834 - INFO - SRSDATA TBDATE: 20251101
2026-07-05 11:42:46,834 - INFO - SRSTDT: 1125
2026-07-05 11:42:46,834 - INFO - STEP 3: Validating dates
2026-07-05 11:42:46,834 - INFO - ✓ Date validation passed
2026-07-05 11:42:46,834 - INFO - STEP 4: Parsing RPVB data
2026-07-05 11:42:46,854 - INFO - Parsed 1207 records from /sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/input/RPVBDATA.txt
2026-07-05 11:42:46,861 - INFO - RPVB1: 1207 records
2026-07-05 11:42:46,861 - INFO - STEP 5: Applying filters
2026-07-05 11:42:46,870 - INFO - RPVB2 (ACCTSTA in D,S,R): 776 records
2026-07-05 11:42:46,871 - INFO - RPVB3 (with DATESTLD): 776 records
2026-07-05 11:42:46,871 - INFO - STEP 6: Creating REPO dataset
2026-07-05 11:42:46,871 - INFO - No previous REPO data found: No such file or directory (os error 2): /sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output/REPO/REPS_1025.parquet

This error occurred with the following context stack:
        [1] 'parquet scan'
        [2] 'sink'

2026-07-05 11:42:46,871 - INFO - REPO combined data: 776 records
2026-07-05 11:42:46,880 - INFO - Wrote 776 records to /sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output/REPO/REPS_1125.parquet
2026-07-05 11:42:46,880 - INFO - STEP 7: Creating REPOWH dataset
2026-07-05 11:42:46,881 - INFO - Removed 0 duplicate records
2026-07-05 11:42:46,885 - INFO - Wrote 776 records to /sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output/REPOWH/REPS_1125.parquet
2026-07-05 11:42:46,885 - INFO - ============================================================
2026-07-05 11:42:46,885 - INFO - PROCESSING COMPLETED SUCCESSFULLY
2026-07-05 11:42:46,885 - INFO - ============================================================
2026-07-05 11:42:46,885 - INFO - RPVB1: 1207 records
2026-07-05 11:42:46,885 - INFO - RPVB2: 776 records
2026-07-05 11:42:46,885 - INFO - RPVB3: 776 records
2026-07-05 11:42:46,885 - INFO - REPO: 776 records
2026-07-05 11:42:46,885 - INFO - REPOWH: 776 records
2026-07-05 11:42:46,885 - INFO - ============================================================
