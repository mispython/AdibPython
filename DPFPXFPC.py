2026-07-29 15:55:22,021 - WARNING - PBBLNFMT.py not found, using default sector formats
2026-07-29 15:55:22,021 - INFO - ============================================================
2026-07-29 15:55:22,021 - INFO - START JOB: EIBWHP04
2026-07-29 15:55:22,021 - INFO - ============================================================
2026-07-29 15:55:22,021 - INFO - MEMORY [JOB_START]: 170.3 MB (0.3%)
2026-07-29 15:55:22,021 - INFO - REPTDATE: 2026-07-31
2026-07-29 15:55:22,021 - INFO - Period: Month=07/Week=4 vs Month=07/Week=3
2026-07-29 15:55:22,021 - INFO - Validating input files...
2026-07-29 15:55:22,021 - INFO -   LOAN_CURRENT: loan074.sas7bdat (3085.6 MB)
2026-07-29 15:55:22,021 - INFO -   LOAN_PREVIOUS: loan073.sas7bdat (696.4 MB)
2026-07-29 15:55:22,021 - INFO -   ULOAN: uloan074.sas7bdat (4.3 MB)
2026-07-29 15:55:22,021 - INFO - Starting data processing...
2026-07-29 15:55:22,022 - INFO - MEMORY [START]: 170.3 MB (0.3%)
2026-07-29 15:55:22,022 - INFO - ========================================
2026-07-29 15:55:22,022 - INFO - STEP 1: Reading previous period loan data...
2026-07-29 15:55:22,022 - INFO - Reading SAS7BDAT: loan073.sas7bdat
2026-07-29 15:55:25,481 - INFO - UTF-8 failed, trying latin1 encoding...
2026-07-29 15:55:38,443 - INFO - Read 623,910 rows, 159 columns
2026-07-29 15:55:38,732 - INFO - Kept 5 columns: ['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'BALANCE']
2026-07-29 15:55:38,783 - INFO - Previous period loans after filter: 4,295 rows
2026-07-29 15:55:40,220 - INFO - MEMORY [AFTER_PREVIOUS_LOAD]: 788.3 MB (1.2%)
2026-07-29 15:55:40,220 - INFO - ========================================
2026-07-29 15:55:40,220 - INFO - STEP 2: Processing current period loan data...
2026-07-29 15:55:40,220 - INFO - Reading SAS7BDAT in chunks: loan074.sas7bdat (chunk size: 10,000)
2026-07-29 15:55:40,399 - ERROR - JOB FAILED: UnicodeDecodeError: 'utf-8' codec can't decode byte 0xdd in position 15: invalid continuation byte
