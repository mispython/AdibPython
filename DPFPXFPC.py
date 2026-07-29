2026-07-29 16:04:34,590 - WARNING - PBBLNFMT.py not found, using default sector formats
2026-07-29 16:04:34,590 - INFO - ============================================================
2026-07-29 16:04:34,590 - INFO - START JOB: EIBWHP04
2026-07-29 16:04:34,591 - INFO - ============================================================
2026-07-29 16:04:34,591 - INFO - MEMORY [JOB_START]: 168.3 MB (0.3%)
2026-07-29 16:04:34,591 - INFO - REPTDATE: 2026-07-31
2026-07-29 16:04:34,591 - INFO - Period: Month=07/Week=4 vs Month=07/Week=3
2026-07-29 16:04:34,591 - INFO - Validating input files...
2026-07-29 16:04:34,591 - INFO -   LOAN_CURRENT: loan074.sas7bdat (3085.6 MB)
2026-07-29 16:04:34,591 - INFO -   LOAN_PREVIOUS: loan073.sas7bdat (696.4 MB)
2026-07-29 16:04:34,591 - INFO -   ULOAN: uloan074.sas7bdat (4.3 MB)
2026-07-29 16:04:34,591 - INFO - Starting data processing...
2026-07-29 16:04:34,591 - INFO - MEMORY [START]: 168.3 MB (0.3%)
2026-07-29 16:04:34,591 - INFO - ========================================
2026-07-29 16:04:34,591 - INFO - STEP 1: Reading previous period loan data...
2026-07-29 16:04:34,591 - INFO - Reading SAS7BDAT: loan073.sas7bdat
2026-07-29 16:04:34,591 - INFO - Trying encoding: utf-8
2026-07-29 16:04:37,895 - WARNING - Encoding utf-8 failed: 'utf-8' codec can't decode byte 0xd8 in position 11: invalid continuation byte
2026-07-29 16:04:37,902 - INFO - Trying encoding: latin1
2026-07-29 16:04:51,366 - INFO - Successfully read with encoding: latin1
2026-07-29 16:04:51,366 - INFO - Read 623,910 rows, 159 columns
2026-07-29 16:04:51,672 - INFO - Kept 5 columns: ['ACCTNO', 'NOTENO', 'SECTORCD', 'PRODUCT', 'BALANCE']
2026-07-29 16:04:51,720 - INFO - Previous period loans after filter: 4,295 rows
2026-07-29 16:04:52,557 - INFO - MEMORY [AFTER_PREVIOUS_LOAD]: 186.9 MB (0.3%)
2026-07-29 16:04:52,557 - INFO - ========================================
2026-07-29 16:04:52,557 - INFO - STEP 2: Processing current period loan data...
2026-07-29 16:04:52,557 - INFO - Current period file size: 3085.6 MB
2026-07-29 16:04:52,557 - INFO - Reading SAS7BDAT in chunks: loan074.sas7bdat (chunk size: 5,000)
2026-07-29 16:04:52,557 - INFO - Trying encoding: utf-8
2026-07-29 16:04:52,820 - INFO - Successfully read with encoding: utf-8
2026-07-29 16:04:52,820 - INFO - First chunk: 5,000 rows, 412 columns
2026-07-29 16:04:53,142 - ERROR - JOB FAILED: UnicodeDecodeError: 'utf-8' codec can't decode byte 0xdd in position 15: invalid continuation byte
You have mail in /var/spool/mail/sas_edw_dev
