Connecting to SAS...
SAS Connection established. Subprocess id is 2643738

SAS session: Access Method         = STDIO
SAS Config name       = default
SAS Config file       = /sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sascfg.py
WORK Path             = /sas/saswork/SAS_work16D60028571A_svdwh004/
SAS Version           = 9.04.01M9P06052025
SASPy Version         = 5.102.0
Teach me SAS          = False
Batch                 = False
Results               = Pandas
SAS Session Encoding  = latin1
Python Encoding value = latin_1
SAS process Pid value = 2643738
SASsession started    = Fri Nov 13 18:49:16 2026


/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")

======================================================================
PHASE 1: DuckDB reads and enriches
======================================================================
  Report date: 13/11/2026 (Week 2)
  Weekly/Month-end: False
  Enriched view created  (0.9s)

======================================================================
PHASE 1A: Exporting DYDP to CSV via DuckDB
======================================================================
100% ▕██████████████████████████████████████▏ (00:00:53.20 elapsed)     
  ✓ CSV written: /tmp/DYDP_import.csv  (53.2s)

======================================================================
PHASE 1B: Computing summary aggregates in DuckDB
======================================================================
100% ▕██████████████████████████████████████▏ (00:00:04.00 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:04.41 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:05.31 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:04.22 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:02.39 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:03.08 elapsed)     
  ✓ All summaries computed  (26.9s)
    DDMV=0  DYIBU=322  DYMVNT=794  DYBRDP=586  DYDDCR=10  DYDPS=6  DYACE=7

======================================================================
PHASE 2: Writing SAS datasets
======================================================================

Importing DYDP from /tmp/DYDP_import.csv via PROC IMPORT...
  ✓ TEMP.DYDP created (0.4s)
  ⚠ [Errno 2] No such file or directory: '/tmp/DYDP_import.csv'
The libref specified is not assigned in this SAS Session.
  ✓ MIS.DYPOSN11  (1 rows)
The libref specified is not assigned in this SAS Session.
  ✓ MIS.DYMVNT11 (created)
The libref specified is not assigned in this SAS Session.
  ✓ MIS.DYIBU11 (created)
The libref specified is not assigned in this SAS Session.
  ✓ MIS.DYDDCR11 (created)
The libref specified is not assigned in this SAS Session.
  ✓ MIS.DYBRDP11 (created)
The libref specified is not assigned in this SAS Session.
  ✓ MIS.DYDPS11 (created)
The libref specified is not assigned in this SAS Session.
  ✓ MIS.DYACE11 (created)
  ⚠ MISQ.DDMV: No data

======================================================================
✓ EIBDDEPE Complete
======================================================================
SAS Connection terminated. Subprocess id was 2643738
