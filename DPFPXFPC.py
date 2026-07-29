DEBUG: Created/verified output directory: sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWHP04
2026-07-29 14:59:09,873 - INFO - [EIBWHP04.py:403] - ============================================================
2026-07-29 14:59:09,873 - INFO - [EIBWHP04.py:404] - Starting EIBWHP04 script execution
2026-07-29 14:59:09,873 - INFO - [EIBWHP04.py:405] - ============================================================
2026-07-29 14:59:09,873 - INFO - [EIBWHP04.py:331] - ========== START JOB EIBWHP04 ==========
2026-07-29 14:59:09,873 - DEBUG - [EIBWHP04.py:72] - ============================================================
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:73] - DEBUG INFORMATION:
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:74] - Python version: 3.9.25 (main, Nov 26 2025, 08:47:37) 
[GCC 8.5.0 20210514 (Red Hat 8.5.0-28)]
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:75] - Current working directory: /sas/python/virt_edw/Data_Warehouse/MIS
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:76] - Script location: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP04.py
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:77] - BASE_DIR: /sas/python/virt_edw/Data_Warehouse/MIS
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:78] - INPUT_DIR: /sas/python/virt_edw/Data_Warehouse/MIS/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWHP04
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:79] - OUTPUT_DIR: /sas/python/virt_edw/Data_Warehouse/MIS/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWHP04
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:80] - PREV_DATE: 2026-07-28 14:59:09.873530
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:81] - REPTMON: 202607, NOWK: 30
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:82] - REPTMON1: 202606, NOWK1: 25
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:83] - LOG_FILE: sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWHP04/EIBWHP04_20260728.log
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:84] - OUTPUT_DATASET: sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWHP04/EIBWHP04_20260728.txt
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:85] - Input datasets:
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:87] -   LOAN_CURRENT: sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWHP04/loan20260730.sas7bdat
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:88] -     Exists: False
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:89] -     Size: N/A bytes
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:87] -   LOAN_PREVIOUS: sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWHP04/loan20260625.sas7bdat
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:88] -     Exists: False
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:89] -     Size: N/A bytes
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:87] -   ULOAN: sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWHP04/uloan20260730.sas7bdat
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:88] -     Exists: False
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:89] -     Size: N/A bytes
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:90] - ============================================================
2026-07-29 14:59:09,874 - INFO - [EIBWHP04.py:337] - Processing date: 2026-07-28
2026-07-29 14:59:09,874 - INFO - [EIBWHP04.py:338] - Current month: 202607, Week: 30
2026-07-29 14:59:09,874 - INFO - [EIBWHP04.py:339] - Previous month: 202606, Week: 25
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:342] - Starting DELETE STEP
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:98] - DISP DELETE - Attempting to delete: sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWHP04/EIBWHP04_20260728.txt
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:111] - File does not exist, nothing to delete: sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWHP04/EIBWHP04_20260728.txt
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:344] - DELETE STEP completed
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:347] - Starting SHR VALIDATION
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:349] - Validating LOAN_CURRENT...
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:115] - DISP SHR - Validating: sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWHP04/loan20260730.sas7bdat
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:116] -   Absolute path: /sas/python/virt_edw/Data_Warehouse/MIS/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWHP04/loan20260730.sas7bdat
2026-07-29 14:59:09,874 - DEBUG - [EIBWHP04.py:117] -   Parent directory exists: False
2026-07-29 14:59:09,874 - ERROR - [EIBWHP04.py:121] - Parent directory does not exist: sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWHP04
2026-07-29 14:59:09,874 - ERROR - [EIBWHP04.py:373] - FILE NOT FOUND ERROR: Parent directory does not exist: sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWHP04
2026-07-29 14:59:09,875 - DEBUG - [EIBWHP04.py:374] - Stack trace:
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP04.py", line 350, in run_job
    disp_shr(path)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP04.py", line 122, in disp_shr
    raise FileNotFoundError(error_msg)
FileNotFoundError: Parent directory does not exist: sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWHP04

2026-07-29 14:59:09,875 - INFO - [EIBWHP04.py:409] - Script completed with exit code: 8
