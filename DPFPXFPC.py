EIBWHP01 REPORT GENERATED 27-07-2026
REPTMON: 07, NOWK: 4
================================================================================
BNM RECORDS:     623910
LOAN RECORDS:    6232608
REPORT DATE: 27-07-2026
================================================================================

EIBWHP01: REPORT ON PRODUCTS 131,132,720,725 AS AT 27/07/26

Obs    BNMCODE                         AMOUNT          WEIGHTED

  No records found

EIBWHP01: SMI ACCTS (CUSTCD 66,67,68,69) AS AT 27/07/26

Obs    BNMCODE                         AMOUNT          WEIGHTED

  No records found



[FMT] Loaded SECTCD: 0 entries
[FMT] Using fallback SECTCD mapping
========== START JOB EIBWHP01 ==========
[DATE] Report: 27/07/26
[DATE] REPTMON=07, NOWK=4
[DATE] REPTMON1=07, NOWK1=3
[WARN] Using latest loan as current: loan064.sas7bdat
[WARN] Previous BNM not found: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWHP01/loan073.sas7bdat
[WARN] Using same file for previous (no alternative found)

[READ] Loading files (parquet cache)...
[READ] Using cache: lnnote.parquet
  LNNOTE: 6,232,608 rows
[READ] Using cache: loan064.parquet
  Current BNM: 623,910 rows
[READ] Using cache: loan064.parquet
  Previous BNM: 623,910 rows

[PROCESS] Building expanded loan data...
[PROCESS] Filtering products...
  Current BNM after product filter: 4,295
  Previous BNM after product filter: 4,295
[PROCESS] Processing LNNOTE...
  LNNOTE rows after sector mapping: 0
[PROCESS] Merging with BNM files...
  Merged rows: 4,295
[PROCESS] Expanding by SECTA and SECTB...
  Expanded rows: 4,295

[PROCESS] Summarising all customers...
  ALL: No data with non-zero DISBURSE
[PROCESS] Summarising SMI (CUSTCD 66-69)...
  SMI filtered rows: 0
  SMI: No data found

[OUTPUT] Writing report to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWHP01/EIBWHP01.txt...
  Output written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWHP01/EIBWHP01.txt
