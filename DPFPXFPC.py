[FMT] Loaded SECTCD: 0 entries
========== START JOB EIBWHP01 ==========
[DATE] Report: 28/07/26
[DATE] REPTMON=07, NOWK=4
[DATE] REPTMON1=07, NOWK1=3
[WARN] Using latest loan as current: loan064.sas7bdat
[WARN] Using same file for previous (no alternative found)

[READ] Loading files to parquet cache (if needed)...
[READ] Using cache: loan064.parquet
[READ] Using cache: loan064.parquet
[READ] Using cache: lnnote.parquet
  Current BNM: 623,910 rows
  Previous BNM: 623,910 rows
  LNNOTE: 6,232,608 rows
[PROCESS] Starting DuckDB processing...
[PROCESS] Filtering products...
  Current BNM filtered: 4,295
  Previous BNM filtered: 4,295
[PROCESS] Getting LNNOTE data...
100% ▕██████████████████████████████████████▏ (00:00:02.91 elapsed)     
  LNNOTE rows: 6,232,608
[PROCESS] Computing EFFAPR for LNNOTE...
  Processed chunk 5/32
  Processed chunk 10/32
  Processed chunk 15/32
  Processed chunk 20/32
  Processed chunk 25/32
  Processed chunk 30/32
  LNNOTE processed: 6,232,608
[PROCESS] Merging data...
  Merged rows: 4,295
[PROCESS] Computing DISBURSE/REPAID...
  After filtering zero DISBURSE: 0
[WARN] No records with DISBURSE > 0

[PROCESS] Summarising all customers...
  ALL: No data found
[PROCESS] Summarising SMI (CUSTCD 66-69)...
  SMI: No data found

[OUTPUT] Writing report to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWHP01/EIBWHP01.txt...
  Output written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWHP01/EIBWHP01.txt
========== END JOB EIBWHP01 ==========



production output:

EIBWHP01: REPORT ON PRODUCTS 131,132,720,725 AS AT  22/07/26                                       07:14 Thursday, July 23, 2026   1 
ALL CUSTOMERS                                                                                                                        
                                                                                                                                     
Obs       BNMCODE                           AMOUNT      WEIGHTED                                                                     
                                                                                                                                     
  1    6734000001000Y                 1,557,814.51    .                                                                              
  2    6734000002000Y                   811,104.87    .                                                                              
  3    6734000003000Y                 4,581,562.30             0                                                                     
  4    6734000005001Y                 3,074,040.01    .                                                                              
  5    6734000005002Y                   238,229.58    .                                                                              
  6    6734000005003Y                   162,661.89    .                                                                              
  7    6734000005004Y                   487,947.92    .                                                                              
  8    6734000005006Y                   249,561.53    .                                                                              
  9    6734000006100Y                28,861,403.92    .000005587                                                                     
 10    6734000006300Y                 1,566,435.02    .                                                                              
 11    6734000007000Y                 5,250,959.23             0                                                                     
 12    6734000008310Y                   925,716.68    .                                                                              
 13    6734000008320Y                       118.60    .                                                                              
 14    6734000009000Y                 3,225,819.38    .                                                                              


python output:

EIBWHP01 REPORT GENERATED 28-07-2026
REPTMON: 07, NOWK: 4
================================================================================
BNM RECORDS:     623910
LOAN RECORDS:    6232608
REPORT DATE: 28-07-2026
================================================================================

EIBWHP01: REPORT ON PRODUCTS 131,132,720,725 AS AT 28/07/26

Obs    BNMCODE                         AMOUNT          WEIGHTED

  No records found

EIBWHP01: SMI ACCTS (CUSTCD 66,67,68,69) AS AT 28/07/26

Obs    BNMCODE                         AMOUNT          WEIGHTED

  No records found



totally different.. help me fix
