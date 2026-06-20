Current working directory: /sas/python/virt_edw/Data_Warehouse/MIS
Deposit path exists: True
MNI path exists: True
IMNI path exists: True
Output path exists: True

Files in /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQREMT:
  - curn124.sas7bdat
  - savg124.sas7bdat
  - CURN124.parquet
  - SAVG124.parquet
  - MAREMORE
  - REMIT.parquet
  - REMIT.csv
  - REMIT_sorted.parquet
  - REMIT_sorted.csv
  - SAVG124.csv
  - CURN124.csv

Files in /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQREMT:
  - curn124.sas7bdat
  - savg124.sas7bdat
  - CURN124.parquet
  - SAVG124.parquet
  - MAREMORE
  - REMIT.parquet
  - REMIT.csv
  - REMIT_sorted.parquet
  - REMIT_sorted.csv
  - SAVG124.csv
  - CURN124.csv

Files in /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIQREMT:
  - savg124.sas7bdat
  - curn124.sas7bdat
  - ISAVG124.parquet
  - ICURN124.parquet
  - ISAVG124.csv
  - ICURN124.csv
*** TEST MODE - Using date: 2026-12-23 (December Week 4) ***
NOWK: 4, REPTMON: 12, REPTYEAR: 2026
SDESC: PUBLIC BANK BERHAD
SDATE: 2026-12-23
Created REPTDATE files in /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQREMT

Looking for MAREMORE file at: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQREMT/MAREMORE
MAREMORE file found! Size: 3050850 bytes

Attempting to parse REMIT file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQREMT/MAREMORE
Read 11826 lines from REMIT file
Created DataFrame with 11826 records
After cleaning, 11638 records remain
Successfully parsed 11638 records from MAREMORE
Created REMIT with 8858 records and NONDEBIT with 2780 records
Created REMIT_sorted with 8858 records
Created NONDEBIT_sorted with 2780 records
Created REMIT_FINAL with 4791 records

Looking for SAS files with pattern: *124*
SAVG: savg124.sas7bdat (lowercase)
CURN: curn124.sas7bdat (lowercase)
ISAVG: savg124.sas7bdat (lowercase, in IMNI)
ICURN: curn124.sas7bdat (lowercase, in IMNI)

Reading SAS files...
Attempting to read: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQREMT/savg124.sas7bdat
  Successfully read /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQREMT/savg124.sas7bdat, 4241108 rows, 25 columns
  Selected columns: ['ACCTNO', 'PRODCD', 'COSTCTR']
  Loaded SAVG with 4241108 records
Attempting to read: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQREMT/curn124.sas7bdat
  Successfully read /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQREMT/curn124.sas7bdat, 915692 rows, 29 columns
  Selected columns: ['ACCTNO', 'PRODCD', 'COSTCTR']
  Loaded CURN with 915692 records
Attempting to read: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIQREMT/savg124.sas7bdat
  Successfully read /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIQREMT/savg124.sas7bdat, 2262899 rows, 25 columns
  Selected columns: ['ACCTNO', 'PRODCD', 'COSTCTR']
  Loaded ISAVG with 2262899 records
Attempting to read: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIQREMT/curn124.sas7bdat
  Successfully read /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIQREMT/curn124.sas7bdat, 915692 rows, 29 columns
  Selected columns: ['ACCTNO', 'PRODCD', 'COSTCTR']
  Loaded ICURN with 915692 records

Combining 4 datasets...
Combined DEP dataset with 8335391 records
Filtered DEP with valid PRODCD: 8135928 records
Unique DEP records by ACCTNO: 7284287 records
Converted DEP ACCTNO to Int64

Proceeding with merge and reporting...
Created DEP_SORTED with 4791 records

================================================================================
REPORT 1: BANKERS CHEQUE WITH DEBITTED A/C (CONVENTIONAL)
================================================================================

Summary by Category:
shape: (2, 2)
┌──────────┬─────────┐
│ CATEGORY ┆ LEDGBAL │
│ ---      ┆ ---     │
│ str      ┆ f64     │
╞══════════╪═════════╡
│ CA       ┆ 0.0     │
│ SA       ┆ 0.0     │
└──────────┴─────────┘

TOTAL BC/DD AMOUNT: 0.00

Sample Detailed Records (first 10):
shape: (10, 6)
┌────────────┬────────────┬──────────┬─────────┬──────────────────────────┬─────────┐
│ ACCTNO     ┆ PAYMODE    ┆ CATEGORY ┆ LEDGBAL ┆ NAME                     ┆ COSTCTR │
│ ---        ┆ ---        ┆ ---      ┆ ---     ┆ ---                      ┆ ---     │
│ i64        ┆ str        ┆ str      ┆ f64     ┆ str                      ┆ f64     │
╞════════════╪════════════╪══════════╪═════════╪══════════════════════════╪═════════╡
│ 3108524306 ┆ 3108524306 ┆ CA       ┆ 0.0     ┆ ING CHIONG ENTERPRISE    ┆ 190.0   │
│ 3244971900 ┆ 3244971900 ┆ CA       ┆ 0.0     ┆ DAPHNE LEONG & ELLIE LAW ┆ 38.0    │
│ 3159752323 ┆ 3159752323 ┆ CA       ┆ 0.0     ┆ GLUCK HARDWARE AND TIMBE ┆ 167.0   │
│ 3188008136 ┆ 3188008136 ┆ CA       ┆ 0.0     ┆ HOTEL EXCELSIOR (IPOH) S ┆ 5.0     │
│ 3205960833 ┆ 3205960833 ┆ CA       ┆ 0.0     ┆ GOODWOOD HOTEL SDN BHD   ┆ 7.0     │
│ 3193084402 ┆ 3193084402 ┆ CA       ┆ 0.0     ┆ KEMAJUAN DERAS SDN BHD   ┆ 106.0   │
│ 3191495118 ┆ 3191495118 ┆ CA       ┆ 0.0     ┆ PERABUT KUSYEN ANDA SDN  ┆ 30.0    │
│ 3230911318 ┆ 3230911318 ┆ CA       ┆ 0.0     ┆ GLOCON BUILDER SDN BHD   ┆ 81.0    │
│ 3122927328 ┆ 3122927328 ┆ CA       ┆ 0.0     ┆ CONSOLIDATED FERTILISER  ┆ 201.0   │
│ 3215501532 ┆ 3215501532 ┆ CA       ┆ 0.0     ┆ GKV GLOBAL SERVICES      ┆ 16.0    │
└────────────┴────────────┴──────────┴─────────┴──────────────────────────┴─────────┘

================================================================================
REPORT 2: BANKERS CHEQUE WITH DEBITTED A/C NOT FOUND IN FISS (CONV&ISLM)
================================================================================

Summary by Category:
shape: (4, 2)
┌──────────┬─────────┐
│ CATEGORY ┆ LEDGBAL │
│ ---      ┆ ---     │
│ str      ┆ f64     │
╞══════════╪═════════╡
│ CA       ┆ 0.0     │
│ FD       ┆ 0.0     │
│ OTHER    ┆ 0.0     │
│ SA       ┆ 0.0     │
└──────────┴─────────┘

TOTAL BC/DD AMOUNT: 0.00

Sample Detailed Records (first 10):
shape: (10, 5)
┌────────────┬────────────┬──────────┬─────────┬─────────────────────────────────┐
│ ACCTNO     ┆ PAYMODE    ┆ CATEGORY ┆ LEDGBAL ┆ NAME                            │
│ ---        ┆ ---        ┆ ---      ┆ ---     ┆ ---                             │
│ i64        ┆ str        ┆ str      ┆ f64     ┆ str                             │
╞════════════╪════════════╪══════════╪═════════╪═════════════════════════════════╡
│ 3239578413 ┆ 3239578413 ┆ CA       ┆ 0.0     ┆ TINK AGENCY SDN. BHD.           │
│ 363490620  ┆ 363490620  ┆ CA       ┆ 0.0     ┆ EATRICE CHAI SHEAU ERN        … │
│ 3156445932 ┆ 3156445932 ┆ CA       ┆ 0.0     ┆ ALAGARSAMY RENGASAMY            │
│ 37373405   ┆ 37373405   ┆ CA       ┆ 0.0     ┆ H LAY SHE                     … │
│ 317271901  ┆ 317271901  ┆ CA       ┆ 0.0     ┆ AN WAN YEE                    … │
│ 30566225   ┆ 30566225   ┆ CA       ┆ 0.0     ┆ HAMAD ASMAR BIN ABDUL         … │
│ 35339710   ┆ 35339710   ┆ CA       ┆ 0.0     ┆ ZLIN BINTI MAHMUD             … │
│ 3237045807 ┆ 3237045807 ┆ CA       ┆ 0.0     ┆ PERNIAGAAN JIN LONG WANG        │
│ 37001509   ┆ 37001509   ┆ CA       ┆ 0.0     ┆ TIK A & T                     … │
│ 3232256801 ┆ 3232256801 ┆ CA       ┆ 0.0     ┆ OX ENORME SUPPLY (M) SDN        │
└────────────┴────────────┴──────────┴─────────┴─────────────────────────────────┘

================================================================================
REPORT 3: BANKERS CHEQUE WITH NON-DEBITTED A/C
================================================================================
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQREMT.py", line 573, in <module>
    nondebit_processed = nondebit_invalid.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.InvalidOperationError: conversion from `str` to `i64` failed in column 'PAYMODE' for 2618 out of 2780 values: ["OTHERS", "OTHERS", … "OTHERS"]

Did not show all failed cases as there were too many.
