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

Files in /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIQREMT:
  - savg124.sas7bdat
  - curn124.sas7bdat
  - ISAVG124.parquet
  - ICURN124.parquet
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
SAVG: savg124.sas7bdat
CURN: curn124.sas7bdat
ISAVG: savg124.sas7bdat
ICURN: curn124.sas7bdat

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

Proceeding with merge and reporting...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQREMT.py", line 438, in <module>
    dep_merged = dep_deduped.join(remit_for_merge, on='ACCTNO', how='right', suffix='_remit')
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 128, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 8242, in join
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.SchemaError: datatypes of join keys don't match - `ACCTNO`: f64 on left does not match `ACCTNO`: i64 on right (and no other type was available to cast to)





the savg and curn is in lowercase, isavg and icurn are both savg and curn, no i's, just different path
