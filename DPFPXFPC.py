NOWK: 4, REPTMON: 12, REPTYEAR: 2025
SDESC: PUBLIC BANK BERHAD
REPTDATE: 2025-12-31
SDATE: 2025-12-23

Loading SAS7BDAT files:
  UNCLAIM: unclaim2025.sas7bdat
  NOTUNCLAIM: notunclaim2025.sas7bdat
Loaded unclaim2025.sas7bdat with 1979 records
Columns: ['paymode', 'ledgbal', 'acctno', 'status', 'name', 'category']
Data types: [String, Float64, Float64, String, String, String]
Loaded notunclaim2025.sas7bdat with 2613 records
Columns: ['acctno', 'ledgbal', 'status', 'paymode', 'name', 'category']
Data types: [Float64, Float64, String, String, String, String]
Combined dataset has 4592 records
UNCLAIM records: 1979
NONDEBIT records: 2613
Saved UNCLAIM with 1979 records
Saved NONDEBIT with 2613 records
Saved UNCLAIM_FINAL with 1979 records

Looking for SAS7BDAT files:
  SAVG: savg124.sas7bdat
  CURN: curn124.sas7bdat
  ISAVG: savg124.sas7bdat
  ICURN: curn124.sas7bdat
Loaded savg124.sas7bdat with 4241108 records
Loaded curn124.sas7bdat with 915692 records
Loaded savg124.sas7bdat with 2262899 records
Loaded curn124.sas7bdat with 915692 records
Combined DEP dataset has 8335391 records
After filtering PRODCD: 8135928 records
Saved DEP with 7284287 records
UNCLAIM for merge has 1979 records
UNCLAIM acctno type: Float64
Merged dataset has 1979 records
After BC assignment: 1979 records
Saved DEP_FINAL with 1979 records

================================================================================
BANKERS CHEQUE WITH DEBITTED A/C (CONVENTIONAL)
================================================================================

BC/DD AMOUNT by Category (DEBITTED):
shape: (3, 2)
┌──────────┬───────────┐
│ category ┆ ledgbal   │
│ ---      ┆ ---       │
│ str      ┆ f64       │
╞══════════╪═══════════╡
│ CA       ┆ 5.2923e6  │
│ OTHER    ┆ 144113.91 │
│ SA       ┆ 974203.75 │
└──────────┴───────────┘

TOTAL BC/DD AMOUNT: 6,410,614.54

================================================================================
BANKERS CHEQUE WITH DEBITTED A/C NOT FOUND IN FISS (CONV&ISLM)
================================================================================

BC/DD AMOUNT by Category (NOT FOUND):
shape: (3, 2)
┌──────────┬──────────┐
│ category ┆ ledgbal  │
│ ---      ┆ ---      │
│ str      ┆ f64      │
╞══════════╪══════════╡
│ CA       ┆ 2.6717e6 │
│ OTHER    ┆ 433.39   │
│ SA       ┆ 47803.63 │
└──────────┴──────────┘

TOTAL BC/DD AMOUNT: 2,719,971.03
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQUCLM.py", line 448, in <module>
    nondebit_processed = nondebit_sorted.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.InvalidOperationError: conversion from `str` to `f64` failed in column 'paymode' for 2613 out of 2613 values: ["", "EDVD", … "EDVD"]
