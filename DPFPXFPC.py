2026-07-05 11:36:43,654 - INFO - ============================================================
2026-07-05 11:36:43,654 - INFO - Starting REPO Processing Pipeline
2026-07-05 11:36:43,654 - INFO - ============================================================
2026-07-05 11:36:43,654 - INFO - STEP 1: Extracting dates from input files
2026-07-05 11:36:43,654 - INFO - RPVBDATA date: 20251201
2026-07-05 11:36:43,654 - INFO - SRSDATA date: 20251101
2026-07-05 11:36:43,654 - INFO - STEP 2: Calculating report dates
2026-07-05 11:36:43,654 - INFO - REPTDATE: 2025-11-30 (1125)
2026-07-05 11:36:43,654 - INFO - PREVDATE: 2025-10-31 (1025)
2026-07-05 11:36:43,654 - INFO - SRSDATE: 2025-11-01 (1125)
2026-07-05 11:36:43,654 - INFO - STEP 3: Validating dates
2026-07-05 11:36:43,654 - INFO - ✓ Date validation passed
2026-07-05 11:36:43,654 - INFO - STEP 4: Parsing RPVB data
2026-07-05 11:36:43,657 - INFO - Parsed 776 records from /sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/input/RPVBDATA.txt
2026-07-05 11:36:43,665 - INFO - Raw data: 776 records
2026-07-05 11:36:43,665 - INFO - STEP 5: Processing RPVB data (UPCASE + dates)
2026-07-05 11:36:43,678 - ERROR - Processing failed: conversion from `str` to `i32` failed in column 'MM1' for 14 out of 776 values: ["0.", "0.", … "0."]

Did not show all failed cases as there were too many.
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMREPO.py", line 355, in <module>
    results = main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMREPO.py", line 261, in main
    RPVB1 = process_rpvb_data(raw_data)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMREPO.py", line 145, in process_rpvb_data
    df = df.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.InvalidOperationError: conversion from `str` to `i32` failed in column 'MM1' for 14 out of 776 values: ["0.", "0.", … "0."]

Did not show all failed cases as there were too many
