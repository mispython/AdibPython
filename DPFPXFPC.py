2026-06-28 17:25:54,749 - INFO - Format mappings created successfully
2026-06-28 17:25:54,811 - INFO - Report Date: 2025-12-31
2026-06-28 17:25:54,811 - INFO - REPTMON: 12, REPTYEAR: 25
2026-06-28 17:25:54,811 - INFO - SDESC: PUBLIC BANK BERHAD
2026-06-28 17:25:54,821 - INFO - REPTDATE saved to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQFAR2
2026-06-28 17:25:54,821 - INFO - Processing TRUST data from cisdepxn.sas7bdat
2026-06-28 17:25:54,821 - INFO - Reading SAS file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQFAR2/cisdepxn.sas7bdat
2026-06-28 17:44:55,565 - INFO - Successfully read cisdepxn.sas7bdat: 6,117,754 rows, 166 columns
2026-06-28 17:45:56,928 - INFO - TRUST records: 3,591
2026-06-28 17:45:56,938 - INFO - Processing deposit data from cisdepd.sas7bdat
2026-06-28 17:45:56,948 - INFO - Reading SAS file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQFAR2/cisdepd.sas7bdat
2026-06-28 17:52:27,461 - INFO - Successfully read cisdepd.sas7bdat: 7,733,240 rows, 104 columns
2026-06-28 17:52:31,664 - INFO - DEPOSIT records: 7,733,240
2026-06-28 17:52:37,739 - INFO - RPT_BASE records: 7,736,831
2026-06-28 17:52:39,732 - ERROR - Error in main processing: conversion from `str` to `i64` failed in column 'BRANCH' for 260 out of 260 values: ["59.0", "283.0", … "185.0"]

Did not show all failed cases as there were too many.
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQFAR2_CONV_INSURANCE.py", line 377, in main
    generate_conventional_txt_report(final_summary, output_path, reptdate)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQFAR2_CONV_INSURANCE.py", line 195, in generate_conventional_txt_report
    pivot_table = pivot_table.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.InvalidOperationError: conversion from `str` to `i64` failed in column 'BRANCH' for 260 out of 260 values: ["59.0", "283.0", … "185.0"]

Did not show all failed cases as there were too many.
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQFAR2_CONV_INSURANCE.py", line 386, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQFAR2_CONV_INSURANCE.py", line 377, in main
    generate_conventional_txt_report(final_summary, output_path, reptdate)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQFAR2_CONV_INSURANCE.py", line 195, in generate_conventional_txt_report
    pivot_table = pivot_table.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.InvalidOperationError: conversion from `str` to `i64` failed in column 'BRANCH' for 260 out of 260 values: ["59.0", "283.0", … "185.0"]

Did not show all failed cases as there were too many.
