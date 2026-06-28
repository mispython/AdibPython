2026-06-28 16:50:17,819 - INFO - PBBDPFMT formats loaded successfully
2026-06-28 16:50:17,819 - INFO - Report Date: 2025-12-31
2026-06-28 16:50:17,819 - INFO - REPTMON: 12, REPTYEAR: 25
2026-06-28 16:50:17,819 - INFO - SDESC: PUBLIC BANK BERHAD (ISLAMIC)
2026-06-28 16:50:17,827 - INFO - REPTDATE saved to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIQFISF
2026-06-28 16:50:17,828 - INFO - Processing Islamic deposit data from cisdepi.sas7bdat
2026-06-28 16:50:17,828 - INFO - Reading SAS file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIQFISF/cisdepi.sas7bdat
2026-06-28 16:52:15,719 - INFO - Successfully read cisdepi.sas7bdat: 2,836,464 rows, 94 columns
2026-06-28 16:52:18,218 - ERROR - Error in Islamic processing: conversion from `str` to `i64` failed in column 'PRODCD' for 710 out of 2836464 values: ["IR070", "IR070", … "IR070"]

Did not show all failed cases as there were too many.
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQFISF_ISLAMIC_INSURANCE.py", line 322, in main
    deposit_df = process_islamic_deposit_data(deposit1_path)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQFISF_ISLAMIC_INSURANCE.py", line 93, in process_islamic_deposit_data
    cisdepi_df = cisdepi_df.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.InvalidOperationError: conversion from `str` to `i64` failed in column 'PRODCD' for 710 out of 2836464 values: ["IR070", "IR070", … "IR070"]

Did not show all failed cases as there were too many.
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQFISF_ISLAMIC_INSURANCE.py", line 365, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQFISF_ISLAMIC_INSURANCE.py", line 322, in main
    deposit_df = process_islamic_deposit_data(deposit1_path)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQFISF_ISLAMIC_INSURANCE.py", line 93, in process_islamic_deposit_data
    cisdepi_df = cisdepi_df.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.InvalidOperationError: conversion from `str` to `i64` failed in column 'PRODCD' for 710 out of 2836464 values: ["IR070", "IR070", … "IR070"]

Did not show all failed cases as there were too many.
