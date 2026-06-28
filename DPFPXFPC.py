2026-06-28 17:16:22,722 - INFO - PBBDPFMT formats loaded successfully
2026-06-28 17:16:22,722 - INFO - Report Date: 2025-12-31
2026-06-28 17:16:22,722 - INFO - REPTMON: 12, REPTYEAR: 25
2026-06-28 17:16:22,722 - INFO - SDESC: PUBLIC BANK BERHAD (ISLAMIC)
2026-06-28 17:16:22,730 - INFO - REPTDATE saved to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIQFISF
2026-06-28 17:16:22,730 - INFO - Processing Islamic deposit data from cisdepi.sas7bdat
2026-06-28 17:16:22,730 - INFO - Reading SAS file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIQFISF/cisdepi.sas7bdat
2026-06-28 17:18:22,710 - INFO - Successfully read cisdepi.sas7bdat: 2,836,464 rows, 94 columns
2026-06-28 17:18:26,210 - INFO - Islamic DEPOSIT records: 2,836,464
2026-06-28 17:18:26,210 - INFO - Applying product format mappings
2026-06-28 17:18:29,437 - INFO - Unique PRODCD_FORMATTED values: ['DDMAND', 'FDMAND', 'DFIXED', 'DSVING', 'IR070']
2026-06-28 17:18:31,730 - INFO - RPT_BASE records: 2,836,464
2026-06-28 17:18:32,326 - ERROR - Error in Islamic processing: conversion from `str` to `i64` failed in column 'BRANCH' for 1 out of 266 values: ["TOTAL"]
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQFISF_ISLAMIC_INSURANCE.py", line 361, in main
    generate_islamic_txt_report(final_summary, output_path, reptdate)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQFISF_ISLAMIC_INSURANCE.py", line 205, in generate_islamic_txt_report
    pivot_table = pivot_table.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.InvalidOperationError: conversion from `str` to `i64` failed in column 'BRANCH' for 1 out of 266 values: ["TOTAL"]
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQFISF_ISLAMIC_INSURANCE.py", line 370, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQFISF_ISLAMIC_INSURANCE.py", line 361, in main
    generate_islamic_txt_report(final_summary, output_path, reptdate)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQFISF_ISLAMIC_INSURANCE.py", line 205, in generate_islamic_txt_report
    pivot_table = pivot_table.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.InvalidOperationError: conversion from `str` to `i64` failed in column 'BRANCH' for 1 out of 266 values: ["TOTAL"]
