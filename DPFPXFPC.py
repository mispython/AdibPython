============================================================
EIMIR102 SAS to Python Conversion
============================================================

1. Processing REPTDATE...
   Report Date: 250726
   Day of Month: 25

2. Loading data...
   Loans: 663747, Branches: 376
   Branch columns: ['BRHCODE', 'BRANCH']
   Branch data type: [String, String]

3. Categorizing loans...
   Categorized records: 389474
   Categorized BRANCH type: Float64

4. Fixing data types for merge...
   Converted categorized BRANCH to Int64
   Error converting branch BRANCH: conversion from `str` to `i64` failed in column 'BRANCH' for 376 out of 376 values: ["PCS", "JSS", … "ASR"]

Did not show all failed cases as there were too many.

5. Merging with branch data...
   Merged records: 1201

6. Generating 17-bucket report (EIMAR102-A)...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR102.py:175: DeprecationWarning: `pl.count()` is deprecated. Please use `pl.len()` instead.
(Deprecated in version 0.20.5)
  pl.count().alias("NOACC")
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR102.py", line 584, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR102.py", line 524, in main
    generate_17_bucket_report_text(results_17, variables)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR102.py", line 364, in generate_17_bucket_report_text
    f.write(f"CATEGORY {cat}: {cat_data['branches'][0]['TYPE'] if cat_data['branches'] else 'Unknown'}\n")
KeyError: 'TYPE'
