============================================================
EIBMRNID - EXACT SAS OUTPUT
============================================================
📂 Reading data to determine appropriate report date...
  Most recent maturity date in data: 2025-08-23
  Adjusted Report Date: 30/06/2025
  Adjusted Start Date: 01/06/2025

Report Date: 30/06/2025
Start Date: 01/06/2025

📂 Processing data...
  Original NID records: 580
  Merged with TRNCH
  Records with positive balance: 580

💾 Saving processed data to Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/Job/Output/EIBMRNID_DATA.parquet
  Saved 580 records to Parquet

📊 Creating Table 1...
  Using report date: 2025-06-30
  Table 1 filtered records (standard logic): 0
  Trying alternative logic for historical data...
  Table 1 filtered records (alternative logic): 28
  Using alternative logic for historical data
  Table 1 summary: 1 maturity buckets
  Table 1 records: 28

📊 Creating Table 2...
  Table 2 - NID Count: 0, Volume: 0.00

📊 Creating Table 3...
  Table 3 filtered records: 0
  Table 3 filtered records (alternative): 28
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/EIBMRNID_NID.py", line 565, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/EIBMRNID_NID.py", line 531, in main
    label = row['remfmtb'].strip()
AttributeError: 'NoneType' object has no attribute 'strip'
