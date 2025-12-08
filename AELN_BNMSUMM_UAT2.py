============================================================
EIBMRNID - EXACT SAS OUTPUT
============================================================
Report Date: 30/11/2025
Start Date: 01/11/2025

📂 Processing data...
  Merged with TRNCH

💾 Saving processed data to Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/Job/Output/EIBMRNID_DATA.parquet
  Saved 580 records to Parquet

📊 Creating Table 1...
  Table 1 records: 0
📊 Creating Table 2...
  Table 2 - NID Count: 0, Volume: 0.00
📊 Creating Table 3...
  Warning: No records found for Table 3 yield calculation

💾 Writing SAS-format output to: /sas/python/virt_edw/Data_Warehouse/MIS/Job/Output/EIBMRNID_REPORT.TXT

✅ SAS report generated: /sas/python/virt_edw/Data_Warehouse/MIS/Job/Output/EIBMRNID_REPORT.TXT
✅ Parquet data saved: /sas/python/virt_edw/Data_Warehouse/MIS/Job/Output/EIBMRNID_DATA.parquet

📊 Final Summary:
  Total processed records: 580
  Table 1 (Outstanding): 0 records
  Table 2 (Trading): 0 NIDs, RM 0.00
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/EIBMRNID_NID.py", line 450, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/EIBMRNID_NID.py", line 442, in main
    print(f"  Table 3 (Yield): {overall_yield:.4f}% overall")
UnboundLocalError: local variable 'overall_yield' referenced before assignment
