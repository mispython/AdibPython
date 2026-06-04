2026-06-04 15:34:10,229 - INFO - STEP 5: Saving output as SAS7BDAT
2026-06-04 15:34:10,229 - INFO - ============================================================
2026-06-04 15:34:10,230 - ERROR -   Error writing SAS7BDAT: module 'pyreadstat' has no attribute 'write_sas7bdat'
2026-06-04 15:34:10,230 - WARNING -   Falling back to CSV output at /sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output/LNR6999/LNR6999/R69990526.csv
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/EIBMLNRP.py", line 341, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/EIBMLNRP.py", line 314, in main
    write_sas7bdat(df, sas7bdat_path, reptdate, reptdt, reptmon, reptyear, 
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/EIBMLNRP.py", line 226, in write_sas7bdat
    pyreadstat.write_sas7bdat(
AttributeError: module 'pyreadstat' has no attribute 'write_sas7bdat'
