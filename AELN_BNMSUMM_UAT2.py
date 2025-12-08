============================================================
EIBMRNID - EXACT SAS OUTPUT
============================================================
Report Date: 30/11/2025

📂 Processing data...
  Merged with TRNCH

💾 Writing SAS-format output to: /sas/python/virt_edw/Data_Warehouse/MIS/Job/Output/EIBMRNID_REPORT.TXT
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/EIBMRNID_NID.py", line 291, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/EIBMRNID_NID.py", line 277, in main
    write_sas_exact_output(final_output_file, reptdate, tbl1_sum, tbl2_stats, overall_yield)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/EIBMRNID_NID.py", line 123, in write_sas_exact_output
    nidcnt, nidvol = tbl2_stats['nidcnt'], tbl2_stats['nidvol']
TypeError: tuple indices must be integers or slices, not str
