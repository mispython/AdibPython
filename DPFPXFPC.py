2026-06-28 15:20:50,248 - INFO - Format mappings created successfully
2026-06-28 15:20:50,249 - INFO - Report Date: 2025-12-31
2026-06-28 15:20:50,249 - INFO - REPTMON: 12, REPTYEAR: 25
2026-06-28 15:20:50,249 - INFO - SDESC: PUBLIC BANK BERHAD
2026-06-28 15:20:50,249 - INFO - ⚠️  TEST MODE ENABLED: Will limit to 10,000 rows after reading
2026-06-28 15:20:50,265 - INFO - REPTDATE saved to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQFAR2
2026-06-28 15:20:50,265 - INFO - Processing TRUST data from cisdepxn.sas7bdat
2026-06-28 15:20:50,265 - INFO - Reading SAS file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQFAR2/cisdepxn.sas7bdat
2026-06-28 15:30:53,208 - INFO - TEST MODE: Limiting to first 10,000 rows (from 6,117,754)
2026-06-28 15:30:53,368 - INFO - Successfully read cisdepxn.sas7bdat: 10,000 rows, 166 columns
2026-06-28 15:32:13,448 - INFO - TRUST records: 0
2026-06-28 15:32:13,454 - INFO - Processing deposit data from cisdepd.sas7bdat
2026-06-28 15:32:13,464 - INFO - Reading SAS file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQFAR2/cisdepd.sas7bdat
2026-06-28 15:36:33,571 - INFO - TEST MODE: Limiting to first 10,000 rows (from 7,733,240)
2026-06-28 15:36:33,572 - INFO - Successfully read cisdepd.sas7bdat: 10,000 rows, 104 columns
2026-06-28 15:36:36,947 - INFO - DEPOSIT records: 10,000
2026-06-28 15:36:36,948 - INFO - Applying format mappings using replace() method
2026-06-28 15:36:37,034 - INFO - RPT_BASE records: 10,000

========================================================================================================================
APPORTIONMENT OF PREMIUM PAID TO MDIC BY BRANCH (CONVENTIONAL)
========================================================================================================================
BRANCH              DDMAND              DEBIT CARD (E)      DFIXED              DSVING              FDMAND              FFIXED              
------------------------------------------------------------------------------------------------------------------------
2026-06-28 15:36:37,119 - ERROR - Error in main processing: unsupported format string passed to NoneType.__format__
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQFAR2_CONV_INSURANCE.py", line 354, in main
    generate_tabular_report(final_summary, output_path)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQFAR2_CONV_INSURANCE.py", line 274, in generate_tabular_report
    line = f"{row['BRANCH']:<20}"
TypeError: unsupported format string passed to NoneType.__format__
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQFAR2_CONV_INSURANCE.py", line 366, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQFAR2_CONV_INSURANCE.py", line 354, in main
    generate_tabular_report(final_summary, output_path)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQFAR2_CONV_INSURANCE.py", line 274, in generate_tabular_report
    line = f"{row['BRANCH']:<20}"
TypeError: unsupported format string passed to NoneType.__format__
