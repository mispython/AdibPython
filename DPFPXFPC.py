2026-06-28 14:02:05,076 - INFO - Format mappings created successfully
2026-06-28 14:02:05,076 - INFO - Report Date: 2025-12-31
2026-06-28 14:02:05,076 - INFO - REPTMON: 12, REPTYEAR: 25
2026-06-28 14:02:05,076 - INFO - SDESC: PUBLIC BANK BERHAD
2026-06-28 14:02:05,113 - INFO - REPTDATE saved to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQFAR2
2026-06-28 14:02:05,113 - INFO - Processing TRUST data from cisdepxn.sas7bdat
2026-06-28 14:02:05,114 - INFO - Reading SAS file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQFAR2/cisdepxn.sas7bdat
2026-06-28 14:02:05,117 - ERROR - Error reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQFAR2/cisdepxn.sas7bdat: Unable to read from file
2026-06-28 14:02:05,117 - WARNING - PIDMFIN.cisdepxn is empty or not found
2026-06-28 14:02:05,117 - INFO - Processing deposit data from cisdepd.sas7bdat
2026-06-28 14:02:05,117 - INFO - Reading SAS file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQFAR2/cisdepd.sas7bdat
2026-06-28 14:06:28,117 - INFO - Successfully read cisdepd.sas7bdat: 7733240 rows, 104 columns
2026-06-28 14:06:33,359 - INFO - DEPOSIT records: 7733240
2026-06-28 14:06:33,359 - ERROR - Error in main processing: 'Expr' object has no attribute 'map_dict'
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQFAR2_CONV_INSURANCE.py", line 345, in main
    rpt_base = apply_format_mappings(rpt_base)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQFAR2_CONV_INSURANCE.py", line 249, in apply_format_mappings
    pl.col('PRODCD').map_dict(PRODBRH).alias('PRODCD_FORMATTED')
AttributeError: 'Expr' object has no attribute 'map_dict'
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQFAR2_CONV_INSURANCE.py", line 379, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQFAR2_CONV_INSURANCE.py", line 345, in main
    rpt_base = apply_format_mappings(rpt_base)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQFAR2_CONV_INSURANCE.py", line 249, in apply_format_mappings
    pl.col('PRODCD').map_dict(PRODBRH).alias('PRODCD_FORMATTED')
AttributeError: 'Expr' object has no attribute 'map_dict'


this is the actual error
