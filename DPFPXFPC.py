2026-06-28 14:20:24,098 - INFO - Format mappings created successfully
2026-06-28 14:20:24,099 - INFO - Report Date: 2025-12-31
2026-06-28 14:20:24,099 - INFO - REPTMON: 12, REPTYEAR: 25
2026-06-28 14:20:24,099 - INFO - SDESC: PUBLIC BANK BERHAD
2026-06-28 14:20:24,129 - INFO - REPTDATE saved to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQFAR2
2026-06-28 14:20:24,129 - INFO - Processing TRUST data from cisdepxn.sas7bdat
2026-06-28 14:20:24,129 - INFO - Reading SAS file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQFAR2/cisdepxn.sas7bdat
2026-06-28 14:29:30,592 - INFO - Successfully read cisdepxn.sas7bdat: 6117754 rows, 166 columns
2026-06-28 14:30:23,659 - INFO - TRUST records: 3591
2026-06-28 14:30:23,664 - INFO - Processing deposit data from cisdepd.sas7bdat
2026-06-28 14:30:23,673 - INFO - Reading SAS file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQFAR2/cisdepd.sas7bdat
2026-06-28 14:34:15,487 - INFO - Successfully read cisdepd.sas7bdat: 7733240 rows, 104 columns
2026-06-28 14:34:18,880 - INFO - DEPOSIT records: 7733240
2026-06-28 14:34:18,968 - ERROR - Error in main processing: 'Expr' object has no attribute 'map_dict'
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
