2026-06-28 17:13:57,712 - INFO - PBBDPFMT formats loaded successfully
2026-06-28 17:13:57,712 - INFO - Report Date: 2025-12-31
2026-06-28 17:13:57,712 - INFO - REPTMON: 12, REPTYEAR: 25
2026-06-28 17:13:57,712 - INFO - SDESC: PUBLIC BANK BERHAD (ISLAMIC)
2026-06-28 17:13:57,721 - INFO - REPTDATE saved to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIQFISF
2026-06-28 17:13:57,721 - INFO - Processing Islamic deposit data from cisdepi.sas7bdat
2026-06-28 17:13:57,721 - INFO - Reading SAS file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIQFISF/cisdepi.sas7bdat
2026-06-28 17:14:40,819 - INFO - Successfully read cisdepi.sas7bdat: 2,836,464 rows, 94 columns
2026-06-28 17:14:41,959 - INFO - Islamic DEPOSIT records: 2,836,464
2026-06-28 17:14:41,959 - INFO - Applying product format mappings
2026-06-28 17:14:43,161 - INFO - Unique PRODCD_FORMATTED values: ['DFIXED', 'DSVING', 'IR070', 'DDMAND', 'FDMAND']
2026-06-28 17:14:44,061 - INFO - RPT_BASE records: 2,836,464
2026-06-28 17:14:44,320 - ERROR - Error in Islamic processing: type String is incompatible with expected type Int64
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQFISF_ISLAMIC_INSURANCE.py", line 353, in main
    final_summary = pl.concat([summary, total_summary], how="diagonal")
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 247, in concat
    out = wrap_df(plr.concat_df_diagonal(elems))
polars.exceptions.SchemaError: type String is incompatible with expected type Int64
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQFISF_ISLAMIC_INSURANCE.py", line 375, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQFISF_ISLAMIC_INSURANCE.py", line 353, in main
    final_summary = pl.concat([summary, total_summary], how="diagonal")
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 247, in concat
    out = wrap_df(plr.concat_df_diagonal(elems))
polars.exceptions.SchemaError: type String is incompatible with expected type Int64
