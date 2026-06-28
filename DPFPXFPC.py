2026-06-28 14:44:26,838 - INFO - Format mappings created successfully
2026-06-28 14:44:26,838 - INFO - Report Date: 2025-12-31
2026-06-28 14:44:26,838 - INFO - REPTMON: 12, REPTYEAR: 25
2026-06-28 14:44:26,838 - INFO - SDESC: PUBLIC BANK BERHAD
2026-06-28 14:44:26,852 - INFO - REPTDATE saved to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQFAR2
2026-06-28 14:44:26,852 - INFO - Processing TRUST data from cisdepxn.sas7bdat
2026-06-28 14:44:26,852 - INFO - Reading SAS file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQFAR2/cisdepxn.sas7bdat
2026-06-28 14:55:10,526 - INFO - Successfully read cisdepxn.sas7bdat: 6,117,754 rows, 166 columns
2026-06-28 14:56:56,374 - INFO - TRUST records: 3,591
2026-06-28 14:56:56,384 - INFO - Processing deposit data from cisdepd.sas7bdat
2026-06-28 14:56:56,400 - INFO - Reading SAS file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQFAR2/cisdepd.sas7bdat
2026-06-28 15:01:13,382 - INFO - Successfully read cisdepd.sas7bdat: 7,733,240 rows, 104 columns
2026-06-28 15:01:16,824 - INFO - DEPOSIT records: 7,733,240
2026-06-28 15:01:16,848 - INFO - Applying format mappings using replace() method
2026-06-28 15:01:20,855 - INFO - RPT_BASE records: 7,736,831
2026-06-28 15:01:21,665 - ERROR - Error in main processing: type String is incompatible with expected type Float64
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQFAR2_CONV_INSURANCE.py", line 394, in main
    final_summary = pl.concat([summary, total_summary], how="diagonal")
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 247, in concat
    out = wrap_df(plr.concat_df_diagonal(elems))
polars.exceptions.SchemaError: type String is incompatible with expected type Float64
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQFAR2_CONV_INSURANCE.py", line 407, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQFAR2_CONV_INSURANCE.py", line 394, in main
    final_summary = pl.concat([summary, total_summary], how="diagonal")
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 247, in concat
    out = wrap_df(plr.concat_df_diagonal(elems))
polars.exceptions.SchemaError: type String is incompatible with expected type Float64


for the sake of testing, since it takes quite some times to test, please read the file less (rows/obs) just for testing
