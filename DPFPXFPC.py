2026-06-28 16:27:14,107 - INFO - Format mappings created successfully
2026-06-28 16:27:14,108 - INFO - Report Date: 2025-12-31
2026-06-28 16:27:14,108 - INFO - REPTMON: 12, REPTYEAR: 25
2026-06-28 16:27:14,108 - INFO - SDESC: PUBLIC BANK BERHAD
2026-06-28 16:27:14,113 - INFO - REPTDATE saved to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQFAR2
2026-06-28 16:27:14,113 - INFO - Processing TRUST data from cisdepxn.sas7bdat
2026-06-28 16:27:14,113 - INFO - Reading SAS file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQFAR2/cisdepxn.sas7bdat
2026-06-28 16:37:59,348 - INFO - Successfully read cisdepxn.sas7bdat: 6,117,754 rows, 166 columns
2026-06-28 16:38:49,715 - INFO - TRUST records: 3,591
2026-06-28 16:38:49,722 - INFO - Processing deposit data from cisdepd.sas7bdat
2026-06-28 16:38:49,731 - INFO - Reading SAS file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQFAR2/cisdepd.sas7bdat
2026-06-28 16:44:35,610 - INFO - Successfully read cisdepd.sas7bdat: 7,733,240 rows, 104 columns
2026-06-28 16:44:38,870 - INFO - DEPOSIT records: 7,733,240
2026-06-28 16:44:42,649 - INFO - RPT_BASE records: 7,736,831
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQFAR2_CONV_INSURANCE.py:177: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  pivot_table = summary_df.pivot(
2026-06-28 16:44:43,573 - INFO - TXT report saved to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQFAR2/EIBQFAR2_CONVENTIONAL_REPORT.txt
2026-06-28 16:44:43,575 - INFO - Parquet files saved to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQFAR2
2026-06-28 16:44:43,576 - INFO - PROCESSING COMPLETED SUCCESSFULLY
