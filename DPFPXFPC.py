Running EIBDCITX for 29/06/2026 (WK=4) - Processing YESTERDAY'S data
================================================================================

Loading input files...
  ✓ Loaded DPFL: 89,493 rows from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/DPFL.txt
  ✓ Loaded EQFL: 421 rows from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/UTSASDCID_20260629.txt
  ✓ Loaded CRA: 1 rows from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/DPCRATXT_20260629
  Note: pyreadstat doesn't support column selection, loading full file then filtering...
  ✓ Loaded EQRATE: 57 rows from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/eqrate260629.sas7bdat
  Loading MNITB Saving...
  ✓ Loading from Parquet cache: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/cache/intg_dp_acct_saving.parquet
  ✓ Loaded MNITB Saving: 6,634,478 rows (2 columns)
  Loading MNITB Current...
  ✓ Loading from Parquet cache: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/cache/intg_dp_acct_current.parquet
  ✓ Loaded MNITB Current: 1,118,698 rows (2 columns)
  Note: pyreadstat doesn't support column selection, loading full file then filtering...
  ✓ Loaded DCID: 210 rows from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/dcid0629.sas7bdat

================================================================================

Processing DPST...
  DPST after merge: 89,493 rows

Processing EQ data...
  EQ after date filter: 300 rows
  EQC: 150 rows, EQI: 150 rows

Processing Customer Leg...
  EQDCI after join: 5 rows
  Note: No CRA records with valid status, creating empty CRA DataFrame
  DEPO combined: 7,753,176 rows
  Note: No CRA or DEPO data to join, skipping CRA processing
  Combined EQDCI: 5 rows
  Customer MYR: 4 rows, FCY: 1 rows

Writing customer text output to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/DCITXT.txt...

Processing Interbank Leg...
  Interbank MYR: 0 rows, FCY: 0 rows
  ✓ Text output written to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/DCITXT.txt

Building DCI final output...
  DCI final: 2 aggregated records

Writing output files...
  ✓ Parquet written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/DCI_260629.parquet
  ✗ Could not write SAS dataset: module 'pyreadstat' has no attribute 'write_sas7bdat'
  ✓ CSV written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/DCI_260629.csv

================================================================================
EIBDCITX completed successfully for 29/06/2026 (Yesterday's data)!
================================================================================
