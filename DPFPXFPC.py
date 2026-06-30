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
  ✓ Loaded MNITB Saving: 6,634,478 rows
  Loading MNITB Current...
  ✓ Loading from Parquet cache: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/cache/intg_dp_acct_current.parquet
  ✓ Loaded MNITB Current: 1,118,698 rows
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
  Note: No CRA records with valid status
  DEPO combined: 7,753,176 rows
  Note: No CRA or DEPO data to join
  Combined EQDCI: 5 rows
  Customer MYR: 4 rows, FCY: 1 rows

Processing Interbank Leg...
  Interbank MYR: 0 rows, FCY: 0 rows

Writing DCITXT output to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/DCITXT.txt...
  ✓ DCITXT written to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/DCITXT.txt

Building DCI final output...
  Combined data for DCI: 4 rows
  DCI final: 2 aggregated records

Writing output files...
  ✓ Parquet written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/DCI_260629.parquet

Writing SAS dataset to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/BNMK_DCI064.sas7bdat...
  Connecting to SAS session...
Using SAS Config named: default
SAS Connection established. Subprocess id is 976298

  Writing SAS dataset: BNMK_DCI064...
The libref specified is not assigned in this SAS Session.
/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
  ✓ SAS dataset written using saspy: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/BNMK_DCI064.sas7bdat
  ✓ CSV written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/DCI_260629.csv

================================================================================
EIBDCITX completed successfully for 29/06/2026 (Yesterday's data)!
================================================================================
SAS Connection terminated. Subprocess id was 976298
