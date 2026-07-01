Running EIBDCITX for 29/06/26 (WK=4)
================================================================================

Loading input files...
  ✓ Loaded DPFL: 89,493 rows
  ✓ Loaded EQFL: 421 rows
  ✓ Loaded CRA: 1,563 rows
  Note: pyreadstat doesn't support column selection, loading full file then filtering...
  ✓ Loaded EQRATE: 57 rows
  Loading MNITB Saving...
  ✓ Loading from Parquet cache: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/cache/intg_dp_acct_saving.parquet
  ✓ Loaded MNITB Saving: 6,634,478 rows
  Loading MNITB Current...
  ✓ Loading from Parquet cache: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/cache/intg_dp_acct_current.parquet
  ✓ Loaded MNITB Current: 1,118,698 rows
  Note: pyreadstat doesn't support column selection, loading full file then filtering...
  ✓ Loaded DCID: 210 rows

================================================================================

Processing DPST...
  DPST after merge: 89,493 rows

Processing EQ data...
  EQ after date filter: 300 rows
  EQC: 150 rows, EQI: 150 rows

Processing Customer Leg...
  EQDCI after join: 5 rows
  DEPO combined: 7,753,176 rows
  CRA after processing: 34 rows
  Combined EQDCI: 39 rows
  Customer MYR: 38 rows, FCY: 1 rows

Processing Interbank Leg...
  Interbank MYR: 0 rows, FCY: 0 rows

Writing DCITXT output to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/DCITXT.txt...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDCITX.py", line 888, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDCITX.py", line 743, in main
    write_cus_row(f, obs, row)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDCITX.py", line 417, in write_cus_row
    f"{obs:>4} "
ValueError: Unknown format code 'f' for object of type 'str'
