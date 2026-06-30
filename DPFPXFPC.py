Running EIBDCITX for 29/06/2026 (WK=4)
================================================================================

Loading input files...
  ✓ Loaded DPFL: 89,493 rows
  ✓ Loaded EQFL: 421 rows
  Loading CRA from: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/DPCRATXT_20260629
  File size: 1,472,346 bytes
  Record length: 942 bytes
  Found 1,563 records
  Parsed 1,563 CRA records
  ✓ Loaded CRA: 1,563 rows
  CRA sample data (first 2 rows):
shape: (2, 12)
┌────────┬─────────────────────────────────┬────────────┬─────────────────────────────────┬───┬───────┬────────────┬────────┬──────────────┐
│ BRANCH ┆ CUSTICKETNO                     ┆ INVCURAC   ┆ CUSTNAME                        ┆ … ┆ TENOR ┆ INV_STATUS ┆ ACCINT ┆ CUSTCODE_DB2 │
│ ---    ┆ ---                             ┆ ---        ┆ ---                             ┆   ┆ ---   ┆ ---        ┆ ---    ┆ ---          │
│ str    ┆ str                             ┆ i64        ┆ str                             ┆   ┆ i64   ┆ str        ┆ f64    ┆ null         │
╞════════╪═════════════════════════════════╪════════════╪═════════════════════════════════╪═══╪═══════╪════════════╪════════╪══════════════╡
│        ┆ aa@@@@@@@@@@@@@@@@@@@@@@@@@@@@… ┆ 3124483820 ┆ @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@… ┆ … ┆ 5     ┆            ┆ 0.0    ┆ null         │
│        ┆ aa@@@@@@@@@@@@@@@@@@@@@@@@@@@@… ┆ 5105722033 ┆ @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@… ┆ … ┆ 5     ┆            ┆ 0.0    ┆ null         │
└────────┴─────────────────────────────────┴────────────┴─────────────────────────────────┴───┴───────┴────────────┴────────┴──────────────┘
  Available INV_STATUS values: ['']
  Note: pyreadstat doesn't support column selection, loading full file then filtering...
  ✓ Loaded EQRATE: 57 rows
  ✓ Loaded MNITB Saving: 6,634,478 rows
  ✓ Loaded MNITB Current: 1,118,698 rows
  Note: pyreadstat doesn't support column selection, loading full file then filtering...
  ✓ Loaded DCID: 210 rows (only TICKETNO, CUSTCODE)

================================================================================

Processing DPST...
  DPST after merge: 89,493 rows

Processing EQ data...
  EQ after date filter: 300 rows
  EQC: 150 rows, EQI: 150 rows

Processing Customer Leg...
  CRA after status filter: 0 rows
  Warning: No CRA records with valid status
  Available INV_STATUS values: ['']
  EQDCI after join: 5 rows
  Using only EQDCI data: 5 rows
  Customer MYR: 4 rows, FCY: 1 rows

Processing Interbank Leg...
  Interbank MYR: 0 rows, FCY: 0 rows

Writing DCITXT output to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/DCITXT.txt...
  ✓ DCITXT written to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/DCITXT.txt

Building DCI final output...
  Combined data for DCI: 4 rows
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDCITX.py", line 904, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDCITX.py", line 817, in main
    dcimyr = dcimyr.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "PREMREC"; valid columns: ["TICKETNO", "CUSTNAME", "NEWIC", "CUSTCODE", "INVCURAC", "ALTCURAC", "ACCINT", "CUSTCODE_right", "CUSTICKETNO", "BRANCH", "INVCURR", "ALTCURR", "INVAMT", "ALTAMT", "TENOR", "STATUSIND", "DCIRT", "STARTDT", "MATDT", "PREMPAID", "TYPE", "SPOTRT", "ACCINTRM", "PREMPAIDRM"]
