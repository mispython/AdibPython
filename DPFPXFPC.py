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
┌────────┬───────────────────┬────────────┬─────────────────┬───┬───────┬────────────┬────────┬──────────────┐
│ BRANCH ┆ CUSTICKETNO       ┆ INVCURAC   ┆ CUSTNAME        ┆ … ┆ TENOR ┆ INV_STATUS ┆ ACCINT ┆ CUSTCODE_DB2 │
│ ---    ┆ ---               ┆ ---        ┆ ---             ┆   ┆ ---   ┆ ---        ┆ ---    ┆ ---          │
│ str    ┆ str               ┆ i64        ┆ str             ┆   ┆ i64   ┆ str        ┆ f64    ┆ i64          │
╞════════╪═══════════════════╪════════════╪═════════════════╪═══╪═══════╪════════════╪════════╪══════════════╡
│ 156    ┆ TMR/CRA001/000001 ┆ 3124483820 ┆ THEI CHIEW YONG ┆ … ┆ 5     ┆ ACT        ┆ 0.0    ┆ 78           │
│ 156    ┆ TMR/CRA001/000002 ┆ 5105722033 ┆ CHONG CHEE YEN  ┆ … ┆ 5     ┆ ACT        ┆ 0.0    ┆ 78           │
└────────┴───────────────────┴────────────┴─────────────────┴───┴───────┴────────────┴────────┴──────────────┘
  Available INV_STATUS values: ['ACT', 'CCC', 'CES']
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
  CRA after status filter: 1,530 rows
  CRA after DEPO join: 34 rows
  EQDCI after join: 5 rows
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDCITX.py", line 932, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDCITX.py", line 636, in main
    eqdci = pl.concat([dp_cra, eqdci])
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 234, in concat
    out = wrap_df(plr.concat_df(elems))
polars.exceptions.ShapeError: unable to append to a DataFrame of width 16 with a DataFrame of width 21
