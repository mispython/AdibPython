Running EIBDCITX for 29/06/2026 (WK=4) - Processing YESTERDAY'S data
================================================================================

Loading input files...
  ✓ Loaded DPFL: 89,493 rows from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/DPFL.txt
  ✓ Loaded EQFL: 420 rows from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/UTSASDCID_20260629.txt
  ✓ Loaded CRA: 1 rows from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/DPCRATXT_20260629
  ✓ Loaded EQRATE: 57 rows from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/eqrate260629.sas7bdat
  ✓ Loaded MNITB Saving: 6,634,478 rows
  ✓ Loaded MNITB Current: 1,118,698 rows
  ✓ Loaded DCID: 210 rows from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/dcid0629.sas7bdat

================================================================================

Processing DPST...
  DPST after merge: 89,493 rows

Processing EQ data...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDCITX.py", line 625, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDCITX.py", line 377, in main
    eq = eqfl.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "ACCINTRM"; valid columns: ["             ", "     29062026", "   ", "                                   ", "          ", "  ", "  _duplicated_0", "      ", "   _duplicated_0", "   _duplicated_1", "   _duplicated_2", "   _duplicated_3", "   _duplicated_4", "            .00", "            .00_duplicated_0", "            .00_duplicated_1", "          _duplicated_0", "          _duplicated_1", "          _duplicated_2", "          _duplicated_3", "          _duplicated_4", "    0", "   .0000000", "   .0000000_duplicated_0", "      .0000", "             .00", "             .00_duplicated_0", "             .00_duplicated_1", "      .0000_duplicated_0", "   .0000000_duplicated_1", "            .00_duplicated_2", "            .00_duplicated_3", "            .00_duplicated_4", "            .00_duplicated_5", "            .00_duplicated_6", "                    ", " ", "                    _duplicated_0"]
