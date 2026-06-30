Running EIBDCITX for 29/06/2026 (WK=4) - Processing YESTERDAY'S data
================================================================================

Loading input files...
  ✓ Loaded DPFL: 89,493 rows from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/DPFL.txt
  ✓ Loaded EQFL: 421 rows from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/UTSASDCID_20260629.txt
  ✓ Loaded CRA: 1 rows from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/DPCRATXT_20260629
  Note: pyreadstat doesn't support column selection, loading full file then filtering...
  ✓ Loaded EQRATE: 57 rows from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/eqrate260629.sas7bdat
  Loading MNITB Saving...
  Converting SAS to Parquet (first time, this may take a moment)...
  Note: pyreadstat doesn't support column selection, loading full file then filtering...
  ✓ Converted and cached to Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/cache/intg_dp_acct_saving.parquet
  ✓ Loaded MNITB Saving: 6,634,478 rows (2 columns)
  Loading MNITB Current...
  Converting SAS to Parquet (first time, this may take a moment)...
  Note: pyreadstat doesn't support column selection, loading full file then filtering...
  ✓ Converted and cached to Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/cache/intg_dp_acct_current.parquet
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
  DEPO combined: 7,753,176 rows
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDCITX.py", line 795, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDCITX.py", line 590, in main
    dp_cra = dp_cra.join(depo, on="INVCURAC", how="inner")
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 128, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 8242, in join
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.SchemaError: datatypes of join keys don't match - `INVCURAC`: null on left does not match `INVCURAC`: f64 on right (and no other type was available to cast to)
