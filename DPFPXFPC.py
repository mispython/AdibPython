============================================================
EIIDLCRM - BNM LCR Reporting (Islamic Banking)
============================================================
SAS Connection established. Subprocess id is 3894293


Date: 05/10/2026 Week:1 Mon:10
Template: 70 items
  CIS equity warning: File /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/ciscustdly.sas7bdat does not exist!
CIS: 0 records

Treasury...
  K1/K3 warning: File /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/lcrktblall.sas7bdat does not exist!
  UTSAS warning: File /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/equaiutms261005.sas7bdat does not exist!
  Treasury: 0 records

Banking...
  fd warning: File /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/lcrfd05.sas7bdat does not exist!
  sa warning: File /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/lcrsa05.sas7bdat does not exist!
  ca warning: File /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/lcrca05.sas7bdat does not exist!
  fcyca warning: File /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/lcrfcyca05.sas7bdat does not exist!
  Banking: 0 records

Insurance split...
Total: 0 records
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIDLCRM.py", line 749, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIDLCRM.py", line 624, in main
    df = df.with_columns([(pl.col('amt') / 1000).round(2).alias('amt_k')])
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "amt"; valid columns: []
SAS Connection terminated. Subprocess id was 3894293
