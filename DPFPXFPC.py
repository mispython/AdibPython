Processing date: 2026-08-31 (SAS date: 24349)
Reading LNNOTE in chunks...
Filtered LNNOTE rows: 0
Available columns: ['ACCTNO', 'NAME', 'TAXNO', 'ORGTYPE', 'GUAREND', 'BANKNO', 'APPCODE', 'ACCBRCH', 'CUSTCODE', 'PURPOSE']...
WARNING: No data in LNNOTE after filtering!
Check if filters are correct or if file has data
Reading LNCOMM in chunks...
LNCOMM rows after filter: 1066036
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBLTRRF.py", line 418, in <module>
    lncomm
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "INTAMT"; valid columns: ["ACCTNO", "COMMNO", "CORGAMT"]
