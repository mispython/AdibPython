REPTMON: 07, RDATE: 310726
Read 771 records from lntrrf07.sas7bdat
Available columns: ['curbal', 'costctr', 'accrual', 'balance', 'product', 'censust', 'cinstcl', 'natguar', 'cgcgur', 'tranche', 'sch', 'cvar02', 'cvar01', 'cvar06', 'cvar03', 'cvar04', 'cvar14', 'cvar13', 'cvar08', 'cvar09', 'cvar10', 'cvar17', 'cvar11', 'cvar05', 'cvar07', 'cvar12', 'cvar15', 'branch', 'cvar16']
TRRF dataset created with 31 records
NPGS dataset processed with 771 records
Processing 576 records for SCH=7q
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRTRRF.py", line 344, in <module>
    eibrtrrf()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRTRRF.py", line 113, in eibrtrrf
    process_scheme(npgs_df, "7q", rdate, output)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRTRRF.py", line 182, in process_scheme
    npgs5_df = npgs5_df.rename({"cvar17_clean": "cvar17"})
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 5079, in rename
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.DuplicateError: column 'cvar17' is duplicate
