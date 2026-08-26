Using SAS Config named: default
SAS Connection established. Subprocess id is 2161274

REPTMON: 07, RDATE: 310726
Available columns: ['curbal', 'costctr', 'accrual', 'balance', 'product', 'censust', 'sch', 'cinstcl', 'natguar', 'tranche', 'cvar02', 'cvar01', 'cvar06', 'cvar03', 'cvar04', 'cvar14', 'cvar13', 'cvar08', 'cvar09', 'cvar10', 'cvar11', 'cvar05', 'cvar07', 'cvar12', 'cvar15', 'branch']
/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRSMEE.py", line 244, in <module>
    eibrsmee()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRSMEE.py", line 126, in eibrsmee
    npgs_df = npgs_df.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "NP"; valid columns: ["curbal", "costctr", "accrual", "balance", "product", "censust", "sch", "cinstcl", "natguar", "tranche", "cvar02", "cvar01", "cvar06", "cvar03", "cvar04", "cvar14", "cvar13", "cvar08", "cvar09", "cvar10", "cvar11", "cvar05", "cvar07", "cvar12", "cvar15", "branch", "cvarxx"]
SAS Connection terminated. Subprocess id was 2161274
