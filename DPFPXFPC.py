SAS Connection established. Subprocess id is 3139027

REPTMON: 07, REPTMON1: 06, RDATE: 310726
Reading SAS7BDAT files...
LN columns: 29, DP columns: 29
All columns: ['curbal', 'costctr', 'accrual', 'balance', 'product', 'censust', 'sch', 'cinstcl', 'natguar', 'tranche', 'cvar02', 'cvar01', 'cvar06', 'cvar03', 'cvar04', 'cvar14', 'cvar13', 'cvar08', 'cvar09', 'cvar10', 'cvar17', 'cvar11', 'cvar05', 'cvar07', 'cvar12', 'cvar15', 'branch', 'cvar16', 'cr']
LN schema: Schema([('curbal', Float64), ('costctr', Float64), ('accrual', Float64), ('balance', Float64), ('product', Float64), ('censust', Float64), ('sch', String), ('cinstcl', String), ('natguar', String), ('tranche', String), ('cvar02', String), ('cvar01', Float64), ('cvar06', Float64), ('cvar03', String), ('cvar04', String), ('cvar14', String), ('cvar13', String), ('cvar08', Float64), ('cvar09', Float64), ('cvar10', Float64), ('cvar17', Float64), ('cvar11', Float64), ('cvar05', Float64), ('cvar07', String), ('cvar12', String), ('cvar15', String), ('branch', Float64), ('cvar16', String), ('cr', String)])
DP schema: Schema([('curbal', Float64), ('costctr', Float64), ('accrual', Float64), ('balance', Float64), ('product', Float64), ('censust', Float64), ('sch', String), ('cinstcl', String), ('natguar', String), ('tranche', String), ('cvar02', String), ('cvar01', Float64), ('cvar06', Float64), ('cvar03', String), ('cvar04', String), ('cvar14', String), ('cvar13', String), ('cvar08', Float64), ('cvar09', Float64), ('cvar10', Float64), ('cvar17', Float64), ('cvar11', Float64), ('cvar05', Float64), ('cvar07', String), ('cvar12', String), ('cvar15', String), ('branch', Float64), ('cvar16', String), ('cr', String)])
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRSMEZ.py", line 327, in <module>
    eibrsmez()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRSMEZ.py", line 122, in eibrsmez
    npgs_df = npgs_df.with_columns(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "NP"; valid columns: ["curbal", "costctr", "accrual", "balance", "product", "censust", "sch", "cinstcl", "natguar", "tranche", "cvar02", "cvar01", "cvar06", "cvar03", "cvar04", "cvar14", "cvar13", "cvar08", "cvar09", "cvar10", "cvar17", "cvar11", "cvar05", "cvar07", "cvar12", "cvar15", "branch", "cvar16", "cr"]
SAS Connection terminated. Subprocess id was 3139027
