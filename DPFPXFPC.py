SAS Connection established. Subprocess id is 3097187

REPTMON: 07, REPTMON1: 06, RDATE: 310726
Reading SAS7BDAT files...
LN columns: 29, DP columns: 29
All columns: ['curbal', 'costctr', 'accrual', 'balance', 'product', 'censust', 'sch', 'cinstcl', 'natguar', 'tranche', 'cvar02', 'cvar01', 'cvar06', 'cvar03', 'cvar04', 'cvar14', 'cvar13', 'cvar08', 'cvar09', 'cvar10', 'cvar17', 'cvar11', 'cvar05', 'cvar07', 'cvar12', 'cvar15', 'branch', 'cvar16', 'cr']
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRSMEZ.py", line 296, in <module>
    eibrsmez()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRSMEZ.py", line 72, in eibrsmez
    smez_df = pl.concat([ln_df, dp_df])
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 234, in concat
    out = wrap_df(plr.concat_df(elems))
polars.exceptions.SchemaError: type String is incompatible with expected type Null
SAS Connection terminated. Subprocess id was 3097187
