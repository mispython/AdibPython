Loaded FDMTHLY with 431257 records
Saved FDMTHLY_SORTED with 431257 records
Saved FDMTHLY with 431257 records
Loaded CURN124 with 154763 records
Saved CURN with 154757 records (after filtering)
Loaded SAVG124 with 2262899 records
Added CURN with 154757 records
Added FDMTHLY with 431257 records
Combined DEPOSIT dataset has 2848913 records
DEPOSIT records after filtering: 2848614
Loaded FLOAT with 18927 records
Saved FLOAT with 18927 records
FLOAT summary records: 18927
Merged DEPOSIT with FLOAT: 18927 records
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIPCIFLO.py", line 254, in <module>
    deposit_processed = deposit_processed.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 8701, in drop
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: "new_float" not found
