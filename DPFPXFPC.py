OWK: 4, REPTMON: 05, REPTYEAR: 2026
SDESC: PUBLIC BANK BERHAD
REPTDATE: 2026-05-31
SDATE: 2026-05-23

Reading binary flat file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQPIUC/MAREMUC5
Successfully parsed 5353 records
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQPIUC.py", line 356, in <module>
    df = df.filter(pl.all().is_not_null())
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 5325, in filter
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ComputeError: The predicate passed to 'LazyFrame.filter' expanded to multiple expressions: 

        col("ACCTNO").is_not_null(),
        col("LEDGBAL").is_not_null(),
        col("STATUS").is_not_null(),
        col("PAYMODE").is_not_null(),
        col("NAME").is_not_null(),
This is ambiguous. Try to combine the predicates with the 'all' or `any' expression.
