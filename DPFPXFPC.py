Step 1: Setting report date...
Report Date: 2026-09-08 18:45:21.402995, RDATE: 24357
Step 2: Processing credit facility table...
First few lines of crftabl.txt:
Line 0: '1BKT20260831'
Line 1: 'PBF        1SGXX               JSS/000587/06                                                                                                                   +0000000009000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00N1           2500001815'
Line 2: 'PBF        1SGLX               JSS/000325/95                                                                                                                   +0000000009000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00N0           2500001912'
Line 3: 'PBF        1LCB2               TDA/000011/06                                                                                                                   +0000000009000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00N0           2500003708'
Line 4: 'PBF        1BAX2               JSS/000071/98                                                                                                                   +0000000005300.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00N0           2500003902'
Step 3: Merging with master account data...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBTNPGS.py", line 151, in <module>
    crft_merged = crft_data.join(mast_data, on='ACCTNO', how='inner')
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
polars.exceptions.SchemaError: datatypes of join keys don't match - `ACCTNO`: i64 on left does not match `ACCTNO`: f64 on right (and no other type was available to cast to)
