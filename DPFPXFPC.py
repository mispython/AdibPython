============================================================
EIMIR102 SAS to Python Conversion
============================================================

1. Processing REPTDATE...
   Report Date: 250726
   Day of Month: 25

2. Loading data...
   Loans: 663747, Branches: 376
   Branch columns: ['BRHCODE', 'BRANCH']

3. Categorizing loans...
   Categorized records: 389474

4. Merging with branch data...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR102.py", line 573, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR102.py", line 503, in main
    merged_data = categorized.join(
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
polars.exceptions.SchemaError: datatypes of join keys don't match - `BRANCH`: f64 on left does not match `BRANCH`: i64 on right (and no other type was available to cast to)
