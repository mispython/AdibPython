============================================================
EIMIR103 SAS to Python Conversion - NPL Report
============================================================

1. Setting report date (yesterday)...
   Report Date: 250726
   Report Date (display): 25/07/26

2. Loading loan data from loantemp.sas7bdat...
   Loaded 663747 loan records
   Columns: ['ACCTNO', 'NOTENO', 'CAP', 'NAME', 'LSTTRNCD', 'CURBAL', 'COLLDESC', 'CENSUS', 'ORGBAL', 'FEEDUE']...
   Total loans: 663747

3. Loading branch data from LKP_BRANCH...
   Warning: Could not parse LKP_BRANCH: found more fields than defined in 'Schema'

Consider setting 'truncate_ragged_lines=True'.
   Total branches: 0

4. Categorizing NPL loans...
   NPL candidates: 1858

5. Merging with branch data...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR103.py", line 641, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR103.py", line 542, in main
    merged_npl = npl_categorized.join(
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
