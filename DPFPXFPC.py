============================================================
EIBAABBA - Account Analysis Report
============================================================
Input path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod
Output path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBAABBA/ABBALST.txt
*** TEST MODE - Row limit: 1000 per dataset ***

Report Date: 12/07/2026
Snapshot Date: 12/07/2026
Week: 4, SDD: 23
------------------------------------------------------------
Reading LNNOTE data...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBAABBA.py", line 535, in <module>
    eibaabba()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBAABBA.py", line 441, in eibaabba
    abba_df = process_abba_data(input_path, snapshot_date, test_limit)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBAABBA.py", line 226, in process_abba_data
    abba_df.filter(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 5325, in filter
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.InvalidOperationError: 'is_in' cannot check for List(Int64) values in String data
