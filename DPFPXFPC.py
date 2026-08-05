EIBMCOSR: Starting cost analysis processing...
  Report date: 04/08/26, Period: 01/08/26 to 04/08/26, Months: 8 to 8
  Loaded rate data: 0 rows
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMCOSR.py", line 1269, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMCOSR.py", line 1217, in main
    totsum, missname, except_df = process_main(rv, rate)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMCOSR.py", line 248, in process_main
    totsum = totsum.sort('trandt')
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 5938, in sort
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "trandt"; valid columns: []
