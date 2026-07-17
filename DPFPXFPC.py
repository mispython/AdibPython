Reporting Date: 2026-07-16
Report Period: Week 2, Month 07
Reading files: rep2072, rep4072, ELW072
Reading: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWKAPE/rep2072.sas7bdat
Warning: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWKAPE/rep2072.sas7bdat not found or empty
Reading: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWKAPE/rep4072.sas7bdat
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWKAPE.py", line 194, in <module>
    rep2_TRANSFORMED = rep2_COMBINED.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "NETAMT"; valid columns: ["ELDAY", "BNMCODE", "UTSTY", "UTREF", "AMOUNT"]



rep2, rep4 and elw should be in lowercase
