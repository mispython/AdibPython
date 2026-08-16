Starting EIQPROM2 processing...
Report Date: 31/07/26
Report Month: 07

Step 1: Loading and filtering PROMOTE.LOAN data...
  Records in RLSLIST: 55773

Step 2: Processing PBB data...
  Records in PBBNAME after merge: 50173
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIEMCRLS.py", line 190, in <module>
    pbbname = pbbname.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "NEWIC"; valid columns: ["NAMELN1", "NAMELN2", "NAMELN3", "NAMELN4", "NAMELN5", "ACCTNO", "SECPHONE", "PRIPHONE"]
