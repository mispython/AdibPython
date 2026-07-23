============================================================
EIMAR301 SAS to Python Conversion - Multi-Report System
============================================================

1. Processing REPTDATE with previous month (datetime/timedelta)...
   Current Date: 230726
   Previous Month Date: 2026-06-01

2. Loading and filtering HP Direct loans...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR301.py", line 750, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR301.py", line 644, in main
    filtered_loans = load_and_filter_loans(HPD_LIST, variables)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR301.py", line 204, in load_and_filter_loans
    merged = filtered.join(
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
You have mail in /var/spool/mail/sas_edw_dev


btw it is EIMIR301 not EIMAR301. anything you need from my side to verify?
