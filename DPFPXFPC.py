NOWK: 4, REPTMON: 12, REPTYEAR: 2025
SDESC: PUBLIC BANK BERHAD
REPTDATE: 2025-12-31
SDATE: 2025-12-23

Loading SAS7BDAT files:
  UNCLAIM: unclaim2025.sas7bdat
  NOTUNCLAIM: notunclaim2025.sas7bdat
Loaded unclaim2025.sas7bdat with 1979 records
Loaded notunclaim2025.sas7bdat with 2613 records
Combined dataset has 4592 records
UNCLAIM records: 1979
NONDEBIT records: 2613
Saved UNCLAIM with 1979 records
Saved NONDEBIT with 2613 records
Saved UNCLAIM_FINAL with 1979 records

Looking for SAS7BDAT files:
  SAVG: savg124.sas7bdat
  CURN: curn124.sas7bdat
  ISAVG: savg124.sas7bdat
  ICURN: curn124.sas7bdat
Loaded savg124.sas7bdat with 4241108 records
Loaded curn124.sas7bdat with 915692 records
Loaded savg124.sas7bdat with 2262899 records
Loaded curn124.sas7bdat with 915692 records
Combined DEP dataset has 8335391 records
After filtering PRODCD: 8135928 records
Saved DEP with 7284287 records
UNCLAIM for merge has 1979 records
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQUCLM.py", line 319, in <module>
    dep_merged = dep_deduped.join(unclaim_for_merge, on='ACCTNO', how='right', suffix='_unclaim')
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
polars.exceptions.SchemaError: datatypes of join keys don't match - `ACCTNO`: f64 on left does not match `ACCTNO`: i64 on right (and no other type was available to cast to)
