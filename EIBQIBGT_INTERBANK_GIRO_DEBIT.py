==================================================
TEST MODE ENABLED - Using hardcoded test date
==================================================
TEST DATE: 2025-12-23
NOWK: 4, REPTMON: 12, REPTYEAR: 2025
SDESC: PUBLIC BANK BERHAD
==================================================

Reading IBG file from: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/MNI/IBG_YEAREND.txt
AttributeError with str.strip(): 'ExprStringNameSpace' object has no attribute 'strip'
Trying alternative approach without strip...
IBG records (alternative): 16467
NONDEBIT records (alternative): 259

Looking for SAS files with pattern: 124 (December Week 4)
Expected files: savg124.sas7bdat, curn124.sas7bdat
SAVG records: 4241108
CURN records: 915692
ISAVG records: 2262899
ICURN records: 915692
DEP records: 7284287
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQIBGT.py", line 347, in <module>
    dep_merged = dep_deduped.join(ibg_for_merge, on='ACCTNO', how='right', suffix='_ibg')
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
