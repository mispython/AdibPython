Current date: 2026-08-05
Report date (yesterday): 2026-08-04
Expected date format (DDMMYY): 040826

Reading GL file...
File date: 2026-07-31 (DDMMYY: 310726)
Expected date (DDMMYY): 040826
WARNING: GL file extraction date (310726) does not match expected date (040826)
Using file date for processing...

Processing 9 data lines
Line 3: Could not parse balance '31/07/264929416.43'
Line 4: Could not parse balance '31/07/26198312089.38'
Line 6: Not enough parts (1): -
Line 8: Could not parse balance '31/07/260.00'

Created DataFrame with 4 records
Sample GLITEMs: ['S-TLF', 'S-REMISIERFD', 'S-GUARANTEE', 'S-RCF']

First few rows:
shape: (4, 4)
┌──────────────┬──────────┬──────┬───────────┐
│ GLITEM       ┆ DATEX    ┆ SIGN ┆ BALANCE   │
│ ---          ┆ ---      ┆ ---  ┆ ---       │
│ str          ┆ str      ┆ str  ┆ f64       │
╞══════════════╪══════════╪══════╪═══════════╡
│ S-TLF        ┆ 31/07/26 ┆ +    ┆ 2.4499e8  │
│ S-RCF        ┆ 31/07/26 ┆ +    ┆ 4.43539e7 │
│ S-GUARANTEE  ┆ 31/07/26 ┆ +    ┆ 5.7e6     │
│ S-REMISIERFD ┆ 31/07/26 ┆ +    ┆ 0.0       │
└──────────────┴──────────┴──────┴───────────┘
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIVMNLGL.py", line 233, in <module>
    df_gl
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/lazy.py", line 1088, in __call__
    rv = self.function(slp, *args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/expr/expr.py", line 4655, in _wrap
    return function(sl[0], *args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/expr/expr.py", line 4879, in wrap_f
    return x.map_elements(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/series/series.py", line 5838, in map_elements
    self._s.map_elements(
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIVMNLGL.py", line 235, in <lambda>
    pl.col('DATEX').map_elements(lambda x: ddmmyy8_to_date(str(x)), return_dtype=pl.Datetime).alias('DATE'),
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIVMNLGL.py", line 28, in ddmmyy8_to_date
    dd, mm, yy2 = int(s[0:2]), int(s[2:4]), int(s[4:6])
ValueError: invalid literal for int() with base 10: '/0'
