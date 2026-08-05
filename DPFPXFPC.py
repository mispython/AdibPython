Current date: 2026-08-05
Report date (yesterday): 2026-08-04
Expected date format (DDMMYY): 040826

Reading GL file...
File date: 2026-07-31 (DDMMYY: 310726)
Expected date (DDMMYY): 040826
WARNING: GL file extraction date (310726) does not match expected date (040826)
Using file date for processing...

Processing 17 data lines
Line 6: Not enough parts (1): -
Line 16: Not enough parts (1): -

Created DataFrame with 13 records
Sample GLITEMs: ['F249120BP', '137070', 'F132121BBNM', 'F147100', 'F142199C', '132110', 'F142199D', 'F144111', '139110', 'F142199E']

First few rows:
shape: (5, 4)
┌─────────────┬──────────┬──────┬───────────┐
│ GLITEM      ┆ DATEX    ┆ SIGN ┆ BALANCE   │
│ ---         ┆ ---      ┆ ---  ┆ ---       │
│ str         ┆ str      ┆ str  ┆ f64       │
╞═════════════╪══════════╪══════╪═══════════╡
│ 137070      ┆ 31/07/26 ┆ +    ┆ 0.0       │
│ 132110      ┆ 31/07/26 ┆ +    ┆ 1.0089e9  │
│ 139110      ┆ 31/07/26 ┆ +    ┆ 1.6607e7  │
│ 149120      ┆ 31/07/26 ┆ -    ┆ 570339.41 │
│ F132121BBNM ┆ 31/07/26 ┆ +    ┆ 1.0154e7  │
└─────────────┴──────────┴──────┴───────────┘
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIMNLGL.py", line 229, in <module>
    DETAIL = df_gl.with_columns([
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
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIMNLGL.py", line 230, in <lambda>
    pl.col('DATEX').map_elements(lambda x: ddmmyy8_to_date(str(x)), return_dtype=pl.Datetime).alias('DATE'),
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIMNLGL.py", line 29, in ddmmyy8_to_date
    dd, mm, yy2 = int(s[0:2]), int(s[2:4]), int(s[4:6])
ValueError: invalid literal for int() with base 10: '/0'
