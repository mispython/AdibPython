Current date: 2026-08-05
Report date (yesterday): 2026-08-04
Expected date format (DDMMYY): 040826

Reading GL file...
File date: 2026-07-31 (DDMMYY: 310726)
Expected date (DDMMYY): 040826
WARNING: GL file extraction date (310726) does not match expected date (040826)
Using file date for processing...

Processing 68 data lines
Line 27: Not enough parts (1): -
Line 28: Not enough parts (1): -
Line 30: Not enough parts (1): -
Line 35: Not enough parts (1): -
Line 58: Not enough parts (1): -

Created DataFrame with 57 records
Sample GLITEMs: ['F142510FDA', 'F142630C', 'F144111RM', '149120', 'F143110VCB', 'F134600RC', 'F142699OPE', 'F249230PTAX', 'F247610', 'F142199B']

First few rows:
shape: (5, 7)
┌──────────────┬──────────┬──────┬──────────┬──────┬─────┬─────┐
│ GLITEM       ┆ DATE     ┆ SIGN ┆ BALANCE  ┆ YY   ┆ MM  ┆ DD  │
│ ---          ┆ ---      ┆ ---  ┆ ---      ┆ ---  ┆ --- ┆ --- │
│ str          ┆ str      ┆ str  ┆ f64      ┆ i64  ┆ i64 ┆ i64 │
╞══════════════╪══════════╪══════╪══════════╪══════╪═════╪═════╡
│ 149120       ┆ 31/07/26 ┆ -    ┆ 1.5017e8 ┆ 2026 ┆ 7   ┆ 31  │
│ 142199       ┆ 31/07/26 ┆ -    ┆ 3.6472e7 ┆ 2026 ┆ 7   ┆ 31  │
│ F144611FXSDC ┆ 31/07/26 ┆ +    ┆ 0.0      ┆ 2026 ┆ 7   ┆ 31  │
│ F142630C     ┆ 31/07/26 ┆ +    ┆ 0.0      ┆ 2026 ┆ 7   ┆ 31  │
│ 142699       ┆ 31/07/26 ┆ -    ┆ 1.7933e8 ┆ 2026 ┆ 7   ┆ 31  │
└──────────────┴──────────┴──────┴──────────┴──────┴─────┴─────┘

Processing P1 conditions...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMNLGL.py", line 323, in <module>
    results_p1 = process_gl_data(df_gl, conditions_p1, 'P1')
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMNLGL.py", line 176, in process_gl_data
    result = result.with_columns(total_col.alias('TOTAL'))
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "BALANCE"; valid columns: ["ITEM", "WEEK", "MONTH", "QTR", "HALFYR", "YEAR", "LAST"]
