Current date: 2026-08-05
Report date (yesterday): 2026-08-04
Expected date format (DDMMYY): 040826

Reading GL file...
Raw date string: '20260731                                                                        
'
Cleaned date string: '20260731                                                                        '
File date: 2026-07-31 (DDMMYY: 310726)
Expected date (DDMMYY): 040826
WARNING: GL file extraction date (310726) does not match expected date (040826)
File date: 2026-07-31
Expected: 2026-08-04
Using file date for processing...

Processing 68 data lines
Line 27: Not enough parts (1): -
Line 28: Not enough parts (1): -
Line 30: Not enough parts (1): -
Line 35: Not enough parts (1): -
Line 58: Not enough parts (1): -

Created DataFrame with 57 records
Columns: ['GLITEM', 'DATE', 'SIGN', 'BALANCE', 'YY', 'MM', 'DD']
Sample GLITEMs: ['F133110ODVIB', 'F143110VIB', 'F143620FNFBI', '139110', '134200', 'F144611FXSDC', 'F142510FDA', 'F142630C', 'F142600FBI', 'F142600PBB']

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
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMNLGL.py", line 331, in <module>
    results_p1 = process_gl_data(df_gl, conditions_p1, 'P1')
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMNLGL.py", line 190, in process_gl_data
    glfile = pl.concat(rows) if rows else pl.DataFrame()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 234, in concat
    out = wrap_df(plr.concat_df(elems))
polars.exceptions.SchemaError: type Float64 is incompatible with expected type Null
