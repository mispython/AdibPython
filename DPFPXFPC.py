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
Sample GLITEMs: ['F142699C', 'F137610FXSH', '144111', 'F142199C', 'F142699D', 'F141301', '149120', 'F142600PBB', '134200', 'F133620FNFBI']

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
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMNLGL.py", line 303, in <module>
    results_p1 = process_gl_data(df_gl, conditions_p1, 'P1')
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMNLGL.py", line 152, in process_gl_data
    pl.lit(item).alias('ITEM'),
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/lit.py", line 218, in lit
    return wrap_expr(plr.lit(item, allow_object, is_scalar=True))
TypeError: cannot create expression literal for value of type Expr.

Hint: Pass `allow_object=True` to accept any value and create a literal of type Object.
You have mail in /var/spool/mail/sas_edw_dev
