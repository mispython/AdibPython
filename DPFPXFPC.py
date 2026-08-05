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
Sample GLITEMs: ['1F142199C', '1F134600RC', '1F142199B', '142699SGD', '149120', '142199', '142699USD', '1F137650FXCDS', '1F143620OPE', '1F144140CAGA']

First few rows:
shape: (5, 7)
┌───────────────┬──────────┬──────┬──────────┬──────┬─────┬─────┐
│ GLITEM        ┆ DATE     ┆ SIGN ┆ BALANCE  ┆ YY   ┆ MM  ┆ DD  │
│ ---           ┆ ---      ┆ ---  ┆ ---      ┆ ---  ┆ --- ┆ --- │
│ str           ┆ str      ┆ str  ┆ f64      ┆ i64  ┆ i64 ┆ i64 │
╞═══════════════╪══════════╪══════╪══════════╪══════╪═════╪═════╡
│ 149120        ┆ 31/07/26 ┆ -    ┆ 1.5017e8 ┆ 2026 ┆ 7   ┆ 31  │
│ 142199        ┆ 31/07/26 ┆ -    ┆ 3.6472e7 ┆ 2026 ┆ 7   ┆ 31  │
│ 1F144611FXSDC ┆ 31/07/26 ┆ +    ┆ 0.0      ┆ 2026 ┆ 7   ┆ 31  │
│ 1F142630C     ┆ 31/07/26 ┆ +    ┆ 0.0      ┆ 2026 ┆ 7   ┆ 31  │
│ 142699        ┆ 31/07/26 ┆ -    ┆ 1.7933e8 ┆ 2026 ┆ 7   ┆ 31  │
└───────────────┴──────────┴──────┴──────────┴──────┴─────┴─────┘

Processing P1 conditions...

Processing P2 conditions...

Processing complete!
