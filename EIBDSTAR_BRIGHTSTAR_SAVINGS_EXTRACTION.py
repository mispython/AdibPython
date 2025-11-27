REPTDATE = 2025-11-26, PREVDATE = 2025-11-25
RDATE (SAS format) = 24071
MON=11, DAY=26, YEAR=25
Filter date (reptdte) = 251126
SAVING.csv shape: (8320, 81)
Top 20 PRODUCT values by count:
shape: (20, 4)
┌───────────┬───────┬────────────┬─────────────┐
│ PRODUCT   ┆ count ┆ has_opendt ┆ avg_balance │
│ ---       ┆ ---   ┆ ---        ┆ ---         │
│ str       ┆ u32   ┆ u32        ┆ f64         │
╞═══════════╪═══════╪════════════╪═════════════╡
│ 111411318 ┆ 139   ┆ 81         ┆ 1.143885    │
│ 112625330 ┆ 99    ┆ 6          ┆ 2.625       │
│ 32720087  ┆ 83    ┆ 28         ┆ 1.1         │
│ 121001344 ┆ 77    ┆ 39         ┆ 2.025974    │
│ 12319023  ┆ 40    ┆ 1          ┆ 4.0         │
│ …         ┆ …     ┆ …          ┆ …           │
│ 102325296 ┆ 31    ┆ 0          ┆ 4.0         │
│ 110525309 ┆ 31    ┆ 2          ┆ 1.25        │
│ 111125315 ┆ 29    ┆ 0          ┆ null        │
│ 103125304 ┆ 29    ┆ 3          ┆ 2.4         │
│ 53023150  ┆ 28    ┆ 0          ┆ null        │
└───────────┴───────┴────────────┴─────────────┘
PRODUCT values containing '208': ['111208317', '120820343', '120821342', '120822342', '120823342', '20818039', '20819039', '20822039', '20823039', '32122080', '32212082', '32222081', '32322082', '32922088', '72624208', '72721208', '72722208', '72723208']
Traceback (most recent call last):
  File "/pythonITD/mis_dev/sas_migration/BRIGHTSTAR/EIBDSTAR_BRIGHTSTAR_SAVINGS_EXTRACTION.py", line 91, in <module>
    saving
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10020, in with_columns
    self.lazy()
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 330, in wrapper
    return function(*args, **kwargs)
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2335, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.InvalidOperationError: conversion from `str` to `i64` failed in column 'OPENDT' for 14 out of 14 values: ["J", "J", … "J"]

Did not show all failed cases as there were too many.
