============================================================
EIVD GL PROCESSING STARTED (EIVDNLGL)
============================================================
Processing date: 2026-07-08
Store directory: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIVDNLGL
============================================================
Detected encoding: ascii
Successfully read file with encoding: ascii
Total lines: 9

First 5 lines of data:
Line 1: '20260708                                                                        '
  GLITEM: '20260708  '
  BALANCE: '20260708'

Line 2: '1S-RCF              08/07/26                    36,353,900.00                                                                        '
  GLITEM: '1S-RCF    '
  DATEX: '08/07/26'
  BALANCE: '1'

Line 3: '1S-TLF              08/07/26                   250,737,245.49                                                                        '
  GLITEM: '1S-TLF    '
  DATEX: '08/07/26'
  BALANCE: '1'

Line 4: '1S-BA F             08/07/26                     4,353,267.90                                                                        '
  GLITEM: '1S-BA F   '
  DATEX: '08/07/26'
  BALANCE: '1'

Line 5: '1S-SM F             08/07/26                             0.00                                                                        '
  GLITEM: '1S-SM F   '
  DATEX: '08/07/26'
  BALANCE: '1'

Header date found: 20260708

Parsed 7 rows from GL file
Columns: ['GLITEM', 'DATEX', 'BALANCE', 'SIGN']

Data sample:
shape: (7, 4)
┌───────────────┬────────┬─────────┬──────┐
│ GLITEM        ┆ DATEX  ┆ BALANCE ┆ SIGN │
│ ---           ┆ ---    ┆ ---     ┆ ---  │
│ str           ┆ str    ┆ f64     ┆ str  │
╞═══════════════╪════════╪═════════╪══════╡
│ 1S-RCF        ┆ 080726 ┆ 1.0     ┆      │
│ 1S-TLF        ┆ 080726 ┆ 1.0     ┆      │
│ 1S-BA F       ┆ 080726 ┆ 1.0     ┆      │
│ 1S-SM F       ┆ 080726 ┆ 1.0     ┆      │
│ 1S-GUARANTEE  ┆ 080726 ┆ 1.0     ┆      │
│ 1S-REMISIERFD ┆ 080726 ┆ 1.0     ┆      │
│ 1S-FIXED DEP  ┆ 080726 ┆ 1.0     ┆      │
└───────────────┴────────┴─────────┴──────┘

Unique GLITEMs in file (7):
  '1S-BA F'
  '1S-FIXED DEP'
  '1S-GUARANTEE'
  '1S-RCF'
  '1S-REMISIERFD'
  '1S-SM F'
  '1S-TLF'

GL Date from file: 080726
REPT Date: 080726
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIVDNLGL.py", line 290, in <module>
    R = GL_SUM.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "BALANCE_CALC"; valid columns: ["ITEM", "WEEK", "MONTH", "QTR", "HALFYR", "YEAR", "LAST", "BALANCE"]
