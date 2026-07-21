
============================================================
Bad Debt Write-Off List Generation
============================================================
Report Date: 20/07/2026
Week: 4, Previous Month: 06
*** TEST MODE: Limiting to 500000 rows ***
============================================================

STEP 1: DEBUG - Checking BORSTAT values...
  Reading: lnnote.sas7bdat - 500000 rows, 2.0s

  BORSTAT column info:
  Data type: String
  Unique values:
    'W' : 4862 rows
    '' : 493191 rows
    'X' : 1636 rows
    'P' : 44 rows
    'F' : 169 rows
    'Y' : 1 rows
    'K' : 63 rows
    'C' : 11 rows
    'M' : 1 rows
    'I' : 9 rows
    '0' : 3 rows
    'R' : 6 rows
    'D' : 1 rows
    'A' : 3 rows
  Found 'A' values in sample

STEP 2: Reading NPLA data...
  Reading: lnnote.sas7bdat - 500000 rows, 2.1s

  Checking BORSTAT values in full dataset...
  BORSTAT distribution:
    'W' : 4862 rows
    '' : 493191 rows
    'X' : 1636 rows
    'P' : 44 rows
    'F' : 169 rows
    'Y' : 1 rows
    'K' : 63 rows
    'C' : 11 rows
    'M' : 1 rows
    'I' : 9 rows
    '0' : 3 rows
    'R' : 6 rows
    'D' : 1 rows
    'A' : 3 rows

  Exact match 'A': 3 rows
  After strip 'A': 3 rows

  Checking for possible numeric codes...

  Using stripped 'A' filtering...

  NPLA rows: 3

STEP 3: Reading IIS and SP data...
  Reading: iis.sas7bdat - 135725 rows, 0.2s
  Reading: sp2.sas7bdat - 135725 rows, 0.2s
  IIS rows: 135725
  SP rows: 135725


STEP 4: Combining NPL data...
  NPL combined rows: 135728

STEP 5: Reading CCRIS data...
Warning: File not found: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/icredmsubac0726.sas7bdat
  CCRIS rows: 0

STEP 6: Reading HPD loan data...
  Reading: lnnote.sas7bdat - 500000 rows, 1.5s
  HPD loan rows: 2093

STEP 7: Merging data...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIFTXT1.py", line 517, in <module>
    df_loan = df_npl.join(df_credsub, on=['ACCTNO', 'NOTENO'], how='left').join(
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
polars.exceptions.ColumnNotFoundError: unable to find column "ACCTNO"; valid columns: []
