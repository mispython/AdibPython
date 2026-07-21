
============================================================
Bad Debt Write-Off List Generation
============================================================
Report Date: 20/07/2026
Week: 4, Previous Month: 06
*** TEST MODE: Limiting to 500000 rows ***
============================================================

STEP 1: DEBUG - Checking BORSTAT values...
  Reading: lnnote.sas7bdat - 1000 rows, 0.9s

  BORSTAT column info:
  Data type: String
  Unique values (first 1000 rows):
    'W' : 35 rows
    '' : 923 rows
    'X' : 39 rows
    'P' : 1 rows
    'F' : 2 rows
  No 'A' values found in sample

STEP 2: Reading NPLA data...
  Reading: lnnote.sas7bdat - 500000 rows, 4.1s

  Checking BORSTAT values in full dataset...
  BORSTAT distribution:
    'Y' : 1 rows
    'P' : 44 rows
    '' : 493191 rows
    'C' : 11 rows
    'X' : 1636 rows
    'R' : 6 rows
    'M' : 1 rows
    'F' : 169 rows
    '0' : 3 rows
    'A' : 3 rows
    'D' : 1 rows
    'I' : 9 rows
    'W' : 4862 rows
    'K' : 63 rows

  Exact match 'A': 3 rows
  After strip 'A': 3 rows

  Using stripped 'A' filtering...

  NPLA rows: 3

STEP 3: Reading IIS and SP data...
  Reading: iis.sas7bdat - 135725 rows, 0.3s
  Reading: sp2.sas7bdat - 135725 rows, 0.4s
  IIS rows: 135725
  SP rows: 135725


STEP 4: Combining NPL data...
  NPL combined rows: 135728

STEP 5: Reading CCRIS data...
  Looking for: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/icredmsubac0726.sas7bdat
  Reading: icredmsubac0726.sas7bdat - 500000 rows, 20.2s
  Found CCRIS file: icredmsubac0726.sas7bdat
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIFTXT1.py", line 445, in <module>
    df_credsub = df_credsub.filter(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 5079, in rename
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: "ACCTNUM" not found
