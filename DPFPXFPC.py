============================================================
Bad Debt Write-Off List Generation
============================================================
Report Date: 20/07/2026
Week: 4, Previous Month: 06
*** TEST MODE: Limiting to 100000 rows ***
============================================================

STEP 1: DEBUG - Checking BORSTAT values...
  Reading: lnnote.sas7bdat - 100 rows, 0.5s

  BORSTAT column info:
  Data type: String
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIFTXT1.py:194: DeprecationWarning: `pl.count()` is deprecated. Please use `pl.len()` instead.
(Deprecated in version 0.20.5)
  unique_vals = df_sample.group_by('BORSTAT').agg(pl.count())
  Unique values:
    'W' : 4 rows
    '' : 94 rows
    'X' : 2 rows
  No 'A' values found in sample
  Trying with string strip...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIFTXT1.py", line 208, in <module>
    pl.col('BORSTAT').cast(pl.Utf8).str.strip().alias('BORSTAT_STRIPPED')
AttributeError: 'ExprStringNameSpace' object has no attribute 'strip'
