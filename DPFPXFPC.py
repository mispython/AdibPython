
============================================================
Bad Debt Write-Off List Generation
============================================================
Report Date: 20/07/2026
Week: 4, Previous Month: 06
*** TEST MODE: Limiting to 500000 rows ***
============================================================

STEP 1: DEBUG - Checking BORSTAT values...
  Reading: lnnote.sas7bdat - 500000 rows, 1.7s

  BORSTAT column info:
  Data type: String
  Unique values (first 1000 rows):
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
  Reading: lnnote.sas7bdat - 500000 rows, 2.3s

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
  Looking for: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/icredmsubac0726.sas7bdat
Warning: File not found: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/icredmsubac0726.sas7bdat
Warning: File not found: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/ICREDMSUBAC0726.sas7bdat
Warning: File not found: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/icredmsubac07026.sas7bdat
Warning: File not found: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/ICREDMSUBAC07026.sas7bdat
  CCRIS file not found - creating empty DataFrame
  CCRIS rows: 0

STEP 6: Reading HPD loan data...
  Reading: lnnote.sas7bdat - 500000 rows, 1.6s
  HPD loan rows: 2093

STEP 7: Merging data...
  Merged loan rows: 135728

STEP 8: Calculating derived fields...
  Reading: lnnote.sas7bdat - 500000 rows, 4.9s
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIFTXT1.py", line 564, in <module>
    df_loan = df_loan.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "Y"; valid columns: ["NAME", "ACCTNO", "NOTENO", "IIS", "OI", "TOTIIS", "SP", "MARKETVL", "BRANCH", "BRNO", "BRABBR", "DAYS", "FACILITY", "LOANTYPE", "GUAREND", "CUSTCODE", "LOANTYPE_right", "LASTTRAN", "LSTTRNCD", "CURBAL", "INTAMT", "APPVALUE", "COLLDESC", "PAIDIND", "ORGBAL", "NETPROC", "MATUREDT", "BORSTAT", "MARKETVL_right", "INTEARN4", "PAYAMT", "FEETOTAL", "FEETOT2", "FEEAMT3", "POSTNTRN", "BIRTHDT", "SCORE2", "NFEEAMT5", "COLLYEAR", "DELQCD", "ECSRRSRV", "MODELDES", "CONTRTYPE", "AKPK_STATUS", "NACOSPADT", "CP", "FEEAMTA", "BALANCE", "FEEAMT5", "ISSXDTE"]



the test limit increased to 500,000 rows
