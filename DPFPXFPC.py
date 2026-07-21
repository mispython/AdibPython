
============================================================
Bad Debt Write-Off List Generation
============================================================
Report Date: 20/07/2026
Week: 4, Previous Month: 06
*** TEST MODE: Limiting to 1000 rows ***
============================================================

STEP 1: Reading NPLA data...
  Reading: lnnote.sas7bdat - 1000 rows, 134.9s
  NPLA rows: 0

STEP 2: Reading IIS and SP data...
  Reading: iis.sas7bdat - 1000 rows, 0.2s
  Reading: sp2.sas7bdat - 1000 rows, 0.3s
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIFTXT1.py:220: DeprecationWarning: use of `how='outer'` should be replaced with `how='full'`.
(Deprecated in version 0.20.29)
  df_npl_data = df_sp.join(df_iis, on=['ACCTNO', 'NOTENO'], how='outer').select([
  IIS rows: 1000
  SP rows: 1000

STEP 3: Combining NPL data...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIFTXT1.py", line 234, in <module>
    df_npl = pl.concat([df_npla, df_npl_data]).with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 234, in concat
    out = wrap_df(plr.concat_df(elems))
polars.exceptions.SchemaError: type Float64 is incompatible with expected type Int32
