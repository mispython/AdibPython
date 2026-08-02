[WARN] FORATE source at /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWCRMA/forate.sas7bdat has unrecognized columns ['CURCODE', 'SPOTRATE', 'REPTDATE'] -- expected either (CURCODE, FORATE) or a CNTLOUT-style (FMTNAME, START, LABEL, ...) catalog dump. Skipping conversion.
[WARN] saving: no FORATE for currencies ['XAU'] -- MTDAVBAL/CURBAL left unconverted for these rows.
[WARN] current: no FORATE for currencies ['HKD', 'JPY', 'CAD', 'USD', 'IDR', 'CHF', 'SGD', 'THB', 'EUR', 'NZD', 'GBP', 'CNY', 'AUD'] -- MTDAVBAL/CURBAL left unconverted for these rows.
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWCRMA.py", line 280, in <module>
    FD = build_deposit_table(DEPO_FD, IDEPO_FD, "fd")
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWCRMA.py", line 255, in build_deposit_table
    combined = pl.concat([depo, idepo], how="vertical", rechunk=True)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 234, in concat
    out = wrap_df(plr.concat_df(elems))
polars.exceptions.SchemaError: type Float64 is incompatible with expected type Null
