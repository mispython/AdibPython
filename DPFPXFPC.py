============================================================
Bad Debt Write-Off List Generation
============================================================
Report Date: 23/11/2026  Week: 4  Prev Month: 10
============================================================

STEP 1+2: Streaming enrh_ln_note (ENTITY_CD='PIBB', BORSTAT='A')...
  offset  8,250,000  kept    297  RSS 323MB  5245s
  NPLA survivors: 297
  NPLA ready: 297 rows

STEP 3: IIS / SP2...
  Reading: iis.sas7bdat - 135,725 rows, 6.6s
  Reading: sp2.sas7bdat - 135,725 rows, 4.4s
  IIS: 135,725  SP2: 135,725

STEP 4: Combine NPL...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIFTXT1.py", line 297, in <module>
    df_npl = pl.concat([df_npla, df_npl_data], how='vertical_relaxed')
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 236, in concat
    out = wrap_ldf(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ComputeError: schema names differ: got NAME, expected ACCTNO
