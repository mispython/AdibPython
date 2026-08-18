Reading input files...
  Read imast: 1697 rows
  Read imast2: 2246 rows
  Read icred: 10414 rows
  Read isuba: 61339 rows
  Read iprov: 87 rows
  Read iamsubacc: 0 rows
  Read ibtrad: 3418 rows
  Read ibtdtl: 3418 rows
  Read lnacct: 1205962 rows

Processing MAST...
MAST processed: 1697 rows

Processing MAST2...
MAST2 processed

Processing CRED...
CRED processed: 3419 rows

Processing BNM Trade data...
BNM Trade data processed

Processing SUBA...
SUBA processed: 61339 rows (SUBA9: 1184, SUBA_MAIN: 10411)

Processing ACCT...
ACCT processed: 1644 rows

Processing BTR2...
BTR2 processed: 10411 rows

Processing SUBCR...
SUBCR processed: 895 rows

Creating final SUBA...
Final SUBA processed: 895 rows

Writing output files...
ACCTCRED written: 1644 records
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIWBTCR_ISLAMIC_WEEKLY_BANKTRADE_CCR.py", line 797, in <module>
    subacred_output = suba_final.select([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10148, in select
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "fconcept"; valid columns: ["acctnox", "ficody", "ficode", "apcode", "branch", "oldbrh", "custcode_clean", "custfiss", "sector_clean", "sectfiss", "facility", "faccode", "forcurr", "pdbind", "outstand", "unearned", "repaid", "disburse", "curbal", "intamt", "oth_charge", "noteno", "instalm", "nodays", "arrears", "tfr02i", "mtd_tawidh_amt", "mtd_gharamah_amt", "apprlim2", "firstdisbdt", "dataxx", "odxsamt", "biltot", "noteterm", "syndicat", "specialf", "purposes", "payfreqc", "fdisbdt", "sm_status1", "sm_dat1", "rmsbba", "score1", "score2", "dnbfisme", "lu_add1", "lu_add2", "lu_add3", "lu_add4", "lu_town_city", "lu_postcode", "lu_state_cd", "lu_country_cd", "ia_lru", "sm_status", "sm_datestr", "outx", "undrawn"]
