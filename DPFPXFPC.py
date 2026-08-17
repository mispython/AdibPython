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
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIWBTCR_ISLAMIC_WEEKLY_BANKTRADE_CCR.py", line 244, in <module>
    mast = data['imast'].filter(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "9999"; valid columns: ["ficode", "facils", "applcode","acctnox", "name", "name2", "name3", "name4", "name5", "name6", "postcode", "tfid", "custcodx", "retailid", "state","score1", "score2", "busregn", "birthdtx", "sector", "settled", "sm_status", "ia_lru", "sm_date", "review_dt", "acct_block_ind", "acct_block_reason", "ascore_ltst", "ascore_perm", "apvdate", "industrial_sector_cd", "legal_action_cd","legal_action_dt", "e_invoice_ind", "ccpt_ltst_review_dt", "fdb_tag", "fdb_tag_dt", "fdb_scoring_dt", "acct_write_off_status", "acctno", "custcode", "dnbfisme", "birthdt"]
