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
MAST columns: ['ficode', 'facils', 'applcode', 'acctnox', 'name', 'name2', 'name3', 'name4', 'name5', 'name6', 'postcode', 'tfid', 'custcodx', 'retailid', 'state', 'score1', 'score2', 'busregn', 'birthdtx', 'sector']

Processing MAST2...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIWBTCR_ISLAMIC_WEEKLY_BANKTRADE_CCR.py:290: DeprecationWarning: `str.concat` is deprecated; use `str.join` instead. Note also that the default `delimiter` for `str.join` is an empty string, not a hyphen.
  pl.col("aano").cast(pl.Utf8).str.concat("|").alias("allrefno"),
MAST2 processed

Processing CRED...
CRED processed: 3419 rows

Processing SUBA...
SUBA processed: 61339 rows (SUBA9: 16539, SUBA_MAIN: 10411)

Processing ACCT...
ACCT processed: 28180 rows

==================================================
PROCESSING COMPLETE
==================================================
Processing Date: 2026-08-15
MAST rows: 1697
CRED rows: 3419
SUBA rows: 61339
SUBA9 rows: 28180
ACCT rows: 28180
==================================================
