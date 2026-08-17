Reading input files...
  Read imast: 1697 rows
  Read imast2: 2246 rows
  Read icred: 10414 rows
  Read isuba: 61339 rows
  Read iprov: 87 rows
  Read iamsubacc: 0 rows
  Read ibtrd: 3418 rows
  Read ibtdtl: 3418 rows
  Read lnacct: 1205962 rows

Processing MAST...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIWBTCR_ISLAMIC_WEEKLY_BANKTRADE_CCR.py", line 267, in <module>
    pl.col("CUSTCODE").cast(pl.Int32).map_dict(CUSTCODE_FORMAT, default="99").alias("CUSTFISS"),
AttributeError: 'Expr' object has no attribute 'map_dict'
You have mail in /var/spool/mail/sas_edw_dev
