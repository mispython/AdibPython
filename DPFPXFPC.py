============================================================
EIBQINST - Trustee and Client Account Quarterly Reporting
============================================================

Report Date: 2026-07-31 (Week: 4)

Processing Trustee Accounts...
  FLOAT: 18927 records loaded
  IBGPIDM: 7609 records loaded
Error loading REMIT/UNCLAIM: unable to append to a DataFrame of width 9 with a DataFrame of width 6
  REMIT: 0 records loaded
Error loading CURRENT: arithmetic on string and numeric not allowed, try an explicit cast first
Error loading FD: unable to find column "intpaybl"; valid columns: ["bankno", "reptno", "fmtcode", "branch", "acctno", "statec", "purpose", "custcd", "depodte", "name", "cdno", "openind", "curbal", "origamt", "orgdate", "matdate", "rate", "accttype", "term", "matid", "intplan", "payment", "renewal", "intpdytd", "intpay", "intdate", "lastactv", "intfreq", "intfreqid", "pendint", "curcode", "lmatdate", "proratio", "fdhold", "costctr", "collno", "inttfracct", "prn_disp_opt", "prn_renew", "prn_tfr_acct", "amtind", "forate", "forbal", "curbalus"]
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQINST.py", line 459, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQINST.py", line 259, in main
    saca = pl.concat([sa, ca, fd]) if any(len(df)>0 for df in [sa,ca,fd]) else pl.DataFrame()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 234, in concat
    out = wrap_df(plr.concat_df(elems))
polars.exceptions.ShapeError: unable to append to a DataFrame of width 7 with a DataFrame of width 0
