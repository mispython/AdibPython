Processing for month: 07 (Report Date: 2026-07-30)
Report Date based on: current date minus 1 day
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMTRUT.py", line 157, in <module>
    read_sas7bdat(SACA / "fd.sas7bdat").select(fd_base_cols),
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10148, in select
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "PRODUCT"; valid columns: ["BANKNO", "REPTNO", "FMTCODE", "BRANCH", "ACCTNO", "STATEC", "PURPOSE", "CUSTCD", "DEPODTE", "NAME", "CDNO", "OPENIND", "CURBAL", "ORIGAMT", "ORGDATE", "MATDATE", "RATE", "ACCTTYPE", "TERM", "MATID", "INTPLAN", "PAYMENT", "RENEWAL", "INTPDYTD", "INTPAY", "INTDATE", "LASTACTV", "INTFREQ", "INTFREQID", "PENDINT", "CURCODE", "LMATDATE", "PRORATIO", "FDHOLD", "COSTCTR", "COLLNO", "INTTFRACCT", "PRN_DISP_OPT", "PRN_RENEW", "PRN_TFR_ACCT", "AMTIND", "FORATE", "FORBAL", "CURBALUS"]
