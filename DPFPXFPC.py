Report Date: 31/05/26
Start Date: 23/05/26
Week: 4
Month: 05
Year: 2026
SAS Connection established. Subprocess id is 744388

✓ SAS session initialized

Processing UMA...
✓ 31,915 UMA records (BNKIND=PBB)

Processing Saving Accounts...
✓ 4,282,476 saving accounts
✓ savg054.parquet
✓ savg054.sas7bdat

Processing Current Accounts...
✓ 919,361 current accounts (852,110 regular, 67,251 FCY)
✓ curn054.parquet
✓ curn054.sas7bdat
✓ fcy054.parquet
/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
✓ fcy054.sas7bdat

Creating department summary...
✓ dept054.parquet
✓ dept054.sas7bdat
✓ Department summary created

Processing Fixed Deposits...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQDISE.py", line 622, in <module>
    fd = fd.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "BIC"; valid columns: ["BANKNO", "REPTNO", "FMTCODE", "BRANCH", "ACCTNO", "STATEC", "PURPOSE", "CUSTCD", "DEPODTE", "NAME", "CDNO", "OPENIND", "CURBAL", "ORIGAMT", "ORGDATE", "MATDATE", "RATE", "ACCTTYPE", "TERM", "MATID", "INTPLAN", "PAYMENT", "RENEWAL", "INTPDYTD", "INTPAY", "INTDATE", "LASTACTV", "INTFREQ", "INTFREQID", "PENDINT", "CURCODE", "LMATDATE", "PRORATIO", "FDHOLD", "COSTCTR", "COLLNO", "INTTFRACCT", "PRN_DISP_OPT", "PRN_RENEW", "PRN_TFR_ACCT", "AMTIND", "FORATE", "FORBAL", "CURBALUS", "INTPAY_NUM", "FORATE_NUM", "CURBAL_NUM", "ACCTTYPE_NUM", "INTPLAN_NUM"]
SAS Connection terminated. Subprocess id was 744388
