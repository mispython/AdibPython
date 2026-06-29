SAS Connection established. Subprocess id is 714770

✓ SAS session initialized
Report Date: 31/05/26, Week: 4, Month: 05

Processing Savings...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQDISE.py", line 167, in <module>
    saving = (saving
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "1"; valid columns: ["OPENMH", "AC_OPEN_STATUS_CD", "TRACKCD", "CUSTCODE", "OPENIND", "PURPOSE", "USER3", "INTPLAN_IBCA", "AVGAMT", "INTPDPYR", "NAME", "BDATE", "DEPTYPE", "BRANCH", "SECTOR", "INT1", "INTPD", "DEBIT", "STMT_CYCLE", "TAXNO", "CLOSEMH", "LEDGBAL", "INTPLAN", "INTPAYBL", "BONUTYPE", "PRODUCT", "YTDAVAMT", "NXT_STMT_CYCLE_DT", "DNBFISME", "POST_IND_MAINT_DT", "INSTRUCTIONS", "STATE", "INTCYCODE", "INACTIVE", "AVGBAL", "INTRATE", "POST_IND_EXP_DT", "SCHIND", "PREVBRNO", "STATCD", "EXODDATE", "INTYTD", "ACCYTD", "BONUSANO", "MTDLOWBA", "ORGTYPE", "ORGCODE", "BENINTPD", "RISKCODE", "CLOSEDT", "TEMPODDT", "CASH_DEPOSIT_LIMIT_IND", "CURBAL", "DPMTDBAL", "LASTTRAN", "PBIND", "USER5", "CREDIT", "E_INVOICE_IND", "CASH_DEPOSIT_AMOUNT_AGG", "COSTCTR", "SECOND", "MAILCODE", "CURCODE", "CHQFLOAT", "SOURCE_INCOME_CURRENCY_CD", "POST_IND", "RACE", "SERVICE", "MTDAVBAL", "OPENDT", "FEEPD", "ACCTNO", "BANKNO", "USER2", "DTLSTCUST", "PRIN_ACCT", "PSREASON", "REOPENDT", "INTRSTPD", "CHGIND"]
SAS Connection terminated. Subprocess id was 714770
