EIBDDEPE - Daily Deposit Position Extract
============================================================

Reading DPTRBL Parquet...
  Loaded 11,857,406 records
  Using current date: 2026-06-16
Report Date: 16/06/2026, Week: 2
============================================================
  Filtered: 8,829,575 records
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIDDEPE.py", line 146, in <module>
    df = df.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "MOVEMENT"; valid columns: ["BANKNO", "REPTNO", "FMTCODE", "BRANCH", "ACCTNO", "NAME", "TAXNO", "DEBIT", "CREDIT", "CLOSEDT", "REOPENDT", "CUSTCODE", "HOLDAMT", "INTPAID", "ODPLAN", "RATE1", "RATE2", "RATE3", "RATE4", "RATE5", "TODRATE", "FLATRATE", "BASERATE", "ODSTAT", "ORGCODE", "ORGTYPE", "LIMIT1", "LIMIT2", "LIMIT3", "LIMIT4", "LIMIT5", "INTYTD", "FEEPD", "PURPOSE", "COL1", "COL2", "COL3", "COL4", "COL5", "SECTOR", "USER2", "USER3", "RISKCODE", "LEDGBAL", "DATELSTDEP", "L_DEP", "STATCD", "DECEASED1", "DECEASED2", "DECEASED3", "LASTTRAN", "RETURNS_Y", "STMT_CYCLE", "CHGIND", "AVGAMT", "PRODUCT", "RACE", "BALHOLD", "DEPTYPE", "INT1", "INTPD", "INTPLAN", "COMB_INDC", "CURBAL", "CHQFLOAT", "IA_LRU", "INT2", "MTDLOWBA", "BENINTPD", "STATE", "INTCYCODE", "APPRLIMT", "ODINTCHR", "ODINTACC", "CURCODE", "PBIND", "INTRATE", "STMT_DT", "YTDAVAMT", "OPENIND", "BDATE", "INACTIVE", "SECOND", "ODXSAMT", "BONUTYPE", "SERVICE", "BONUSANO", "USER5", "TRACKCD", "EXODDATE", "TEMPODDT", "SCHIND", "PREVBRNO", "AVGBAL", "COSTCTR", "AUTHORISE_LIMIT", "CRRCODE", "CCRICODE", "FAACRR", "POST_IND", "CENSUST", "ACCPROF", "MAXPROF", "INTRSTPD", "MTDAVBAL", "VB", "BILLERIND", "MODIFIED_FACILITY_IND", "DTLSTCUST", "INTPDPYR", "OPENDT", "OTC_PRIN_ACCT", "OPNDATEA", "ACCOUNTNO", "LEDBALX", "BRANCHA", "ACCTNOA", "DATEOPNA", "OPENINDA", "LSTACT", "CURRCODE", "DORMIND", "POSTIND", "TBDATE", "ACCTNAME", "OLDIC", "CLOSDATE", "OPENDATE", "DEPTNO", "PURPCODE", "SICODE", "AVLBAL", "SERCODE", "PRODCODE", "STAFFID", "OFFDAT1", "OFFDAT2", "OFFDAT3", "BMCUPDT", "NEWIC", "LSTWDRW", "MTDACCM", "OPENDT_STR", "CLOSEDT_STR"]
