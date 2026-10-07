[    0.00s] computed date macros
[   50.04s] read MNITB_CURRENT (1132084 rows)
MNITB columns: ['BRANCH', 'ACCTNO', 'NAME', 'TAXNO', 'DEBIT', 'CREDIT', 'CLOSEDT', 'REOPENDT', 'CUSTCODE', 'ODPLAN', 'RATE1', 'RATE2', 'RATE3', 'RATE4', 'RATE5', 'TODRATE', 'FLATRATE', 'BASERATE', 'ODSTAT', 'ORGCODE', 'ORGTYPE', 'LIMIT1', 'LIMIT2', 'LIMIT3', 'LIMIT4', 'LIMIT5', 'INTYTD', 'FEEPD', 'PURPOSE', 'COL1', 'COL2', 'COL3', 'COL4', 'COL5', 'SECTOR', 'USER2', 'USER3', 'RISKCODE', 'LEDGBAL', 'DATE_LST_DEP', 'OPENIND', 'STATCD', 'LASTTRAN', 'RETURNS_Y', 'CHGIND', 'AVGAMT', 'PRODUCT', 'RACE', 'DEPTYPE', 'INT1', 'INTPD', 'INTPLAN', 'CURBAL', 'CHQFLOAT', 'IA_LRU', 'MTDLOWBA', 'BENINTPD', 'STATE', 'INTCYCODE', 'APPRLIMT', 'ODINTCHR', 'ODINTACC', 'CURCODE', 'INTRATE', 'YTDAVAMT', 'BDATE', 'INACTIVE', 'SECOND', 'ODXSAMT', 'BONUTYPE', 'SERVICE', 'BONUSANO', 'USER5', 'TRACKCD', 'EXODDATE', 'TEMPODDT', 'PREVBRNO', 'AVGBAL', 'COSTCTR', 'AUTHORISE_LIMIT', 'CRRCODE', 'CCRICODE', 'FAACRR', 'POST_IND', 'CENSUST', 'ACCPROF', 'MAXPROF', 'INTRSTPD', 'MTDAVBAL', 'VB', 'BILLERIND', 'MODIFIED_FACILITY_IND', 'DTLSTCUST', 'INTPDPYR', 'OPENDT', 'MAILCODE', 'OMNILOAN', 'OMNITRDB', 'DSR', 'REPAY_TYPE_CD', 'MTD_REPAID_AMT', 'INDUSTRIAL_SECTOR_CD', 'MTD_DISBURSED_AMT', 'MTD_REPAY_TYPE10_AMT', 'MTD_REPAY_TYPE20_AMT', 'MTD_REPAY_TYPE30_AMT', 'RRIND', 'RRCOUNT', 'RRAPPRVDT', 'RREXPIRYDT', 'RRMAINDT', 'RRPERIOD', 'RRCOMPLDT', 'PB_ENTERPRISE_REG_DT', 'PSREASON', 'INSTRUCTIONS', 'WRITE_DOWN_BAL', 'PROP_DEVELOP_FIN_IND', 'NXT_STMT_CYCLE_DT', 'DPMTDBAL', 'INTPAYBL', 'OPENMH', 'CLOSEMH', 'ACCYTD', 'PB_ENTERPRISE_TAG', 'DNBFISME', 'FORATE', 'FORBAL', 'CURBALUS', 'PRIN_ACCT', 'STMT_CYCLE', 'ENTITY_CD', 'COV_OPT_OUT', 'INTPLAN_IBCA', 'PB_ENTERPRISE_PACKAGE_CD', 'L_DEP', 'E_INVOICE_IND', 'CASH_DEPOSIT_LIMIT_IND', 'SOURCE_INCOME_CURRENCY_CD', 'CASH_DEPOSIT_AMOUNT_AGG', 'POST_IND_MAINT_DT', 'POST_IND_EXP_DT', 'LAST_LIMIT_REV_DATE', 'NEXT_LIMIT_REV_DATE', 'FDB_TAG', 'FDB_TAG_DT', 'FDB_SCORING_DT', 'WRIOFF_CLOSE_FILE_TAG', 'WRIOFF_CLOSE_FILE_TAG_DT']
  ENTITY_CD dtype=String distinct(head 30)=['', 'PIBB']
  PRODUCT dtype=Float64 distinct(head 30)=[3.0, 5.0, 9.0, 11.0, 12.0, 13.0, 15.0, 20.0, 22.0, 23.0, 24.0, 25.0, 26.0, 30.0, 31.0, 32.0, 34.0, 40.0, 41.0, 42.0, 43.0, 50.0, 55.0, 57.0, 58.0, 64.0, 65.0, 66.0, 67.0, 68.0]
  CENSUST dtype=Float64 distinct(head 30)=[0.0, 250.0, 252.0, 300.0, 301.0, 302.0, 303.0, 304.0, 305.0, 306.0, 1000.0, 30243.0, 30244.0, 30245.0, 30246.0, 50001.0, 50002.0, 60001.0, 60002.0, 60003.0, 80247.0, 80249.0, 80251.0, 80253.0]
[    0.10s] filtered CA (0 rows)
  CA columns sample: []
[   17.08s] read LIMIT_OVERDFT (1093140 rows)
[    0.20s] parsed ODLMT (154889 rows)
[    0.02s] joined ODLMT (0 rows)
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDP169.py", line 281, in <module>
    gp3 = read_fixed_width(
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDP169.py", line 175, in read_fixed_width
    return pl.DataFrame(exprs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 391, in __init__
    self._df = sequence_to_pydf(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/construction/dataframe.py", line 466, in sequence_to_pydf
    return _sequence_to_pydf_dispatcher(
  File "/usr/lib64/python3.9/functools.py", line 888, in wrapper
    return dispatch(args[0].__class__)(*args, **kw)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/construction/dataframe.py", line 542, in _sequence_to_pydf_dispatcher
    return to_pydf(**common_params)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/construction/dataframe.py", line 645, in _sequence_of_series_to_pydf
    column_names, schema_overrides = _unpack_schema(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/construction/dataframe.py", line 231, in _unpack_schema
    col = col[0]
TypeError: 'ExprNameNameSpace' object is not subscriptable
