REPTDATE = 2025-11-26, PREVDATE = 2025-11-25
RDATE (SAS format) = 24071
MON=11, DAY=26, YEAR=25
Filter date (reptdte) = 251126
SAVING.csv columns: ['AC_OPEN_STATUS_CD', 'ACCTNO', 'ACCYTD', 'AVGAMT', 'AVGBAL', 'BANKNO', 'BDATE', 'BENINTPD', 'BONUSANO', 'BONUTYPE', 'BRANCH', 'CASH_DEPOSIT_LIMIT_IND', 'CHGIND', 'CHQFLOAT', 'CLOSEDT', 'CLOSEMH', 'CONVDT', 'COSTCTR', 'CREDIT', 'CURBAL', 'CURCODE', 'CUSTCODE', 'DEBIT', 'DEPTYPE', 'DNBFISME', 'DPMTDBAL', 'DTLSTCUST', 'EXODDATE', 'FEEPD', 'FMTCODE', 'INACTIVE', 'INSTRUCTIONS', 'INT1', 'INTCYCODE', 'INTPAYBL', 'INTPD', 'INTPDPYR', 'INTPLAN', 'INTRATE', 'INTRSTPD', 'INTYTD', 'LASTTRAN', 'LEDGBAL', 'MAILCODE', 'MTDAVBAL', 'MTDLOWBA', 'NAME', 'NXT_STMT_CYCLE_DT', 'ODXSAMT', 'OPENDT', 'OPENIND', 'OPENMH', 'ORGCODE', 'ORGTYPE', 'PBIND', 'POST_IND', 'POST_IND_EXP_DT', 'POST_IND_MAINT_DT', 'PREVBRNO', 'PRIN_ACCT', 'PRODUCT', 'PSREASON', 'PURPOSE', 'RACE', 'REOPENDT', 'RISKCODE', 'SCHIND', 'SECOND', 'SECTOR', 'SERVICE', 'SSADATE', 'STATCD', 'STATE', 'STMT_CYCLE', 'TAXNO', 'TEMPODDT', 'TRACKCD', 'USER2', 'USER3', 'USER5', 'YTDAVAMT']
SAVING.csv shape: (8320, 81)
SAVING.csv dtypes: [Int64, String, Int64, Int64, String, String, Float64, Float64, Int64, Int64, String, Int64, String, Int64, Int64, String, Int64, String, String, Float64, Float64, String, String, Int64, Int64, Int64, Int64, String, String, Float64, Float64, Int64, Float64, Int64, Float64, Float64, String, Int64, String, String, Float64, Float64, Int64, String, Int64, Float64, String, Int64, Float64, String, String, Int64, Int64, String, Int64, Float64, Int64, String, Float64, Float64, Int64, Float64, Int64, Int64, String, Int64, Int64, Int64, Int64, Float64, Float64, Int64, Int64, Int64, String, Int64, String, String, Int64, Int64, Int64]
Traceback (most recent call last):
  File "/pythonITD/mis_dev/sas_migration/BRIGHTSTAR/EIBDSTAR_BRIGHTSTAR_SAVINGS_EXTRACTION.py", line 77, in <module>
    cis.with_columns(pl.col("CUSTOPENDATE").cast(pl.Int64))
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10020, in with_columns
    self.lazy()
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 330, in wrapper
    return function(*args, **kwargs)
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2335, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "N"; valid columns: ["CUSTNO", "BANKNO", "ACCTNOC", "ACCTNO", "ACCTCODE", "RLENCODE", "PRISEC", "RLENTYPE", "RLENDESC", "PROCESSTIME", "GENDER", "CUSTSTAT", "TAXCODE", "TAXID", "CUSTBRCH", "COSTCTR", "CUSTLASTDATECC", "CUSTLASTDATEYY", "CUSTLASTDATEMM", "CUSTLASTDATEDD", "CUSTLASTOPER", "PRIM_OFF", "SEC_OFF", "PRIM_LN_OFF", "SEC_LN_OFF", "RESIDENCY", "RACE", "CITIZENSHIP", "CUSTSINCEDATE", "CUSTOPENDATE", "HRC01", "HRC02", "HRC03", "HRC04", "HRC05", "HRC06", "HRC07", "HRC08", "HRC09", "HRC10", "HRC11", "HRC12", "HRC13", "HRC14", "HRC15", "HRC16", "HRC17", "HRC18", "HRC19", "HRC20", "EXPERIENCE", "HOBBIES", "RELIGION", "LANGUAGE", "INST_SEC", "CUST_CODE", "CUSTCONSENT", "BASICGRPCODE", "MSICCODE", "MASCO2008", "CUSTMNTDATE", "INDORG", "HRCINDC", "HRC998", "HRCPEP", "HRC037", "ADDREF", "CUSTNAME", "PRIPHONE", "SECPHONE", "MOBILEPH", "FAX", "NAMEFMT", "ALIASKEY", "ALIAS", "BNMKEY", "BNMID", "INCOME", "EDUCATION", "OCCUP", "MARITALSTAT", "OWNRENT", "EMPNAME", "DOBCC", "DOBYY", "DOBMM", "DOBDD", "DOBDOR", "SICCODE", "CORPSTATUS", "NETWORTH", "LONGNAME", "JOINTACC", "PRCOUNTRY", "EMPLNAME", "EMPLTYPE", "EMPLSECT", "EMPLDATE", "EMPLTIME", "rownum"]
