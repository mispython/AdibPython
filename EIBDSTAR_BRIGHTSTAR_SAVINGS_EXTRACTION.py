BANKNO	FMTCODE	BRANCH	ACCTNO	NAME	TAXNO	DEBIT	CREDIT	CLOSEDT	REOPENDT	CUSTCODE	ORGCODE	ORGTYPE	INTYTD	FEEPD	PURPOSE	SECTOR	USER2	USER3	RISKCODE	LEDGBAL	OPENIND	STATCD	LASTTRAN	CHGIND	AVGAMT	PRODUCT	RACE	DEPTYPE	INT1	INTPD	INTPLAN	CURBAL	CHQFLOAT	MTDLOWBA	BENINTPD	STATE	INTCYCODE	CURCODE	PBIND	INTRATE	YTDAVAMT	BDATE	INACTIVE	SECOND	ODXSAMT	BONUTYPE	SERVICE	BONUSANO	USER5	TRACKCD	EXODDATE	TEMPODDT	SCHIND	PREVBRNO	AVGBAL	COSTCTR	POST_IND	INTRSTPD	MTDAVBAL	DTLSTCUST	INTPDPYR	OPENDT	PRIN_ACCT	CASH_DEPOSIT_LIMIT_IND	NXT_STMT_CYCLE_DT	STMT_CYCLE	AC_OPEN_STATUS_CD	DNBFISME	DPMTDBAL	INTPAYBL	OPENMH	CLOSEMH	ACCYTD	MAILCODE	CONVDT	PSREASON	INSTRUCTIONS	POST_IND_MAINT_DT	POST_IND_EXP_DT	SSADATE

Traceback (most recent call last):
  File "/pythonITD/mis_dev/sas_migration/BRIGHTSTAR/EIBDSTAR_BRIGHTSTAR_SAVINGS_EXTRACTION.py", line 19, in <module>
    deposit = pl.read_csv(deposit_saving_path)
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 128, in wrapper
    return function(*args, **kwargs)
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 128, in wrapper
    return function(*args, **kwargs)
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 128, in wrapper
    return function(*args, **kwargs)
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/io/csv/functions.py", line 549, in read_csv
    df = _read_csv_impl(
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/io/csv/functions.py", line 697, in _read_csv_impl
    pydf = PyDataFrame.read_csv(
polars.exceptions.ComputeError: could not parse `6.01884E15` as dtype `i64` at column 'BONUSANO' (column number 49)

The current offset in the file is 900 bytes.

You might want to try:
- increasing `infer_schema_length` (e.g. `infer_schema_length=10000`),
- specifying correct dtype with the `schema_overrides` argument
- setting `ignore_errors` to `True`,
- adding `6.01884E15` to the `null_values` list.

Original error: ```remaining bytes non-empty```
