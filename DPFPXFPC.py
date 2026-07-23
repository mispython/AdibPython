EIMRESHI - HP Loan Summary & Detail Report
============================================================
============================================================
Report Date: 22/07/2026
Week: 3
RDATE: 220726
============================================================

Reading loan data from SAS files...
  Reading loantemp.sas7bdat...
  LOANTEMP raw rows: 663,747
  LOANTEMP after filtering: 387,612 rows
  Reading lnnote.sas7bdat (chunked, filtered as it streams)...
    ...scanned 1,792,047 raw rows total          
  LNNOTE after filtering: 386,949 rows
  ACCTNO dtype  -> lnnote: Float64, loantemp: Float64
  NOTENO dtype  -> lnnote: Float64, loantemp: Float64
  ACCTNO sample -> lnnote: [2902719229.0, 2903373211.0, 2904870426.0], loantemp: [8709015015.0, 8826241403.0, 8862941712.0]
  NOTENO sample -> lnnote: [90010.0, 90010.0, 90010.0], loantemp: [90010.0, 90010.0, 94010.0]
  Merging data...
  HP Loans after merge: 386,249 accounts

Processing HP loans...
  WARNING: 275,861 of 386,249 rows had an ISSUEDT value that didn't parse as MMDDYYYY (ISSUE DATE will be blank for these). Sample raw ISSUEDT values: ['8052015217.0', '1232013023.0', '6102015161.0', '5312015151.0', '2132020044.0']
  If this dataset encodes dates differently (e.g. this Islamic/Aitab extract uses a different layout than the conventional file), the date format string on the ISSDTE line may need adjusting for this source.
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHI.py", line 341, in <module>
    df_hploan = df_hploan.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "STATENM"; valid columns: ["ACCTNO", "NAME", "NOTENO", "LOANTYPE", "ISSUEDT", "CURBAL", "APPVALUE", "NOTETERM", "CENSUS", "ORGBAL", "NETPROC", "DEALERNO", "BORSTAT", "STATE", "PAYAMT", "SCORE2", "BALANCE", "CAP", "NAME_right", "LSTTRNCD", "CURBAL_right", "COLLDESC", "CENSUS_right", "ORGBAL_right", "FEEDUE", "LOANSTAT", "BORSTAT_right", "PAYAMT_right", "BILDUE", "BILTOT", "BILPAY", "LSTTRNAM", "DELQCD", "USER5", "BLDATE", "BALANCE_right", "PRODUCT", "BRANCH", "ISSDTE", "NOISTLPD", "LASTRAN", "MATURDT", "THISDATE", "CHECKDT", "DAYDIFF", "ARREAR2", "ARREAR", "ISTLPD", "CRRISK", "MARGINF", "CENSUS9"]
