Starting LONPAC data processing...
Reading from: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMLNPC
Writing to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMLNPC
Processing PA data...
sys:1: UserWarning: CSV malformed: expected 8 rows, actual 16 rows, in chunk starting at byte offset 2724628, length 24032
sys:1: UserWarning: CSV malformed: expected 58 rows, actual 113 rows, in chunk starting at byte offset 2554902, length 169726
sys:1: UserWarning: CSV malformed: expected 58 rows, actual 113 rows, in chunk starting at byte offset 2385176, length 169726
sys:1: UserWarning: CSV malformed: expected 57 rows, actual 113 rows, in chunk starting at byte offset 2215450, length 169726
sys:1: UserWarning: CSV malformed: expected 60 rows, actual 114 rows, in chunk starting at byte offset 2044222, length 171228
sys:1: UserWarning: CSV malformed: expected 56 rows, actual 114 rows, in chunk starting at byte offset 1872994, length 171228
sys:1: UserWarning: CSV malformed: expected 55 rows, actual 113 rows, in chunk starting at byte offset 1703268, length 169726
sys:1: UserWarning: CSV malformed: expected 53 rows, actual 114 rows, in chunk starting at byte offset 1532040, length 171228
sys:1: UserWarning: CSV malformed: expected 59 rows, actual 113 rows, in chunk starting at byte offset 1362314, length 169726
sys:1: UserWarning: CSV malformed: expected 56 rows, actual 113 rows, in chunk starting at byte offset 1192588, length 169726
sys:1: UserWarning: CSV malformed: expected 56 rows, actual 113 rows, in chunk starting at byte offset 1022862, length 169726
sys:1: UserWarning: CSV malformed: expected 54 rows, actual 113 rows, in chunk starting at byte offset 853136, length 169726
sys:1: UserWarning: CSV malformed: expected 57 rows, actual 114 rows, in chunk starting at byte offset 681908, length 171228
sys:1: UserWarning: CSV malformed: expected 57 rows, actual 114 rows, in chunk starting at byte offset 510680, length 171228
sys:1: UserWarning: CSV malformed: expected 58 rows, actual 114 rows, in chunk starting at byte offset 339452, length 171228
sys:1: UserWarning: CSV malformed: expected 55 rows, actual 113 rows, in chunk starting at byte offset 169726, length 169726
sys:1: UserWarning: CSV malformed: expected 57 rows, actual 113 rows, in chunk starting at byte offset 0, length 169726
Error during processing: unable to find column "EXPIRYDT"; valid columns: ["POLICYNO", "NAME", "NEWIC", "OLDIC", "REGNO", "DOBX", "GENDER", "AGENTNO", "BRANCH", "ACCTNOX", "RACE", "MARITAL", "INSURED", "ISSUEDTX","EXPDT", "PREMIUM", "TELNO", "ADDRESS", "TOWN", "POSTCODE", "PROD_CODE", "PROD_DESC", "PROCESS_MTH", "SUBMITDX", "PROPOSALDT", "POLICYNO_OLD", "REGNO_NEW", "AUTO_RENEWAL_IND", "column_29", "ISSUEDT", "EPDT", "SUBMITD", "DOB", "PRODUCT", "NOTENO", "ACCTNO"]
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMLNPC.py", line 448, in process_lonpac_data
    process_pa_data(input_path, output_path, reptyear4)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMLNPC.py", line 147, in process_pa_data
    df_prod = df.select(prod_cols)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10148, in select
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "EXPIRYDT"; valid columns: ["POLICYNO", "NAME", "NEWIC", "OLDIC", "REGNO", "DOBX", "GENDER", "AGENTNO", "BRANCH", "ACCTNOX", "RACE", "MARITAL", "INSURED", "ISSUEDTX", "EXPDT", "PREMIUM", "TELNO", "ADDRESS", "TOWN", "POSTCODE", "PROD_CODE", "PROD_DESC", "PROCESS_MTH", "SUBMITDX", "PROPOSALDT", "POLICYNO_OLD", "REGNO_NEW", "AUTO_RENEWAL_IND", "column_29", "ISSUEDT","EPDT", "SUBMITD", "DOB", "PRODUCT", "NOTENO", "ACCTNO"]
