Traceback (most recent call last):
  File "/sas/pythonITD/mis/job/LOAN/MIS_ENRH_LN_BILL.py", line 267, in <module>
    log1 = set_data(result, "ln", "ctrl", enrh_ln_bill, enrh_ln_bill_ctl)
  File "/sas/pythonITD/mis/job/LOAN/MIS_ENRH_LN_BILL.py", line 226, in set_data
    sas.df2sd(df, table=cur_data, libref='work')
  File "/sas/pythonITD/mis/lib64/python3.9/site-packages/saspy/sasbase.py", line 1577, in df2sd
    return self.dataframe2sasdata(df, table, libref, results, keep_outer_quotes, embedded_newlines, LF, CR, colsep, colrep,
  File "/sas/pythonITD/mis/lib64/python3.9/site-packages/saspy/sasbase.py", line 1690, in dataframe2sasdata
    charlens = self.df_char_lengths(df, encode_errors, char_lengths)
  File "/sas/pythonITD/mis/lib64/python3.9/site-packages/saspy/sasbase.py", line 1476, in df_char_lengths
    if df.dtypes[name].kind in ('O','S','U','V'):
TypeError: list indices must be integers or slices, not str
SAS Connection terminated. Subprocess id was 1979032
