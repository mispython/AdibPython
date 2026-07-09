ARNING: Missing columns: ['YY', 'MM', 'DD', 'DATE', 'GLITEM', 'SIGN', 'BALANCE']
Available columns: ['20260708                                                                        \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00']
Attempting to continue with available columns...
WARNING: Could not parse YY/MM/DD from text file. Using yesterday's date.
WARNING: SIGN or BALANCE columns not found. Continuing without sign adjustment.
Processing GL P1...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDNLGL.py", line 236, in <module>
    results_p1 = process_gl_data(df_gl, 'P1')
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDNLGL.py", line 113, in process_gl_data
    filtered = df_gl.filter(condition)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 5325, in filter
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "GLITEM"; valid columns: ["20260708                                                                        \0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"]
You have mail in /var/spool/mail/sas_edw_dev
