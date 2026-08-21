Report Date: 200826 (20-08-26)
DPDATE: 200826 EQDATE: 200826
DPST records: 90326
DPST TICKETNO sample: ['N319474', 'N362347', 'N362466', 'N377670', 'N380788']
DPST TICKETNO dtype: String
EQTN records: 532
EQTN TICKETNO sample: ['Z31222', 'Z31350', 'Z31156', 'Z31236', 'Z31336']
EQTN TICKETNO dtype: String
Common TICKETNO values: 266
DCID records after merge and filter: 266
Reading CA file...
Reading SA file...
Reading FCY file...
Reference data records: 5229286
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDDCIA.py", line 319, in <module>
    dcid2 = dcid.join(dpdata, left_on="INVCURRAC", right_on="INVCURRAC2", how="left")
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 128, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 8242, in join
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.SchemaError: datatypes of join keys don't match - `INVCURRAC`: str on left does not match `INVCURRAC2`: f64 on right (and no other type was available to cast to)
