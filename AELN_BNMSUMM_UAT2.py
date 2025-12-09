Today: 2025-12-09
Processing last day of previous month: 2025-12-08
Report month: 12, Report year: 25

CIS file: /host/cis/parquet/year=2025/month=12/day=08/CUST_GROUPING_OUT.parquet
RCIS file: /host/cis/parquet/year=2025/month=12/day=08/CUST_GROUPING_RPTCC.parquet

Reading ICISBASEL data from: /host/cis/parquet/year=2025/month=12/day=08/CUST_GROUPING_OUT.parquet
Traceback (most recent call last):
  File "/pythonITD/mis_dev/sas_migration/CIGR/EIIWCIGR_ISLAMIC.py", line 32, in <module>
    icisbasel_df = icisbasel_df.with_columns([
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10020, in with_columns
    self.lazy()
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 330, in wrapper
    return function(*args, **kwargs)
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2335, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ComputeError: cannot compare string with numeric type (f64)
