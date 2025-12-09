Today: 2025-12-09
Processing date: 2025-12-08 (yesterday)
CIS file: /host/cis/parquet/year=2025/month=12/day=08/CUST_GROUPING_OUT.parquet
RCIS file: /host/cis/parquet/year=2025/month=12/day=08/CUST_GROUPING_RPTCC.parquet

Reading CISBASEL data from: /host/cis/parquet/year=2025/month=12/day=08/CUST_GROUPING_OUT.parquet
Columns in CIS file: ['GRPING', 'GROUPNO', 'CUSTNO', 'FULLNAME', 'ACCTNO', 'NOTENO', 'PRODUCT', 'AMTINDC', 'BALAMT', 'TOTAMT', 'RLENCODE', 'PRIMSEC']
Data types: [String, String, String, String, String, String, String, String, String, Float64, String, String]
Traceback (most recent call last):
  File "/pythonITD/mis_dev/sas_migration/CIGR/output/EIBWCIGR.py", line 59, in <module>
    cisbasel_df = cisbasel_df.with_columns([
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10020, in with_columns
    self.lazy()
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 330, in wrapper
    return function(*args, **kwargs)
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2335, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.InvalidOperationError: expected String type, got: f64
