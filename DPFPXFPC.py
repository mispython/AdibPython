Running EIBAEGLD for 2026-06-15, MON=06, DAY=15, NOWK=2
Looking for input file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/gold/goldtran062.sas7bdat
Successfully loaded using pandas
Loaded 8167 rows from SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/gold/goldtran062.sas7bdat
Columns: ['TRXNYY', 'TRXNMM', 'TRXNDD', 'ACCTNO', 'MPURCGM', 'MSALEGM', 'BRANCH', 'MPURCPR', 'MPURCAMT', 'MSALEPR', 'MSALEAMT', 'TRXNDATE', 'REPTDATE', 'CHANNELIND', 'TRANCODE', 'CHANNEL']

Column data types:
[Float64, Float64, Float64, Float64, Float64, Float64, Float64, Float64, Float64, Float64, Float64, Float64, Float64, Binary, Float64, Float64]

REPTDATE data type: Float64
Sample REPTDATE values (first 5): [24266.0, 24266.0, 24266.0, 24266.0, 24266.0]

Filtered to 692 rows for date 2026-06-15
Sample data (first 5 rows):
shape: (5, 18)
┌────────┬────────┬────────┬──────────┬───┬──────────┬─────────┬──────────────┬──────────────┐
│ TRXNYY ┆ TRXNMM ┆ TRXNDD ┆ ACCTNO   ┆ … ┆ TRANCODE ┆ CHANNEL ┆ REPTDATE_INT ┆ REPTDATE_STR │
│ ---    ┆ ---    ┆ ---    ┆ ---      ┆   ┆ ---      ┆ ---     ┆ ---          ┆ ---          │
│ f64    ┆ f64    ┆ f64    ┆ f64      ┆   ┆ f64      ┆ f64     ┆ i64          ┆ str          │
╞════════╪════════╪════════╪══════════╪═══╪══════════╪═════════╪══════════════╪══════════════╡
│ 2026.0 ┆ 6.0    ┆ 15.0   ┆ 6.5700e9 ┆ … ┆ null     ┆ null    ┆ 24272        ┆ 2026-06-15   │
│ 2026.0 ┆ 6.0    ┆ 15.0   ┆ 6.5701e9 ┆ … ┆ null     ┆ null    ┆ 24272        ┆ 2026-06-15   │
│ 2026.0 ┆ 6.0    ┆ 15.0   ┆ 6.5703e9 ┆ … ┆ null     ┆ null    ┆ 24272        ┆ 2026-06-15   │
│ 2026.0 ┆ 6.0    ┆ 15.0   ┆ 6.5704e9 ┆ … ┆ null     ┆ null    ┆ 24272        ┆ 2026-06-15   │
│ 2026.0 ┆ 6.0    ┆ 15.0   ┆ 6.5704e9 ┆ … ┆ null     ┆ null    ┆ 24272        ┆ 2026-06-15   │
└────────┴────────┴────────┴──────────┴───┴──────────┴─────────┴──────────────┴──────────────┘
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/CONVERTED JOBS/EIBAEGLD.py", line 144, in <module>
    temp_goldtran.write_csv(temp_file_txt, separator='|')
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 3099, in write_csv
    self.lazy().sink_csv(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 3459, in sink_csv
    ldf.collect(engine=engine)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ComputeError: datatype binary cannot be written to CSV

Consider using JSON or a binary format.
