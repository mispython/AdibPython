Report Parameters: {'REPTYEAR': '26', 'REPTMON': '06', 'REPTDAY': '16', 'PREVMON': '05', 'PREVDAY': '31', 'RDATE': '16-06-2026', 'RDATEX': '0626', 'SDATE': '60701'}
Parsed 870 records from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/BTPM12.txt

Loaded 870 records from BTPM12
Sample of loaded data:
shape: (10, 6)
┌────────┬────────────┬────────────┬──────────┬────────┬──────────┐
│ BRANCH ┆ ACCTNO     ┆ TRANSREF   ┆ OUTSTAND ┆ MATDT  ┆ LIABCODE │
│ ---    ┆ ---        ┆ ---        ┆ ---      ┆ ---    ┆ ---      │
│ i64    ┆ i64        ┆ str        ┆ f64      ┆ str    ┆ str      │
╞════════╪════════════╪════════════╪══════════╪════════╪══════════╡
│ 20006  ┆ 2501873900 ┆ Y011618000 ┆ 33531.01 ┆ 070119 ┆ PBZ      │
│ 20009  ┆ 2505605133 ┆ Y066656000 ┆ 40245.81 ┆ 180426 ┆ PBZ      │
│ 20201  ┆ 2505707731 ┆ Y080273000 ┆ 69258.93 ┆ 230310 ┆ PBA      │
│ 20201  ┆ 2505707731 ┆ Y080340000 ┆ 69128.29 ┆ 230317 ┆ PBA      │
│ 20201  ┆ 2505707731 ┆ Y080415000 ┆ 68921.41 ┆ 230328 ┆ PBA      │
│ 20201  ┆ 2505707731 ┆ Y080466000 ┆ 68790.27 ┆ 230404 ┆ PBA      │
│ 20201  ┆ 2505707731 ┆ Y080602000 ┆ 68604.63 ┆ 230414 ┆ PBA      │
│ 20201  ┆ 2505707731 ┆ Y080732000 ┆ 68342.76 ┆ 230428 ┆ PBA      │
│ 20201  ┆ 2505707731 ┆ Y080733000 ┆ 68342.76 ┆ 230428 ┆ PBA      │
│ 20201  ┆ 2505707731 ┆ Y080832000 ┆ 68138.75 ┆ 230509 ┆ PBA      │
└────────┴────────────┴────────────┴──────────┴────────┴──────────┘

After date parsing, 870 records remain
After filtering, 843 records remain

Reading SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/btbase_05.sas7bdat
Warning: SAS file not found: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/btbase_05.sas7bdat
Created dummy base data with 20 records

Base records after dedup: 20
BTDTL records after dedup: 843
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDBT12.py", line 279, in <module>
    combt = combt.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "R"; valid columns: ["ACCTNO", "TRANSREF", "PREOUTSTD", "PRODTYPE", "BRANCH", "OUTSTAND", "MATDT", "LIABCODE", "day", "month", "year2", "year", "MATDATE"]
