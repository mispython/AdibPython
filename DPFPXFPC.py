================================================================================
EIBDBT12 - Bank Trade Report
================================================================================
REPTDATE: 2026-06-16
PREVDATE: 2026-05-31
PREVMON: 05
SDATE (SAS): 24288 (Z5: 24288)
RDATEX: 0626
================================================================================
BTDTL: Parsed 870 records
BTDTL after filter: 843 records
BTDTL sample:
shape: (5, 8)
┌────────┬────────────┬──────────┬──────────┬────────────┬─────────────┬──────────┬──────────┐
│ BRANCH ┆ ACCTNO     ┆ TRANSREF ┆ OUTSTAND ┆ MATDATE    ┆ MATDATE_SAS ┆ LIABCODE ┆ FACILITY │
│ ---    ┆ ---        ┆ ---      ┆ ---      ┆ ---        ┆ ---         ┆ ---      ┆ ---      │
│ i64    ┆ i64        ┆ str      ┆ f64      ┆ date       ┆ i64         ┆ str      ┆ str      │
╞════════╪════════════╪══════════╪══════════╪════════════╪═════════════╪══════════╪══════════╡
│ 6      ┆ 2501873900 ┆ Y011618  ┆ 33531.01 ┆ 2007-01-19 ┆ 17185       ┆ 9PB      ┆ 99999    │
│ 9      ┆ 2505605133 ┆ Y066656  ┆ 40245.81 ┆ 2018-04-26 ┆ 21300       ┆ 6PB      ┆ 99999    │
│ 201    ┆ 2505707731 ┆ Y080273  ┆ 69258.93 ┆ 2023-03-10 ┆ 23079       ┆ 0PB      ┆ 99999    │
│ 201    ┆ 2505707731 ┆ Y080340  ┆ 69128.29 ┆ 2023-03-17 ┆ 23086       ┆ 7PB      ┆ 99999    │
│ 201    ┆ 2505707731 ┆ Y080415  ┆ 68921.41 ┆ 2023-03-28 ┆ 23097       ┆ 8PB      ┆ 99999    │
└────────┴────────────┴──────────┴──────────┴────────────┴─────────────┴──────────┴──────────┘
ERROR: BASE file not found for month 05
Tried: input/prod/btbase05.sas7bdat
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS2/EIBDBT12.py", line 220, in <module>
    base = read_base(params['PREVMON'])
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS2/EIBDBT12.py", line 178, in read_base
    raise FileNotFoundError(f"BASE file not found: {base_file}")
FileNotFoundError: BASE file not found: input/prod/btbase05.sas7bdat
