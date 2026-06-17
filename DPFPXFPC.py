================================================================================
EIIDBT12 - Islamic Bank Trade Report
================================================================================
REPTDATE: 2026-06-16
PREVMON: 05
SDATE_SAS: 24288
================================================================================
BTDTL: Parsed 870 records
BTDTL after Islamic filter (keeping qualifying records): 27 records
BTDTL sample:
shape: (5, 7)
┌────────┬────────────┬──────────┬───────────┬─────────────┬──────────┬──────────┐
│ BRANCH ┆ ACCTNO     ┆ TRANSREF ┆ OUTSTAND  ┆ MATDATE_SAS ┆ LIABCODE ┆ FACILITY │
│ ---    ┆ ---        ┆ ---      ┆ ---       ┆ ---         ┆ ---      ┆ ---      │
│ i64    ┆ i64        ┆ str      ┆ f64       ┆ i64         ┆ str      ┆ str      │
╞════════╪════════════╪══════════╪═══════════╪═════════════╪══════════╪══════════╡
│ 3155   ┆ 2850755315 ┆ Y086323  ┆ 208163.54 ┆ 23737       ┆ 7PB      ┆ 99999    │
│ 3078   ┆ 2850383833 ┆ Y089246  ┆ 49322.37  ┆ 23601       ┆ 3PB      ┆ 99999    │
│ 3078   ┆ 2850383833 ┆ Y089247  ┆ 71164.42  ┆ 23639       ┆ 0PB      ┆ 99999    │
│ 3078   ┆ 2850383833 ┆ Y089248  ┆ 124507.51 ┆ 23643       ┆ 4PB      ┆ 99999    │
│ 3078   ┆ 2850383833 ┆ Y089249  ┆ 151443.32 ┆ 23667       ┆ 8PB      ┆ 99999    │
└────────┴────────────┴──────────┴───────────┴─────────────┴──────────┴──────────┘

Reading Islamic BASE: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS2/input/prod/ibtbase05.sas7bdat
BASE columns: ['TRANSREX', 'BRANCH', 'ACCTNO', 'OUTSTAND', 'TRANSREF', 'PRODTYPE', 'DAYS']
BASE records: 14
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS2/EIIDBT12.py", line 215, in <module>
    base = read_base_sas(params['PREVMON'])
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS2/EIIDBT12.py", line 198, in read_base_sas
    pl.col(col_names[7]).alias("DAYS"),
IndexError: list index out of range
