================================================================================
EIBDBT12 - Bank Trade Report
================================================================================
REPTDATE: 2026-06-16
PREVMON: 05
SDATE_SAS: 24288
================================================================================
BTDTL: Parsed 870 records
BTDTL after filter: 843 records

Reading BASE: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS2/input/prod/btbase05.sas7bdat
BASE columns: ['TRANSREX', 'BRANCH', 'ACCTNO', 'OUTSTAND', 'TRANSREF', 'FACILITY', 'PRODTYPE', 'DAYS']
BASE records: 137
BASE after mapping: 137 records
BASE sample (including DAYS):
shape: (5, 5)
┌────────────┬──────────┬───────────┬──────────┬──────┐
│ ACCTNO     ┆ TRANSREF ┆ PREOUTSTD ┆ PRODTYPE ┆ DAYS │
│ ---        ┆ ---      ┆ ---       ┆ ---      ┆ ---  │
│ i64        ┆ str      ┆ f64       ┆ i64      ┆ i64  │
╞════════════╪══════════╪═══════════╪══════════╪══════╡
│ 2500667206 ┆ Y090778  ┆ 50906.19  ┆ 0        ┆ 66   │
│ 2500667206 ┆ Y091056  ┆ 50450.78  ┆ 0        ┆ 33   │
│ 2500830919 ┆ B604100  ┆ 63000.0   ┆ 0        ┆ -9   │
│ 2500830919 ┆ B604114  ┆ 50000.0   ┆ 0        ┆ -9   │
│ 2500830919 ┆ B604354  ┆ 80000.0   ┆ 0        ┆ -10  │
└────────────┴──────────┴───────────┴──────────┴──────┘

After deduplication:
  BASE: 137
  BTDTL: 843

After merge: 137 records

Calculations complete
  Records with OVERDUE > 0: 134

Output written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS2/output/BTRADE/EIBDBT12/DAYBTRD_PM12.txt (137 records)
Parquet saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS2/output/BTRADE/EIBDBT12/DAYBTRD_PM12.parquet

================================================================================
VALIDATION AGAINST PRODUCTION
================================================================================

Comparing specific records:

✗ FAIL ACCTNO: 2500667206, TRANSREF: Y090778
  OVERDUE:   Expected=    67, Got=    67 ✓
  RECOVAMT:  Expected=     -413.70, Got=       13.79 ✗
  DAYS from BASE: 66

✗ FAIL ACCTNO: 2505707731, TRANSREF: Y080273
  OVERDUE:   Expected=  1180, Got=  1180 ✓
  RECOVAMT:  Expected=     -562.80, Got=       18.76 ✗
  DAYS from BASE: 1179

✓ PASS ACCTNO: 2501873900, TRANSREF: Y011618
  OVERDUE:   Expected=  7074, Got=  7074 ✓
  RECOVAMT:  Expected=        0.00, Got=       -0.00 ✓
  DAYS from BASE: 7073

Validation Summary:
  Matches: 1/3
  ✗ SOME TESTS FAILED - Check the differences above

================================================================================
