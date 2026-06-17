================================================================================
EIBDBT12 - Bank Trade Report
================================================================================
REPTDATE: 2026-06-16
PREVMON: 05
SDATE: 2026-07-01
SDATE_SAS: 24299 (expected: 24299 for 2026-07-01)
================================================================================
BTDTL: Parsed 870 records
BTDTL after filter: 843 records

BTDTL sample (checking MATDATE_SAS):
shape: (10, 3)
┌────────────┬──────────┬─────────────┐
│ ACCTNO     ┆ TRANSREF ┆ MATDATE_SAS │
│ ---        ┆ ---      ┆ ---         │
│ i64        ┆ str      ┆ i64         │
╞════════════╪══════════╪═════════════╡
│ 2501873900 ┆ Y011618  ┆ 17196       │
│ 2505605133 ┆ Y066656  ┆ 21311       │
│ 2505707731 ┆ Y080273  ┆ 23090       │
│ 2505707731 ┆ Y080340  ┆ 23097       │
│ 2505707731 ┆ Y080415  ┆ 23108       │
│ 2505707731 ┆ Y080466  ┆ 23115       │
│ 2505707731 ┆ Y080602  ┆ 23125       │
│ 2505707731 ┆ Y080732  ┆ 23139       │
│ 2505707731 ┆ Y080733  ┆ 23139       │
│ 2505707731 ┆ Y080832  ┆ 23150       │
└────────────┴──────────┴─────────────┘

Reading BASE: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS2/input/prod/btbase05.sas7bdat
BASE columns: ['TRANSREX', 'BRANCH', 'ACCTNO', 'OUTSTAND', 'TRANSREF', 'FACILITY', 'PRODTYPE', 'DAYS']
BASE records: 137
BASE after mapping: 137 records
BASE sample (first 5):
shape: (5, 4)
┌────────────┬──────────┬───────────┬──────────┐
│ ACCTNO     ┆ TRANSREF ┆ PREOUTSTD ┆ PRODTYPE │
│ ---        ┆ ---      ┆ ---       ┆ ---      │
│ i64        ┆ str      ┆ f64       ┆ i64      │
╞════════════╪══════════╪═══════════╪══════════╡
│ 2500667206 ┆ Y090778  ┆ 50906.19  ┆ 0        │
│ 2500667206 ┆ Y091056  ┆ 50450.78  ┆ 0        │
│ 2500830919 ┆ B604100  ┆ 63000.0   ┆ 0        │
│ 2500830919 ┆ B604114  ┆ 50000.0   ┆ 0        │
│ 2500830919 ┆ B604354  ┆ 80000.0   ┆ 0        │
└────────────┴──────────┴───────────┴──────────┘

After deduplication:
  BASE: 137
  BTDTL: 843

After merge: 137 records
Unmatched BASE records (no BTDTL match): 3

Using SDATE_SAS: 24299

Calculations complete
  Records with OVERDUE > 0: 134

Output written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS2/output/BTRADE/EIBDBT12/DAYBTRD_PM12.txt (137 records)
Parquet saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS2/output/BTRADE/EIBDBT12/DAYBTRD_PM12.parquet

================================================================================
VALIDATION AGAINST PRODUCTION
================================================================================

Comparing specific records:

✗ FAIL ACCTNO: 2500667206, TRANSREF: Y090778
  OVERDUE:   Expected=    67, Got=    97 ✗
  RECOVAMT:  Expected=     -413.70, Got=       13.79 ✗
     SDATE_SAS: 24299
     MATDATE_SAS: 24203
     Formula: (24299 + 1) - 24203 = 97

✗ FAIL ACCTNO: 2505707731, TRANSREF: Y080273
  OVERDUE:   Expected=  1180, Got=  1210 ✗
  RECOVAMT:  Expected=     -562.80, Got=       18.76 ✗
     SDATE_SAS: 24299
     MATDATE_SAS: 23090
     Formula: (24299 + 1) - 23090 = 1210

✗ FAIL ACCTNO: 2501873900, TRANSREF: Y011618
  OVERDUE:   Expected=  7074, Got=  7104 ✗
  RECOVAMT:  Expected=        0.00, Got=       -0.00 ✓
     SDATE_SAS: 24299
     MATDATE_SAS: 17196
     Formula: (24299 + 1) - 17196 = 7104

Validation Summary:
  Matches: 0/3
  ✗ SOME TESTS FAILED - Check the differences above

================================================================================
SDATE Calculation Details:
  reptdate: 2026-06-16
  sdate (next month): 2026-07-01
  sdate_sas: 24299
================================================================================
