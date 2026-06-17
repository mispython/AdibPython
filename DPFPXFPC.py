 3078 2850383833 Y089248    000         124516.44         124507.51        616              8.93 99999 
 3047 2850228011 Y090755    000          45071.03          45067.65         67              3.38 99999 
 3122 2850593610 Y090805    000         143404.72         143394.00         62             10.72 99999 
 3107 2850516517 Y089683    000         458197.44         458163.51        183             33.93 99999 
 3068 2850331901 Y090696    000          50259.35          50255.60         70              3.75 99999 
 3078 2850383833 Y089249    000         151454.20         151443.32        592             10.88 99999 
 3155 2850755315 Y086323    000         208178.55         208163.54        522             15.01 99999 
 3032 2850152613 Y090821    000         100772.70         100765.17         61              7.53 99999 
 3078 2850383833 Y089247    000          71169.52          71164.42        620              5.10 99999 
 3078 2850383833 Y089246    000          49325.90          49322.37        658              3.53 99999 
 3078 2850383833 Y089251    000          59385.95          59381.67        557              4.28 99999 
 3032 2850152613 Y091087    000          97203.04          97195.76         33              7.28 99999 
 3078 2850383833 Y089250    000          54273.25          54269.35        581              3.90 99999 
 3032 2850152613 Y091000    000          83039.52          83033.31         41              6.21 99999 



================================================================================
EIIDBT12 - Islamic Bank Trade Report
================================================================================
REPTDATE: 2026-06-16
PREVMON: 05
SDATE_SAS: 24288
================================================================================
BTDTL: Parsed 870 records
BTDTL after Islamic filter: 27 records

Reading Islamic BASE: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS2/input/prod/ibtbase05.sas7bdat
BASE columns: ['TRANSREX', 'BRANCH', 'ACCTNO', 'OUTSTAND', 'TRANSREF', 'PRODTYPE', 'DAYS']
BASE records: 14
BASE after mapping: 14 records
BASE sample:
shape: (5, 5)
┌────────────┬──────────┬───────────┬──────────┬──────┐
│ ACCTNO     ┆ TRANSREF ┆ PREOUTSTD ┆ PRODTYPE ┆ DAYS │
│ ---        ┆ ---      ┆ ---       ┆ ---      ┆ ---  │
│ i64        ┆ str      ┆ f64       ┆ i64      ┆ i64  │
╞════════════╪══════════╪═══════════╪══════════╪══════╡
│ 2850152613 ┆ Y090821  ┆ 100772.7  ┆ 0        ┆ 60   │
│ 2850152613 ┆ Y091000  ┆ 83039.52  ┆ 0        ┆ 40   │
│ 2850152613 ┆ Y091087  ┆ 97203.04  ┆ 0        ┆ 32   │
│ 2850228011 ┆ Y090755  ┆ 45071.03  ┆ 0        ┆ 66   │
│ 2850331901 ┆ Y090696  ┆ 50259.35  ┆ 0        ┆ 69   │
└────────────┴──────────┴───────────┴──────────┴──────┘

After deduplication:
  BASE: 14
  BTDTL: 27

After merge: 14 records

Calculations complete
  Records with OVERDUE > 0: 14

================================================================================
DEBUG: Checking specific records
================================================================================

ACCTNO: 2850228011, TRANSREF: Y090755
  PREOUTSTD (from BASE): 45071.03
  OUTSTAND (from BTDTL): 45067.65
  DAYS (from BASE): 66
  OVERDUE: 67
  RECOVAMT: 3.3799999999973807

ACCTNO: 2850331901, TRANSREF: Y090696
  PREOUTSTD (from BASE): 50259.35
  OUTSTAND (from BTDTL): 50255.6
  DAYS (from BASE): 69
  OVERDUE: 70
  RECOVAMT: 3.75

ACCTNO: 2850383833, TRANSREF: Y089246
  PREOUTSTD (from BASE): 49325.899999999994
  OUTSTAND (from BTDTL): 49322.37
  DAYS (from BASE): 657
  OVERDUE: 658
  RECOVAMT: 3.52999999999156

Output written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS2/output/BTRADE/EIIDBT12/DAYBTRD_PM12.txt (14 records)
Parquet saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS2/output/BTRADE/EIIDBT12/DAYBTRD_PM12.parquet

================================================================================
VALIDATION AGAINST PRODUCTION
================================================================================

Comparing specific records:

✗ FAIL ACCTNO: 2850228011, TRANSREF: Y090755
  PREOUTSTD:  Expected=    70184.05, Got=    45071.03 ✗
  OUTSTAND:   Expected=    45067.65, Got=    45067.65 ✓
  OVERDUE:    Expected=    67, Got=    67 ✓
  RECOVAMT:   Expected=    25116.40, Got=        3.38 ✗
  DAYS from BASE: 66

✗ FAIL ACCTNO: 2850331901, TRANSREF: Y090696
  PREOUTSTD:  Expected=    50142.80, Got=    50259.35 ✗
  OUTSTAND:   Expected=    50255.60, Got=    50255.60 ✓
  OVERDUE:    Expected=    70, Got=    70 ✓
  RECOVAMT:   Expected=     -112.80, Got=        3.75 ✗
  DAYS from BASE: 69

✗ FAIL ACCTNO: 2850383833, TRANSREF: Y089246
  PREOUTSTD:  Expected=    49216.32, Got=    49325.90 ✗
  OUTSTAND:   Expected=    49322.37, Got=    49322.37 ✓
  OVERDUE:    Expected=   658, Got=   658 ✓
  RECOVAMT:   Expected=     -106.05, Got=        3.53 ✗
  DAYS from BASE: 657

Validation Summary:
  Matches: 0/3
  ✗ SOME TESTS FAILED - Check the differences above

================================================================================
SDATE Calculation Details:
  reptdate: 2026-06-16
  sdate (next month): 2026-07-01
  sdate_sas: 24288
================================================================================

✓ Output file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS2/output/BTRADE/EIIDBT12/DAYBTRD_PM12.txt
  Size: 1,456 bytes
  Lines: 14
✓ Parquet file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS2/output/BTRADE/EIIDBT12/DAYBTRD_PM12.parquet
  Size: 9,905 bytes

================================================================================
PROCESS COMPLETE
================================================================================
