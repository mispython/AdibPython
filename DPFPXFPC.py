============================================================
EIFLTEXP PROCESSING STARTED
============================================================
MNI Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/MNI
IMNI Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/IMNI
PIDM Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/PIDM
Output Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFLTEXP
============================================================

[STEP 1] Loading FDMTHLY data (MNI + IMNI)...
  - MNI FDMTHLY loaded: 2756145 records
  - IMNI FDMTHLY loaded: 431257 records
  - Combined FDMTHLY: 3187402 records
  - FDMTHLY saved

[STEP 2] Loading CURN data (MNI + IMNI)...
  - MNI CURN124 loaded: 915692 records
  - IMNI CURN124 loaded: 154763 records
  - Combined CURN: 1070455 records
  - CURN filtered (removed PRODUCT=139): 1070184 records

[STEP 3] Loading SAVG data (MNI + IMNI)...
  - MNI SAVG124 loaded: 4241108 records
  - IMNI SAVG124 loaded: 2262899 records

[STEP 4] Adding CURN to dataset list...
  - CURN added with 1070184 records

[STEP 5] Adding FDMTHLY to dataset list...
  - FDMTHLY added with 3187402 records

[STEP 6] Combining all datasets...
  - Total datasets to combine: 4
    1. MNI SAVG124: 4241108 records
    2. IMNI SAVG124: 2262899 records
    3. CURN: 1070184 records
    4. FDMTHLY: 3187402 records
  - Combined data: 10761593 records

  - DEBUG: Sample of prodcd values:
    [(None,), (None,), (None,), (None,), (None,), (None,), (None,), (None,), (None,), (None,), (None,), (None,), (None,), (None,), (None,), (None,), (None,), (None,), (None,), (None,)]
    Unique prodcd values: 4
    Null prodcd count: 7574191
    Sample unique prodcd values: ['42630', None, '42130', '42133']

[STEP 7] Applying filters and transformations...
  - Before PROGCD filter: 10761593
  - After PROGCD filter: 3187402 records
  - DEBUG: Filtered prodcd unique values: ['42630', '42130', '42133']
  - After PRODUCT=166: 3187402 records
  - After PROGCD special: 3187402 records
  - After PRODUCT filter: 3187402 records
  - After INTPAYBL: 3187402 records
  - DEPOSIT saved with 3187402 records

  - DEBUG: Sample DEPOSIT records (first 5):
shape: (5, 8)
┌──────────┬────────┬────────┬─────────┬──────────┬─────────┬─────────┬────────┐
│ acctno   ┆ amtind ┆ branch ┆ curbal  ┆ intpaybl ┆ ledgbal ┆ product ┆ progcd │
│ ---      ┆ ---    ┆ ---    ┆ ---     ┆ ---      ┆ ---     ┆ ---     ┆ ---    │
│ f64      ┆ str    ┆ f64    ┆ f64     ┆ f64      ┆ f64     ┆ f64     ┆ str    │
╞══════════╪════════╪════════╪═════════╪══════════╪═════════╪═════════╪════════╡
│ 1.0000e9 ┆ D      ┆ 2.0    ┆ 1350.7  ┆ 7.44     ┆ 1350.7  ┆ 302.0   ┆ 42130  │
│ 1.0000e9 ┆ D      ┆ 2.0    ┆ 3513.03 ┆ 29.59    ┆ 3513.03 ┆ 302.0   ┆ 42130  │
│ 1.0000e9 ┆ D      ┆ 2.0    ┆ 1508.64 ┆ 1.21     ┆ 1508.64 ┆ 301.0   ┆ 42130  │
│ 1.0000e9 ┆ D      ┆ 2.0    ┆ 2233.49 ┆ 10.5     ┆ 2233.49 ┆ 301.0   ┆ 42130  │
│ 1.0000e9 ┆ D      ┆ 2.0    ┆ 2234.89 ┆ 6.33     ┆ 2234.89 ┆ 301.0   ┆ 42130  │
└──────────┴────────┴────────┴─────────┴──────────┴─────────┴─────────┴────────┘

[STEP 8] Loading FLOAT data...
  - FLOAT loaded: 18927 records
  - FLOAT_SUMMARY saved: 18927 records

  - DEBUG: Sample FLOAT records (first 5):
shape: (5, 2)
┌──────────┬──────────┐
│ acctno   ┆ float    │
│ ---      ┆ ---      │
│ f64      ┆ f64      │
╞══════════╪══════════╡
│ 3.4008e9 ┆ 26184.07 │
│ 3.1397e9 ┆ 1140.0   │
│ 3.9997e9 ┆ 20000.0  │
│ 3.2360e9 ┆ 164.1    │
│ 3.1864e9 ┆ 12496.4  │
└──────────┴──────────┘

[STEP 9] Merging DEPOSIT and FLOAT, applying B AND NOT A...
  - DEPOSIT unique ACCTNO count: 1071746
  - FLOAT unique ACCTNO count: 18927
  - Overlap ACCTNO count: 0
  - FLOAT records without DEPOSIT: 18927

  - DEBUG: FLOAT ACCTNO without DEPOSIT (first 10):
    3244032001
    4578082818
    3127476228
    3236757509
    3176955910
    3145629703
    3178168331
    4431970318
    4656529423
    3187802128
  - Merged records: 3206329
  - FLOAT records not in DEPOSIT (B AND NOT A): 0

  No FLOAT_ONLY records found

[STEP 10] Generating text report...
  - Report saved to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFLTEXP/EIFLTEXP_REPORT.txt
  - Total FLOAT in report: 0.00

============================================================
PROCESSING COMPLETED SUCCESSFULLY
============================================================
