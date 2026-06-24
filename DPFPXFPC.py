============================================================
EIFLTEXP PROCESSING STARTED
============================================================
MNI Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/MNI
IMNI Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/IMNI
PIDM Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/PIDM
Output Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFLTEXP
============================================================

[STEP 1] Loading FDMTHLY data...
  - MNI FDMTHLY loaded: 2756145 records
  - IMNI FDMTHLY loaded: 431257 records
  - Combined FDMTHLY: 3187402 records
  - FDMTHLY saved

[STEP 2] Loading CURN data...
  - MNI CURN124 loaded: 915692 records
  - IMNI CURN124 loaded: 154763 records
  - Combined CURN: 1070455 records
  - CURN filtered (removed PRODUCT=139): 1070184 records

[STEP 3] Loading SAVG data...
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
  - Combined columns: ['acctno', 'amtind', 'branch', 'curbal', 'intpaybl', 'ledgbal', 'product', 'progcd']
  - Unique PROGCD values: shape: (4,)
Series: 'progcd' [str]
[
        null
        "42133"
        "42130"
        "42630"
]

[STEP 7] Applying filters and transformations...
  - After PROGCD filter: 3187402 records
  - After PRODUCT=166: 3187402 records
  - After PROGCD special: 3187402 records
  - After PRODUCT filter: 3187402 records
  - After INTPAYBL: 3187402 records
  - DEPOSIT saved with 3187402 records

[STEP 8] Loading FLOAT data...
  - FLOAT loaded: 18927 records
  - FLOAT_SUMMARY saved: 18927 records
  - FLOAT_SUMMARY sample: shape: (5, 2)
┌──────────┬──────────┐
│ acctno   ┆ float    │
│ ---      ┆ ---      │
│ f64      ┆ f64      │
╞══════════╪══════════╡
│ 3.1909e9 ┆ 4999.6   │
│ 3.0819e9 ┆ 1600.0   │
│ 3.1577e9 ┆ 122991.0 │
│ 4.5474e9 ┆ 7048.1   │
│ 4.0364e9 ┆ 8486.89  │
└──────────┴──────────┘

[STEP 9] Merging DEPOSIT with FLOAT...
  - Merge completed: 3187402 records
  - Float column null count: 3187402
  - Curbal column null count: 0
  - Product column null count: 0
  - FLOAT_ONLY records: 0
  - DEPOSIT_MERGED saved: 3187402 records
  - FLOAT_ONLY saved: 0 records

[STEP 10] Generating text report...
  - deposit_merged records: 3187402
  - float_only records: 0
  - Columns in deposit_merged: ['acctno', 'amtind', 'branch', 'curbal', 'intpaybl', 'ledgbal', 'product', 'progcd', 'float', 'avbal', 'avbaltt', 'curbaltt']
  - Sample of float column: shape: (5,)
Series: 'float' [f64]
[
        null
        null
        null
        null
        null
]
  - Sample of curbal column: shape: (5,)
Series: 'curbal' [f64]
[
        1350.7
        3513.03
        1508.64
        2233.49
        2234.89
]
  - Sample of product column: shape: (5,)
Series: 'product' [f64]
[
        302.0
        302.0
        301.0
        301.0
        301.0
]
  - Report saved to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFLTEXP/EIFLTEXP_REPORT.txt

============================================================
DEPOSIT DATA WITH FLOAT SUMMARY
============================================================
Total FLOAT: 0.00
Total AVBAL: 0.00
Total Records: 3187402

FLOAT ONLY RECORDS (B AND NOT A):
  - Records: 0

============================================================
PROCESSING COMPLETED SUCCESSFULLY
============================================================



what to do? update inputs? or debug logic?
