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

[STEP 7] Applying filters and transformations...
  - After PROGCD filter: 3187402 records
  - After PRODUCT=166: 3187402 records
  - After PROGCD special: 3187402 records
  - After PRODUCT filter: 3187402 records
  - After INTPAYBL: 3187402 records
  - DEPOSIT saved with 3187402 records

[STEP 8] Loading FLOAT data...
  - FLOAT loaded: 18927 records
  - FLOAT columns: ['acctno', 'float', 'branch']
  - FLOAT sample ACCTNO: shape: (10,)
Series: 'acctno' [f64]
[
        3.0600e9
        3.0600e9
        3.0600e9
        3.0600e9
        3.0601e9
        3.0601e9
        3.0601e9
        3.0601e9
        3.0602e9
        3.0603e9
]
  - FLOAT_SUMMARY saved: 18927 records
  - FLOAT_SUMMARY sample: shape: (5, 2)
┌──────────┬─────────┐
│ acctno   ┆ float   │
│ ---      ┆ ---     │
│ f64      ┆ f64     │
╞══════════╪═════════╡
│ 3.0930e9 ┆ 50883.8 │
│ 3.2237e9 ┆ 8115.6  │
│ 4.2337e9 ┆ 16566.0 │
│ 3.1814e9 ┆ 4697.0  │
│ 3.1275e9 ┆ 70.0    │
└──────────┴─────────┘
  - Sample DEPOSIT ACCTNO: shape: (10,)
Series: 'acctno' [f64]
[
        1.0000e9
        1.0000e9
        1.0000e9
        1.0000e9
        1.0000e9
        1.0000e9
        1.0000e9
        1.0000e9
        1.0000e9
        1.0000e9
]
  - Sample FLOAT ACCTNO: shape: (10,)
Series: 'acctno' [f64]
[
        3.0600e9
        3.0600e9
        3.0600e9
        3.0600e9
        3.0601e9
        3.0601e9
        3.0601e9
        3.0601e9
        3.0602e9
        3.0603e9
]
  - Common ACCTNO count: 0

[STEP 9] Merging DEPOSIT with FLOAT...
  - Merge completed: 3187402 records
  - Records with FLOAT values: 0
  - FLOAT_ONLY records: 0
  - DEPOSIT_MERGED saved: 3187402 records
  - FLOAT_ONLY saved: 0 records

[STEP 10] Generating text report...
  - Report saved to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFLTEXP/EIFLTEXP_REPORT.txt

============================================================
DEPOSIT DATA WITH FLOAT SUMMARY
============================================================
Total FLOAT: 0.00
Total AVBAL: 0.00
Total Records: 3187402

============================================================
PROCESSING COMPLETED SUCCESSFULLY
============================================================
