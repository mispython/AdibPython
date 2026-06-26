============================================================
EIFLTEXP PROCESSING STARTED - CONVENTIONAL
============================================================
MNI Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/MNI
IMNI Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/IMNI
PIDM Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/PIDM
Output Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFLTEXP
============================================================

[STEP 1] Loading FDMTHLY data...
  - MNI FDMTHLY loaded: 2756145 records
  - Combined FDMTHLY: 2756145 records
  - FDMTHLY saved

[STEP 2] Loading CURN data...
  - MNI CURN124 loaded: 915692 records
  - Combined CURN: 915692 records
  - CURN filtered (removed PRODUCT=139): 915427 records

[STEP 3] Loading SAVG data...
  - MNI SAVG124 loaded: 4241108 records

[STEP 4] Adding CURN to dataset list...
  - CURN added with 915427 records

[STEP 5] Adding FDMTHLY to dataset list...
  - FDMTHLY added with 2756145 records

[STEP 6] Combining all datasets...
  - Total datasets to combine: 3
    1. MNI SAVG124: 4241108 records
    2. CURN: 915427 records
    3. FDMTHLY: 2756145 records
  - Combined data: 7912680 records

[STEP 7] Applying filters and transformations...
  - After PROGCD filter: 2756145 records
  - After PRODUCT=166: 2756145 records
  - After PROGCD special: 2756145 records
  - After PRODUCT filter: 2756145 records
  - After INTPAYBL: 2756145 records
  - After deduplication by ACCTNO: 920763 records
  - DEPOSIT saved with 920763 records

  - DEBUG: DEPOSIT branch distribution:
shape: (260, 2)
┌────────┬───────┐
│ branch ┆ count │
│ ---    ┆ ---   │
│ i64    ┆ u32   │
╞════════╪═══════╡
│ 2      ┆ 1186  │
│ 3      ┆ 3240  │
│ 4      ┆ 5816  │
│ 5      ┆ 4532  │
│ 6      ┆ 4607  │
│ …      ┆ …     │
│ 292    ┆ 1884  │
│ 293    ┆ 2913  │
│ 294    ┆ 2949  │
│ 295    ┆ 1757  │
│ 296    ┆ 1088  │
└────────┴───────┘

  - DEBUG: Sample DEPOSIT ACCTNOs with branches (first 10):
    ACCTNO: 1227981703, BRANCH: 122
    ACCTNO: 1259252100, BRANCH: 197
    ACCTNO: 1276094805, BRANCH: 257
    ACCTNO: 1401815415, BRANCH: 196
    ACCTNO: 1407158505, BRANCH: 209
    ACCTNO: 1053274506, BRANCH: 126
    ACCTNO: 1376697003, BRANCH: 221
    ACCTNO: 1284704606, BRANCH: 225
    ACCTNO: 1141624812, BRANCH: 141
    ACCTNO: 1379779031, BRANCH: 256

[STEP 8] Loading FLOAT data...
  - FLOAT loaded: 18927 records
  - FLOAT_SUMMARY saved: 18927 records

  - DEBUG: FLOAT branch distribution:
shape: (264, 2)
┌────────┬───────┐
│ branch ┆ count │
│ ---    ┆ ---   │
│ f64    ┆ u32   │
╞════════╪═══════╡
│ 2.0    ┆ 65    │
│ 3.0    ┆ 78    │
│ 4.0    ┆ 128   │
│ 5.0    ┆ 143   │
│ 6.0    ┆ 159   │
│ …      ┆ …     │
│ 296.0  ┆ 20    │
│ 701.0  ┆ 8     │
│ 702.0  ┆ 4     │
│ 703.0  ┆ 26    │
│ 704.0  ┆ 20    │
└────────┴───────┘

  - DEBUG: Sample FLOAT ACCTNOs with branches (first 10):
    ACCTNO: 3060028907, BRANCH: 2.0
    ACCTNO: 3060030515, BRANCH: 2.0
    ACCTNO: 3060038803, BRANCH: 2.0
    ACCTNO: 3060046903, BRANCH: 2.0
    ACCTNO: 3060065215, BRANCH: 2.0
    ACCTNO: 3060092112, BRANCH: 2.0
    ACCTNO: 3060094005, BRANCH: 2.0
    ACCTNO: 3060137409, BRANCH: 3.0
    ACCTNO: 3060243023, BRANCH: 4.0
    ACCTNO: 3060260800, BRANCH: 4.0

[STEP 9] Finding FLOAT records not in DEPOSIT (B AND NOT A)...
  - DEPOSIT unique ACCTNO count: 920763
  - FLOAT unique ACCTNO count: 18927
  - Overlap ACCTNO count: 0
  - FLOAT records without DEPOSIT: 18927
  - FLOAT records not in DEPOSIT (B AND NOT A): 18927

  - DEBUG: Sample FLOAT_ONLY records (first 10):
    ACCTNO: 3213215430, FLOAT: 1000.00
    ACCTNO: 3140035020, FLOAT: 20000.00
    ACCTNO: 5106220205, FLOAT: 88109.56
    ACCTNO: 3103395108, FLOAT: 200.00
    ACCTNO: 3138362624, FLOAT: 1924.25
    ACCTNO: 4910683523, FLOAT: 3073.05
    ACCTNO: 6344376017, FLOAT: 1000.00
    ACCTNO: 3800555519, FLOAT: 3500.00
    ACCTNO: 3209071928, FLOAT: 19729.20
    ACCTNO: 3216214918, FLOAT: 407.40

[STEP 10] Generating text report...
  - Report saved to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFLTEXP/EIFLTEXP_REPORT.txt
  - Total FLOAT in report: 565675682.37

  - FLOAT_ONLY saved: 18927 records
  - Total FLOAT amount in FLOAT_ONLY: 565675682.37

============================================================
B AND NOT A SUMMARY
============================================================
Total FLOAT_ONLY records: 18927
Total FLOAT amount: 565675682.37
============================================================

============================================================
PROCESSING COMPLETED SUCCESSFULLY
============================================================
