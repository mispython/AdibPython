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

[STEP 7] Applying filters and transformations...
  - After PROGCD filter: 3187402 records
  - After PRODUCT=166: 3187402 records
  - After PROGCD special: 3187402 records
  - After PRODUCT filter: 3187402 records
  - After INTPAYBL: 3187402 records
  - After deduplication by ACCTNO: 1071746 records
  - DEPOSIT saved with 1071746 records

  - DEBUG: Sample DEPOSIT ACCTNOs (first 10):
    1354099202
    1994721508
    1332820910
    1433154429
    1274782323
    1104879020
    1343015428
    1390477300
    1340113720
    1222085602

[STEP 8] Loading FLOAT data...
  - FLOAT loaded: 18927 records
  - FLOAT_SUMMARY saved: 18927 records

  - DEBUG: Sample FLOAT ACCTNOs (first 10):
    3222751825
    3177846425
    3149016120
    3999318136
    4662743124
    3129933324
    3092515132
    3086402906
    5113375521
    6319371319

[STEP 9] Finding FLOAT records not in DEPOSIT (B AND NOT A)...
  - DEPOSIT unique ACCTNO count: 1071746
  - FLOAT unique ACCTNO count: 18927
  - Overlap ACCTNO count: 0
  - FLOAT records without DEPOSIT: 18927
  - FLOAT records not in DEPOSIT (B AND NOT A): 18927

  - DEBUG: Sample FLOAT_ONLY records (first 10):
    ACCTNO: 3222751825, FLOAT: 12351.00
    ACCTNO: 3177846425, FLOAT: 44592.00
    ACCTNO: 3149016120, FLOAT: 178.76
    ACCTNO: 3999318136, FLOAT: 1500.00
    ACCTNO: 4662743124, FLOAT: 4887.00
    ACCTNO: 3129933324, FLOAT: 21464.10
    ACCTNO: 3092515132, FLOAT: 13193.88
    ACCTNO: 3086402906, FLOAT: 2155.00
    ACCTNO: 5113375521, FLOAT: 2500.00
    ACCTNO: 6319371319, FLOAT: 1200.00

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
