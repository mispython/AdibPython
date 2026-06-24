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
  - After deduplication by ACCTNO: 1071746 records
  - DEPOSIT saved with 1071746 records

[STEP 8] Loading FLOAT data...
  - FLOAT loaded: 18927 records
  - FLOAT columns: ['acctno', 'float', 'branch']
  - FLOAT_SUMMARY saved: 18927 records

  - Sample FLOAT data:
    ACCTNO: 3158002725, FLOAT: 9125.00
    ACCTNO: 3245845705, FLOAT: 3205.94
    ACCTNO: 3131893112, FLOAT: 5490.42
    ACCTNO: 3123041028, FLOAT: 7000.00
    ACCTNO: 3191345127, FLOAT: 4350.00

[STEP 9] Merging DEPOSIT with FLOAT...

  - DEPOSIT unique ACCTNO count: 1071746
  - FLOAT unique ACCTNO count: 18927

  - DEPOSIT sample ACCTNO:
    1000001725
    1000002720
    1000003230
    1000003521
    1000006118

  - FLOAT sample ACCTNO:
    3060028907
    3060030515
    3060038803
    3060046903
    3060065215

  - Aggregating FLOAT by last 6 digits...
    FLOAT aggregated: 3714 unique keys

  - Merge completed: 1071746 records
  - Records with FLOAT values: 1063349

  - Sample matches found:
    DEPOSIT: 1000001725, FLOAT: 3230241725, Amount: 25908.50
    DEPOSIT: 1000002720, FLOAT: 3130912720, Amount: 27521.52
    DEPOSIT: 1000003230, FLOAT: 3124663230, Amount: 27419.38
    DEPOSIT: 1000003521, FLOAT: 3070643521, Amount: 31368.25
    DEPOSIT: 1000006118, FLOAT: 3123326118, Amount: 12430.76
    DEPOSIT: 1000009431, FLOAT: 3088329431, Amount: 29605.82
    DEPOSIT: 1000013611, FLOAT: 3166813611, Amount: 187669.23
    DEPOSIT: 1000014218, FLOAT: 3086234218, Amount: 25420.50
    DEPOSIT: 1000014509, FLOAT: 3113664509, Amount: 209615.15
    DEPOSIT: 1000014703, FLOAT: 3085084703, Amount: 11620.60

  - FLOAT keys not in DEPOSIT: 43
  - FLOAT_ONLY records: 43
  - DEPOSIT_MERGED saved: 1071746 records
  - FLOAT_ONLY saved: 43 records

[STEP 10] Generating text report...
  - Report saved to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFLTEXP/EIFLTEXP_REPORT.txt

============================================================
DEPOSIT DATA WITH FLOAT SUMMARY
============================================================
Total FLOAT: 163,736,982,518.22
Total AVBAL: -100,979,157,970.85
Total Records: 1071746

FLOAT ONLY RECORDS (B AND NOT A):
  ACCTNO: 4944470247, FLOAT: 7400.00
  ACCTNO: 4924942140, FLOAT: 1800.00
  ACCTNO: 4904112038, FLOAT: 12000.00
  ACCTNO: 4959578844, FLOAT: 441.53
  ACCTNO: 4902959740, FLOAT: 3741.96
  ACCTNO: 4943504047, FLOAT: 10000.00
  ACCTNO: 4971643644, FLOAT: 4900.00
  ACCTNO: 4935056242, FLOAT: 1200.00
  ACCTNO: 4961087545, FLOAT: 2200.00
  ACCTNO: 4954902542, FLOAT: 1692.12

============================================================
PROCESSING COMPLETED SUCCESSFULLY
============================================================
