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
  - FLOAT_SUMMARY saved: 18927 records

  - Sample FLOAT data:
    ACCTNO: 4388180607, FLOAT: 1500.00
    ACCTNO: 3122225923, FLOAT: 4934.00
    ACCTNO: 3123567525, FLOAT: 348.00
    ACCTNO: 3215568723, FLOAT: 28673.45
    ACCTNO: 6318069004, FLOAT: 5298.03

[STEP 9] Merging DEPOSIT with FLOAT...

  - DEPOSIT sample ACCTNO:
    1000001725
    1000001725
    1000001725
    1000001725
    1000001725

  - FLOAT sample ACCTNO:
    3060028907
    3060030515
    3060038803
    3060046903
    3060065215

  - Attempting to match on last 6 digits...

  - Sample keys (last 6 digits):
    DEPOSIT keys: ['1725.0', '1725.0', '1725.0', '1725.0', '1725.0']
    FLOAT keys: ['8907.0', '0515.0', '8803.0', '6903.0', '5215.0']

  - Merge completed: 16295007 records
  - Records with FLOAT values: 16269108

  - Sample matches found:
    DEPOSIT: 1000001725, FLOAT: 3230241725, Amount: 5200.50
    DEPOSIT: 1000001725, FLOAT: 3236151725, Amount: 9319.00
    DEPOSIT: 1000001725, FLOAT: 3993051725, Amount: 6389.00
    DEPOSIT: 1000001725, FLOAT: 4711261725, Amount: 5000.00
    DEPOSIT: 1000001725, FLOAT: 3230241725, Amount: 5200.50
    DEPOSIT: 1000001725, FLOAT: 3236151725, Amount: 9319.00
    DEPOSIT: 1000001725, FLOAT: 3993051725, Amount: 6389.00
    DEPOSIT: 1000001725, FLOAT: 4711261725, Amount: 5000.00
    DEPOSIT: 1000001725, FLOAT: 3230241725, Amount: 5200.50
    DEPOSIT: 1000001725, FLOAT: 3236151725, Amount: 9319.00

  - FLOAT_ONLY records: 0
  - DEPOSIT_MERGED saved: 16295007 records
  - FLOAT_ONLY saved: 0 records

[STEP 10] Generating text report...
  - Report saved to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFLTEXP/EIFLTEXP_REPORT.txt

============================================================
DEPOSIT DATA WITH FLOAT SUMMARY
============================================================
Total FLOAT: 488,965,015,812.28
Total AVBAL: 723,900,616,854.72
Total Records: 16295007

============================================================
PROCESSING COMPLETED SUCCESSFULLY
============================================================
