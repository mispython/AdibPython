============================================================
EIFLTEXP PROCESSING STARTED - BOTH
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
  - Combined data: 10761593 records

[STEP 7] Applying filters and transformations...
  - PROGCD filter SKIPPED (all records kept): 10761593 records
  - After PRODUCT=166: 10761593 records
  - After PROGCD special: 10761593 records (removed 0)
  - After PRODUCT filter: 10760074 records (removed 1519)
  - After INTPAYBL: 10760074 records
  - DEPOSIT saved with 8644418 records

  - DEBUG: DEPOSIT ACCTNO analysis after filters:
    First digit distribution:
      1: 1053802 records
      3: 1067187 records
      4: 3057097 records
      5: 1090105 records
      6: 2358283 records
      7: 17944 records

[STEP 8] Loading FLOAT data...
  - FLOAT loaded: 18927 records
  - FLOAT_SUMMARY saved: 18927 records

  - DEBUG: FLOAT ACCTNO analysis:
    First digit distribution:
      3: 15138 records
      4: 2349 records
      5: 365 records
      6: 1075 records

[STEP 9] Finding FLOAT records not in DEPOSIT (B AND NOT A)...
  - DEPOSIT unique ACCTNO count: 8644418
  - FLOAT unique ACCTNO count: 18927
  - Overlap ACCTNO count: 18927
  - FLOAT records not in DEPOSIT (B AND NOT A): 0

  No FLOAT_ONLY records found

[STEP 10] Generating text report...
  - Report saved to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFLTEXP/EIFLTEXP_REPORT.txt
  - Total FLOAT in report: 0.00

============================================================
PROCESSING COMPLETED SUCCESSFULLY
============================================================
