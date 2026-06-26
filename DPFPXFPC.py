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
  - After PROGCD filter: 3187402 records
  - DEPOSIT saved with 1071746 records

  - DEBUG: DEPOSIT ACCTNO analysis:
    First digit distribution:
      1: 1053802 records
      7: 17944 records

    Sample ACCTNOs from DEPOSIT (first 5 of each first digit):
      1xxx: [1440014222, 1423499725, 1410119006, 1402991235, 1821588713]
      7xxx: [7109315530, 7048770835, 7011036114, 7109523220, 7012441209]

[STEP 8] Loading FLOAT data...
  - FLOAT loaded: 18927 records
  - FLOAT_SUMMARY saved: 18927 records

  - DEBUG: FLOAT ACCTNO analysis:
    First digit distribution:
      3: 15138 records
      4: 2349 records
      5: 365 records
      6: 1075 records

    Sample ACCTNOs from FLOAT (first 5 of each first digit):
      3xxx: [3134919513, 3990149235, 3183622830, 3820244008, 3104288814]
      4xxx: [4694049713, 4305775624, 4118463231, 4695747125, 4780585409]
      5xxx: [5026731622, 5043361904, 5116066517, 5042133525, 5064684033]
      6xxx: [6014887021, 6326999623, 6342102120, 6831670825, 6494318504]

[STEP 9] Finding FLOAT records not in DEPOSIT (B AND NOT A)...
  - DEPOSIT unique ACCTNO count: 1071746
  - FLOAT unique ACCTNO count: 18927
  - Overlap ACCTNO count: 0
  - FLOAT records not in DEPOSIT (B AND NOT A): 18927

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
