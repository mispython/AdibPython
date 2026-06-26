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

  - DEBUG: PROGCD distribution in combined data:
    Total unique PROGCD values: 4
    Top 20 PROGCD values:
      None: 7574191 records
      42130: 2715803 records
      42133: 431257 records
      42630: 40342 records

  - DEBUG: ACCTNO first digit distribution:
      1: 3117225 records
      3: 1068706 records
      4: 3057097 records
      5: 1090105 records
      6: 2358283 records
      7: 70177 records

[STEP 7] Applying filters and transformations...
  - PROGCD filter SKIPPED (all records kept): 10761593 records

  - DEBUG: Before PRODUCT filters: 10761593 records
    Product distribution (top 10):
      200.0: 3089021 records
      204.0: 1700231 records
      304.0: 692163 records
      100.0: 633376 records
      302.0: 366591 records
      207.0: 354001 records
      212.0: 315666 records
      205.0: 290157 records
      300.0: 247299 records
      301.0: 225104 records
  - After PRODUCT=166: 10761593 records
  - After PROGCD special: 3192304 records (removed 7569289)
  - After PRODUCT filter: 3192304 records (removed 0)
  - After INTPAYBL: 3192304 records
  - DEPOSIT saved with 1076648 records

  - DEBUG: DEPOSIT ACCTNO analysis after filters:
    First digit distribution:
      1: 1053802 records
      3: 4902 records
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
  - DEPOSIT unique ACCTNO count: 1076648
  - FLOAT unique ACCTNO count: 18927
  - Overlap ACCTNO count: 179

  - DEBUG: Sample overlapping ACCTNOs (first 10):
    3994083330
    3801916425
    3816965133
    3812096013
    3987123727
    3800537620
    3801446422
    3801015321
    3994073628
    3817109022
  - FLOAT records not in DEPOSIT (B AND NOT A): 18748

[STEP 10] Generating text report...
  - Report saved to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFLTEXP/EIFLTEXP_REPORT.txt
  - Total FLOAT in report: 555097866.80

  - FLOAT_ONLY saved: 18748 records
  - Total FLOAT amount in FLOAT_ONLY: 555097866.80

============================================================
B AND NOT A SUMMARY
============================================================
Total FLOAT_ONLY records: 18748
Total FLOAT amount: 555097866.80
============================================================

============================================================
PROCESSING COMPLETED SUCCESSFULLY
============================================================
