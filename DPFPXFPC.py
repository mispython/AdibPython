============================================================
EIFLTEXP PROCESSING STARTED
============================================================

[STEP 1] Building DEPOSIT dataset...
  Loading MNI SAVG124...
    Reading: savg124.sas7bdat
      Loaded: 4,241,108 records
  Loading IMNI SAVG124...
    Reading: savg124.sas7bdat
      Loaded: 2,262,899 records
  Loading MNI CURN124...
    Reading: curn124.sas7bdat
      Loaded: 915,692 records
  Loading IMNI CURN124...
    Reading: curn124.sas7bdat
      Loaded: 154,763 records
  Loading MNI FDMTHLY...
    Reading: fdmthly.sas7bdat
      Loaded: 2,756,145 records
  Loading IMNI FDMTHLY...
    Reading: fdmthly.sas7bdat
      Loaded: 431,257 records
  MNI CURN filtered: 915,427 records
  IMNI CURN filtered: 154,757 records

  Combined DEPOSIT: 10,761,593 records

[STEP 2] Applying filters...
  After PROGCD filter: 10,686,255
  After PRODUCT=166: 10,686,255
  After PROGCD special: 10,686,255
  After PRODUCT filter: 10,684,756
  After INTPAYBL: 10,684,756

  DEPOSIT saved: 8,569,100 records

[STEP 3] Loading FLOAT data...
    Reading: float.sas7bdat
      Loaded: 18,927 records
  FLOAT loaded: 18,927 unique accounts

[STEP 4] Finding B AND NOT A...
  DEPOSIT unique ACCTNOs: 8,569,100
  FLOAT unique ACCTNOs: 18,927
  Overlap: 18,925
  FLOAT_ONLY (B AND NOT A): 2 records
  - Report: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFLTEXP/EIFLTEXP_REPORT.txt

  Total FLOAT amount: 35,486.38

============================================================
PROCESSING COMPLETED SUCCESSFULLY
============================================================
