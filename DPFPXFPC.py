Connecting to SAS...
SAS Connection established. Subprocess id is 826393

SAS session ready

31   
32   libname MIS "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/MIS"  ;
NOTE: Libref MIS was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/MIS
33   
  libname MIS   -> /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/MIS

35   
36   libname MISQ "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/MISQ"  ;
NOTE: Libref MISQ was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/MISQ
37   
  libname MISQ  -> /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/MISQ

39   
40   libname TEMP "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/TEMP"  ;
NOTE: Libref TEMP was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/TEMP
41   
  libname TEMP  -> /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/TEMP

======================================================================
PHASE 1: DuckDB reads and enriches
======================================================================
  Report date: 16/11/2026 (Week 2)
  Weekly/Month-end: False
  Enriched view created  (2.2s)

======================================================================
PHASE 1A: Export DYDP to CSV via DuckDB
======================================================================
100% ▕██████████████████████████████████████▏ (00:01:05.89 elapsed)     
  ✓ CSV: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/TEMP/DYDP_import.csv  (65.9s, 853.2 MB)

======================================================================
PHASE 1B: Summary aggregates in DuckDB
======================================================================
100% ▕██████████████████████████████████████▏ (00:00:04.39 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:04.79 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:08.40 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:05.19 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:03.42 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:04.60 elapsed)     
  ✓ Summaries computed  (34.5s)
    DDMV=29,554  DYIBU=322  DYMVNT=794  DYBRDP=586  DYDDCR=10  DYDPS=6  DYACE=7

======================================================================
PHASE 2: Writing SAS datasets
======================================================================

Importing DYDP from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/TEMP/DYDP_import.csv via DATA step...
  Reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/TEMP/DYDP_import.csv
  Size: 853.2 MB
  ✓ DATA step import: 39.0s
  ⚠ CURBAL=738.83 DYDPBAL=738.83 APPRLIMT=0 ACCYTD=0 OPENDT=302202 MOVEMENT=0 MVRANGE=5000 CUSTCODE=78 SECOND=0 _ERROR_=1 _N_=1
  ⚠ DYDPBAL=20 APPRLIMT=0 ACCYTD=0 OPENDT=302202 MOVEMENT=0 MVRANGE=5000 CUSTCODE=78 SECOND=0 _ERROR_=1 _N_=2
  ⚠ CURBAL=505.73 DYDPBAL=505.73 APPRLIMT=0 ACCYTD=0 OPENDT=302202 MOVEMENT=0 MVRANGE=5000 CUSTCODE=78 SECOND=0 _ERROR_=1 _N_=3
  ⚠ CURBAL=58935.13 DYDPBAL=58935.13 APPRLIMT=0 ACCYTD=0 OPENDT=302202 MOVEMENT=0 MVRANGE=5000 CUSTCODE=77 SECOND=0 _ERROR_=1 _N_=4
  ⚠ CURBAL=195.55 DYDPBAL=195.55 APPRLIMT=0 ACCYTD=0 OPENDT=302202 MOVEMENT=0 MVRANGE=5000 CUSTCODE=78 SECOND=0 _ERROR_=1 _N_=5
  ⚠ ... and 16 more ERROR lines (suppressed)
  ✓ removed scratch CSV
  ✓ MIS.DYPOSN11  (1 rows)
  ✓ MIS.DYMVNT11 (appended)
  ✓ MIS.DYIBU11 (appended)
  ✓ MIS.DYDDCR11 (appended)
  ✓ MIS.DYBRDP11 (appended)
  ✓ MIS.DYDPS11 (appended)
  ✓ MIS.DYACE11 (appended)
  ✓ MISQ.DDMV (appended, 29,554)

======================================================================
✓ EIBDDEPE Complete
======================================================================
Report Date : 16/11/2026 (Week 2)
DDMV dedupe : 16/10/2026
Output dirs : MIS=/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/MIS
              MISQ=/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/MISQ
              TEMP=/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/TEMP
Records     : DYMVNT=794  DYIBU=322  DYBRDP=586  DYDDCR=10  DYDPS=6  DYACE=7  DDMV=29,554
======================================================================
SAS Connection terminated. Subprocess id was 826393
