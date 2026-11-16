Connecting to SAS...
SAS Connection established. Subprocess id is 778563

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
  Enriched view created  (0.8s)

======================================================================
PHASE 1A: Export DYDP to CSV via DuckDB
======================================================================
100% ▕██████████████████████████████████████▏ (00:01:21.89 elapsed)     
  ✓ CSV: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/TEMP/DYDP_import.csv  (82.0s, 853.3 MB)

======================================================================
PHASE 1B: Summary aggregates in DuckDB
======================================================================
100% ▕██████████████████████████████████████▏ (00:00:02.32 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:06.21 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:05.89 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:08.60 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:05.41 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:04.59 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:05.10 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:02.29 elapsed)     
  ✓ Summaries computed  (41.8s)
    DDMV=29,554  DYIBU=322  DYMVNT=794  DYBRDP=586  DYDDCR=10  DYDPS=6  DYACE=7

======================================================================
PHASE 2: Writing SAS datasets
======================================================================

Importing DYDP from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/TEMP/DYDP_import.csv via DATA step...
  Reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/TEMP/DYDP_import.csv
  Size: 853.3 MB
  ✓ DATA step import: 38.3s
  ⚠ CURBAL=527.53 DYDPBAL=527.53 APPRLIMT=0 ACCYTD=0 OPENDT=923201 MOVEMENT=0 MVRANGE=5000 CUSTCODE=78 SECOND=0 _ERROR_=1 _N_=1
  ⚠ CURBAL=14150.03 DYDPBAL=14150.03 APPRLIMT=0 ACCYTD=0 OPENDT=909201 MOVEMENT=0 MVRANGE=5000 CUSTCODE=78 SECOND=0 _ERROR_=1 _N_=2
  ⚠ CURBAL=3332.22 DYDPBAL=3332.22 APPRLIMT=0 ACCYTD=0 OPENDT=910201 MOVEMENT=400 MVRANGE=5000 CUSTCODE=78 SECOND=0 _ERROR_=1 _N_=3
  ⚠ DYDPBAL=20 APPRLIMT=0 ACCYTD=0 OPENDT=910201 MOVEMENT=0 MVRANGE=5000 CUSTCODE=78 SECOND=0 _ERROR_=1 _N_=4
  ⚠ DYDPBAL=24.83 APPRLIMT=0 ACCYTD=0 OPENDT=911201 MOVEMENT=0 MVRANGE=5000 CUSTCODE=96 SECOND=0 _ERROR_=1 _N_=5
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
SAS Connection terminated. Subprocess id was 778563
