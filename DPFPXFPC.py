Connecting to SAS...
SAS Connection established. Subprocess id is 858013

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
  Enriched view created  (1.6s)

======================================================================
PHASE 1A: Export DYDP to CSV via DuckDB
======================================================================
100% ▕██████████████████████████████████████▏ (00:00:58.90 elapsed)     
  ✓ CSV: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/TEMP/DYDP_import.csv  (58.9s, 838.4 MB)

======================================================================
PHASE 1B: Summary aggregates in DuckDB
======================================================================
100% ▕██████████████████████████████████████▏ (00:00:02.50 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:03.89 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:06.92 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:06.89 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:04.11 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:02.90 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:03.90 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:02.10 elapsed)     
  ✓ Summaries computed  (34.3s)
    DDMV=29,554  DYIBU=322  DYMVNT=794  DYBRDP=586  DYDDCR=10  DYDPS=6  DYACE=7

======================================================================
PHASE 2: Writing SAS datasets
======================================================================

Importing DYDP from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/TEMP/DYDP_import.csv via DATA step...
  Reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/TEMP/DYDP_import.csv
  Size: 838.4 MB
  ✓ DATA step import: 29.7s
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
SAS Connection terminated. Subprocess id was 858013
