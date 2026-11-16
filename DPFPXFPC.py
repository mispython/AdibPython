Connecting to SAS...
SAS Connection established. Subprocess id is 891073

SAS session ready

31   
32   libname MIS "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDDEPE/MIS_PIBB"  ;
NOTE: Libref MIS was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDDEPE/MIS_PIBB
33   
  libname MIS   -> /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDDEPE/MIS_PIBB

35   
36   libname MISQ "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDDEPE/MISQ"  ;
NOTE: Libref MISQ was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDDEPE/MISQ
37   
  libname MISQ  -> /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDDEPE/MISQ

39   
40   libname TEMP "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDDEPE/TEMP"  ;
NOTE: Libref TEMP was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDDEPE/TEMP
41   
  libname TEMP  -> /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDDEPE/TEMP

======================================================================
PHASE 1: DuckDB reads and enriches (Islamic only)
======================================================================
  Report date: 16/11/2026 (Week 2)
  Weekly/Month-end: False
  Enriched view created  (1.1s)

======================================================================
PHASE 1A: Export DYDP to CSV via DuckDB
======================================================================
100% ▕██████████████████████████████████████▏ (00:02:13.40 elapsed)     
  ✓ CSV: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDDEPE/TEMP/DYDP_import.csv  (133.8s, 246.9 MB)

======================================================================
PHASE 1B: Summary aggregates in DuckDB
======================================================================
100% ▕██████████████████████████████████████▏ (00:00:41.50 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:57.20 elapsed)     
100% ▕██████████████████████████████████████▏ (00:01:05.11 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:47.60 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:51.20 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:40.99 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:57.40 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:31.31 elapsed)     
  ✓ Summaries computed  (398.2s)
    DDMV=5,109  DYIBU=264  DYMVNT=128  DYBRDP=531  DYDDCR=8  DYDPS=6  DYACE=0

======================================================================
PHASE 2: Writing SAS datasets
======================================================================

Importing DYDP from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDDEPE/TEMP/DYDP_import.csv via DATA step...
  Reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDDEPE/TEMP/DYDP_import.csv
  Size: 246.9 MB
  ✓ DATA step import: 10.8s
  ✓ removed scratch CSV
  ✓ MIS.DYPOSN11 (created)
  ✓ MIS.DYMVNT11 (created)
  ✓ MIS.DYIBU11 (created)
  ✓ MIS.DYDDCR11 (created)
  ✓ MIS.DYBRDP11 (created)
  ✓ MIS.DYDPS11 (created)
  ⚠ MIS.DYACE11: No data
  ✓ MIS.DDMV (created, 5,109)

======================================================================
✓ EIIDDEPE Complete (Islamic)
======================================================================
Report Date : 16/11/2026 (Week 2)
Output dirs : MIS_PIBB=/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDDEPE/MIS_PIBB
              MISQ=/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDDEPE/MISQ
              TEMP=/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDDEPE/TEMP
Records     : DYMVNT=128  DYIBU=264  DYBRDP=531  DYDDCR=8  DYDPS=6  DYACE=0  DDMV=5,109
======================================================================
SAS Connection terminated. Subprocess id was 891073
