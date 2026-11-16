Connecting to SAS...
SAS Connection established. Subprocess id is 906108

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
100% ▕██████████████████████████████████████▏ (00:01:08.69 elapsed)     
  Enriched table created  (72.3s)

======================================================================
PHASE 1A: Export DYDP to CSV via DuckDB
======================================================================
100% ▕██████████████████████████████████████▏ (00:00:11.17 elapsed)     
  ✓ CSV: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDDEPE/TEMP/DYDP_import.csv  (11.2s, 246.9 MB)

======================================================================
PHASE 1B: Summary aggregates in DuckDB
======================================================================
  ✓ Summaries computed  (3.3s)
    DDMV=5,109  DYIBU=264  DYMVNT=128  DYBRDP=531  DYDDCR=8  DYDPS=6  DYACE=0

======================================================================
PHASE 2: Writing SAS datasets
======================================================================

Importing DYDP from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDDEPE/TEMP/DYDP_import.csv via DATA step...
  Reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDDEPE/TEMP/DYDP_import.csv
  Size: 246.9 MB
  ✓ DATA step import: 14.2s
  ✓ removed scratch CSV
  ✓ MIS.DYPOSN11 (appended)
  ✓ MIS.DYMVNT11 (appended)
  ✓ MIS.DYIBU11 (appended)
  ✓ MIS.DYDDCR11 (appended)
  ✓ MIS.DYBRDP11 (appended)
  ✓ MIS.DYDPS11 (appended)
  ⚠ MIS.DYACE11: No data
  ✓ MIS.DDMV (appended, 5,109)

======================================================================
✓ EIIDDEPE Complete (Islamic)
======================================================================
Report Date : 16/11/2026 (Week 2)
Output dirs : MIS_PIBB=/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDDEPE/MIS_PIBB
              MISQ=/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDDEPE/MISQ
              TEMP=/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDDEPE/TEMP
Records     : DYMVNT=128  DYIBU=264  DYBRDP=531  DYDDCR=8  DYDPS=6  DYACE=0  DDMV=5,109
======================================================================
SAS Connection terminated. Subprocess id was 906108
