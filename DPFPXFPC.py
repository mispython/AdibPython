Connecting to SAS...
SAS Connection established. Subprocess id is 886409

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
  Enriched view created  (1.4s)

======================================================================
PHASE 1A: Export DYDP to CSV via DuckDB
======================================================================
100% ▕██████████████████████████████████████▏ (00:01:43.10 elapsed)     
  ✓ CSV: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDDEPE/TEMP/DYDP_import.csv  (103.8s, 246.9 MB)

======================================================================
PHASE 1B: Summary aggregates in DuckDB
======================================================================
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIDDEPE.py", line 332, in <module>
    ddmv_pl = con.execute("""
_duckdb.BinderException: Binder Error: Referenced column "CAPROD" not found in FROM clause!
Candidate bindings: "PRODCD", "PRODUCT", "SECOND", "CREDIT", "CUSTCODE"

LINE 7:     WHERE DEPTYPE IN ('D','N') AND CURBAL < 0 AND CAPROD IN ('42110','42310')
                                                          ^
SAS Connection terminated. Subprocess id was 886409
