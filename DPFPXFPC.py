Connecting to SAS...
SAS Connection established. Subprocess id is 772051

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
  Enriched view created  (1.7s)

======================================================================
PHASE 1A: Export DYDP to CSV via DuckDB
======================================================================
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDDEPE.py", line 248, in <module>
    con.execute(f"""
_duckdb.ConversionException: Conversion Error: Could not convert string '12102020345' to INT32

LINE 6:            CAST(
                   ^
SAS Connection terminated. Subprocess id was 772051
