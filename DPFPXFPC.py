Connecting to SAS...
SAS Connection established. Subprocess id is 2703618

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
  Report date: 13/11/2026 (Week 2)
  Weekly/Month-end: False
  Enriched view created  (0.9s)

======================================================================
PHASE 1A: Export DYDP to CSV via DuckDB
======================================================================
100% ▕██████████████████████████████████████▏ (00:00:42.60 elapsed)     
  ✓ CSV: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/TEMP/DYDP_import.csv  (42.7s, 899.4 MB)

======================================================================
PHASE 1B: Summary aggregates in DuckDB
======================================================================
100% ▕██████████████████████████████████████▏ (00:00:05.61 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:07.10 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:08.71 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:04.00 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:03.89 elapsed)     
100% ▕██████████████████████████████████████▏ (00:00:03.29 elapsed)     
  ✓ Summaries computed  (36.1s)
    DDMV=29,554  DYIBU=322  DYMVNT=794  DYBRDP=586  DYDDCR=10  DYDPS=6  DYACE=7

======================================================================
PHASE 2: Writing SAS datasets
======================================================================

Importing DYDP from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/TEMP/DYDP_import.csv via DATA step...
  Reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/TEMP/DYDP_import.csv
  Size: 899.4 MB
  ✓ DATA step import: 25.9s
  ⚠ CURBAL=527.53 DYDPBAL=527.53 APPRLIMT=0 ACCYTD=0 OPENDT=9232013266 MOVEMENT=0 MVRANGE=5000 CUSTCODE=78 SECOND=0 _ERROR_=1 _N_=1
  ⚠ CURBAL=14150.03 DYDPBAL=14150.03 APPRLIMT=0 ACCYTD=0 OPENDT=9092013252 MOVEMENT=0 MVRANGE=5000 CUSTCODE=78 SECOND=0 _ERROR_=1 _N_=2
  ⚠ CURBAL=3332.22 DYDPBAL=3332.22 APPRLIMT=0 ACCYTD=0 OPENDT=9102013253 MOVEMENT=400 MVRANGE=5000 CUSTCODE=78 SECOND=0 _ERROR_=1 _N_=3
  ⚠ DYDPBAL=20 APPRLIMT=0 ACCYTD=0 OPENDT=9102013253 MOVEMENT=0 MVRANGE=5000 CUSTCODE=78 SECOND=0 _ERROR_=1 _N_=4
  ⚠ DYDPBAL=24.83 APPRLIMT=0 ACCYTD=0 OPENDT=9112013254 MOVEMENT=0 MVRANGE=5000 CUSTCODE=96 SECOND=0 _ERROR_=1 _N_=5
  ⚠ CURBAL=55.68 DYDPBAL=55.68 APPRLIMT=0 ACCYTD=0 OPENDT=9122013255 MOVEMENT=0 MVRANGE=5000 CUSTCODE=78 SECOND=0 _ERROR_=1 _N_=6
  ⚠ DYDPBAL=76.52 APPRLIMT=0 ACCYTD=0 OPENDT=9122013255 MOVEMENT=0 MVRANGE=5000 CUSTCODE=78 SECOND=0 _ERROR_=1 _N_=7
  ⚠ CURBAL=21.6 DYDPBAL=21.6 APPRLIMT=0 ACCYTD=0 OPENDT=9122013255 MOVEMENT=0 MVRANGE=5000 CUSTCODE=78 SECOND=0 _ERROR_=1 _N_=8
  ⚠ CURBAL=10.34 DYDPBAL=10.34 APPRLIMT=0 ACCYTD=0 OPENDT=9122013255 MOVEMENT=0 MVRANGE=5000 CUSTCODE=78 SECOND=0 _ERROR_=1 _N_=9
  ⚠ _ERROR_=1 _N_=10
  ⚠ CURBAL=246.25 DYDPBAL=246.25 APPRLIMT=0 ACCYTD=0 OPENDT=9132013256 MOVEMENT=0 MVRANGE=5000 CUSTCODE=78 SECOND=0 _ERROR_=1 _N_=11
  ⚠ CURBAL=408.79 DYDPBAL=408.79 APPRLIMT=0 ACCYTD=0 OPENDT=9132013256 MOVEMENT=0 MVRANGE=5000 CUSTCODE=78 SECOND=0 _ERROR_=1 _N_=12
  ⚠ CURBAL=2226.69 DYDPBAL=2226.69 APPRLIMT=0 ACCYTD=0 OPENDT=9172013260 MOVEMENT=-2084.6 MVRANGE=5000 CUSTCODE=78 SECOND=0 _ERROR_=1
  ⚠ CURBAL=93.15 DYDPBAL=93.15 APPRLIMT=0 ACCYTD=0 OPENDT=9172013260 MOVEMENT=0 MVRANGE=5000 CUSTCODE=78 SECOND=0 _ERROR_=1 _N_=14
  ⚠ CURBAL=586.09 DYDPBAL=586.09 APPRLIMT=0 ACCYTD=0 OPENDT=9192013262 MOVEMENT=0 MVRANGE=5000 CUSTCODE=78 SECOND=0 _ERROR_=1 _N_=15
  ⚠ CURBAL=423.38 DYDPBAL=423.38 APPRLIMT=0 ACCYTD=0 OPENDT=9192013262 MOVEMENT=0 MVRANGE=5000 CUSTCODE=78 SECOND=0 _ERROR_=1 _N_=16
  ⚠ CURBAL=24.94 DYDPBAL=24.94 APPRLIMT=0 ACCYTD=0 OPENDT=9192013262 MOVEMENT=0 MVRANGE=5000 CUSTCODE=78 SECOND=0 _ERROR_=1 _N_=17
  ⚠ DYDPBAL=659 APPRLIMT=0 ACCYTD=0 OPENDT=9202013263 MOVEMENT=0 MVRANGE=5000 CUSTCODE=78 SECOND=0 _ERROR_=1 _N_=18
  ⚠ CURBAL=1230.21 DYDPBAL=1230.21 APPRLIMT=0 ACCYTD=0 OPENDT=9202013263 MOVEMENT=-840.3 MVRANGE=5000 CUSTCODE=78 SECOND=0 _ERROR_=1
  ⚠ WARNING: Limit set by ERRORS= option reached.  Further errors of this type will not be printed.
  ⚠ CURBAL=20.42 DYDPBAL=20.42 APPRLIMT=0 ACCYTD=0 OPENDT=9242013267 MOVEMENT=0 MVRANGE=5000 CUSTCODE=78 SECOND=0 _ERROR_=1 _N_=20
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
Report Date : 13/11/2026 (Week 2)
DDMV dedupe : 13/10/2026
Output dirs : MIS=/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/MIS
              MISQ=/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/MISQ
              TEMP=/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDEPE/TEMP
Records     : DYMVNT=794  DYIBU=322  DYBRDP=586  DYDDCR=10  DYDPS=6  DYACE=7  DDMV=29,554
======================================================================
SAS Connection terminated. Subprocess id was 2703618
