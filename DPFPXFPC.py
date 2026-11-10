EIIDMELW - Islamic BNM EL Daily Deposit Extract
============================================================

Connecting to SAS via SASPy (cfgname=default) ...
SAS Connection established. Subprocess id is 193614


21   
22   libname XMISOUT    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDMELW/'  ;
NOTE: Libref XMISOUT was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDMELW
23   
SAS library XMISOUT assigned to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDMELW/

Reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDMELW/DPTRBLGS.parquet ...
Report Date: 11/07/2026
Week: 2, Month: 07, Day: 11
============================================================
ELDAY: DAYC

Processing DPTRBL parquet (Islamic ONLY, COSTCTR 3000-3999)...
  ... 500,000 rows read, 0 kept
  ... 1,000,000 rows read, 0 kept
  ... 1,500,000 rows read, 0 kept
  ... 2,000,000 rows read, 223,478 kept
  ... 2,500,000 rows read, 223,479 kept
  ... 3,000,000 rows read, 223,480 kept
  ... 3,500,000 rows read, 379,565 kept
  ... 4,000,000 rows read, 379,565 kept
  ... 4,500,000 rows read, 379,565 kept
  ... 5,000,000 rows read, 379,574 kept
  ... 5,500,000 rows read, 540,405 kept
  ... 6,000,000 rows read, 930,868 kept
  ... 6,500,000 rows read, 1,280,088 kept
  ... 7,000,000 rows read, 1,603,367 kept
  ... 7,500,000 rows read, 1,821,446 kept
  ... 8,000,000 rows read, 1,821,446 kept
  ... 8,500,000 rows read, 1,821,446 kept
  ... 9,000,000 rows read, 1,821,446 kept
  ... 9,500,000 rows read, 1,821,447 kept
  ... 10,000,000 rows read, 1,821,447 kept
  ... 10,500,000 rows read, 1,821,447 kept
  ... 11,000,000 rows read, 1,975,813 kept
  ... 11,500,000 rows read, 2,336,287 kept
  ... 12,000,000 rows read, 2,611,143 kept
  Rows read: 12,256,290   kept (Islamic): 2,611,143

Generating BNM code records...
  Generated 12,987 BNM code records

Writing to SAS table XMISOUT.MELW072 ...
  Mid-week (Day 11): append (dedupe on REPTDATE)
Table XMISOUT.MELW072 does not exist. This SASdata object will not be useful until the data set is created.
The SAS Data Set XMISOUT.MELW072 does not exist
  NOTE: MELW072 not found — bootstrapping fresh table.

============================================================
EIIDMELW Complete
============================================================
Output  : XMISOUT.MELW072 (/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDMELW/)
Records : 12,987
ELDAY   : DAYC, Week: 2
SAS Connection terminated. Subprocess id was 193614
