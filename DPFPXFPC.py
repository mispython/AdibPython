EIBDMELW - Conventional BNM EL Daily Deposit Extract
============================================================

Connecting to SAS via SASPy (cfgname=default) ...
SAS Connection established. Subprocess id is 840809


21   
22   libname XMISOUT    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDMELW/'  ;
NOTE: Libref XMISOUT was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDMELW
23   
SAS library XMISOUT assigned to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDMELW/

Reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDMELW/DPTRBLGS.parquet ...
Report Date: 11/07/2026
Week: 2, Month: 07, Day: 11
============================================================
ELDAY: DAYC

Processing DPTRBL parquet (CONVENTIONAL, COSTCTR NOT 3000-3999)...
  ... 500,000 rows read, 399,154 kept
  ... 1,000,000 rows read, 750,160 kept
  ... 1,500,000 rows read, 1,158,890 kept
  ... 2,000,000 rows read, 1,288,348 kept
  ... 2,500,000 rows read, 1,681,719 kept
  ... 3,000,000 rows read, 2,028,258 kept
  ... 3,500,000 rows read, 2,133,508 kept
  ... 4,000,000 rows read, 2,555,057 kept
  ... 4,500,000 rows read, 2,967,021 kept
  ... 5,000,000 rows read, 3,376,333 kept
  ... 5,500,000 rows read, 3,614,708 kept
  ... 6,000,000 rows read, 3,614,708 kept
  ... 6,500,000 rows read, 3,614,708 kept
  ... 7,000,000 rows read, 3,614,708 kept
  ... 7,500,000 rows read, 3,765,801 kept
  ... 8,000,000 rows read, 4,151,977 kept
  ... 8,500,000 rows read, 4,526,472 kept
  ... 9,000,000 rows read, 4,915,077 kept
  ... 9,500,000 rows read, 5,329,711 kept
  ... 10,000,000 rows read, 5,742,461 kept
  ... 10,500,000 rows read, 6,142,376 kept
  ... 11,000,000 rows read, 6,214,120 kept
  ... 11,500,000 rows read, 6,214,120 kept
  ... 12,000,000 rows read, 6,233,320 kept
  Rows read: 12,256,290   kept (Conventional): 6,233,320

Generating BNM code records...
  Generated 12,987 BNM code records

Writing to SAS table XMISOUT.MELW072 ...
  Mid-week (Day 11): append (dedupe on REPTDATE)
The SAS Data Set XMISOUT.MELW072 does not exist
  NOTE: MELW072 not found — bootstrapping fresh table.

============================================================
EIBDMELW Complete
============================================================
Output  : XMISOUT.MELW072 (/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDMELW/)
Records : 12,987
ELDAY   : DAYC, Week: 2
SAS Connection terminated. Subprocess id was 840809
