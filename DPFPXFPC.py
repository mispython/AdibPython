EIIDMELW - Islamic BNM EL Daily Deposit Extract
============================================================

Reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDMELW/DPTRBLGS.parquet ...
(sanity print skipped: sort() missing 1 required positional argument: 'by')
Report Date: 11/07/2026
Week: 2, Month: 07, Day: 11
============================================================
ELDAY: DAYC

Processing DPTRBL parquet (Islamic ONLY, COSTCTR 3000-3999)...
  Rows read: 12,256,290   kept (Islamic): 2,611,143

Generating BNM code records...
  Generated 12,987 BNM code records

Writing to MELW072.sas7bdat ...
  Mid-week (Day 11): append (dedupe on REPTDATE)
  ERROR: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDMELW/MELW072.sas7bdat missing but reptday=11 (mid-week).
         Run week-start day first (01/09/16/23).
