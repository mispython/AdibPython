EIIDMELW - Islamic BNM EL Daily Deposit Extract
============================================================

Reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDMELW/DPTRBLGS.parquet ...
TBDATE sample values: [25039.0, 35010.0, 35011.0, 35012.0, 35013.0]
Parsed TBDATE       : 2026-07-11
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

Writing to MELW072.sas7bdat ...
  Mid-week (Day 11): append (dedupe on REPTDATE)
  NOTE: MELW072.sas7bdat not found — bootstrapping fresh file.
        (SAS would error; run day 01/09/16/23 first if strict parity is required.)
ERROR while writing output:
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIDMELW.py", line 486, in main
    _write_sas7bdat_atomic(df_melw, filepath)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIDMELW.py", line 220, in _write_sas7bdat_atomic
    pyreadstat.write_sas7bdat(
AttributeError: module 'pyreadstat' has no attribute 'write_sas7bdat'
