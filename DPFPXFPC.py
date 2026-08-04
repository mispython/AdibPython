======================================================================
BNM LIQUIDITY REPORT - TRADE FINANCE PROCESSING
======================================================================

Report Date: 03/08/2026
Report Year: 2026, Month: 08, Day: 03
Runoff Date: 31/08/2026

--------------------------------------------------
PROCESSING BA TRANSACTIONS (Banker's Acceptance)
--------------------------------------------------

Reading BTDTL data...
  Using exact file: btdtl260803.sas7bdat
  Reading: btdtl260803.sas7bdat
  WARNING: 'PAYAMT' column not found in BTDTL - defaulting to 0 for all rows.
  BTDTL records after filtering: 41474

Reading PBA01 data...
  Using exact file: pba01260803.sas7bdat
  Reading: pba01260803.sas7bdat
  BA records after merge: 18748

Processing BA records...
  BA records created: 37496

--------------------------------------------------
PROCESSING TR TRANSACTIONS (Trade)
--------------------------------------------------

Reading BTDTL data for TR...
  Using exact file: btdtl260803.sas7bdat
  Reading: btdtl260803.sas7bdat
  TR records before processing: 771

Processing TR records...
  TR records created: 1510

--------------------------------------------------
FINAL OUTPUT
--------------------------------------------------

  Records with MISSING remmth (code '07'): 10346
  Missing amount sum: 1,974,049,478.58

  Writing Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT/bt.parquet
  Writing CSV: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT/bt.csv

  Writing SAS7BDAT via saspy: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT/bt.sas7bdat

  Starting SAS session (cfgname='default')...
SAS Connection established. Subprocess id is 22486

  Assigning library XMISOUT -> /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT

21   
22   libname XMISOUT    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT'  ;
NOTE: Libref XMISOUT was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT
23   
  Uploading DataFrame to SAS dataset XMISOUT.bt (8 rows)...
  SAS dataset written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT/bt.sas7bdat
SAS Connection terminated. Subprocess id was 22486

======================================================================
PROCESSING COMPLETE
======================================================================

Output files:
  Parquet:  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT/bt.parquet
  CSV:      /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT/bt.csv
  SAS7BDAT: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT/bt.sas7bdat [OK]

Summary:
  Total BNM Codes: 8
  Total Amount:    5,043,876,386.22

Breakdown by BNMCODE:
--------------------------------------------------
  9321179010000Y:  202,111,092.82
  9321179020000Y:  811,149,542.57
  9321179030000Y: 1,407,964,501.01
  9321179040000Y:  100,713,056.71
  9521179010000Y:  202,111,092.82
  9521179020000Y:  811,149,542.57
  9521179030000Y: 1,407,964,501.01
  9521179040000Y:  100,713,056.71
