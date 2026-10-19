
======================================================================
BNM LIQUIDITY REPORT - TRADE FINANCE PROCESSING
======================================================================

Report Date: 18/10/2026
Report Year: 2026, Month: 10, Day: 18
Runoff Date: 31/10/2026

--------------------------------------------------
PROCESSING BA TRANSACTIONS (Banker's Acceptance)
--------------------------------------------------

Reading BTDTL data...
  Using exact file: btdtl261018.sas7bdat
  Reading: btdtl261018.sas7bdat
  WARNING: 'PAYAMT' column not found in BTDTL - defaulting to 0 for all rows.
  BTDTL records after filtering: 41474

Reading PBA01 data...
  Using exact file: pba01261018.sas7bdat
  Reading: pba01261018.sas7bdat
  BA records after merge: 18748

Processing BA records...
  BA records created: 37496

--------------------------------------------------
PROCESSING TR TRANSACTIONS (Trade)
--------------------------------------------------

Reading BTDTL data for TR...
  Using exact file: btdtl261018.sas7bdat
  Reading: btdtl261018.sas7bdat
  TR records before processing: 771

Processing TR records...
  TR records created: 1510

--------------------------------------------------
FINAL OUTPUT
--------------------------------------------------

  Records with MISSING remmth (code '07'): 31028
  Missing amount sum: 5,675,337,521.25

  Writing SAS7BDAT via saspy: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT/bt.sas7bdat

  Starting SAS session (cfgname='default')...
SAS Connection established. Subprocess id is 1581462

  Assigning library XMISOUT -> /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT

21   
22   libname XMISOUT    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT'  ;
NOTE: Libref XMISOUT was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT
23   
  Uploading DataFrame to SAS dataset XMISOUT.bt (6 rows)...
  SAS dataset written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT/bt.sas7bdat
SAS Connection terminated. Subprocess id was 1581462

======================================================================
PROCESSING COMPLETE
======================================================================

Output file:
  SAS7BDAT: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT/bt.sas7bdat [OK]

Summary:
  Total BNM Codes: 6
  Total Amount:    1,342,588,343.55

Breakdown by BNMCODE:
--------------------------------------------------
  9321909010000Y:  144,255,057.37
  9321909020000Y:  426,326,057.69
  9321909030000Y:  100,713,056.71
  9521909010000Y:  144,255,057.37
  9521909020000Y:  426,326,057.69
  9521909030000Y:  100,713,056.71
