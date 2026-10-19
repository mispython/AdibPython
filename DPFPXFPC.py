No report date provided - will derive from today's run date

======================================================================
EIBMLIBT - LOAN MATURITY PROFILE PROCESSOR
======================================================================

Run date:    19/10/2026
Report date: 19/10/2026 (derived)
Week Number: 4
Report Month: 10
Report Year: 2026

Looking for BTRAD file: btrad10426.sas7bdat
  Reading SAS file...
  Total records read: 48739
  Records after filtering: 25625

  Debug - First few records:
    CUSTCD: 46, PRODCD: 34152, PRODUCT: None, BALANCE: None, PAYAMT: None, BLDATE: None, ISSDTE: 24170.0, EXPRDATE: 24318.0
    CUSTCD: 46, PRODCD: 34152, PRODUCT: None, BALANCE: 137465.62, PAYAMT: None, BLDATE: None, ISSDTE: 24206.0, EXPRDATE: 24356.0
    CUSTCD: 46, PRODCD: 34152, PRODUCT: None, BALANCE: 460017.57999999996, PAYAMT: None, BLDATE: None, ISSDTE: 24212.0, EXPRDATE: 24360.0
    CUSTCD: 46, PRODCD: 34152, PRODUCT: None, BALANCE: 234814.43999999997, PAYAMT: None, BLDATE: None, ISSDTE: 24217.0, EXPRDATE: 24367.0
    CUSTCD: 46, PRODCD: 34152, PRODUCT: None, BALANCE: None, PAYAMT: None, BLDATE: None, ISSDTE: 24182.0, EXPRDATE: 24302.0
  Processed 1000 records...
  Processed 2000 records...
  Processed 3000 records...
  Processed 4000 records...
  Processed 5000 records...
  Processed 6000 records...
  Processed 7000 records...
  Processed 8000 records...
  Processed 9000 records...
  Processed 10000 records...
  Processed 11000 records...
  Processed 12000 records...
  Processed 13000 records...
  Processed 14000 records...
  Processed 15000 records...
  Processed 16000 records...
  Processed 17000 records...
  Processed 18000 records...
  Processed 19000 records...
  Processed 20000 records...
  Processed 21000 records...
  Processed 22000 records...
  Processed 23000 records...
  Processed 24000 records...
  Processed 25000 records...

  Total records processed: 25625
  Output records created: 38824

  Remmth code distribution:
    Code 01: 14597 records
    Code 02: 3087 records
    Code 03: 1707 records
    Code 04: 21 records

  Unique BNMCODEs before filtering:
    9321909010000Y
    9321909020000Y
    9321909030000Y
    9321909040000Y
    9521909010000Y
    9521909020000Y
    9521909030000Y
    9521909040000Y

  Records with missing remmth (code '07'): 38816

  Final aggregated records:
    9321909010000Y: 2,690,696,838.62
    9321909020000Y: 505,067,267.43
    9321909030000Y: 305,961,247.26
    9321909040000Y: 2,968,191.59
    9521909010000Y: 2,690,696,838.62
    9521909020000Y: 505,067,267.43
    9521909030000Y: 305,961,247.26
    9521909040000Y: 2,968,191.59

Writing SAS dataset to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMLIBT/bt.sas7bdat

  Starting SAS session (cfgname='default')...
SAS Connection established. Subprocess id is 1571431

  Assigning library XMISOUT -> /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMLIBT

21   
22   libname XMISOUT    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMLIBT'  ;
NOTE: Libref XMISOUT was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMLIBT
23   
  Uploading DataFrame to SAS dataset XMISOUT.bt (8 rows)...
  SAS dataset written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMLIBT/bt.sas7bdat
SAS Connection terminated. Subprocess id was 1571431

======================================================================
PROCESSING COMPLETED SUCCESSFULLY
======================================================================

Output SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMLIBT/bt.sas7bdat [OK]
Total BNM codes: 8
Total amount: 7,009,387,089.80
