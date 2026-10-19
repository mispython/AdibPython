No report date provided - will derive from today's run date

======================================================================
EIBMLIBT - LOAN MATURITY PROFILE PROCESSOR
======================================================================
CODE VERSION: v3.1-debug (get_report_date + remmth diagnostics)

Run date:    19/10/2026
Report date: 19/10/2026 (derived)
Week Number: 4
Report Month: 10
Report Year: 2026

Looking for BTRAD file: btrad10426.sas7bdat
  Reading SAS file...
  Total records read: 49288
  Records after filtering: 26071

  Debug - First few records:
    CUSTCD: 46, PRODCD: 34152, PRODUCT: None, BALANCE: None, PAYAMT: None, BLDATE: None, ISSDTE: 24206.0, EXPRDATE: 24356.0
    CUSTCD: 46, PRODCD: 34152, PRODUCT: None, BALANCE: None, PAYAMT: None, BLDATE: None, ISSDTE: 24212.0, EXPRDATE: 24360.0
    CUSTCD: 46, PRODCD: 34152, PRODUCT: None, BALANCE: None, PAYAMT: None, BLDATE: None, ISSDTE: 24217.0, EXPRDATE: 24367.0
    CUSTCD: 46, PRODCD: 34152, PRODUCT: None, BALANCE: 732626.46, PAYAMT: None, BLDATE: None, ISSDTE: 24325.0, EXPRDATE: 24437.0
    CUSTCD: 46, PRODCD: 34152, PRODUCT: None, BALANCE: 122442.7, PAYAMT: None, BLDATE: None, ISSDTE: 24353.0, EXPRDATE: 24500.0
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

  Total records processed: 20150
  Output records created: 40300

  DEBUG SUMMARY (records producing remmth=None at final output):
    Total DEBUG_07 hits: 0
    By reason:
      exprdate_none: 0
      bal_zero: 5921
      path_A_unreached: 0
      path_B_unreached: 0
      zero_iter_else: 0
      remmth_still_none: 0

  Remmth code distribution:
    Code 01: 4882 records
    Code 02: 4341 records
    Code 03: 9308 records
    Code 04: 1619 records

  Unique BNMCODEs before filtering:
    9321909010000Y
    9321909020000Y
    9321909030000Y
    9321909040000Y
    9521909010000Y
    9521909020000Y
    9521909030000Y
    9521909040000Y

  Records with missing remmth (code '07'): 40292

  Final aggregated records:
    9321909010000Y: 873,063,000.28
    9321909020000Y: 796,194,415.60
    9321909030000Y: 1,664,707,924.55
    9321909040000Y: 281,287,206.53
    9521909010000Y: 873,063,000.28
    9521909020000Y: 796,194,415.60
    9521909030000Y: 1,664,707,924.55
    9521909040000Y: 281,287,206.53

Writing SAS dataset to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMLIBT/bt.sas7bdat

  Starting SAS session (cfgname='default')...
SAS Connection established. Subprocess id is 1573532

  Assigning library XMISOUT -> /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMLIBT

21   
22   libname XMISOUT    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMLIBT'  ;
NOTE: Libref XMISOUT was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMLIBT
23   
  Uploading DataFrame to SAS dataset XMISOUT.bt (8 rows)...
  SAS dataset written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMLIBT/bt.sas7bdat
SAS Connection terminated. Subprocess id was 1573532

======================================================================
PROCESSING COMPLETED SUCCESSFULLY
======================================================================

Output SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMLIBT/bt.sas7bdat [OK]
Total BNM codes: 8
Total amount: 7,230,505,093.94
