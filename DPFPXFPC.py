No date provided - using today's date: 2026-06-06

======================================================================
EIBMLIBT - LOAN MATURITY PROFILE PROCESSOR
======================================================================

Report Date: 06/06/2026
Week Number: 4
Report Month: 06
Report Year: 2026

Looking for BTRAD file: btrad06426.sas7bdat
  Reading SAS file...
  Total records read: 47350
  Records after filtering: 24442

  Debug - First few records:
    CUSTCD: 46, PRODCD: 34152, PRODUCT: None, BALANCE: 558241.75, PAYAMT: None, BLDATE: None, ISSDTE: 24170.0, EXPRDATE: 24318.0
    CUSTCD: 46, PRODCD: 34152, PRODUCT: None, BALANCE: 137017.88999999998, PAYAMT: None, BLDATE: None, ISSDTE: 24206.0, EXPRDATE: 24356.0
    CUSTCD: 46, PRODCD: 34152, PRODUCT: None, BALANCE: 458518.67, PAYAMT: None, BLDATE: None, ISSDTE: 24212.0, EXPRDATE: 24360.0
    CUSTCD: 46, PRODCD: 34152, PRODUCT: None, BALANCE: 234048.75999999998, PAYAMT: None, BLDATE: None, ISSDTE: 24217.0, EXPRDATE: 24367.0
    CUSTCD: 46, PRODCD: 34152, PRODUCT: None, BALANCE: None, PAYAMT: None, BLDATE: None, ISSDTE: 24156.0, EXPRDATE: 24276.0
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

  Total records processed: 24442
  Output records created: 38408

  Remmth code distribution:
    Code 01: 190 records
    Code 02: 1010 records
    Code 03: 10639 records
    Code 04: 7306 records
    Code 05: 59 records

  Unique BNMCODEs before filtering:
    9321109010000Y
    9321109020000Y
    9321109030000Y
    9321109040000Y
    9321109050000Y
    9521109010000Y
    9521109020000Y
    9521109030000Y
    9521109040000Y
    9521109050000Y

  Records with missing remmth (code '07'): 38398

  Final aggregated records:
    9321109010000Y: 23,002,117.24
    9321109020000Y: 190,993,164.60
    9321109030000Y: 1,922,888,546.10
    9321109040000Y: 1,231,791,608.19
    9321109050000Y: 6,962,475.82
    9521109010000Y: 23,002,117.24
    9521109020000Y: 190,993,164.60
    9521109030000Y: 1,922,888,546.10
    9521109040000Y: 1,231,791,608.19
    9521109050000Y: 6,962,475.82

Writing SAS dataset to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMLIBT/BT.sas7bdat
SAS Connection established. Subprocess id is 60736

  Warning: Could not write SAS dataset using saspy: sasdata() got an unexpected keyword argument 'df'
  Attempting alternative method...
  SAS dataset written successfully using CSV import

Writing Parquet file to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMLIBT/BT.parquet
  Parquet file written successfully

Writing report to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMLIBT/BT_REPORT.txt

======================================================================
PROCESSING COMPLETED SUCCESSFULLY
======================================================================

Output SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMLIBT/BT.sas7bdat
Output Parquet file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMLIBT/BT.parquet
Report file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMLIBT/BT_REPORT.txt
Total BNM codes: 10
Total amount: 6,751,275,823.90
SAS Connection terminated. Subprocess id was 60736
