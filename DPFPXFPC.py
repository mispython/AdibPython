
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
  WARNING: 'PAYAMT' not in BTDTL - defaulting to 0 (SAS-substitute).
  BTDTL records after filtering: 45919

Reading PBA01 data...
  Using exact file: pba01261018.sas7bdat
  Reading: pba01261018.sas7bdat
  BA records after merge: 18919

Processing BA records...
  BA records created: 37838

--------------------------------------------------
PROCESSING TR TRANSACTIONS (Trade)
--------------------------------------------------

Reading BTDTL data for TR...
  Using exact file: btdtl261018.sas7bdat
  Reading: btdtl261018.sas7bdat
  WARNING: 'PAYAMT' not in TR BTDTL - defaulting to 0.
  TR records before processing: 1099

Processing TR records...
  TR records created: 1666

--------------------------------------------------
FINAL OUTPUT
--------------------------------------------------

  Records with MISSING remmth (code '07'): 10
  Missing amount sum: 1,779,563.58

  Writing SAS7BDAT via saspy: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT/bt.sas7bdat

  Starting SAS session (cfgname='default')...
SAS Connection established. Subprocess id is 1607840

  Assigning library XMISOUT -> /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT

21   
22   libname XMISOUT    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT'  ;
NOTE: Libref XMISOUT was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT
23   
  Uploading DataFrame to SAS dataset XMISOUT.bt (8 rows)...
  SAS dataset written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT/bt.sas7bdat
SAS Connection terminated. Subprocess id was 1607840

======================================================================
SUMMARY
======================================================================
  Total BNM Codes: 8
  Total Amount:    7,160,007,488.01

  Breakdown by BNMCODE:
    9321909010000Y:  302,054,833.65
    9321909020000Y:  797,568,008.76
    9321909030000Y: 1,867,755,537.72
    9321909040000Y:  612,625,363.87
    9521909010000Y:  302,054,833.65
    9521909020000Y:  797,568,008.76
    9521909030000Y: 1,867,755,537.72
    9521909040000Y:  612,625,363.87

  SAS7BDAT: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT/bt.sas7bdat [OK]

======================================================================
DIAGNOSTIC REPORT
======================================================================

--- Row counts ---
  BA rows in               : 18919
  BA rows processed        : 18919
  BA rows EXPRDATE null    : 5
  TR rows in               : 1099
  TR rows processed        : 1099
  TR rows EXPRDATE null    : 0

--- PAYAMT state (substitute: always 0 in this extract) ---
  PAYAMT == 0              : 20018
  PAYAMT < 0               : 0
  PAYAMT > 0               : 0

--- NPL override ---
  NPL override fired       : 0   (expect 0 when BLDATE missing)

--- EXPRDATE branch taken ---
  exprdate_far                : 18135
  exprdate_within_8d          : 1878
  exprdate_missing            : 5

--- DAYS buckets ---
  MISSING (BLDATE absent)     : 20018

--- FINAL remmth state ---
  remmth is None           : 5
  remmth == 0.1            : 1878
  remmth computed          : 18135

--- CUST / ITEM distribution ---
  CUST = '08'              : 0
  CUST = '09'              : 20018
  ITEM = '212'             : 0
  ITEM = '219'             : 20018
  PRODUCT==100 override    : 0

--- Records emitted by prefix ---
  prefix 95/96 count       : 19752
  prefix 93/94 count       : 19752

--- Emitted remmth distribution (all) ---
  code 01: 3254
  code 02: 8546
  code 03: 20200
  code 04: 7494
  code 07: 10

--- Emitted remmth distribution (by prefix) ---
  prefix 93:
    code 01: 1627
    code 02: 4273
    code 03: 10100
    code 04: 3747
    code 07: 5
  prefix 95:
    code 01: 1627
    code 02: 4273
    code 03: 10100
    code 04: 3747
    code 07: 5

--- Records dropped as code '07' ---
  count                    : 10
  balance sum              : 1,779,563.58

--- PAYAMT presence ---
  BA: MISSING, TR: MISSING

--- All emitted BNMCODEs ---
  9321909010000Y : 1627
  9321909020000Y : 4273
  9321909030000Y : 10100
  9321909040000Y : 3747
  9321909070000Y : 5
  9521909010000Y : 1627
  9521909020000Y : 4273
  9521909030000Y : 10100
  9521909040000Y : 3747
  9521909070000Y : 5
