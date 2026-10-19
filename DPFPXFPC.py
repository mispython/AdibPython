
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
  BTDTL raw rows: 45952
  [CHK] ISSDTE present: True, EXPRDATE present: True
  BTDTL after filter (ISSDTE>0 OR EXPRDATE>0): 45952
  WARNING: 'PAYAMT' not in BTDTL - defaulting to 0.
  BTDTL after dedup (unique TRANSREF): 45919

Reading PBA01 data...
  Using exact file: pba01261018.sas7bdat
  Reading: pba01261018.sas7bdat
  PBA01 raw rows: 18919
  BA rows after PBA01 ⋈ BTDTL merge: 18919
  BA rows with matching BTDTL: 18914 / 18919

Processing BA records...
  BA records created: 37838

--------------------------------------------------
PROCESSING TR TRANSACTIONS (Trade)
--------------------------------------------------

Reading BTDTL data for TR...
  Using exact file: btdtl261018.sas7bdat
  Reading: btdtl261018.sas7bdat
  BTDTL raw rows for TR: 45952
  WARNING: 'PAYAMT' not in TR BTDTL - defaulting to 0.
  After LIABCODE filter: 23798 (was 45952)
  After DIRCTIND='D' filter: 1099 (was 23798)
  Distinct LIABCODE values in BTDTL: ['BAE', 'BAI', 'BAP', 'BAS', 'BGE', 'BGF', 'BGP', 'BGT', 'BRF', 'BUF', 'BUL', 'DIL', 'FAS', 'FBE', 'FDU', 'FFU', 'FIL', 'FTI', 'FTL', 'IFD', 'IFS', 'IFU', 'ILB', 'ILL', 'ILS', 'ILU', 'LCE', 'PBA', 'PBU', 'PBZ', 'POS', 'SBE', 'SGE', 'SLC', 'TFL', 'TLL']
  Distinct DIRCTIND values in BTDTL: ['D', 'I']
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
SAS Connection established. Subprocess id is 1610551

  Assigning library XMISOUT -> /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT

21   
22   libname XMISOUT    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT'  ;
NOTE: Libref XMISOUT was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT
23   
  Uploading DataFrame to SAS dataset XMISOUT.bt (8 rows)...
  SAS dataset written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT/bt.sas7bdat
SAS Connection terminated. Subprocess id was 1610551

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

--- PAYAMT state ---
  PAYAMT == 0              : 20018
  PAYAMT < 0               : 0
  PAYAMT > 0               : 0

--- NPL override ---
  NPL override fired       : 0   (expect 0)

--- EXPRDATE branch ---
  exprdate_missing         : 5
  exprdate_within_8d       : 1878
  exprdate_far             : 18135

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

--- Raw input totals (BEFORE bucketing) ---
  BA  count                : 18919
  BA  SUM(FCVALUE)         :     3,373,907,336.57
  BA  SUM(UNEARNED)        :        18,017,495.50
  BA  SUM(FCVALUE-UNEARNED):     3,355,889,841.07  <-- compare with SAS BA total
  BA  zero-balance count   : 0
  BA  negative-balance cnt : 0
  BA  EXPRDATE-null count  : 5
  BA  EXPRDATE-null balance:           889,781.79

  TR  count                : 1099
  TR  SUM(OUTSTAND)        :       225,003,684.72  <-- compare with SAS TR total
  TR  zero-balance count   : 266
  TR  negative-balance cnt : 0
  TR  EXPRDATE-null count  : 0
  TR  EXPRDATE-null balance:                 0.00

  GRAND RAW TOTAL          :     3,580,893,525.79  <-- compare with SAS pre-bucket total

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
  balance sum              :         1,779,563.58

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
