No report date provided - will derive from today's run date

======================================================================
EIBMLIBT - LOAN MATURITY PROFILE PROCESSOR
======================================================================
CODE VERSION: v3.2-debug (pinpoint '07' sources)

Run date:    19/10/2026
Report date: 19/10/2026 (derived)
Week Number: 4
Report Month: 10
Report Year: 2026

Looking for BTRAD file: btrad10426.sas7bdat
  Reading SAS file...
  Total records read: 49288
  Records after filtering: 26071

--- SAMPLE RECORD #1 ---
  custcd='46' prodcd='34152' product=34152
  balance=0.0 payamt=0.0 days=0
  bldate=None issdte=2026-04-10 exprdate=2026-09-07
  days_to_expiry=-42 branch=A loop_iters=0
  remmth=0.1  get_remfmt(remmth)='01'

--- SAMPLE RECORD #2 ---
  custcd='46' prodcd='34152' product=34152
  balance=0.0 payamt=0.0 days=0
  bldate=None issdte=2026-04-16 exprdate=2026-09-11
  days_to_expiry=-38 branch=A loop_iters=0
  remmth=0.1  get_remfmt(remmth)='01'

--- SAMPLE RECORD #3 ---
  custcd='46' prodcd='34152' product=34152
  balance=0.0 payamt=0.0 days=0
  bldate=None issdte=2026-04-21 exprdate=2026-09-18
  days_to_expiry=-31 branch=A loop_iters=0
  remmth=0.1  get_remfmt(remmth)='01'

--- SAMPLE RECORD #4 ---
  custcd='46' prodcd='34152' product=34152
  balance=732626.46 payamt=0.0 days=0
  bldate=2026-11-27 issdte=2026-08-07 exprdate=2026-11-27
  days_to_expiry=39 branch=B loop_iters=1
  remmth=1.2580645161290323  get_remfmt(remmth)='03'

--- SAMPLE RECORD #5 ---
  custcd='46' prodcd='34152' product=34152
  balance=122442.7 payamt=0.0 days=0
  bldate=2027-01-29 issdte=2026-09-04 exprdate=2027-01-29
  days_to_expiry=102 branch=B loop_iters=1
  remmth=3.3225806451612905  get_remfmt(remmth)='04'
  Processed 5000 records...
  Processed 10000 records...
  Processed 15000 records...
  Processed 20000 records...

======================================================================
DEBUG REPORT
======================================================================
  Totals: {'records_in': 26071, 'records_exprdate_none': 0, 'records_bal_zero': 5921, 'records_processed': 20150, 'bnm_codes_generated': 40300}

  remfmt distribution BY SOURCE (branch, remfmt):
    93: remfmt='01' -> 4882 records
    93: remfmt='02' -> 4341 records
    93: remfmt='03' -> 9308 records
    93: remfmt='04' -> 1619 records
    95: remfmt='01' -> 4882 records
    95: remfmt='02' -> 4341 records
    95: remfmt='03' -> 9308 records
    95: remfmt='04' -> 1619 records

  '07' sources (branch, loop_iters, remfmt_95, remfmt_93):

  remmth_distribution (95-branch only, in-loop + final):
    Code 01: 4882
    Code 02: 4341
    Code 03: 9308
    Code 04: 1619

======================================================================
CONTINUING TO AGGREGATION
======================================================================

  Raw output rows: 40300

  Sample BNMCODEs (raw, pre-filter):
    '9521909030000Y' (len=14)  slice(7,2)='03'
    '9321909030000Y' (len=14)  slice(7,2)='03'
    '9521909040000Y' (len=14)  slice(7,2)='04'
    '9321909040000Y' (len=14)  slice(7,2)='04'
    '9521909010000Y' (len=14)  slice(7,2)='01'
    '9321909010000Y' (len=14)  slice(7,2)='01'
    '9521909030000Y' (len=14)  slice(7,2)='03'
    '9321909030000Y' (len=14)  slice(7,2)='03'
    '9521909030000Y' (len=14)  slice(7,2)='03'
    '9321909030000Y' (len=14)  slice(7,2)='03'

  Distinct slice(7,2) values BEFORE filter (raw):
    REMFMT='01': 9764 rows
    REMFMT='02': 8682 rows
    REMFMT='03': 18616 rows
    REMFMT='04': 3238 rows

  After aggregation + filter: 8 codes

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
SAS Connection established. Subprocess id is 1576973

  Assigning library XMISOUT -> /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMLIBT

21   
22   libname XMISOUT    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMLIBT'  ;
NOTE: Libref XMISOUT was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMLIBT
23   
  Uploading DataFrame to SAS dataset XMISOUT.bt (8 rows)...
  SAS dataset written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMLIBT/bt.sas7bdat
SAS Connection terminated. Subprocess id was 1576973

======================================================================
PROCESSING COMPLETED
======================================================================

Output SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBMLIBT/bt.sas7bdat [OK]
Total BNM codes: 8
Total amount: 7,230,505,093.94
