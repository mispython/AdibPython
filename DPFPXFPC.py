=====================================================================
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

  [DIAG] BTDTL schema:
    TRANSREX             : String
    CREATTYP             : String
    BRANCH               : Float64
    APPLCODE             : String
    ACCTNO               : String
    SUBACCT              : String
    CREATYYMMDD          : Float64
    EXPIRDS              : Float64
    SYNDICAT             : String
    SPECIALF             : String
    PURPOSES             : String
    AANUMBER             : String
    INTRATE              : Float64
    SPREAD               : Float64
    INFUNDRT             : Float64
    DISCNTB              : Float64
    DISCNTF              : Float64
    TRANXMT              : Float64
    EXCHRTE              : Float64
    FORCURR              : String
    LIABCODE             : String
    BTREL1               : String
    RELFROM              : String
    CURRENCY             : String
    APPRLIMT             : Float64
    APPRLIM2             : Float64
    OFFAPIND             : String
    TFDESC01             : String
    TFCNTR01             : Float64
    TFCNTR03             : Float64
    TFCNTR04             : Float64
    TFINDR01             : String
    TFINDR02             : String
    TFINDR03             : String
    TFINDR04             : String
    TFINDR05             : String
    ISSDTEYYMMDD         : Float64
    SINDICAT             : String
    SUBPROD              : String
    FACLINE              : String
    PRODGRP              : String
    DIRCTIND             : String
    TRANSREL             : String
    COMMRATE             : Float64
    DISCRATE             : Float64
    CONRATE_IND          : String
    INTBASE              : String
    PLUSMINUS            : String
    NUMDAYS              : Float64
    BACOM                : Float64
    DISCOUNT_PROCEED     : Float64
    MTD_TAWIDH_AMT       : Float64
    MTD_GHARAMAH_AMT     : Float64
    REPAY_SOURCE         : String
    REPAY_TYPE_CD        : String
    PROP_DEVELOP_FIN_IND : String
    CLIMATE_PRIN_TAXONOMY_CLASS : String
    CLIMATE_MITIGATE_GP1_FLG : String
    CLIMATE_ADAPT_GP2_FLG : String
    CLIMATE_ENVIRONMT_GP3_FLG : String
    CLIMATE_TRANSITION_GP4_FLG : String
    CLIMATE_PROHIBIT_GP5_FLG : String
    SOURCE_INCOME_CURRENCY_CD : String
    AADATE               : Float64
    REFERRAL_BRANCH      : Float64
    APPL_COMMERCIAL_TAG  : String
    COMBRATE             : Float64
    AA_APPROVED_DT       : Float64
    BTREL2               : String
    BTREL3               : String
    BTREL4               : String
    NOLEVEL              : Float64
    OUTSTAND             : Float64
    CERTNO               : Float64
    COLLECNO             : String
    PAYBKNO              : String
    ADDRLN3              : String
    ADDRLN4              : String
    CNTRY                : String
    PREBKNO              : String
    PRINAMT_MYRX         : Float64
    INTAMT_MYRX          : Float64
    OTH_CHARGEX          : Float64
    TRANSREF             : String
    MATUREDS             : Float64
    RETAILID             : String
    STATE                : String
    SCORE1               : String
    SCORE2               : String
    BUSREGN              : String
    SECTOR               : String
    SM_STATUS            : String
    IA_LRU               : String
    SM_DATE              : Float64
    ASCORE_LTST          : String
    ASCORE_PERM          : String
    APVDATE              : Float64
    INDUSTRIAL_SECTOR_CD : String
    LEGAL_ACTION_CD      : Float64
    LEGAL_ACTION_DT      : Float64
    CCPT_LTST_REVIEW_DT  : Float64
    FDB_TAG              : String
    FDB_TAG_DT           : Float64
    FDB_SCORING_DT       : Float64
    CUSTCODE             : Float64
    DNBFISME             : String
    ACCTNON              : Float64
    ACCTNOX              : Float64
    PRINAMT              : Float64
    INTAMT               : Float64
    INTYTD               : Float64
    FIXFLT               : String
    CALBASP              : Float64
    INTAMT_MYR           : Float64
    PRINAMT_MYR          : Float64
    TENOR_INT            : Float64
    OTH_CHARGE           : Float64
    IRIA                 : Float64
    REPTDAT1             : Float64
    RDAY1                : Float64
    ISSYY                : String
    ISSMM                : String
    ISSDTX               : String
    FACILITY             : String
    PREFIX               : String
    CREATDS              : Float64
    ISSDTE               : Float64
    MATDATE              : Float64
    EXPRDATE             : Float64
    DIA_PAST01_MTH       : Float64
    DIA_PAST02_MTH       : Float64
    DIA_PAST03_MTH       : Float64
    DIA_PAST04_MTH       : Float64
    DIA_PAST05_MTH       : Float64
    DIA_PAST06_MTH       : Float64
    DIA_PAST07_MTH       : Float64
    DIA_PAST08_MTH       : Float64
    DIA_PAST09_MTH       : Float64
    DIA_PAST10_MTH       : Float64
    DIA_PAST11_MTH       : Float64
    DIA_PAST12_MTH       : Float64
    FORATE               : String
  WARNING: 'PAYAMT' column not found in BTDTL - defaulting to 0 for all rows.
  BTDTL records after filtering: 41474

Reading PBA01 data...
  Using exact file: pba01261018.sas7bdat
  Reading: pba01261018.sas7bdat

  [DIAG] PBA01 schema:
    TRANSREF             : String
    CERTNO               : Float64
    FCVALUE              : Float64
    YIELD                : Float64
    UTRDF                : String
    UNEARNED             : Float64
    BRANCH               : Float64
    UTCAT                : String
    MATDATE              : Float64
    LNTYPE               : String
    FO_DEAL_ID           : String
  BA records after merge: 18748

Processing BA records...
  BA records created: 37496

--------------------------------------------------
PROCESSING TR TRANSACTIONS (Trade)
--------------------------------------------------

Reading BTDTL data for TR...
  Using exact file: btdtl261018.sas7bdat
  Reading: btdtl261018.sas7bdat
  WARNING: 'PAYAMT' column not found in TR BTDTL - defaulting to 0.
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
SAS Connection established. Subprocess id is 1583599

  Assigning library XMISOUT -> /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT

21   
22   libname XMISOUT    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT'  ;
NOTE: Libref XMISOUT was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT
23   
  Uploading DataFrame to SAS dataset XMISOUT.bt (6 rows)...
  SAS dataset written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT/bt.sas7bdat
SAS Connection terminated. Subprocess id was 1583599

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

======================================================================
DIAGNOSTIC REPORT
======================================================================

--- Row counts ---
  BA rows in            : 18748
  BA rows processed     : 18748
  BA rows exprdate NULL : 8
  TR rows in            : 771
  TR rows processed     : 771
  TR rows exprdate NULL : 0

--- PAYAMT state across all processed rows ---
  PAYAMT == 0           : 19519
  PAYAMT < 0            : 0
  PAYAMT > 0            : 0

--- EXPRDATE branch taken ---
  exprdate_within_8d          : 901
  exprdate_le_runoff          : 15522
  exprdate_far                : 3088
  exprdate_missing            : 8
  exprdate_le_runoff         : 15522
  exprdate_within_8d         : 901
  exprdate_far               : 3088

--- BLDATE state ---
  BLDATE <= 0           : 19519
  BLDATE <= runoff_sas  : 0
  BLDATE >  runoff_sas  : 0

--- DAYS past due buckets ---
  <0        : 0
  0-30      : 19519
  31-89     : 0
  90-180    : 0
  >180      : 0

--- FINAL remmth state before code '07' filter ---
  remmth is None        : 15530
  remmth == 0.1         : 901
  remmth computed       : 3088

--- CUST / ITEM distribution ---
  CUST = '08'           : 0
  CUST = '09'           : 19519
  ITEM = '212'          : 0
  ITEM = '219'          : 19519
  PRODUCT==100 override : 0

--- Records emitted by prefix ---
  prefix 95/96 count    : 19503
  prefix 93/94 count    : 19503

--- Emitted remmth distribution (before 07 filter) ---
  code 01: 1802
  code 02: 5066
  code 03: 1110
  code 07: 31028

--- BNMCODE distribution (before 07 filter) ---
  9321909010000Y: 901
  9321909020000Y: 2533
  9321909030000Y: 555
  9321909070000Y: 15514
  9521909010000Y: 901
  9521909020000Y: 2533
  9521909030000Y: 555
  9521909070000Y: 15514

--- Records dropped as code '07' ---
  count                 : 31028
  balance sum           : 5,675,337,521.25
  07-drop remmth code 07: 31028

======================================================================

--- PAYAMT column presence ---
  BA extract PAYAMT : MISSING
  TR extract PAYAMT : MISSING

--- All unique BNMCODEs emitted (before 07 filter) ---
  9321909010000Y : 901
  9321909020000Y : 2533
  9321909030000Y : 555
  9321909070000Y : 15514
  9521909010000Y : 901
  9521909020000Y : 2533
  9521909030000Y : 555
  9521909070000Y : 15514
