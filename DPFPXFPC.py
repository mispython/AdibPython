======================================================================
BNM LIQUIDITY REPORT - TRADE FINANCE PROCESSING
======================================================================

Report Date: 18/10/2026
Runoff Date: 31/10/2026  (reference for all REMMTH logic)

Reading BTDTL data...
  Using exact file: btdtl261018.sas7bdat
  Reading: btdtl261018.sas7bdat
  BTDTL raw rows: 45952
  BTDTL columns : ['AADATE', 'AANUMBER', 'AA_APPROVED_DT', 'ACCTNO', 'ACCTNON', 'ACCTNOX', 'ADDRLN3', 'ADDRLN4', 'APPLCODE', 'APPL_COMMERCIAL_TAG', 'APPRLIM2', 'APPRLIMT', 'APVDATE', 'ASCORE_LTST', 'ASCORE_PERM', 'BACOM', 'BRANCH', 'BTREL1', 'BTREL2', 'BTREL3', 'BTREL4', 'BUSREGN', 'CALBASP', 'CCPT_LTST_REVIEW_DT', 'CERTNO', 'CLIMATE_ADAPT_GP2_FLG', 'CLIMATE_ENVIRONMT_GP3_FLG', 'CLIMATE_MITIGATE_GP1_FLG', 'CLIMATE_PRIN_TAXONOMY_CLASS', 'CLIMATE_PROHIBIT_GP5_FLG', 'CLIMATE_TRANSITION_GP4_FLG', 'CNTRY', 'COLLECNO', 'COMBRATE', 'COMMRATE', 'CONRATE_IND', 'CREATDS', 'CREATTYP', 'CREATYYMMDD', 'CURRENCY', 'CUSTCODE', 'DIA_PAST01_MTH', 'DIA_PAST02_MTH', 'DIA_PAST03_MTH', 'DIA_PAST04_MTH', 'DIA_PAST05_MTH', 'DIA_PAST06_MTH', 'DIA_PAST07_MTH', 'DIA_PAST08_MTH', 'DIA_PAST09_MTH', 'DIA_PAST10_MTH', 'DIA_PAST11_MTH', 'DIA_PAST12_MTH', 'DIRCTIND', 'DISCNTB', 'DISCNTF', 'DISCOUNT_PROCEED', 'DISCRATE', 'DNBFISME', 'EXCHRTE', 'EXPIRDS', 'EXPRDATE', 'FACILITY', 'FACLINE', 'FDB_SCORING_DT', 'FDB_TAG', 'FDB_TAG_DT', 'FIXFLT', 'FORATE', 'FORCURR', 'IA_LRU', 'INDUSTRIAL_SECTOR_CD', 'INFUNDRT', 'INTAMT', 'INTAMT_MYR', 'INTAMT_MYRX', 'INTBASE', 'INTRATE', 'INTYTD', 'IRIA', 'ISSDTE', 'ISSDTEYYMMDD', 'ISSDTX', 'ISSMM', 'ISSYY', 'LEGAL_ACTION_CD', 'LEGAL_ACTION_DT', 'LIABCODE', 'MATDATE', 'MATUREDS', 'MTD_GHARAMAH_AMT', 'MTD_TAWIDH_AMT', 'NOLEVEL', 'NUMDAYS', 'OFFAPIND', 'OTH_CHARGE', 'OTH_CHARGEX', 'OUTSTAND', 'PAYBKNO', 'PLUSMINUS', 'PREBKNO', 'PREFIX', 'PRINAMT', 'PRINAMT_MYR', 'PRINAMT_MYRX', 'PRODGRP', 'PROP_DEVELOP_FIN_IND', 'PURPOSES', 'RDAY1', 'REFERRAL_BRANCH', 'RELFROM', 'REPAY_SOURCE', 'REPAY_TYPE_CD', 'REPTDAT1', 'RETAILID', 'SCORE1', 'SCORE2', 'SECTOR', 'SINDICAT', 'SM_DATE', 'SM_STATUS', 'SOURCE_INCOME_CURRENCY_CD', 'SPECIALF', 'SPREAD', 'STATE', 'SUBACCT', 'SUBPROD', 'SYNDICAT', 'TENOR_INT', 'TFCNTR01', 'TFCNTR03', 'TFCNTR04', 'TFDESC01', 'TFINDR01', 'TFINDR02', 'TFINDR03', 'TFINDR04', 'TFINDR05', 'TRANSREF', 'TRANSREL', 'TRANSREX', 'TRANXMT']
  WARNING: 'PAYAMT' not in BTDTL - defaulting to 0. SAS KEEPs PAYAMT, so check you are reading the same file/version as SAS (column name/case).

--------------------------------------------------
PROCESSING BA TRANSACTIONS (Banker's Acceptance)
--------------------------------------------------
  BTDTL after filter (ISSDTE>0 OR EXPRDATE>0): 45952
  BTDTL after NODUPKEY (TRANSREF, ISSDTE): 45949

Reading PBA01 data...
  Using exact file: pba01261018.sas7bdat
  Reading: pba01261018.sas7bdat
  PBA01 raw rows: 18919
  PBA01 columns : ['BRANCH', 'CERTNO', 'FCVALUE', 'FO_DEAL_ID', 'LNTYPE', 'MATDATE', 'TRANSREF', 'UNEARNED', 'UTCAT', 'UTRDF', 'YIELD']
    FCVALUE   present in PBA01: True
    UNEARNED  present in PBA01: True
    CUSTCD    present in PBA01: False
    CUSTCODE  present in PBA01: False
    PRODUCT   present in PBA01: False
    BLDATE    present in PBA01: False
    MATDATE   present in PBA01: True
  BA rows after PBA01 x BTDTL merge: 18919
  BA rows with matching BTDTL: 18914

Processing BA records...
  BA records created: 37838

--------------------------------------------------
PROCESSING TR TRANSACTIONS (Trade)
--------------------------------------------------
    OUTSTAND  present in BTDTL: True
    CUSTCD    present in BTDTL: False
    CUSTCODE  present in BTDTL: True
    PRODUCT   present in BTDTL: False
    BLDATE    present in BTDTL: False
    MATDATE   present in BTDTL: True
    LIABCODE  present in BTDTL: True
    DIRCTIND  present in BTDTL: True
  After LIABCODE filter: 23798 (was 45952)
  After DIRCTIND='D' filter: 1099 (was 23798)

Processing TR records...
  TR records created: 2198

--------------------------------------------------
FINAL OUTPUT
--------------------------------------------------

  Records with MISSING remmth (code '07'): 6598, amount 1,189,491,152.25

  Writing SAS7BDAT via saspy: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT/bt.sas7bdat

  Starting SAS session (cfgname='default')...
SAS Connection established. Subprocess id is 1622466

  Assigning library XMISOUT -> /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT

21   
22   libname XMISOUT    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT'  ;
NOTE: Libref XMISOUT was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT
23   
  Uploading DataFrame to SAS dataset XMISOUT.bt (8 rows)...
  SAS dataset written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT/bt.sas7bdat
SAS Connection terminated. Subprocess id was 1622466

======================================================================
SUMMARY
======================================================================
  Total BNM Codes: 8
  Total Amount:    5,972,295,899.33

  Breakdown by BNMCODE:
    9321909010000Y:     213,115,507.92
    9321909020000Y:     837,778,900.83
    9321909030000Y:   1,637,028,753.77
    9321909040000Y:     298,224,787.15
    9521909010000Y:     213,115,507.92
    9521909020000Y:     837,778,900.83
    9521909030000Y:   1,637,028,753.77
    9521909040000Y:     298,224,787.15

  Comparison with production SAS bt.sas7bdat (18/10/2026):
    BNMCODE                       PYTHON                 SAS              DIFF
    9321909010000Y        213,115,507.92      213,115,507.92             -0.00
    9321909020000Y        837,778,900.83      837,778,900.83             -0.00
    9321909030000Y      1,637,028,753.77    1,637,028,753.80             -0.03  <-- DIFF
    9321909040000Y        298,224,787.15      298,224,787.15             -0.00
    9521909010000Y        213,115,507.92      213,115,507.92             -0.00
    9521909020000Y        837,778,900.83      837,778,900.83             -0.00
    9521909030000Y      1,637,028,753.77    1,637,028,753.80             -0.03  <-- DIFF
    9521909040000Y        298,224,787.15      298,224,787.15             -0.00

  SAS7BDAT: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT/bt.sas7bdat [OK]

======================================================================
DIAGNOSTIC REPORT
======================================================================

--- Row counts ---
  ba_rows                     : 18919
  ba_rows_processed           : 18919
  ba_expr_null                : 5
  tr_rows                     : 1099
  tr_rows_processed           : 1099
  tr_expr_null                : 0

--- EXPRDATE vs RUNOFFDT branch ---
  expr_missing_or_le_runoff   : 3299
  expr_within_8d_of_runoff    : 1203
  expr_far                    : 15516

--- PAYAMT / DAYS ---
  payamt_zero                 : 20018
  payamt_negative             : 0
  payamt_positive             : 0
  days_gt_89                  : 0
  npl_override_fired          : 0

--- Final REMMTH state ---
  remmth_missing_final        : 3299
  remmth_0_1_final            : 1203
  remmth_computed_final       : 15516

--- CUST / ITEM ---
  cust_08                     : 0
  cust_09                     : 20018
  item_212                    : 0
  item_219                    : 20018
  product_100_override        : 0

--- Raw input totals (BEFORE bucketing) ---
  BA SUM(FCVALUE)          :     3,373,907,336.57
  BA SUM(UNEARNED)         :        18,017,495.50
  BA SUM(FCVALUE-UNEARNED) :     3,355,889,841.07
  BA zero / negative bal   : 0 / 0
  BA EXPRDATE-null balance :           889,781.79
  TR SUM(OUTSTAND)         :       225,003,684.72
  TR zero / negative bal   : 266 / 0
  GRAND RAW TOTAL          :     3,580,893,525.79

--- Emitted records by prefix / REMFMT code ---
  prefix 93: 01=1203, 02=4021, 03=9699, 04=1796, 07=3299
  prefix 95: 01=1203, 02=4021, 03=9699, 04=1796, 07=3299

--- Dropped as code '07' (missing REMMTH) ---
  records                  : 6598
  amount (both prefixes)   :     1,189,491,152.25
You have mail in /var/spool/mail/sas_edw_dev
