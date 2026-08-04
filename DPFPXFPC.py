======================================================================
BNM LIQUIDITY REPORT - TRADE FINANCE PROCESSING
======================================================================

Report Date: 04/08/2026
Report Year: 2026, Month: 08, Day: 04
Runoff Date: 31/08/2026

--------------------------------------------------
PROCESSING BA TRANSACTIONS (Banker's Acceptance)
--------------------------------------------------

Reading BTDTL data...
  WARNING: Exact file not found. Using latest: btdtl260803.sas7bdat (dated 2026-08-03)
  Reading: btdtl260803.sas7bdat
  BA Processing Error: unable to find column "PAYAMT"; valid columns: ["TRANSREX", "CREATTYP", "BRANCH", "APPLCODE", "ACCTNO", "SUBACCT", "CREATYYMMDD", "EXPIRDS", "SYNDICAT", "SPECIALF", "PURPOSES", "AANUMBER", "INTRATE", "SPREAD", "INFUNDRT", "DISCNTB", "DISCNTF", "TRANXMT", "EXCHRTE", "FORCURR", "LIABCODE", "BTREL1", "RELFROM", "CURRENCY", "APPRLIMT", "APPRLIM2", "OFFAPIND", "TFDESC01", "TFCNTR01", "TFCNTR03", "TFCNTR04", "TFINDR01", "TFINDR02", "TFINDR03", "TFINDR04", "TFINDR05", "ISSDTEYYMMDD", "SINDICAT", "SUBPROD", "FACLINE", "PRODGRP", "DIRCTIND", "TRANSREL", "COMMRATE", "DISCRATE", "CONRATE_IND", "INTBASE", "PLUSMINUS", "NUMDAYS", "BACOM", "DISCOUNT_PROCEED", "MTD_TAWIDH_AMT", "MTD_GHARAMAH_AMT", "REPAY_SOURCE", "REPAY_TYPE_CD", "PROP_DEVELOP_FIN_IND", "CLIMATE_PRIN_TAXONOMY_CLASS", "CLIMATE_MITIGATE_GP1_FLG", "CLIMATE_ADAPT_GP2_FLG", "CLIMATE_ENVIRONMT_GP3_FLG", "CLIMATE_TRANSITION_GP4_FLG", "CLIMATE_PROHIBIT_GP5_FLG", "SOURCE_INCOME_CURRENCY_CD", "AADATE", "REFERRAL_BRANCH", "APPL_COMMERCIAL_TAG", "COMBRATE", "AA_APPROVED_DT", "BTREL2", "BTREL3", "BTREL4", "NOLEVEL", "OUTSTAND", "CERTNO", "COLLECNO", "PAYBKNO", "ADDRLN3", "ADDRLN4", "CNTRY", "PREBKNO", "PRINAMT_MYRX", "INTAMT_MYRX", "OTH_CHARGEX", "TRANSREF", "MATUREDS", "RETAILID", "STATE", "SCORE1", "SCORE2", "BUSREGN", "SECTOR", "SM_STATUS", "IA_LRU", "SM_DATE", "ASCORE_LTST", "ASCORE_PERM", "APVDATE", "INDUSTRIAL_SECTOR_CD", "LEGAL_ACTION_CD", "LEGAL_ACTION_DT", "CCPT_LTST_REVIEW_DT", "FDB_TAG", "FDB_TAG_DT", "FDB_SCORING_DT", "CUSTCODE", "DNBFISME", "ACCTNON", "ACCTNOX", "PRINAMT", "INTAMT", "INTYTD", "FIXFLT", "CALBASP", "INTAMT_MYR", "PRINAMT_MYR", "TENOR_INT", "OTH_CHARGE", "IRIA", "REPTDAT1", "RDAY1", "ISSYY", "ISSMM", "ISSDTX", "FACILITY", "PREFIX", "CREATDS", "ISSDTE", "MATDATE", "EXPRDATE", "DIA_PAST01_MTH", "DIA_PAST02_MTH", "DIA_PAST03_MTH", "DIA_PAST04_MTH", "DIA_PAST05_MTH", "DIA_PAST06_MTH", "DIA_PAST07_MTH", "DIA_PAST08_MTH", "DIA_PAST09_MTH", "DIA_PAST10_MTH", "DIA_PAST11_MTH", "DIA_PAST12_MTH", "FORATE"]
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDLIBT.py", line 473, in main
    btdtl = btdtl.select(['TRANSREF', 'ISSDTE', 'EXPRDATE', 'PAYAMT'])
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10148, in select
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "PAYAMT"; valid columns: ["TRANSREX", "CREATTYP", "BRANCH", "APPLCODE", "ACCTNO", "SUBACCT", "CREATYYMMDD", "EXPIRDS", "SYNDICAT", "SPECIALF", "PURPOSES", "AANUMBER", "INTRATE", "SPREAD", "INFUNDRT", "DISCNTB", "DISCNTF", "TRANXMT", "EXCHRTE", "FORCURR", "LIABCODE", "BTREL1", "RELFROM", "CURRENCY", "APPRLIMT", "APPRLIM2", "OFFAPIND", "TFDESC01", "TFCNTR01", "TFCNTR03", "TFCNTR04", "TFINDR01", "TFINDR02", "TFINDR03", "TFINDR04", "TFINDR05", "ISSDTEYYMMDD", "SINDICAT", "SUBPROD", "FACLINE", "PRODGRP", "DIRCTIND", "TRANSREL", "COMMRATE", "DISCRATE", "CONRATE_IND", "INTBASE", "PLUSMINUS", "NUMDAYS", "BACOM", "DISCOUNT_PROCEED", "MTD_TAWIDH_AMT", "MTD_GHARAMAH_AMT", "REPAY_SOURCE", "REPAY_TYPE_CD", "PROP_DEVELOP_FIN_IND", "CLIMATE_PRIN_TAXONOMY_CLASS", "CLIMATE_MITIGATE_GP1_FLG", "CLIMATE_ADAPT_GP2_FLG", "CLIMATE_ENVIRONMT_GP3_FLG", "CLIMATE_TRANSITION_GP4_FLG", "CLIMATE_PROHIBIT_GP5_FLG", "SOURCE_INCOME_CURRENCY_CD", "AADATE", "REFERRAL_BRANCH", "APPL_COMMERCIAL_TAG", "COMBRATE", "AA_APPROVED_DT", "BTREL2", "BTREL3", "BTREL4", "NOLEVEL", "OUTSTAND", "CERTNO", "COLLECNO", "PAYBKNO", "ADDRLN3", "ADDRLN4", "CNTRY", "PREBKNO", "PRINAMT_MYRX", "INTAMT_MYRX", "OTH_CHARGEX", "TRANSREF", "MATUREDS", "RETAILID", "STATE", "SCORE1", "SCORE2", "BUSREGN", "SECTOR", "SM_STATUS", "IA_LRU", "SM_DATE", "ASCORE_LTST", "ASCORE_PERM", "APVDATE", "INDUSTRIAL_SECTOR_CD", "LEGAL_ACTION_CD", "LEGAL_ACTION_DT", "CCPT_LTST_REVIEW_DT", "FDB_TAG", "FDB_TAG_DT", "FDB_SCORING_DT", "CUSTCODE", "DNBFISME", "ACCTNON", "ACCTNOX", "PRINAMT", "INTAMT", "INTYTD", "FIXFLT", "CALBASP", "INTAMT_MYR", "PRINAMT_MYR", "TENOR_INT", "OTH_CHARGE", "IRIA", "REPTDAT1", "RDAY1", "ISSYY", "ISSMM", "ISSDTX", "FACILITY", "PREFIX", "CREATDS", "ISSDTE", "MATDATE", "EXPRDATE", "DIA_PAST01_MTH", "DIA_PAST02_MTH", "DIA_PAST03_MTH", "DIA_PAST04_MTH", "DIA_PAST05_MTH", "DIA_PAST06_MTH", "DIA_PAST07_MTH", "DIA_PAST08_MTH", "DIA_PAST09_MTH", "DIA_PAST10_MTH", "DIA_PAST11_MTH", "DIA_PAST12_MTH", "FORATE"]

--------------------------------------------------
PROCESSING TR TRANSACTIONS (Trade)
--------------------------------------------------

Reading BTDTL data for TR...
  WARNING: Exact file not found. Using latest: btdtl260803.sas7bdat (dated 2026-08-03)
  Reading: btdtl260803.sas7bdat
  TR records before processing: 771

Processing TR records...
  TR records created: 1510

--------------------------------------------------
FINAL OUTPUT
--------------------------------------------------

  Records with MISSING remmth (code '07'): 722
  Missing amount sum: 93,430,998.10

  Writing Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT/bt.parquet
  Writing CSV: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT/bt.csv

  Writing SAS7BDAT via saspy: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT/bt.sas7bdat

  Starting SAS session (cfgname='default')...
SAS Connection established. Subprocess id is 21498

  Assigning library XMISOUT -> /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT

21   
22   libname XMISOUT    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT'  ;
NOTE: Libref XMISOUT was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT
23   
  Uploading DataFrame to SAS dataset XMISOUT.bt (8 rows)...
  SAS dataset written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT/bt.sas7bdat
SAS Connection terminated. Subprocess id was 21498

======================================================================
PROCESSING COMPLETE
======================================================================

Output files:
  Parquet:  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT/bt.parquet
  CSV:      /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT/bt.csv
  SAS7BDAT: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIBDLIBT/bt.sas7bdat [OK]

Summary:
  Total BNM Codes: 8
  Total Amount:    283,812,668.06

Breakdown by BNMCODE:
--------------------------------------------------
  9321179010000Y:    5,646,848.12
  9321179020000Y:   95,824,306.47
  9321179030000Y:   37,559,832.89
  9321179040000Y:    2,875,346.55
  9521179010000Y:    5,646,848.12
  9521179020000Y:   95,824,306.47
  9521179030000Y:   37,559,832.89
  9521179040000Y:    2,875,346.55

it should be reading yesterday's report day (-1) before processing since the input is from yesterday's date. and fix other errors if occur
