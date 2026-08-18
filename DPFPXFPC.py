Reading input files...
  Read imast: 1697 rows
  Read imast2: 2246 rows
  Read icred: 10414 rows
  Read isuba: 61339 rows
  Read iprov: 87 rows
  Read iamsubacc: 0 rows
  Read ibtrad: 3418 rows
  Read ibtdtl: 3418 rows
  Read lnacct: 1205962 rows

Processing MAST...
MAST processed: 1697 rows

Processing MAST2...
MAST2 processed

Processing CRED...
CRED processed: 3419 rows

Processing BNM Trade data...
BNM Trade data processed

Processing SUBA...
SUBA processed: 61339 rows (SUBA9: 1184, SUBA_MAIN: 10411)

Processing ACCT...
ACCT processed: 1644 rows

Processing BTR2...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIWBTCR_ISLAMIC_WEEKLY_BANKTRADE_CCR.py", line 516, in <module>
    btr2 = btr2.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "Y"; valid columns: ["rectype", "transref", "syscode", "transtyp", "creattyp", "ficode", "applcode", "acctnox", "ccrisfac", "subacct", "posidate", "outstand", "maturedx", "prodcode", "prinamt_myrx", "intamt_myrx", "oth_chargex", "acctno", "matureds", "nodays", "arrears", "instalm", "transrex", "outstandx", "balance", "intrecv", "unearned", "liabcode", "utrdf", "repaid", "disburse", "mtd_tawidh_amt", "mtd_gharamah_amt", "intrate", "commrate", "discrate", "combrate", "prinamt_myrx_right", "intamt_myrx_right", "oth_chargex_right", "prodgrp", "rectype_right", "syscode_right", "transtyp_right", "creattyp_right", "ficode_right", "applcode_right", "ccrisfac_right", "subacct_right", "revolvg", "creatds", "expirds", "syndicat", "specialf", "purposes", "fconcept", "aanumber", "intrate_right", "spread", "infundrt", "discntb", "discntf", "tranxmt", "exchrte", "forcurr", "liabcode_right", "btrel", "relfrom", "currency", "limtcurm", "limtcurf", "offapind", "workerid", "reimbrid", "tfdesc01", "tfdesc02", "tfdesc03", "tfdesc04", "tfcntr01", "tfcntr02", "tfcntr03", "tfcntr04", "tfcntr05", "tfcntr06", "tfcntr07", "tfcntr08", "tfcntr09", "tfcntr10", "tfcntr11", "tfcntr12", "tfindr01", "tfindr02", "tfindr03", "tfindr04", "tfindr05", "tfindr06", "tfindr07", "tfindr08", "tfindr09", "tfindr10", "tfindr11", "tfindr12", "sindicat", "batype", "accptcom", "sublimit", "subprod", "facline", "prodgrp_right", "intrecv_right", "icurbal", "dcurbal", "dbalance", "dirctind", "transrel", "commrate_right", "discrate_right", "intbase", "plusminus", "numdays", "bacom", "ori_aalimit", "discount_proceed", "mtd_tawidh_amt_right", "mtd_gharamah_amt_right", "repay_source", "repay_type_cd", "prop_develop_fin_ind", "climate_prin_taxonomy_class", "climate_mitigate_gp1_flg", "climate_adapt_gp2_flg", "climate_environmt_gp3_flg", "climate_transition_gp4_flg", "climate_prohibit_gp5_flg", "source_income_currency_cd", "aadate", "referral_branch", "appl_commercial_tag", "combrate_right", "acctno_right", "expysdt", "aa_approved_dt", "aano", "facility", "faccode", "typeprc", "typeprc_sfs"]
