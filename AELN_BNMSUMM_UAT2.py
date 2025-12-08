============================================================
EIBMRNID - EXACT SAS OUTPUT
============================================================
Report Date: 30/11/2025

📂 Processing data...
  Merged with TRNCH
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/EIBMRNID_NID.py", line 280, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/EIBMRNID_NID.py", line 228, in main
    tbl1 = tbl1_filtered.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10020, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 330, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2335, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "heldmkt"; valid columns: ["nid_acctno", "nid_refno", "acctno", "nid_cdno", "custname", "newic", "branch", "custcode", "trancheno", "startdt", "matdt", "intrate", "intpltdrate", "curbal", "lstintpaydt", "nxtintpaydt", "intfrq", "nidstat", "accint", "cdstat", "poststat", "early_wddt", "early_wdproc", "early_telrid", "early_offid1", "early_offid2", "cancdt", "canc_telrid", "canc_offid1", "canc_offid2", "appldt", "appldttm", "appl_telrid", "appl_offid1", "appl_offid2", "printdt", "print_telrid", "print_offid1", "print_offid2", "maintdt", "maint_telrid", "maint_offid1", "maint_offid2", "pledgestat", "nid_odacctno", "ccollno", "pledgedt", "pledge_telrid", "pledge_offid1", "pledge_offid2", "cpledgedt", "cpledge_telrid", "cpledge_offid1", "cpledge_offid2", "term", "suitability_assessment_cd", "sales_staff_id", "termid", "intpd", "costctr", "product", "brabbr", "curcode", "custcd", "trn_stat", "trn_offerstartdt", "trn_offerenddt", "trnstartdt", "trnmatdt", "term_right", "termid_right", "intfrq_right", "intrate_right", "creatdt", "creatstaffid", "maintdt_right", "maintstaffid", "intplrate_bid", "intplrate_offer", "remmth"]
