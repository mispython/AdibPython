SAS Connection established. Subprocess id is 3033462

EIMBNM01: Starting Public Bank Berhad loan summary reports...
  Report date: 2026-08-31  MM=08 YY=2026 WK=4
  input check: SASD_LOAN  exists=True  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNM01/loan08.sas7bdat
  input check: BNM_LOAN   exists=True  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNM01/loan084.sas7bdat
  input check: BNM_LNWOF  exists=True  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNM01/lnwof084.sas7bdat
  input check: BNM_LNWOD  exists=True  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNM01/lnwod084.sas7bdat
  input check: DISPAY     exists=True  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNM01/dispaymth08.sas7bdat
  input check: BTRAD      exists=True  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNM01/btrad08426.sas7bdat
  input check: LNCOMM     exists=True  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/enrh_ln_comm_m08.sas7bdat
  input check: LNFEE      exists=True  /stgsrcsys/host/uat/lnfee084.sas7bdat
  loan_base rows: 2109074
  dispay_df rows: 1820970
  bnm_loan rows: 2022453
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMBNM01.py", line 1316, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMBNM01.py", line 912, in main
    alm_df = build_alm(bnm_loan, rv)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMBNM01.py", line 465, in build_alm
    noteno   = int(row.get('noteno') or 0)
ValueError: cannot convert float NaN to integer
SAS Connection terminated. Subprocess id was 3033462
