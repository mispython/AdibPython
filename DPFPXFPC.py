SAS Connection established. Subprocess id is 3066293

  [+    0.0s] EIMBNM01: Starting Public Bank Berhad loan summary reports...
  [+    0.0s] Report date: 2026-08-31  MM=08 YY=2026 WK=4  MM2=07
  [+    0.0s] input check: SASD_LOAN       exists=True   /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNM01/loan08.sas7bdat
  [+    0.0s] input check: BNM_LOAN        exists=True   /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNM01/loan084.sas7bdat
  [+    0.0s] input check: BNM_LNWOF       exists=True   /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNM01/lnwof084.sas7bdat
  [+    0.0s] input check: BNM_LNWOD       exists=True   /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNM01/lnwod084.sas7bdat
  [+    0.0s] input check: BNM_LNWOF_PREV  exists=True   /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNM01/lnwof074.sas7bdat
  [+    0.0s] input check: BNM_LNWOD_PREV  exists=True   /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNM01/lnwod074.sas7bdat
  [+    0.0s] input check: BNM_LOAN_PREV   exists=True   /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNM01/loan074.sas7bdat
  [+    0.0s] input check: DISPAY          exists=True   /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNM01/dispaymth08.sas7bdat
  [+    0.0s] input check: BTRAD           exists=True   /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNM01/btrad08426.sas7bdat
  [+    0.0s] input check: LNCOMM          exists=True   /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/enrh_ln_comm_m08.sas7bdat
  [+    0.0s] input check: LNFEE           exists=True   /stgsrcsys/host/uat/maa/lnfee084.sas7bdat
  [+    0.0s] input check: MFRS_DIR        exists=True   /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNM01/mfrs
  [+    0.0s] input check: REPORT_DIR      exists=True   /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMBNM01
  [+    0.0s] STAGE 1: build_loan_dataset
  [+    0.0s]   reading SASD_LOAN ...
  [+    7.0s]   SASD_LOAN:  2,034,697 rows
  [+    7.0s]   reading BNM_LOAN ...
  [+  186.6s]   BNM_LOAN:  2,022,453 rows
  [+  186.6s]   reading BNM_LNWOF ...
  [+  189.8s]   BNM_LNWOF:     74,269 rows
  [+  189.8s]   reading BNM_LNWOD ...
  [+  189.8s]   BNM_LNWOD:          0 rows
  [+  189.8s]   reading BNM_LNWOF (prev) ...
  [+  192.5s]   BNM_LNWOF(prev):     74,377 rows
  [+  192.5s]   reading BNM_LNWOD (prev) ...
  [+  192.5s]   BNM_LNWOD(prev):          0 rows
  [+  192.5s]   reading BNM_LOAN (prev) ...
  [+  348.2s]   BNM_LOAN(prev):  2,018,264 rows
  [+  353.3s]   merging 5 frames (SAS MERGE emulation) ...
  [+  353.9s]   merge step 2/5:     86,621 rows
  [+  362.1s]   merge step 3/5:  2,097,327 rows
  [+  373.2s]   merge step 4/5:  2,124,042 rows
  [+  384.7s]   merge step 5/5:  2,124,042 rows
  [+  385.2s] loan_base rows: 2,124,042
  [+  385.2s] STAGE 2: build_dispay
  [+  385.2s]   reading DISPAY ...
  [+  394.9s]   DISPAY raw:  1,821,574 rows
  [+  395.5s]   DISPAY filtered:  1,821,388 rows
  [+  397.8s]   DISPAY merged:  1,821,388 rows
  [+  397.9s] dispay_df rows: 1,821,388
  [+  397.9s] STAGE 3: read BNM_LOAN + build_cl_fee + merge
  [+  563.8s] bnm_loan raw rows: 2,022,453
  [+  563.8s]   reading LNFEE ...
  [+ 1368.3s]   LNFEE raw: 40,019,853 rows
  [+ 1375.9s]   LNFEE CL aggregated:          9 rows
  [+ 1411.4s]   CL_FEE merged:  2,022,453 rows, clfee sum = 497,836.90
  [+ 1411.5s] bnm_loan rows: 2,022,453
  [+ 1411.5s] STAGE 4: build_alm
  [+ 1411.5s]   reading LNCOMM ...
  [+ 1417.0s]   LNCOMM raw:  1,066,036 rows
  [+ 1420.1s]   ALM pre-filter:  2,022,453 rows
  [+ 1420.7s]   ALM post-base mask:          0 rows
  [+ 1421.0s] alm_df rows: 0
  [+ 1421.0s] STAGE 5: merge DISPAY into ALM
  [+ 1421.0s] alm_df rows after DISPAY merge: 0
  [+ 1421.0s] STAGE 6: apply_prodesc
  [+ 1421.0s] alm_df rows after prodesc: 0
  [+ 1421.0s] STAGE 7: build_pbif (RDL2PBIF)
  [+ 1421.1s] pbif_df rows: 0
  [+ 1421.1s] STAGE 8: ALL LOANS summary
  [+ 1421.1s] almnew_df rows: 0
  [+ 1421.1s] STAGE 9: ALM2 / COM3 / ALM2NEW
  [+ 1421.1s] alm2new_src rows: 0
  [+ 1421.1s] STAGE 10: SME subsets
  [+ 1421.1s] STAGE 11: build_btrade
  [+ 1421.1s]   reading BTRAD ...
  [+ 1425.7s]   BTRAD raw:     46,739 rows
  [+ 1425.8s]   BTRAD after DIRCTIND filter:     23,851
  [+ 1425.9s]   BTRAD (prodcd 34*):     23,851
  [+ 1426.1s]   writing MFRS.MAST_BR ...
  [+ 1426.4s]   MFRS.MAST_BR written:      3,796 rows
  [+ 1426.4s] alm_bt_df rows: 23,851  mast_bt_df rows: 3,796
  [+ 1426.5s] STAGE 12: sector breakdowns
  [+ 1426.6s] STAGE 13: Total Commercial Retail by product
  [+ 1426.6s] STAGE 14: flush report
  [+ 1426.6s] Written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMBNM01/eimbnm01_report.txt
  [+ 1426.6s] EIMBNM01: Processing complete.
SAS Connection terminated. Subprocess id was 3066293
