SAS Connection established. Subprocess id is 3092900

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
  [+    7.1s]   SASD_LOAN:  2,034,697 rows
  [+    7.1s]   reading BNM_LOAN ...
  [+   57.8s]   BNM_LOAN:  2,022,747 rows
  [+   57.8s]   reading BNM_LNWOF ...
  [+   60.4s]   BNM_LNWOF:     74,269 rows
  [+   60.4s]   reading BNM_LNWOD ...
  [+   60.4s]   BNM_LNWOD:          0 rows
  [+   60.4s]   reading BNM_LNWOF (prev) ...
  [+   62.9s]   BNM_LNWOF(prev):     74,377 rows
  [+   62.9s]   reading BNM_LNWOD (prev) ...
  [+   62.9s]   BNM_LNWOD(prev):          0 rows
  [+   62.9s]   reading BNM_LOAN (prev) ...
  [+  116.4s]   BNM_LOAN(prev):  2,018,558 rows
  [+  118.0s]   merging 5 frames (SAS MERGE emulation) ...
  [+  118.4s]   merge step 2/5:     86,327 rows
  [+  135.8s]   merge step 3/5:  2,097,327 rows
  [+  157.0s]   merge step 4/5:  2,124,042 rows
  [+  172.2s]   merge step 5/5:  2,124,042 rows
  [+  173.1s] loan_base rows: 2,124,042
  [+  173.1s] STAGE 2: build_dispay
  [+  173.1s]   reading DISPAY ...
  [+  182.8s]   DISPAY raw:  1,821,574 rows
  [+  183.3s]   DISPAY filtered:  1,821,388 rows
  [+  186.3s]   DISPAY merged:  1,821,388 rows
  [+  186.4s] dispay_df rows: 1,821,388
  [+  186.4s] STAGE 3: read BNM_LOAN + build_cl_fee + merge
  [+  241.9s] bnm_loan raw rows: 2,022,747
  [+  241.9s]   reading LNFEE ...
