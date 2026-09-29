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
(virt_edw_dev) [sas_edw_dev@svdwh004 MIS]$ /sas/python/virt_edw_dev/bin/python /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/test123.py

==============================================================================
1. Reading BNM_LOAN (loan084.sas7bdat)
==============================================================================
Read 2,022,453 rows x 417 cols in 166.5s

Columns actually present (lowercased):
['acctno', 'noteno', 'payefdt', 'dobcis', 'u2raceco', 'eir_adj', 'exprdate', 'statecd', 'riskrte', 'custcode', 'product', 'branch', 'appvalue', 'noteterm', 'flag3', 'costctr', 'billtype', 'intrate', 'spread', 'intearn', 'accrual', 'secure', 'loanstat', 'borstat', 'marketvl', 'payamt', 'payfreq', 'bilpay', 'paytype', 'acctyind', 'restbalc', 'payind', 'feeamt', 'nxtbil', 'bldate', 'issdte', 'fisspurp', 'apprlimt', 'undrawn', 'apprlmtacct', 'bal_aft_eir', 'ccy', 'forate', 'fcybal', 'dnbfisme', 'dnbfi_ori', 'vb', 'siacctno', 'abm_hl', 'ia_lru', 'ascore_perm', 'ascore_ltst', 'unearned1', 'unearned2', 'unearned', 'fdcertno', 'fdacctno', 'ccris_instlamt', 'industrial_sector_cd', 'ascore_comm', 'sch_repay_term', 'wrioff_close_file_tag', 'wrioff_close_file_tag_dt', 'remmfiss', 'remainmt', 'curbal', 'balance', 'apprlim2', 'custfiss', 'sectfiss', 'dayarr', 'mtdavbal_mis', 'taxno', 'orgtype', 'guarend', 'purpose', 'assmdate', 'lsttrncd', 'intamt', 'colldesc', 'mailing_add_ind', 'dlivrydt', 'commno', 'corpcode', 'sector', 'paidind', 'orgbal', 'netproc', 'feedue', 'ntint', 'rebate', 'liabcode', 'refinanc', 'dealerno', 'maturedt', 'collmake', 'earnterm', 'intearn2', 'intearn3', 'intearn4', 'nxduedt', 'rebind', 'apprdate', 'postntrn', 'lsttrnam', 'ntapr', 'origrate', 'vinno', 'restind', 'totpdeop', 'postcode', 'intpdytd', 'accruytd', 'accrueop', 'ntindex', 'mtdint', 'score1', 'num_pay_bil_instl', 'accostct', 'intinytd', 'prevbrno', 'score2', 'crispurp', 'paidoff', 'feeytd', 'feeamt14', 'feeamt15', 'feeamt16', 'feeearn1', 'feeearn2', 'collyear', 'delqcd', 'appormt', 'costfund', 'pofficer', 'nplcrr', 'form2', 'form1', 'modeldes', 'ytdearns', 'intrate2', 'ratelmt2', 'mailcode', 'sitype', 'varstdte', 'usmargin', 'usedit', 'stopdebit', 'sm_status', 'cashprice', 'reaccrual', 'latenotice', 'guarnotice', 'intbasis', 'memoacc', 'excesspay', 'billeadday', 'deathdate', 'intstdte', 'cfindex', 'exratio', 'nplcrrbpa', 'cpnstdte', 'insolvency_ind', 'numcpns', 'rsn', 'ptmnate', 'early_settle_fee_charge_flg', 'mniaplmt', 'commno_old', 'times_renewed', 'staff_free_int_loan_amt', 'marked_payment_ind', 'num_pay_bil_int', 'nurs_tag', 'nur_startdt', 'nurs_tagdt', 'nurs_enddt', 'nurs_counter', 'wrioff_dt', 'wrioff_amt', 'cum_wrioff', 'recover_cost', 'disposed_amt', 'dsr', 'repay_source', 'repay_type_cd', 'mtd_repaid_amt', 'prompt_pay_tracker', 'refinanc_ln', 'old_fi', 'old_macc_no', 'old_subacc_no', 'repo_order_issue_dt', 'num_repo_order_issue', 'court_order_apply_dt', 'court_order_obtain_dt', 'auto_reprice_diff_instl_amt', 'risk_grade_class', 'repay_proposal_cd', 'ltst_mgb_score', 'pct_index_intrate', 'remain_term_maturity', 'billing_add_ind', 'prop_add_ind', 'floor_rt_under', 'floor_rt_over', 'stmt_gen_ind', 'akpk_status', 'digital_rr_status_cd', 'mora_benchmark_amt', 'tfa_nurs_tag', 'tfa_nurs_tag_dt', 'tfa_nurs_start_dt', 'tfa_nurs_end_dt', 'tfa_nurs_counter', 'tfa_dig_status_cd', 'tfa_dig_status_dt', 'repay_proposal_dt', 'manual_rr_tag', 'manual_rr_dt', 'auto_ext_tag', 'auto_ext_tag_dt', 'auto_reprice_instl_amt', 'bullet_repay_ind', 'balloon_repay_ind', 'prop_develop_fin_ind', 'dia_past01_mth', 'dia_past02_mth', 'dia_past03_mth', 'dia_past04_mth', 'dia_past05_mth', 'dia_past06_mth', 'dia_past07_mth', 'dia_past08_mth', 'dia_past09_mth', 'dia_past10_mth', 'dia_past11_mth', 'dia_past12_mth', 'dia_past13_mth', 'dia_past14_mth', 'dia_past15_mth', 'dia_past16_mth', 'dia_past17_mth', 'dia_past18_mth', 'dia_past19_mth', 'dia_past20_mth', 'dia_past21_mth', 'dia_past22_mth', 'dia_past23_mth', 'dia_past24_mth', 'orig_restind', 'restind_end_dt', 'com_fee_notice_ind', 'num_mora', 'akpk_ra_tag', 'akpk_ra_tag_dt', 'akpk_ra_dig_status_cd', 'akpk_ra_dig_status_dt', 'akpk_ra_start_dt', 'akpk_ra_end_dt', 'akpk_ra_orig_spread', 'akpk_ra_dly_int_accrual', 'akpk_ra_mtd_int_accrual', 'akpk_ra_mth_int_waiver_amt', 'akpk_ra_cumm_int_waiver_amt', 'akpk_ra_mth_int_cap_amt', 'akpk_ra_cumm_int_cap_amt', 'akpk_ra_orig_ceiling_rt', 'tra_rr_ind', 'index_pricing', 'num_rr', 'dayarr_mora', 'repay_mode', 'rr_erequest_num', 'rr_type', 'rr_appr_date', 'hi_tag', 'hi_tag_dt', 'hi_dig_status_cd', 'hi_dig_status_dt', 'repo_order_expiry_dt', 'flood_mo_tag', 'flood_mo_dt', 'impaired_hp_tag', 'legal_notice_instruct_dt', 'legal_notice_issue_dt', 'paras_tag', 'paras_tag_dt', 'judge_amt', 'judge_dt', 'judge_maint_dt', 'pre_bkrupt_notice_dt', 'court_order_perpetual_ind', 'hp_stage_trsf_ind', 'cumm_paid_bill_amt', 'cumm_paid_bill_pct', 'akpk_matrix_type', 'akpk_matrix_date', 'deviation_cd', 'int_advice_ind', 'flood_mo_package_cd', 'fdb_tag', 'fdb_tag_dt', 'fdb_scoring_dt', 'marked_payment_amt', 'e_invoice_ind', 'legal_maturity_dt', 'climate_prin_taxonomy_class', 'rr_il_reclass_dt', 'climate_mitigate_gp1_flg', 'climate_adapt_gp2_flg', 'climate_environmt_gp3_flg', 'climate_transition_gp4_flg', 'climate_prohibit_gp5_flg', 'wos_received_dt', 'wos_settled_dt', 'wos_tag', 'court_order_update_dt', 'fraud_tag', 'fraud_tag_dt', 'source_income_currency_cd', 'vehi_make_category', 'int_jan_to_jun_amt', 'int_jul_to_dec_amt', 'rr_untag_repay_cnt', 'rr_untag_date', 'aging_fast_tracker', 'earmark_notice_dt', 'earmark_amt', 'multi_currency_tag', 'goodwill_ind', 'goodwill_curr_rate', 'goodwill_curr_amt', 'rrstg1', 'tra_eff_dt', 'digital_rr_status_dt', 'lmostdate', 'lmoenddate', 'tra_rr_accept_dt', 'rr_appl_date', 'nacospadt', 'lock_in_end_dt', 'schbil_instl_dt', 'schbil_int_dt', 'lastbil_instl_dt', 'lastbil_int_dt', 'fdb', 'cp', 'sm_date', 'staff_free_int_ind', 'omnibus_facility_ind', 'synratio', 'cjfee', 'mo_instl_arr', 'mniapdte', 'uslimit', 'oldnotebldate', 'oldnotedayarr', 'mo_tag', 'mostdte', 'moenddte', 'mo_main_dt', 'userid', 'postdate', 'timelate', 'nonaccrual', 'sendbill', 'lnuser2', 'usindex', 'refnoteno', 'score1mi', 'score2ct', 'fullrel_dt', 'rrcountdte', 'valuation_dt', 'commtype', 'escrowrbal', 'ln_utilise_locat_cd', 'interdue', 'nplind', 'totbnp', 'cagatag', 'u1classi', 'u3mycode', 'u4reside', 'f1relmod', 'f5acconv', 'dobmni', 'freleas', 'tot_migr', 'lasttran', 'payefdto', 'newbal', 'census0', 'census1', 'census3', 'census4', 'census5', 'make', 'model', 'regno', 'exregno', 'collage', 'mtharr', 'mtharr_ccris', 'dateregv', 'vehi_chassis_num', 'vehi_engine_num', 'pointamt', 'ceilingo', 'ceilingu', 'user5', 'primofhp', 'cano', 'curavmth', 'cubalytd']

==============================================================================
2. Dtypes and sample values for filter-chain columns
==============================================================================
  acctno        dtype=float64     nulls=         0  sample=[2000001605.0, 2000003304.0, 2000004627.0, 2000005719.0, 2000006617.0]
  noteno        dtype=float64     nulls=    25,152  sample=[10014.0, 16.0, 12.0, 30010.0, 20010.0]
  prodcd        -- NOT PRESENT --
  paidind       dtype=object      nulls=         0  sample=['M', 'M', 'M', 'M', 'M']
  acctype       -- NOT PRESENT --
  balance       dtype=float64     nulls=        92  sample=[352702.17243509996, 193489.87841489998, 43874.959071, 27209.241445199998, 523.1661462999999]
  bal_aft_eir   dtype=float64     nulls=         0  sample=[352702.17243509996, 193489.87841489998, 43874.959071, 27209.241445199998, 523.1661462999999]
  oribal        -- NOT PRESENT --
  eir_adj       dtype=float64     nulls=   942,279  sample=[-780.27, -1.63, -362.71, -10.04, -1017.88]
  cjfee         dtype=float64     nulls= 1,517,088  sample=[0.0, 0.0, 0.0, 9753.439999999999, 0.0]
  cusedamt      -- NOT PRESENT --
  rleasamt      -- NOT PRESENT --
  product       dtype=float64     nulls=         0  sample=[5.0, 5.0, 5.0, 247.0, 212.0]
  commno        dtype=float64     nulls=    25,152  sample=[101.0, 101.0, 0.0, 100.0, 0.0]
  noacct        -- NOT PRESENT --
  dnbfisme      dtype=object      nulls=         0  sample=['0', '0', '0', '0', '0']
  retailid      -- NOT PRESENT --

==============================================================================
3. PRODCD breakdown
==============================================================================

==============================================================================
4. PAIDIND breakdown
==============================================================================
paidind
M    1983792
       25152
P      13489
C         20

==============================================================================
5. ACCTYPE breakdown
==============================================================================

==============================================================================
6. EIR_ADJ detail
==============================================================================
dtype: float64
nulls: 942,279 / 2,022,453  (46.6%)
nonnull sample: [-780.27, -1.63, -362.71, -10.04, -1017.88, -906.04, -189.61, -37.12, -65.92, -274.03]
describe: 
count    1.080174e+06
mean    -2.546150e+02
std      7.765076e+02
min     -3.136157e+05
25%     -3.582600e+02
50%     -1.937700e+02
75%     -7.628000e+01
max      4.100310e+05
Name: eir_adj, dtype: float64

==============================================================================
7. BALANCE (bal_aft_eir) detail
==============================================================================
bal_aft_eir:
  dtype: float64
  nulls: 0
  describe:
  count    2.022453e+06
mean     1.771997e+05
std      1.113953e+07
min     -2.989260e+03
25%      2.592783e+04
50%      5.873534e+04
75%      1.524822e+05
max      1.539612e+10
  round(2) == 0 or -0 : 37,410

balance:
  dtype: float64
  nulls: 92
  describe:
  count    2.022361e+06
mean     1.773438e+05
std      1.113978e+07
min     -4.482400e+02
25%      2.611256e+04
50%      5.897131e+04
75%      1.526994e+05
max      1.539612e+10
  round(2) == 0 or -0 : 37,402


==============================================================================
8. Emulating build_alm() filter chain
==============================================================================
Starting rows: 2,022,453

mask  drop_pc          : drops     13,413  keeps  2,009,040
mask  drop_zero        : drops     18,793  keeps  2,003,660
mask  keep_prodcd      : keeps          0  drops  2,022,453

combined base_mask     : keeps          0  of 2,022,453

  cond_ln1 (rleasamt!=0 ...)      :          0
  cond_ln2 (rleasamt==0 ... 600:699):        604
  cond_ln3 (commno>0 & cusedamt>0):          0
  ln_acctype                      :          0
  ln_eligible (any of 1,2,3)      :        604

After base_mask: 0 rows total, 0 are acctype='LN', 0 of those are NOT eligible.

==============================================================================
9. Sample rows surviving base_mask
==============================================================================
NO ROWS SURVIVE. Sample of first 5 rows for inspection:
         acctno   noteno prodcd paidind acctype         oribal  product
0  2.000002e+09  10014.0              M          352702.172435        5
1  2.000003e+09     16.0              M          193489.878415        5
2  2.000005e+09     12.0              M           43874.959071        5
3  2.000006e+09  30010.0              M           27209.241445      247
4  2.000007e+09  20010.0              M             523.166146      212

==============================================================================
10. LNFEE file profile
==============================================================================
