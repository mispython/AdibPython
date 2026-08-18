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

--- DEBUG: IBTrad Data ---
IBTrad columns: ['acctno', 'apprlimt', 'transrex', 'creattyp', 'branch', 'applcode', 'subacct', 'creatyymmdd', 'expirds', 'syndicat', 'specialf', 'purposes', 'aanumber', 'intrate', 'spread', 'infundrt', 'discntb', 'discntf', 'tranxmt', 'exchrte', 'forcurr', 'liabcode', 'btrel1', 'relfrom', 'currency', 'apprlim2', 'offapind', 'tfdesc01', 'tfcntr01', 'tfcntr03', 'tfcntr04', 'tfindr01', 'tfindr02', 'tfindr03', 'tfindr04', 'tfindr05', 'issdteyymmdd', 'sindicat', 'sublimit', 'subprod', 'facline', 'prodgrp', 'dirctind', 'transrel', 'commrate', 'discrate', 'conrate_ind', 'intbase', 'plusminus', 'numdays', 'bacom', 'discount_proceed', 'mtd_tawidh_amt', 'mtd_gharamah_amt', 'repay_source', 'repay_type_cd', 'prop_develop_fin_ind', 'climate_prin_taxonomy_class', 'climate_mitigate_gp1_flg', 'climate_adapt_gp2_flg', 'climate_environmt_gp3_flg', 'climate_transition_gp4_flg', 'climate_prohibit_gp5_flg', 'source_income_currency_cd', 'aadate', 'referral_branch', 'appl_commercial_tag', 'combrate', 'aa_approved_dt', 'btrel2', 'btrel3', 'btrel4', 'nolevel', 'outstand', 'certno', 'collecno', 'paybkno', 'addrln3', 'addrln4', 'cntry', 'prebkno', 'prinamt_myrx', 'intamt_myrx', 'oth_chargex', 'transref', 'matureds', 'retailid', 'state', 'score1', 'score2', 'busregn', 'sector', 'sm_status', 'ia_lru', 'sm_date', 'ascore_ltst', 'ascore_perm', 'apvdate', 'industrial_sector_cd', 'legal_action_cd', 'legal_action_dt', 'ccpt_ltst_review_dt', 'fdb_tag', 'fdb_tag_dt', 'fdb_scoring_dt', 'custcode', 'dnbfisme', 'acctnon', 'acctnox', 'prinamt', 'intamt', 'intytd', 'fixflt', 'calbasp', 'intamt_myr', 'prinamt_myr', 'tenor_int', 'oth_charge', 'reptdat1', 'rday1', 'issyy', 'issmm', 'issdtx', 'iria', 'facility', 'prefix', 'creatds', 'issdte', 'matdate', 'exprdate', 'dia_past01_mth', 'dia_past02_mth', 'dia_past03_mth', 'dia_past04_mth', 'dia_past05_mth', 'dia_past06_mth', 'dia_past07_mth', 'dia_past08_mth', 'dia_past09_mth', 'dia_past10_mth', 'dia_past11_mth', 'dia_past12_mth', 'balance', 'fcvalue', 'yield', 'utrdf', 'unearned', 'utcat', 'fo_deal_id', 'transref_pba', 'intrecv', 'custcd', 'origmt', 'remainmt', 'sectorcd', 'statecd', 'amtind', 'curbal', 'fisspurp', 'collcd', 'sectorz', 'sectorzz', 'prodcd', 'disburse', 'repaid', 'repaidprin', 'disbursefx', 'repaidprinfx', 'mtdavbal_mis']
IBTrad REPAID stats: [{'total_repaid': 41300059.14, 'max_repaid': 2154000.0, 'min_repaid': 20000.0, 'count_repaid_gt_0': 233, 'count_null': 3185}]
Sample IBTrad records with repaid > 0:
shape: (5, 3)
┌──────────┬──────────┬──────────┐
│ acctnox  ┆ transref ┆ repaid   │
│ ---      ┆ ---      ┆ ---      │
│ f64      ┆ str      ┆ f64      │
╞══════════╪══════════╪══════════╡
│ 2.8500e9 ┆ B622013  ┆ 86000.0  │
│ 2.8500e9 ┆ B625035  ┆ 130000.0 │
│ 2.8500e9 ┆ B614946  ┆ 199000.0 │
│ 2.8500e9 ┆ B623403  ┆ 274000.0 │
│ 2.8500e9 ┆ B633729  ┆ 334000.0 │
└──────────┴──────────┴──────────┘
Columns related to repayment: ['repay_source', 'repay_type_cd', 'paybkno', 'reptdat1', 'repaid', 'repaidprin', 'repaidprinfx']

BTRAX REPAID stats: [{'total_repaid': 41300059.14, 'max_repaid': 2154000.0, 'count_repaid_gt_0': 233}]
BTRAX unique records: 3417
BTRAX total records: 3418

CRED columns before join: []
BTRAX columns: ['acctnox', 'transrex', 'repaid', 'disburse', 'mtd_tawidh_amt', 'mtd_gharamah_amt', 'intrate', 'commrate', 'discrate', 'combrate', 'prinamt_myrx', 'intamt_myrx', 'oth_chargex', 'prodgrp']

CRED REPAID after join: [{'total_repaid': 0.0, 'max_repaid': None, 'count_repaid_gt_0': 0}]

Final CRED REPAID stats: [{'total_repaid': 0.0, 'max_repaid': 0.0, 'count_repaid_gt_0': 0}]
BNM Trade data processed

Processing SUBA...
SUBA processed: 61339 rows (SUBA9: 1184, SUBA_MAIN: 10411)

Processing ACCT...
ACCT processed: 1644 rows

Processing BTR2...

BTR2 REPAID stats: [{'total_repaid': 0.0, 'max_repaid': 0.0, 'count_repaid_gt_0': 0}]
BTR2 processed: 10411 rows

Processing SUBCR...
SUBCR processed: 895 rows

Creating final SUBA...
Final SUBA processed: 895 rows

Processing PROVISIONS...
PROVISIONS processed: 87 rows

Processing REPAID7B...

--- DEBUG: REPAID7B Processing ---
BTR2 columns available: ['repaid', 'repay_source', 'repay_type_cd']
BTR2 REPAID stats: [{'total_repaid': 0.0, 'max_repaid': 0.0, 'min_repaid': 0.0, 'count_repaid_gt_0': 0}]
REPAY_SOURCE values: ['1200', '']
REPAY_TYPE_CD values: ['', '10']
BTR2 records with repaid > 0: 0
No records with repaid > 0 found in BTR2

Writing output files...
ACCTCRED written: 1644 records
SUBACRED written: 895 records
CREDITPO written: 895 records
PROVISIO written: 87 records
REPAID7B: No data to write (0 records with repaid > 0)
REPAID7B: Empty file created

==================================================
PROCESSING COMPLETE
==================================================
Processing Date: 2026-08-15
MAST rows: 1697
CRED rows: 3419
SUBA rows: 61339
ACCT rows: 1644
BTR2 rows: 10411
SUBCR rows: 895
Final SUBA rows: 895
PROVI rows: 87
BTRPAY rows: 0

Output files written to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIWBTCR
==================================================
