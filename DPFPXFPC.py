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
Using columns: transref=transrex, acctno=acctnox

BTRAX sample after fix:
shape: (3, 6)
┌────────────┬──────────┬─────────┬──────────┬────────────────┬──────────────────┐
│ acctnox    ┆ transrex ┆ repaid  ┆ disburse ┆ mtd_tawidh_amt ┆ mtd_gharamah_amt │
│ ---        ┆ ---      ┆ ---     ┆ ---      ┆ ---            ┆ ---              │
│ str        ┆ str      ┆ f64     ┆ f64      ┆ f64            ┆ f64              │
╞════════════╪══════════╪═════════╪══════════╪════════════════╪══════════════════╡
│ 2850000632 ┆ B622013  ┆ 86000.0 ┆ null     ┆ 0.0            ┆ 0.0              │
│ 2850000632 ┆ B626884  ┆ null    ┆ null     ┆ 0.0            ┆ 0.0              │
│ 2850000632 ┆ B633311  ┆ null    ┆ null     ┆ 0.0            ┆ 0.0              │
└────────────┴──────────┴─────────┴──────────┴────────────────┴──────────────────┘
CRED sample:
shape: (3, 2)
┌────────────┬──────────┐
│ acctnox    ┆ transrex │
│ ---        ┆ ---      │
│ str        ┆ str      │
╞════════════╪══════════╡
│ 2850151812 ┆ G408623  │
│ 2850026916 ┆ B624514  │
│ 2850290002 ┆ B627689  │
└────────────┴──────────┘
Matching records between CRED and BTRAX: 3418

CRED REPAID after join: [{'total_repaid': 41300059.14000001, 'max_repaid': 2154000.0, 'count_repaid_gt_0': 233}]
Final CRED REPAID stats: [{'total_repaid': 41300059.14000001, 'max_repaid': 2154000.0, 'count_repaid_gt_0': 233}]
BNM Trade data processed

Processing SUBA...
SUBA processed: 61339 rows (SUBA9: 1184, SUBA_MAIN: 10411)

Processing ACCT...
ACCT processed: 1644 rows

Processing BTR2...

BTR2 REPAID stats: [{'total_repaid': 124958556.22, 'max_repaid': 2154000.0, 'count_repaid_gt_0': 705}]
BTR2 processed: 10429 rows

Processing SUBCR...
SUBCR processed: 895 rows

Creating final SUBA...
Final SUBA processed: 895 rows

Processing PROVISIONS...
PROVISIONS processed: 87 rows

Processing REPAID7B...

--- DEBUG: REPAID7B Processing ---
BTR2 columns available: ['repaid', 'repay_source', 'repay_type_cd']
BTR2 REPAID stats: [{'total_repaid': 124958556.22, 'max_repaid': 2154000.0, 'min_repaid': 0.0, 'count_repaid_gt_0': 705}]
REPAY_SOURCE values: ['', '1200']
REPAY_TYPE_CD values: ['', '10']
BTR2 records with repaid > 0: 705
Sample REPAID7B records:
shape: (5, 5)
┌────────────┬──────────┬──────────┬──────────────┬───────────────┐
│ acctnox    ┆ facility ┆ repaid   ┆ repay_source ┆ repay_type_cd │
│ ---        ┆ ---      ┆ ---      ┆ ---          ┆ ---           │
│ str        ┆ str      ┆ f64      ┆ str          ┆ str           │
╞════════════╪══════════╪══════════╪══════════════╪═══════════════╡
│ 2850000632 ┆ 34470    ┆ 86000.0  ┆ 1200         ┆ 10            │
│ 2850000632 ┆ 34470    ┆ 86000.0  ┆ 1200         ┆ 10            │
│ 2850000632 ┆ 34470    ┆ 86000.0  ┆ 1200         ┆ 10            │
│ 2850010916 ┆ 34470    ┆ 130000.0 ┆ 1200         ┆ 10            │
│ 2850010916 ┆ 34470    ┆ 130000.0 ┆ 1200         ┆ 10            │
└────────────┴──────────┴──────────┴──────────────┴───────────────┘
REPAID7B aggregated: 149 rows

Writing output files...
ACCTCRED written: 1644 records
SUBACRED written: 895 records
CREDITPO written: 895 records
PROVISIO written: 87 records
REPAID7B written: 149 records

==================================================
PROCESSING COMPLETE
==================================================
Processing Date: 2026-08-15
MAST rows: 1697
CRED rows: 3425
SUBA rows: 61339
ACCT rows: 1644
BTR2 rows: 10429
SUBCR rows: 895
Final SUBA rows: 895
PROVI rows: 87
BTRPAY rows: 149

Output files written to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIWBTCR
==================================================
