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
IBTrad has 'transrex' column: True
IBTrad has 'transref' column: True
IBTrad has 'acctnox' column: True
IBTrad has 'acctno' column: True
Using columns: transref=transrex, acctno=acctnox

BTRAX records: 3418
BTRAX acctnox type: String
CRED acctnox type: String
BTRAX transrex type: String
CRED transrex type: String
BTRAX sample records:
shape: (3, 6)
┌──────────────┬────────────┬─────────┬──────────┬────────────────┬──────────────────┐
│ acctnox      ┆ transrex   ┆ repaid  ┆ disburse ┆ mtd_tawidh_amt ┆ mtd_gharamah_amt │
│ ---          ┆ ---        ┆ ---     ┆ ---      ┆ ---            ┆ ---              │
│ str          ┆ str        ┆ f64     ┆ f64      ┆ f64            ┆ f64              │
╞══════════════╪════════════╪═════════╪══════════╪════════════════╪══════════════════╡
│ 2850000632.0 ┆ B622013000 ┆ 86000.0 ┆ null     ┆ 0.0            ┆ 0.0              │
│ 2850000632.0 ┆ B626884000 ┆ null    ┆ null     ┆ 0.0            ┆ 0.0              │
│ 2850000632.0 ┆ B633311000 ┆ null    ┆ null     ┆ 0.0            ┆ 0.0              │
└──────────────┴────────────┴─────────┴──────────┴────────────────┴──────────────────┘
CRED sample records:
shape: (3, 2)
┌────────────┬──────────┐
│ acctnox    ┆ transrex │
│ ---        ┆ ---      │
│ str        ┆ str      │
╞════════════╪══════════╡
│ 2850155016 ┆ G410517  │
│ 2850500226 ┆ G396890  │
│ 2850256328 ┆ G391786  │
└────────────┴──────────┘

Matching records between CRED and BTRAX: 0

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
