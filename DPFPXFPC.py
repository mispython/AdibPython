Step 1: Setting report date...
Report Date: 2026-09-08 09:56:31.715694, RDATE: 24357
Using COLL_FILE: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_20260831
Using DESC_FILE: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_20260831
Step 2: Processing credit facility table...
CRFTABL data shape: (0, 6)
CRFTABL columns: ['RECTYP1', 'TFID', 'SUBACCT', 'ACCTNO', 'PREIND', 'CENSUST']
First few ACCTNO: []
Step 3: Merging with master account data...
MAST data shape: (61295, 4)
After MAST merge: (0, 10)
CRFT_FINAL shape: (0, 8)
Step 4: Processing credit data...
CRED data shape: (131908, 19)
After CRFT merge: (0, 25)
After filter: (0, 25)
Step 5: Summarizing credit outstanding...
CRED1 data shape: (0, 2)
Step 6: Processing provision data...
CRED2 final shape: (0, 4)
Step 7: Processing subaccount data...
SUBA final shape: (0, 3)
Step 8: Processing collateral data...
First 200 chars of LCCRISEX_DESC_20260831:
'00000000133008 \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x002976                                                                            DNP                                                                   '
Number of lines: 21578194
COLL data shape: (21578194, 2)
COLL data first rows: shape: (5, 2)
┌──────────────┬────────┐
│ CCOLLNO      ┆ ACCTNO │
│ ---          ┆ ---    │
│ str          ┆ i64    │
╞══════════════╪════════╡
│ 000000001330 ┆ null   │
│              ┆ null   │
│              ┆ null   │
│              ┆ null   │
│              ┆ null   │
└──────────────┴────────┘
First 200 chars of LCCRISEX_DESC_20260831:
'00000000133008 \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x002976                                                                            DNP                                                                   '
Number of lines: 21578194
DESC data shape: (21578194, 4)
DESC data first rows: shape: (5, 4)
┌──────────────┬─────────┬─────────┬───────────┐
│ CCOLLNO      ┆ CINSTCL ┆ NATGUAR ┆ CENSUS    │
│ ---          ┆ ---     ┆ ---     ┆ ---       │
│ str          ┆ str     ┆ str     ┆ str       │
╞══════════════╪═════════╪═════════╪═══════════╡
│ 000000001330 ┆ 08      ┆        ┆  │
│              ┆         ┆         ┆           │
│              ┆         ┆         ┆           │
│              ┆         ┆         ┆           │
│              ┆         ┆         ┆           │
└──────────────┴─────────┴─────────┴───────────┘
Killed
