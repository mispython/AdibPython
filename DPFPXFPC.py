============================================================
EIBAABBA - Account Analysis Report
============================================================
Input path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod
Output path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBAABBA/ABBALST.txt
*** TEST MODE - Row limit: 1000 per dataset ***

Report Date: 12/07/2026
Snapshot Date: 12/07/2026
Week: 4, SDD: 23
------------------------------------------------------------
Reading LNNOTE data...
  [diag] Total rows read: 1,000
  [diag] Rows passing PAIDIND != 'P': 91
  [diag] Rows passing LOANTYPE in [110-119, 139-140]: 0
  [diag] Rows passing RISKRATE in [2,3,4]: 132
  [diag] LOANTYPE value counts (top 10):
shape: (10, 2)
┌──────────┬───────┐
│ LOANTYPE ┆ count │
│ ---      ┆ ---   │
│ i64      ┆ u32   │
╞══════════╪═══════╡
│ 212      ┆ 186   │
│ 600      ┆ 70    │
│ 359      ┆ 60    │
│ 15       ┆ 58    │
│ 70       ┆ 52    │
│ 228      ┆ 49    │
│ 247      ┆ 48    │
│ 234      ┆ 46    │
│ 5        ┆ 40    │
│ 227      ┆ 37    │
└──────────┴───────┘
  [diag] RISKRATE value counts (top 10):
shape: (5, 2)
┌──────────┬───────┐
│ RISKRATE ┆ count │
│ ---      ┆ ---   │
│ i64      ┆ u32   │
╞══════════╪═══════╡
│ 0        ┆ 853   │
│ 4        ┆ 116   │
│ 1        ┆ 15    │
│ 2        ┆ 10    │
│ 3        ┆ 6     │
└──────────┴───────┘
  [diag] PAIDIND value counts (top 10):
shape: (3, 2)
┌─────────┬───────┐
│ PAIDIND ┆ count │
│ ---     ┆ ---   │
│ str     ┆ u32   │
╞═════════╪═══════╡
│ P       ┆ 909   │
│ M       ┆ 90    │
│ 0       ┆ 1     │
└─────────┴───────┘
No LNNOTE data found
