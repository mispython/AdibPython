Successfully created /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/PBBRDAL.parquet
Total records: 44
Warning: PBBMRDLF.py not found. Creating empty reference data.
2026-07-17 09:17:10,425 - INFO - Processing for date: 30062026
2026-07-17 09:17:10,425 - INFO - Report year: 2026, Month: 06, Week: 4
2026-07-17 09:17:10,425 - WARNING - PBBMRDLF not available. Creating empty DataFrame.
2026-07-17 09:17:10,425 - WARNING - PBBRDAL data is empty!
2026-07-17 09:17:10,426 - INFO - Loading BNM data from: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIPWRDAL/alw064.sas7bdat
2026-07-17 09:17:10,463 - INFO - BNM data loaded: 14787 rows, columns: ['ITCODE', 'AMTIND', 'AMOUNT']
2026-07-17 09:17:10,463 - INFO - Loading PBCS data from: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIPWRDAL/cclw064.sas7bdat
2026-07-17 09:17:10,465 - INFO - PBCS data loaded: 5 rows, columns: ['ITCODE', 'AMTIND', 'AMOUNT']
2026-07-17 09:17:10,465 - INFO - Combined data: 14792 rows
2026-07-17 09:17:10,465 - INFO - ALW data loaded: 14792 rows
2026-07-17 09:17:10,465 - INFO - ALW columns: ['ITCODE', 'AMTIND', 'AMOUNT']
2026-07-17 09:17:10,465 - INFO - ALW sample:
shape: (5, 3)
┌────────────────┬────────┬───────────┐
│ ITCODE         ┆ AMTIND ┆ AMOUNT    │
│ ---            ┆ ---    ┆ ---       │
│ str            ┆ str    ┆ f64       │
╞════════════════╪════════╪═══════════╡
│ NSSTS          ┆ D      ┆ 3.1592e9  │
│ SSTS           ┆ D      ┆ 5.4823e10 │
│ 3051000000000Y ┆        ┆ 3.3744e11 │
│ 3051000000000Y ┆        ┆ 2.4079e8  │
│ 3091000000000Y ┆        ┆ 1.9723e10 │
└────────────────┴────────┴───────────┘
2026-07-17 09:17:10,465 - INFO - Using ALW data only
2026-07-17 09:17:10,465 - INFO - RDAL before filtering: 14792 rows
2026-07-17 09:17:10,466 - INFO - RDAL sample:
shape: (5, 3)
┌────────────────┬────────┬───────────┐
│ ITCODE         ┆ AMTIND ┆ AMOUNT    │
│ ---            ┆ ---    ┆ ---       │
│ str            ┆ str    ┆ f64       │
╞════════════════╪════════╪═══════════╡
│ NSSTS          ┆ D      ┆ 3.1592e9  │
│ SSTS           ┆ D      ┆ 5.4823e10 │
│ 3051000000000Y ┆        ┆ 3.3744e11 │
│ 3051000000000Y ┆        ┆ 2.4079e8  │
│ 3091000000000Y ┆        ┆ 1.9723e10 │
└────────────────┴────────┴───────────┘
2026-07-17 09:17:10,481 - INFO - RDAL after filtering unwanted: 14791 rows
2026-07-17 09:17:10,481 - INFO - Loading loan data from: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIPWRDAL/lnnote.sas7bdat
