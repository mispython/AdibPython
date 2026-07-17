Created PBBRDAL dataset with 44 records
shape: (44, 1)
┌────────────────┐
│ ITCODE         │
│ ---            │
│ str            │
╞════════════════╡
│ 3313002000000Y │
│ 3313003000000Y │
│ 4019000000000Y │
│ 4216060000000Y │
│ 4261076000000Y │
│ …              │
│ 7318000000000Y │
│ 7411000000000Y │
│ 7412000000000Y │
│ 7413000000000Y │
│ 7414000000000Y │
└────────────────┘
Warning: PBBMRDLF.py not found. Creating empty reference data.
2026-07-17 09:41:23,326 - INFO - Processing for date: 30062026
2026-07-17 09:41:23,326 - INFO - Report year: 2026, Month: 06, Week: 4
2026-07-17 09:41:23,326 - WARNING - PBBMRDLF not available. Creating empty DataFrame.
2026-07-17 09:41:23,326 - WARNING - PBBRDAL data is empty!
2026-07-17 09:41:23,326 - INFO - Loading BNM data from: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIPWRDAL/alw064.sas7bdat
2026-07-17 09:41:23,357 - INFO - BNM data loaded: 14787 rows, columns: ['ITCODE', 'AMTIND', 'AMOUNT']
2026-07-17 09:41:23,357 - INFO - Loading PBCS data from: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIPWRDAL/cclw064.sas7bdat
2026-07-17 09:41:23,358 - INFO - PBCS data loaded: 5 rows, columns: ['ITCODE', 'AMTIND', 'AMOUNT']
2026-07-17 09:41:23,359 - INFO - Combined data: 14792 rows
2026-07-17 09:41:23,359 - INFO - ALW data loaded: 14792 rows
2026-07-17 09:41:23,359 - INFO - ALW columns: ['ITCODE', 'AMTIND', 'AMOUNT']
2026-07-17 09:41:23,359 - INFO - ALW sample:
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
2026-07-17 09:41:23,360 - INFO - Using ALW data only
2026-07-17 09:41:23,391 - INFO - RDAL before filtering: 14792 rows
2026-07-17 09:41:23,391 - INFO - RDAL sample:
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
2026-07-17 09:41:23,395 - INFO - RDAL after filtering unwanted: 14791 rows
2026-07-17 09:41:23,395 - INFO - Loading loan data with filtering...
2026-07-17 09:41:23,395 - INFO - Loading filtered loan data from: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIPWRDAL/lnnote.sas7bdat
2026-07-17 09:41:23,395 - ERROR - Error loading loan file /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIPWRDAL/lnnote.sas7bdat: read_sas7bdat() got an unexpected keyword argument 'rows_limit'
2026-07-17 09:41:23,395 - WARNING - Loan file not found
2026-07-17 09:41:23,395 - INFO - No loan data available
2026-07-17 09:41:23,396 - INFO - RDAL final row count: 14791
2026-07-17 09:41:23,397 - INFO - RDAL filtered (no F/#): 14784 rows
2026-07-17 09:41:23,402 - INFO - AL data rows: 14441
2026-07-17 09:41:23,402 - INFO - OB data rows: 314
2026-07-17 09:41:23,402 - INFO - SP data rows: 380
2026-07-17 09:41:23,459 - INFO - RDAL file written to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIPWRDAL/rdal_pbcs.txt
2026-07-17 09:41:23,493 - INFO - NSRS file written to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIPWRDAL/nsrs_rdal_pbcs.txt

======================================================================
Processing complete!
======================================================================
Output files:
  - RDAL: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIPWRDAL/rdal_pbcs.txt
  - NSRS: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIPWRDAL/nsrs_rdal_pbcs.txt




below is the PBBMRDLF.py program

#!/usr/bin/env python3
"""
File Name: PBBMRDLF
Creates PBBRDAL dataset with ITCODE values
"""

import polars as pl

# ITCODE data from CARDS section
itcode_data = [
    "3313002000000Y",
    "3313003000000Y",
    "4019000000000Y",
    "4216060000000Y",
    "4261076000000Y",
    "4261085000000Y",
    "4263076000000Y",
    "4263085000000Y",
    "4269981000000Y",
    "4313002000000Y",
    "4313003000000Y",
    "7200000008310Y",
    "7300000003000Y",
    "7300000006100Y",
    "7300000008310Y",
    "7300000008320Y",
    "5422000000000Y",
    "4017000000000Y",
    "3051577000000Y",
    "3054077000000Y",
    "3055060000000Y",
    "3055061000000Y",
    "3055076000000Y",
    "3055077000000Y",
    "3056000000000Y",
    "3400010000310Y",
    "3400010008100Y",
    "3400020000100Y",
    "3400020000110Y",
    "3400000000132Y",
    "3400077000420Y",
    "3400078000132Y",
    "3415100000000Y",
    "3415200000000Y",
    "3415900000000Y",
    "3416000000000Y",
    "3420000000420Y",
    "7211500000000Y",
    "7312000000000Y",
    "7318000000000Y",
    "7411000000000Y",
    "7412000000000Y",
    "7413000000000Y",
    "7414000000000Y",
]

# Create DataFrame (this is like the SAS dataset PBBRDAL)
df = pl.DataFrame({
    "ITCODE": pl.Series(itcode_data, dtype=pl.Utf8)
})

print(f"Created PBBRDAL dataset with {len(df)} records")
print(df)
