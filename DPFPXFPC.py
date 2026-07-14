Processing Islamic report date...
Islamic Report Date: 13072026, Week: 4, Month: 07, Year: 2026
Processing Islamic Fixed Deposits...
Loaded 484048 records from SAS file

============================================================
DEBUG: Checking INTPLAN values in the data
============================================================
Unique INTPLAN values found: 118
shape: (20, 1)
┌─────────┐
│ INTPLAN │
│ ---     │
│ f64     │
╞═════════╡
│ 229.0   │
│ 230.0   │
│ 232.0   │
│ 272.0   │
│ 273.0   │
│ …       │
│ 334.0   │
│ 335.0   │
│ 336.0   │
│ 338.0   │
│ 339.0   │
└─────────┘

Records with INTPLAN between 470-475 (foreign currency): 13
Records with INTPLAN = 42630: 0

Sample foreign currency records (INTPLAN between 470-475):
shape: (10, 4)
┌─────────┬───────────┬─────────┬────────┐
│ INTPLAN ┆ CURBAL    ┆ OPENIND ┆ CUSTCD │
│ ---     ┆ ---       ┆ ---     ┆ ---    │
│ f64     ┆ f64       ┆ str     ┆ f64    │
╞═════════╪═══════════╪═════════╪════════╡
│ 475.0   ┆ 43000.0   ┆ O       ┆ 78.0   │
│ 475.0   ┆ 15000.0   ┆ O       ┆ 78.0   │
│ 475.0   ┆ 16000.0   ┆ O       ┆ 78.0   │
│ 475.0   ┆ 100000.0  ┆ O       ┆ 78.0   │
│ 475.0   ┆ 55000.0   ┆ O       ┆ 78.0   │
│ 475.0   ┆ 1.0872e6  ┆ O       ┆ 78.0   │
│ 475.0   ┆ 13000.0   ┆ O       ┆ 78.0   │
│ 475.0   ┆ 48000.0   ┆ O       ┆ 78.0   │
│ 475.0   ┆ 102000.0  ┆ O       ┆ 78.0   │
│ 475.0   ┆ 108113.59 ┆ O       ┆ 78.0   │
└─────────┴───────────┴─────────┴────────┘
============================================================

After filtering: 440708 records

Analyzing INTPLAN values for BIC mapping...
Total unique INTPLAN values: 117

Top 20 INTPLAN values by count:
shape: (20, 3)
┌─────────┬───────┬───────────────┐
│ INTPLAN ┆ COUNT ┆ TOTAL_BALANCE │
│ ---     ┆ ---   ┆ ---           │
│ f64     ┆ u32   ┆ f64           │
╞═════════╪═══════╪═══════════════╡
│ 229.0   ┆ 725   ┆ 1.2238e7      │
│ 230.0   ┆ 3137  ┆ 6.2404e7      │
│ 232.0   ┆ 1     ┆ 85482.08      │
│ 272.0   ┆ 1     ┆ 1.0663e7      │
│ 273.0   ┆ 17    ┆ 1.7188e9      │
│ …       ┆ …     ┆ …             │
│ 334.0   ┆ 6369  ┆ 1.6504e9      │
│ 335.0   ┆ 15    ┆ 3.1375e6      │
│ 336.0   ┆ 13530 ┆ 2.9216e9      │
│ 338.0   ┆ 621   ┆ 4.2712e7      │
│ 339.0   ┆ 538   ┆ 3.2858e7      │
└─────────┴───────┴───────────────┘

BIC distribution after mapping:
shape: (117, 3)
┌───────┬───────┬───────────────┐
│ BIC   ┆ COUNT ┆ TOTAL_BALANCE │
│ ---   ┆ ---   ┆ ---           │
│ str   ┆ u32   ┆ f64           │
╞═══════╪═══════╪═══════════════╡
│ 229.0 ┆ 725   ┆ 1.2238e7      │
│ 230.0 ┆ 3137  ┆ 6.2404e7      │
│ 232.0 ┆ 1     ┆ 85482.08      │
│ 272.0 ┆ 1     ┆ 1.0663e7      │
│ 273.0 ┆ 17    ┆ 1.7188e9      │
│ …     ┆ …     ┆ …             │
│ 952.0 ┆ 59    ┆ 927719.01     │
│ 953.0 ┆ 1     ┆ 9000.0        │
│ 987.0 ┆ 13    ┆ 487613.0      │
│ 991.0 ┆ 23    ┆ 1.6742e6      │
│ 993.0 ┆ 33    ┆ 294888.88     │
└───────┴───────┴───────────────┘

Generating Islamic tabulate report...
Records with BIC='42630' (foreign currency): 13
Islamic report generated: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIQLIQP/ISLAMIC_LIQP_REPORT_13072026.txt

============================================================
ISLAMIC LIQUIDITY PROFILE SUMMARY
============================================================
Report Date: 13/07/2026
Week: 4, Month: 07, Year: 2026
Total Islamic FD records: 440,708
Foreign Islamic FD records: 13
Total Islamic foreign FD amount: 1,932,814.58

Islamic Maturity Breakdown:
  >6 MTHS - 1 YR:    1,316,157.08 (68.1%)
  >3 - 6 MTHS:      453,657.50 (23.5%)
  >1 MTH - 3 MTHS:      163,000.00 (8.4%)

Islamic processing complete!
