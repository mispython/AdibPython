Loaded FDMTHLY with 2756145 records
Saved FDMTHLY_SORTED with 2756145 records
Saved FDMTHLY with 2756145 records
Loaded CURN124 with 915692 records
Saved CURN with 915427 records (after filtering)
Loaded SAVG124 with 4241108 records
Added CURN with 915427 records
Added FDMTHLY with 2756145 records
Combined DEPOSIT dataset has 7912680 records
DEPOSIT records: 7836142
Loaded FLOAT with 18927 records
Saved FLOAT with 18927 records
FLOAT summary records: 18927
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIPCBFLO.py:219: DeprecationWarning: use of `how='outer'` should be replaced with `how='full'`.
(Deprecated in version 0.20.29)
  deposit_merged = deposit_sorted.join(
Merged DEPOSIT with FLOAT: 7839120 records
DEPOSIT final records: 15949
EXCEPT records: 2978

================================================================================
SUMMARY BY BRANCH:
================================================================================
shape: (260, 4)
┌────────┬───────────┬────────────┬──────────┐
│ branch ┆ float     ┆ minusfloat ┆ floatori │
│ ---    ┆ ---       ┆ ---        ┆ ---      │
│ f64    ┆ f64       ┆ f64        ┆ f64      │
╞════════╪═══════════╪════════════╪══════════╡
│ 2.0    ┆ 4.7103e6  ┆ 2.5918e7   ┆ 3.1314e7 │
│ 3.0    ┆ 4.1853e6  ┆ 4.3775e7   ┆ 4.8000e7 │
│ 4.0    ┆ 3.3917e6  ┆ 2.3338e7   ┆ 2.6974e7 │
│ 5.0    ┆ 5.2849e6  ┆ 5.2625e7   ┆ 5.8229e7 │
│ 6.0    ┆ 2.7937e6  ┆ 1.3765e7   ┆ 1.7404e7 │
│ …      ┆ …         ┆ …          ┆ …        │
│ 292.0  ┆ 1.6667e6  ┆ 1.2015e7   ┆ 1.3865e7 │
│ 293.0  ┆ 1.8739e6  ┆ 1.5435e7   ┆ 1.7309e7 │
│ 294.0  ┆ 2.0771e6  ┆ 7.1486e6   ┆ 9.2338e6 │
│ 295.0  ┆ 202918.42 ┆ 2.0212e6   ┆ 2.2241e6 │
│ 296.0  ┆ 236780.44 ┆ 7.6121e6   ┆ 7.8488e6 │
└────────┴───────────┴────────────┴──────────┘

================================================================================
TOTALS:
================================================================================
FLOAT: 468,933,840.14
MINUSFLOAT: 5,753,653,745.83
FLOATORI: 6,246,126,638.19

================================================================================
All output files saved to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIPCBFLO
PROCESSING COMPLETED SUCCESSFULLY
================================================================================


summary of output:

/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIPCBFLO.py:219: DeprecationWarning: use of `how='outer'` should be replaced with `how='full'`.
(Deprecated in version 0.20.29)

anything to be changed?

plus, the output seems to show in exponential number, i need normal numbers. no "e" something
