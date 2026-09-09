REPTMON: 08, RDATE: 310826
NPGS Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBSNPGS/NPGS
NPGSI Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBSNPGS/NPGSI
Output Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBSNPGS

Processing SC53 datasets...
Read btnpgs08.sas7bdat: 14 records, columns: ['cvar01', 'cvar06', 'cvar03', 'cvar04', 'cvar14', 'cvar13', 'cvar08', 'cvar09', 'cvar10', 'cvar11', 'censust', 'sch', 'branch', 'cinstcl', 'natguar', 'cr', 'cvar02', 'product', 'cvar05', 'cvar07', 'cvar12', 'cvar15']
Read lnnpgs08.sas7bdat: 1287 records, columns: ['product', 'censust', 'sch', 'cinstcl', 'natguar', 'cr', 'cvar02', 'cvar01', 'cvar06', 'cvar03', 'cvar04', 'cvar14', 'cvar13', 'cvar08', 'cvar09', 'cvar10', 'cvar11', 'cvar05', 'cvar07', 'cvar12', 'cvar15', 'branch']
Read dpnpgs08.sas7bdat: 8 records, columns: ['cvar13', 'cvar04', 'cvar08', 'cvar06', 'cvar01', 'branch', 'product', 'censust', 'sch', 'cinstcl', 'natguar', 'cr', 'cvar02', 'cvar03', 'cvar05', 'cvar07', 'cvar10', 'cvar09', 'cvar11', 'cvar12', 'cvar14', 'cvar15']
SC53 records: 39

Processing SCEI datasets...
Read lnipgs08.sas7bdat: 2135 records, columns: ['product', 'censust', 'sch', 'cinstcl', 'natguar', 'cvar02', 'cvar01', 'cvar06', 'cvar03', 'cvar04', 'cvar14', 'cvar13', 'cvar08', 'cvar09', 'cvar10', 'cvar11', 'cvar05', 'cvar07', 'cvar12', 'cvar15', 'branch']
SCEI records: 1

Processing OTH datasets...
Read lnnpgs08.sas7bdat: 1287 records, columns: ['product', 'censust', 'sch', 'cinstcl', 'natguar', 'cr', 'cvar02', 'cvar01', 'cvar06', 'cvar03', 'cvar04', 'cvar14', 'cvar13', 'cvar08', 'cvar09', 'cvar10', 'cvar11', 'cvar05', 'cvar07', 'cvar12', 'cvar15', 'branch']
OTH records: 1269

Combining all datasets...
SC53: 39 records
SCEI: 1 records
OTH: 1269 records
Total NPGS records: 1309

Writing COMBT.txt...

============================================================
PUBLIC BANK BERHAD
DETAIL OF ACCTS NON-PG FOR SUBMISSION TO CGC @ 310826
============================================================

Processing complete. Files: COMBT.txt, COMBR.txt
Output directory: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBSNPGS
Total records: 1309
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBSNPGS.py:389: DeprecationWarning: `pl.count()` is deprecated. Please use `pl.len()` instead.
(Deprecated in version 0.20.5)
  counts = npgs_df.group_by("cvar02").agg(pl.count().alias("records"))

Records by CVAR02:
  1H: 64
  1Z: 497
  2H: 1
  2Z: 32
  3H: 3
  3Z: 171
  4Z: 8
  5H: 4
  5S: 4
  5Z: 1
  6H: 1
  E1: 39
  E2: 1
  E6: 3
  F5: 175
  G1: 2
  H6: 303
