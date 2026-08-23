Using SAS Config named: default
SAS Connection established. Subprocess id is 1000403

REPTMON: 07, RDATE: 310726
Input path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRTLIO
Output path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRTLIO
DP columns: ['cvar13', 'cvar04', 'cvar08', 'cvar06', 'cvar01', 'branch', 'product', 'censust', 'sch', 'cinstcl', 'natguar', 'cvar02', 'cvar03', 'cvar05', 'cvar07', 'cvar10', 'cvar09', 'cvar11', 'cvar12', 'cvar14', 'cvar15']
DP shape: (0, 21)
LN columns: ['product', 'censust', 'sch', 'cinstcl', 'natguar', 'cvar02', 'cvar01', 'cvar06', 'cvar03', 'cvar04', 'cvar14', 'cvar13', 'cvar08', 'cvar09', 'cvar10', 'cvar11', 'cvar05', 'cvar07', 'cvar12', 'cvar15', 'branch']
LN shape: (2105, 21)
DP is empty, using only LN data
Combined shape: (2105, 21)
TL dataset shape: (0, 4)
TL dataset is empty, skipping SAS export
NPGS3 filtered shape: (2, 22)
Error writing NPGS3 to sas7bdat via pyreadstat: module 'pyreadstat' has no attribute 'write_sas7bdat'
NPGS3 sas7bdat written successfully via saspy
Generating report using CGCRPT module...

Output files in /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRTLIO:
  npgs3.parquet (7,309 bytes)
  npgs3.sas7bdat (131,072 bytes)
  sc167r.txt (814 bytes)
  sc167t.txt (346 bytes)
  tl.parquet (487 bytes)

Processing complete. Output files written to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRTLIO
SAS Connection terminated. Subprocess id was 1000403
