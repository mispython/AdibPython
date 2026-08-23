Using SAS Config named: default
SAS Connection established. Subprocess id is 833999

REPTMON: 07, RDATE: 310726
Input path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRTLIO
Output path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRTLIO
DP columns: ['cvar13', 'cvar04', 'cvar08', 'cvar06', 'cvar01', 'branch', 'product', 'censust', 'sch', 'cinstcl', 'natguar', 'cvar02', 'cvar03', 'cvar05', 'cvar07', 'cvar10', 'cvar09', 'cvar11', 'cvar12', 'cvar14','cvar15']
DP shape: (0, 21)
LN columns: ['product', 'censust', 'sch', 'cinstcl', 'natguar', 'cvar02', 'cvar01', 'cvar06', 'cvar03', 'cvar04', 'cvar14', 'cvar13', 'cvar08', 'cvar09', 'cvar10', 'cvar11', 'cvar05', 'cvar07', 'cvar12', 'cvar15','branch']
LN shape: (2105, 21)
DP is empty, using only LN data
Combined shape: (2105, 21)
TL dataset is empty, skipping TL output
NPGS3 filtered shape: (2, 22)
/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
Generating report using CGCRPT module...

Output files in /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRTLIO:
  npgs3.parquet (7,309 bytes)
  sc167r.txt (814 bytes)
  sc167t.txt (346 bytes)
  tl.parquet (487 bytes)

Processing complete. Output files written to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRTLIO
SAS Connection terminated. Subprocess id was 833999
