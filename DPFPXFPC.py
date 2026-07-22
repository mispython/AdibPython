============================================================
Bad Debt Write-Off List (Filtered by NPL.LIST)
============================================================
Report Date: 21/07/2026
Output Directory: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT2/
Output File 1: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT2/wofftex1.txt
Output File 2: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT2/wofftext.txt
Week: 4, Previous Month: 06
============================================================

STEP 1: Reading NPLA data (Active accounts with BORSTAT='A')...
  NPLA rows: 7447

STEP 2: Reading IIS and SP data...
  IIS rows: 135725
  SP rows: 135725

STEP 3: Combining NPL data...
  NPL combined rows: 143172

STEP 4: Reading CCRIS data...
  Looking for: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/icredmsubac0726.sas7bdat
  Found CCRIS file: icredmsubac0726.sas7bdat
  CCRIS rows: 1100791

STEP 5: Reading HPD loan data...
  HPD loan rows: 4081262

STEP 6: Merging data...
  Merged loan rows: 143172

STEP 7: Calculating derived fields...
  Calculations completed in 63.2s
  Loan records: 143172

STEP 8: Reading customer names...
  Customer names: 12580070

STEP 9: Reading guarantor information...
  Guarantor entries: 1677218

STEP 10: Reading previous balance...
  SASLN rows: 20827

STEP 11: Final merge...
  WOFF before filtering by NPL.LIST: 143172
STEP 12: Filtering by NPL.LIST...
Warning: File not found: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/LIST.sas7bdat
  WARNING: NPL.LIST file not found - no filtering applied
  WOFF after filtering by NPL.LIST: 143172

STEP 13: Saving output files...
Error writing /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/WOFFTXT.sas7bdat: module 'pyreadstat' has no attribute 'write_sas7bdat'

============================================================
SUMMARY
============================================================
Accounts in filtered write-off list: 143172
Total exposure: RM 29,216,019.87

Writing output files to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT2/
  Writing record 143100/143172
  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT2/wofftex1.txt written with 143172 rows
  File size: 90,484,712 bytes

Creating final formatted output...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIFTXT2.py", line 843, in <module>
    'NOTENO': float(line[60:65]) if line[60:65].strip() else 0,
ValueError: could not convert string to float: ' 0A  '
You have mail in /var/spool/mail/sas_edw_dev
