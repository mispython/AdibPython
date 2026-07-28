Processing Bad Debt Write-Off List (Conventional - Filtered by NPL.LIST)
Report Date: 27/07/2026
Week: 3, Previous Month: 06
Reading LNNOTE...
Successfully read 6232608 records from LNNOTE
Step 1: Creating NPLA (BORSTAT='A' only, no loan type exclusion)...
Deriving HPD loan rows from LNNOTE...
HPD loan records: 182575
Step 2: Reading IIS and SP data...
  Note: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIFFTXT1_TXT2/iis.sas7bdat has no column(s) ['sp', 'marketvl'] - continuing without them (not marked required).
Removing specific account: ACCTNO=8396846630, NOTENO=90011
NPL records: 143172
Step 3: Reading CREDMSUBAC...
CREDMSUBAC records: 3863289
Merging NPL, CREDSUB, and LOAN data...
Merged loan records: 143172
Step 5: Calculating derived fields...
Derived fields calculated
Step 6: Reading customer names...
Customer records: 12580070
Step 7: Reading liability data...
Guarantor records processed: 1678302
Step 8: Reading previous month balance...
Previous balance records: 623910
Step 9: Merging all data...
Step 10: Filtering by NPL.LIST file...
NPL.LIST accounts: 0
Accounts after NPL.LIST filter: 0
Step 11: Merging customer names...
Write-off candidates: 0

Bad Debt Write-Off List (Conventional - Filtered) Generation Complete
Step 12: Writing fixed-width output file...
Step 13-14: Writing final formatted output...

Output files generated:
  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFFTXT2/wofftext.txt (Final formatted output)
  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFFTXT2/wofftex1.txt (Intermediate output)
  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIFFTXT1_TXT2/list_filtered.parquet (Filtered data file)
  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIFFTXT1_TXT2/WOFFTXT.parquet (Final dataset)

Accounts in filtered write-off list: 0

Key Differences from EIFFTXT1:
  - Filtered by existing NPL.LIST file (only pre-approved accounts)
  - No LOANTYPE exclusion in NPLA WHERE clause
  - Deleted ACCTNO 8396846630, NOTENO 90011
  - RIND = 'D' (Conventional)
  - BIZTYPE = 'C' (Conventional)
  - Uses CREDMSUBAC (Conventional CCRIS)
