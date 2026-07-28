Processing Bad Debt Write-Off List (Conventional Banking)
Report Date: 27/07/2026
Week: 3, Previous Month: 06
Reading LNNOTE (optimized, single pass)...
Successfully read 6232608 records from LNNOTE
Step 1: Creating NPLA...
Deriving HPD loan rows from the already-loaded LNNOTE frame...
HPD loan records: 182575
Step 2: Reading IIS and SP data...
  Note: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIFFTXT1/iis.sas7bdat has no column(s) ['sp', 'marketvl'] - continuing without them (not marked required).
NPL records: 135895
Step 3: Reading CREDMSUBAC...
Warning: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIFFTXT1/credmsubac0726.sas7bdat not found.
Merging NPL, CREDSUB, and LOAN data...
Merged loan records: 135895
Step 5: Calculating derived fields (vectorized)...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIFFTXT1.py:646: FutureWarning: Downcasting object dtype arrays on .fillna, .ffill, .bfill is deprecated and will change in a future version. Call result.infer_objects(copy=False) instead. To opt-in to the future behavior, set `pd.set_option('future.no_silent_downcasting', True)`
  df_loan['days'] = df_loan['days'].fillna(0).astype(int) if 'days' in df_loan.columns else 0
Derived fields calculated
Step 6: Reading customer names...
Customer records: 12580070
Step 7: Reading liability data...
Error reading liability data: Required SAS file not found: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIFFTXT1/liab.sas7bdat
Step 8: Reading previous month balance...
Previous balance records: 623910
Step 9: Merging all data...
Step 10: Filtering write-off candidates...
Write-off candidates: 0

Bad Debt Write-Off List (Conventional) Generation Complete
Step 11: Writing fixed-width output file...
Step 12-14: Writing final formatted output...

Output files generated:
  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFFTXT1/wofftext.txt (Final formatted output)
  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFFTXT1/wofftex1.txt (Intermediate output)
  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIFFTXT1/list.parquet (Data file)

Accounts identified for write-off: 0

Key Differences from EIIFTXT1 (Islamic):
  - RIND = 'D' (Domestic/Conventional) vs 'I' (Islamic)
  - BIZTYPE = 'C' (Conventional) vs 'I' (Islamic)
  - Uses CREDMSUBAC vs ICREDMSUBAC (CCRIS)
You have mail in /var/spool/mail/sas_edw_dev


the not found liab, actually the file naming is lnliab.sas7bdat
