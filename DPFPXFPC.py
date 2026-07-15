Calculating report dates...
Report Date: 30062026, Week: 4
DEPOBACK_PATH: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/MNI
BNM_PATH: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQFDSP
OUTPUT_PATH: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQFDSP
Copying files from DEPOBACK to BNM...
Copied: fdmthly.sas7bdat
Processing FDMTHLY data...
Read 2756145 records from fdmthly.sas7bdat
Columns: STATE, CUSTCODE, BIC, LSTMATDT, BRANCH, ACCTNO, PURPOSE, NAME, OPENIND, CURBAL, ORGDATE, MATDATE, RATE, ACCTTYPE, TERM, INTPLAN, RENEWAL, INTPAY, INTDATE, LASTACTV, AMTIND, FORATE
Loaded 2756145 records from fdmthly.sas7bdat

============================================================
DEBUG: Checking CUSTCODE values
============================================================
Sample CUSTCODE values (first 20 unique):
shape: (20, 1)
┌──────────┐
│ CUSTCODE │
│ ---      │
│ str      │
╞══════════╡
│ 66       │
│ 43       │
│ 79       │
│ 41       │
│ 38       │
│ …        │
│ 73       │
│ 48       │
│ 59       │
│ 77       │
│ 46       │
└──────────┘
CUSTCODE dtype: String

Records with CUSTCODE in group 1 (81-84): 2
Records with CUSTCODE in group 2 (85-99): 41,295

Records after filtering OPENIND in ['O','D']: 2,756,145 (filtered out 0)

Calculating REMMTH for each record...
Records with valid REMMTH (not null): 6 (0.00%)
Records after filtering negative REMMTH: 0

Summarizing data...
Records after summary: 0

Records in summary with CUSTCODE in target ranges:
  Group 1 (81-84): 0
  Group 2 (85-99): 0

Creating BNM codes...
Records after BNMCODE creation: 0

No BNMCODEs created. Checking REMMTH values...
No records with target CUSTCODEs in summary

Generating reports...
No data for 42130
No data for 42132
Generating FCY FD report...

Saving processed data...

SUMMARY STATISTICS:
============================================================
Total ALM records: 0
Total ALMDEPT records: 0
Report Date: 30/06/2026
Week: 4, Month: 06, Year: 2026

No records in ALMDEPT.

Check the debug output above to see:
1. How many records had valid REMMTH
2. How many records have target CUSTCODEs
3. What REMMTH values exist for target CUSTCODEs

Processing complete!

Exporting to CSV for review...
