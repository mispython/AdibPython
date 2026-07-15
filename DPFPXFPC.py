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
│ 39       │
│ 48       │
│ 62       │
│ 75       │
│ 73       │
│ …        │
│ 06       │
│ 37       │
│ 79       │
│ 57       │
│ 46       │
└──────────┘
CUSTCODE dtype: String

Records with CUSTCODE in group 1 (81-84): 2
Records with CUSTCODE in group 2 (85-99): 41,295

Records after filtering OPENIND in ['O','D']: 2,756,145

Calculating REMMTH for each record...

Summarizing data...
Records after summary: 73

Creating BNM codes...
Using string CUSTCODE values
Records after BNMCODE creation: 0

Generating reports...
No data for 42130
No data for 42132
Generating FCY FD report...

Saving processed data...

SUMMARY STATISTICS:
============================================================
Total ALM records: 2,756,145
Total ALMDEPT records: 0
Report Date: 30/06/2026
Week: 4, Month: 06, Year: 2026

No records in ALMDEPT. Check CUSTCODE values in the dataset.

Debug: Check what CUSTCODE values exist:
shape: (10, 1)
┌──────────┐
│ CUSTCODE │
│ ---      │
│ str      │
╞══════════╡
│ 67       │
│ 72       │
│ 78       │
│ 84       │
│ 35       │
│ 32       │
│ 62       │
│ 51       │
│ 30       │
│ 40       │
└──────────┘

Processing complete!

Exporting to CSV for review...
