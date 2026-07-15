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
DEBUG: Checking MATDATE values
============================================================
MATDATE dtype: Float64

Sample MATDATE values (first 10):
shape: (10, 1)
┌─────────────┐
│ MATDATE     │
│ ---         │
│ f64         │
╞═════════════╡
│ 2.0260325e7 │
│ 2.0260211e7 │
│ 2.0260317e7 │
│ 2.0260105e7 │
│ 2.0260209e7 │
│ 2.0260615e7 │
│ 2.0260129e7 │
│ 2.0260314e7 │
│ 2.0260314e7 │
│ 2.0260116e7 │
└─────────────┘

Records with null MATDATE: 0
Records with non-null MATDATE: 2,756,145
MATDATE range: 19981121.0 to 20301226.0

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
│ 43       │
│ 06       │
│ 95       │
│ 66       │
│ 78       │
│ …        │
│ 49       │
│ 39       │
│ 30       │
│ 34       │
│ 46       │
└──────────┘
CUSTCODE dtype: String

Records with CUSTCODE in group 1 (81-84): 2
Records with CUSTCODE in group 2 (85-99): 41,295

Records after filtering OPENIND in ['O','D']: 2,756,145 (filtered out 0)

Calculating REMMTH for each record...
MATDATE is numeric - converting SAS dates directly...
Records with valid REMMTH (not null): 6 (0.00%)
Records after filtering negative REMMTH: 0

Summarizing data...
Records after summary: 0

Creating BNM codes...
No records in ALM summary

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
1. MATDATE values and their format
2. How many records had valid REMMTH
3. How many records have target CUSTCODEs

Processing complete!

Exporting to CSV for review...
