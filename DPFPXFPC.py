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
MATDATE range: 19981121 to 20301226
MATDATE date range: 1998-11-21 to 2030-12-26

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
│ 48       │
│ 37       │
│ 78       │
│ 72       │
│ …        │
│ 57       │
│ 79       │
│ 39       │
│ 33       │
│ 67       │
└──────────┘
CUSTCODE dtype: String

Records with CUSTCODE in group 1 (81-84): 2
Records with CUSTCODE in group 2 (85-99): 41,295

Records after filtering OPENIND in ['O','D']: 2,756,145 (filtered out 0)

Calculating REMMTH for each record...
MATDATE is in YYYYMMDD format - converting directly...
Records with valid REMMTH (not null): 2,756,145 (100.00%)
Records after filtering negative REMMTH: 486,837

Summarizing data...
Records after summary: 2,916

Records in summary with CUSTCODE in target ranges:
  Group 1 (81-84): 0
  Group 2 (85-99): 582

Creating BNM codes...
Records after BNMCODE creation: 582

Generating reports...
Report saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQFDSP/REPORT_42130_42130_30062026.txt
No data for 42132
Generating FCY FD report...
FCY FD report saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQFDSP/REPORT_42630_30062026.txt

Saving processed data...

SUMMARY STATISTICS:
============================================================
Total ALM records: 486,837
Total ALMDEPT records: 582
Report Date: 30/06/2026
Week: 4, Month: 06, Year: 2026

Amount Distribution by BNMCODE prefix:
  42130:       765,663,246.69
  42630:       914,591,731.01

Processing complete!

Exporting to CSV for review...
